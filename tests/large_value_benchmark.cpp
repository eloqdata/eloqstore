// tests/large_value_benchmark.cpp
//
// Informational benchmarks for the zero-copy large-value read/write paths.
// Excluded from the normal (non-benchmark) test run.
//
// Run with:
//   sudo prlimit --memlock=unlimited ./large_value_benchmark -t '[benchmark]'
//
// All results are informational; no assertions gate on specific latency values.

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../include/eloq_store.h"
#include "../include/error.h"
#include "../include/global_registered_memory.h"
#include "../include/io_string_buffer.h"
#include "../include/kv_options.h"
#include "../include/types.h"
#include "common.h"

namespace
{
namespace chrono = std::chrono;
using Clock = chrono::steady_clock;

// 256 KB segments, 64 MB chunks, 256 MB pool (1024 segments).
// Bench 2's M=32 batch on 10 MB values needs 1280 segments; the 10 MB
// case is consequently capped at M=16 (640 segs) so it fits. Bumping
// the pool to 512 MB caused read-side benchmarks to hang at higher
// concurrency (likely an io_uring registration / scheduler interaction
// at large iovec counts), so the pool stays at 256 MB.
constexpr uint32_t kBenchSeg = 256 * 1024;
constexpr size_t kBenchChunk = 64ULL * 1024 * 1024;
constexpr size_t kBenchPool = 256ULL * 1024 * 1024;

void FillPattern(char *dst, size_t n, uint64_t seed)
{
    for (size_t i = 0; i < n; ++i)
        dst[i] = static_cast<char>((seed * 2654435761u + i) & 0xFF);
}

// ---- Harness ---------------------------------------------------------------

class BenchHarness
{
public:
    BenchHarness()
        : mem_(std::make_unique<eloqstore::GlobalRegisteredMemory>(
              kBenchSeg, kBenchChunk, kBenchPool))
    {
    }

    BenchHarness(const BenchHarness &) = delete;
    BenchHarness &operator=(const BenchHarness &) = delete;

    std::vector<eloqstore::GlobalRegisteredMemory *> Pointers() const
    {
        return {mem_.get()};
    }

    // Force one synchronous round-trip so BootstrapRing has completed and
    // GlobalRegMemIndexBase is valid before any write/read uses the pool.
    void BindStore(eloqstore::EloqStore *store)
    {
        eloqstore::ReadRequest r;
        r.SetArgs(eloqstore::TableIdent{"__warmup__", 0}, "__warmup_key__");
        store->ExecSync(&r);
        base_ = store->GlobalRegMemIndexBase(0);
    }

    uint32_t SegmentSize() const
    {
        return kBenchSeg;
    }
    eloqstore::GlobalRegisteredMemory *Memory()
    {
        return mem_.get();
    }
    uint16_t RegBase() const
    {
        return base_;
    }

    eloqstore::IoStringBuffer MakeBuf(size_t size, uint64_t seed)
    {
        const uint32_t seg = kBenchSeg;
        const size_t num_segs = (size + seg - 1) / seg;
        eloqstore::IoStringBuffer buf;
        size_t remaining = size;
        for (size_t i = 0; i < num_segs; ++i)
        {
            auto [ptr, chunk_idx] = mem_->GetSegment([]() {});
            size_t nbytes = std::min<size_t>(seg, remaining);
            FillPattern(ptr, nbytes, seed + i);
            if (nbytes < seg)
                std::memset(ptr + nbytes, 0, seg - nbytes);
            buf.Append({ptr, static_cast<uint16_t>(base_ + chunk_idx)});
            remaining -= nbytes;
        }
        buf.SetSize(size);
        return buf;
    }

    void RecycleBatch(eloqstore::BatchWriteRequest &req)
    {
        for (auto &e : req.batch_)
            e.RecycleLargeValue(mem_.get(), base_);
    }

    void RecycleRead(eloqstore::ReadRequest &req)
    {
        if (auto *iosb =
                std::get_if<eloqstore::IoStringBuffer>(&req.large_value_dest_);
            iosb != nullptr && !iosb->Fragments().empty())
            iosb->Recycle(mem_.get(), base_);
    }

private:
    std::unique_ptr<eloqstore::GlobalRegisteredMemory> mem_;
    uint16_t base_{0};
};

// ---- KvOptions factory -----------------------------------------------------

// file_amplify_factor=0 suppresses automatic data compaction so benchmarks
// control exactly when compaction runs.
// seg_amp=0: DoCompactSegmentFile's per-file skip check is
//   `factor <= saf_limit` → `factor <= 0` is always false for non-empty files,
//   so all live segments are rewritten when CompactRequest is submitted.
//   Auto segment-compaction is also disabled (MapperExceedsAmplification
//   returns false when amplify_factor==0).
eloqstore::KvOptions MakeBenchOpts(BenchHarness &h,
                                   uint8_t seg_amp = 0,
                                   uint32_t yield_every = 8)
{
    eloqstore::KvOptions opts;
    opts.num_threads = 1;
    opts.num_retained_archives = 0;
    opts.archive_interval_secs = 0;
    opts.file_amplify_factor = 0;
    opts.segment_file_amplify_factor = seg_amp;
    opts.segment_compact_yield_every = yield_every;
    opts.store_path = {test_path};
    opts.pages_per_file_shift = 8;  // 256 pages per data file = 1 MB
    opts.segment_size = h.SegmentSize();
    opts.segments_per_file_shift = 3;  // 8 segments per segment file = 2 MB
    opts.data_append_mode = true;
    opts.buffer_pool_size = 16 * eloqstore::MB;
    opts.write_buffer_size = 0;
    opts.write_buffer_ratio = 0.0;
    opts.global_registered_memories = h.Pointers();
    return opts;
}

// ---- Write helpers ---------------------------------------------------------

bool WriteLarge(eloqstore::EloqStore *store,
                BenchHarness &h,
                const eloqstore::TableIdent &tbl,
                const std::string &key,
                size_t size,
                uint64_t seed,
                uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        key, h.MakeBuf(size, seed), ts, eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    h.RecycleBatch(req);
    return req.Error() == eloqstore::KvError::NoError;
}

bool WriteStr(eloqstore::EloqStore *store,
              const eloqstore::TableIdent &tbl,
              const std::string &key,
              std::string value,
              uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(key, std::move(value), ts, eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    return req.Error() == eloqstore::KvError::NoError;
}

void DeleteKey(eloqstore::EloqStore *store,
               const eloqstore::TableIdent &tbl,
               const std::string &key,
               uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(key, std::string{}, ts, eloqstore::WriteOp::Delete);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
}

// ---- Latency helpers -------------------------------------------------------

struct Pct
{
    int64_t p50_us{-1};
    int64_t p99_us{-1};
};

Pct ComputePct(std::vector<int64_t> nanos)
{
    if (nanos.empty())
        return {};
    std::sort(nanos.begin(), nanos.end());
    Pct p;
    p.p50_us = nanos[nanos.size() / 2] / 1000;
    p.p99_us = nanos[nanos.size() * 99 / 100] / 1000;
    return p;
}

// Time n synchronous reads of a large value; recycles fragments after each.
std::vector<int64_t> TimeLargeReads(eloqstore::EloqStore *store,
                                    BenchHarness &h,
                                    const eloqstore::TableIdent &tbl,
                                    const std::string &key,
                                    int n)
{
    std::vector<int64_t> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i)
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, key);
        // Opt into the IoStringBuffer destination so the dispatch actually
        // fetches segments. Without this, large_value_dest_ defaults to
        // std::monostate (metadata-only path) and segment reads are
        // skipped -- which falsely makes "large-value reads" look like
        // 100 GB/s memory-bandwidth measurements.
        r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        auto t0 = Clock::now();
        store->ExecSync(&r);
        auto t1 = Clock::now();
        if (r.Error() == eloqstore::KvError::NoError)
            v.push_back(
                chrono::duration_cast<chrono::nanoseconds>(t1 - t0).count());
        h.RecycleRead(r);
    }
    return v;
}

// Time n synchronous reads of a small/overflow value (result in req.value_).
std::vector<int64_t> TimeStrReads(eloqstore::EloqStore *store,
                                  const eloqstore::TableIdent &tbl,
                                  const std::string &key,
                                  int n)
{
    std::vector<int64_t> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i)
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, key);
        auto t0 = Clock::now();
        store->ExecSync(&r);
        auto t1 = Clock::now();
        if (r.Error() == eloqstore::KvError::NoError)
            v.push_back(
                chrono::duration_cast<chrono::nanoseconds>(t1 - t0).count());
    }
    return v;
}

// Drop the OS page cache for every regular file under `tbl`'s partition
// directory (data / segment / manifest). Per-file fsync first so the
// kernel can evict (DONTNEED won't drop dirty pages). System-wide
// ::sync() was previously used here but is far too expensive in a
// benchmark loop -- it flushes every dirty page on the box, even from
// unrelated processes / partitions, and per-config evictions add up
// to minutes.
void EvictPageCache(const eloqstore::KvOptions &opts,
                    const eloqstore::TableIdent &tbl)
{
    namespace fs = std::filesystem;
    fs::path dir = fs::path(opts.store_path[0]) / tbl.ToString();
    if (!fs::exists(dir))
        return;
    for (const auto &entry : fs::recursive_directory_iterator(dir))
    {
        if (!entry.is_regular_file())
            continue;
        int fd = ::open(entry.path().c_str(), O_RDONLY);
        if (fd < 0)
            continue;
        ::fsync(fd);
        ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        ::close(fd);
    }
}

// Zero-padded numeric key so keys sort lexicographically by index. Used
// by the random and windowed-locality benchmarks so "close indices"
// translate to "close in the index tree" (adjacent entries in the same
// or neighboring data pages).
std::string MakeNumericKey(const char *prefix, int idx)
{
    char buf[24];
    std::snprintf(buf, sizeof(buf), "%s%010d", prefix, idx);
    return std::string(buf);
}

struct ConcurrentReadResult
{
    Pct pct;                // per-request latency over all threads
    int64_t wall_us{0};     // wall-clock spanning thread spawn -> join
    size_t total_bytes{0};  // num_threads * per_thread_reads * bytes_per_read
};

// Run N=`num_threads` client threads against `store`, each issuing
// `per_thread_reads` reads of `bytes_per_read` bytes via `ExecAsyn`, with
// at most `k_inflight` requests outstanding per thread at any instant. The
// per-request latency is measured from each ExecAsyn submit to the
// matching Wait return; the aggregate throughput is total bytes divided
// by the wall time across all threads.
//
// `prepare(req, thread_id, sample_i, slot)` configures one ReadRequest
// (set key, set large_value_dest_, etc.) before submission. The same
// slot index is reused across the in-flight window so callers using
// per-slot destination buffers can index by (thread_id, slot).
//
// `recycle(req)` is called after each Wait. Callers using IoStringBuffer
// destinations should recycle fragments here; pinned-mode callers leave
// it empty.
template <typename Prepare, typename Recycle>
ConcurrentReadResult RunConcurrentReads(eloqstore::EloqStore *store,
                                        int num_threads,
                                        int k_inflight,
                                        int per_thread_reads,
                                        size_t bytes_per_read,
                                        Prepare prepare,
                                        Recycle recycle)
{
    std::vector<std::vector<int64_t>> per_thread_times(num_threads);

    auto wall_start = Clock::now();

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back(
            [&, t]()
            {
                auto &times = per_thread_times[t];
                times.reserve(per_thread_reads);

                std::vector<std::unique_ptr<eloqstore::ReadRequest>> slots(
                    k_inflight);
                std::vector<int64_t> submit_ns(k_inflight, 0);

                int submitted = 0;
                int completed = 0;

                auto now_ns = []()
                {
                    return chrono::duration_cast<chrono::nanoseconds>(
                               Clock::now().time_since_epoch())
                        .count();
                };

                // Prime the pipeline -- submit up to k_inflight requests.
                for (int s = 0; s < k_inflight && submitted < per_thread_reads;
                     ++s)
                {
                    slots[s] = std::make_unique<eloqstore::ReadRequest>();
                    prepare(*slots[s], t, submitted, s);
                    submit_ns[s] = now_ns();
                    REQUIRE(store->ExecAsyn(slots[s].get()));
                    ++submitted;
                }

                // Drain + refill in FIFO order. Wait on the request at
                // slot (completed % k_inflight): we filled slots in
                // ascending order so this slot holds the oldest
                // outstanding request.
                while (completed < per_thread_reads)
                {
                    int slot = completed % k_inflight;
                    slots[slot]->Wait();
                    int64_t done = now_ns();
                    if (slots[slot]->Error() == eloqstore::KvError::NoError)
                    {
                        times.push_back(done - submit_ns[slot]);
                    }
                    recycle(*slots[slot]);
                    ++completed;

                    if (submitted < per_thread_reads)
                    {
                        slots[slot] =
                            std::make_unique<eloqstore::ReadRequest>();
                        prepare(*slots[slot], t, submitted, slot);
                        submit_ns[slot] = now_ns();
                        REQUIRE(store->ExecAsyn(slots[slot].get()));
                        ++submitted;
                    }
                }
            });
    }
    for (auto &th : threads)
        th.join();

    auto wall_end = Clock::now();

    ConcurrentReadResult result;
    result.wall_us =
        chrono::duration_cast<chrono::microseconds>(wall_end - wall_start)
            .count();
    result.total_bytes = static_cast<size_t>(num_threads) *
                         static_cast<size_t>(per_thread_reads) * bytes_per_read;

    std::vector<int64_t> all;
    size_t total = 0;
    for (auto &v : per_thread_times)
        total += v.size();
    all.reserve(total);
    for (auto &v : per_thread_times)
        all.insert(all.end(), v.begin(), v.end());
    result.pct = ComputePct(std::move(all));
    return result;
}

struct ConcurrencyConfig
{
    int num_threads;
    int k_inflight;
};

constexpr ConcurrencyConfig kConcurrencyConfigs[] = {
    {1, 1}, {1, 2}, {2, 1}, {2, 2}, {4, 1}, {4, 2}};

}  // namespace

// ============================================================================
// Benchmark 1: concurrent random reads -- large-value vs overflow-page
// ============================================================================
//
// Pre-write a dataset of N distinct keys per size case, sized so that
// `N * value_size` is well above any in-process cache. Between writes
// and reads we ::sync() and posix_fadvise(DONTNEED) every file in the
// partition directory so the OS page cache is dropped for the dataset.
//
// Concurrency: N_threads client threads, each maintaining K_inflight
// outstanding requests via ExecAsyn / Wait (FIFO). Total in-flight
// = N_threads * K_inflight. Per-request latency is submit->Wait time.
// Aggregate throughput is total bytes read across all threads divided
// by wall time (spawn -> join).
//
// Without concurrency these benchmarks measured single-stream latency;
// with N*K outstanding reads the storage path can drive more disk queue
// depth and the throughput numbers reflect what a real client (e.g. an
// inference server with multiple in-flight requests) would see.
// ============================================================================
TEST_CASE("Benchmark: concurrent random reads (large-value vs overflow-page)",
          "[benchmark][read-latency]")
{
    constexpr size_t kTargetDataset = 256ULL * 1024 * 1024;
    constexpr int kMinKeys = 256;
    constexpr int kMaxKeys = 2048;
    constexpr int kPerThreadReads = 100;

    BenchHarness h;
    eloqstore::KvOptions opts = MakeBenchOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    struct SizeCase
    {
        size_t bytes;
        const char *label;
    };
    const SizeCase cases[] = {
        {128 * 1024, "128 KB"},
        {1 * 1024 * 1024, "  1 MB"},
        {4 * 1024 * 1024, "  4 MB"},
        {10 * 1024 * 1024, " 10 MB"},
    };

    std::cout << "\n=== Benchmark 1: concurrent random reads "
                 "(large-value vs overflow-page) ===\n"
              << "  target_dataset=" << (kTargetDataset / (1024 * 1024))
              << " MiB  reads/thread=" << kPerThreadReads
              << "  (OS page cache dropped before each read phase)\n\n"
              << std::left << std::setw(9) << "Size" << std::setw(14) << "Path"
              << std::setw(8) << "NxK" << std::setw(12) << "p50 (us)"
              << std::setw(12) << "p99 (us)" << "Aggregate MB/s\n"
              << std::string(70, '-') << "\n"
              << std::flush;

    uint64_t seed = 0x1000;
    uint64_t ts = 1;

    for (size_t ci = 0; ci < std::size(cases); ++ci)
    {
        const auto &c = cases[ci];
        int N = static_cast<int>(std::min<size_t>(
            kMaxKeys, std::max<size_t>(kMinKeys, kTargetDataset / c.bytes)));

        eloqstore::TableIdent tbl_lv{"b1_lv_" + std::to_string(ci), 0};
        eloqstore::TableIdent tbl_ov{"b1_ov_" + std::to_string(ci), 0};

        // -- Populate large-value table --
        for (int i = 0; i < N; ++i)
        {
            REQUIRE(WriteLarge(store,
                               h,
                               tbl_lv,
                               MakeNumericKey("k_", i),
                               c.bytes,
                               seed++,
                               ts++));
        }

        // -- Populate overflow-page table --
        std::string filler(c.bytes, 'x');
        for (int i = 0; i < N; ++i)
        {
            REQUIRE(
                WriteStr(store, tbl_ov, MakeNumericKey("k_", i), filler, ts++));
        }

        for (auto cfg : kConcurrencyConfigs)
        {
            EvictPageCache(opts, tbl_lv);
            EvictPageCache(opts, tbl_ov);

            // Per-thread RNGs, seeded deterministically.
            std::vector<std::mt19937_64> rngs(cfg.num_threads);
            for (int t = 0; t < cfg.num_threads; ++t)
                rngs[t].seed(0xCAFEBABEull +
                             static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ull +
                             ci);
            std::uniform_int_distribution<int> pick(0, N - 1);

            auto prep_lv = [&](eloqstore::ReadRequest &r,
                               int t,
                               int /*sample*/,
                               int /*slot*/)
            {
                std::string key = MakeNumericKey("k_", pick(rngs[t]));
                r.SetArgs(tbl_lv, key);
                // Opt into IoStringBuffer; without this, dispatch falls
                // into metadata-only mode and segment reads are skipped.
                r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
            };
            auto recycle_lv = [&](eloqstore::ReadRequest &r)
            { h.RecycleRead(r); };

            auto lv_res = RunConcurrentReads(store,
                                             cfg.num_threads,
                                             cfg.k_inflight,
                                             kPerThreadReads,
                                             c.bytes,
                                             prep_lv,
                                             recycle_lv);

            // Re-seed for overflow side so the random sequence is
            // independent of the large-value run.
            for (int t = 0; t < cfg.num_threads; ++t)
                rngs[t].seed(0xDEADC0DEull +
                             static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ull +
                             ci);

            auto prep_ov = [&](eloqstore::ReadRequest &r,
                               int t,
                               int /*sample*/,
                               int /*slot*/)
            {
                std::string key = MakeNumericKey("k_", pick(rngs[t]));
                r.SetArgs(tbl_ov, key);
            };
            auto recycle_noop = [&](eloqstore::ReadRequest &) {};

            auto ov_res = RunConcurrentReads(store,
                                             cfg.num_threads,
                                             cfg.k_inflight,
                                             kPerThreadReads,
                                             c.bytes,
                                             prep_ov,
                                             recycle_noop);

            auto print_row =
                [&](const char *path, const ConcurrentReadResult &res)
            {
                char nk[12];
                std::snprintf(
                    nk, sizeof(nk), "%dx%d", cfg.num_threads, cfg.k_inflight);
                std::cout << std::left << std::setw(9) << c.label
                          << std::setw(14) << path << std::setw(8) << nk;
                if (res.pct.p50_us < 0)
                {
                    std::cout << std::setw(12) << "failed" << std::setw(12)
                              << "failed" << "n/a";
                }
                else
                {
                    double mbps = res.wall_us > 0
                                      ? static_cast<double>(res.total_bytes) /
                                            static_cast<double>(res.wall_us)
                                      : 0.0;
                    std::cout << std::setw(12) << res.pct.p50_us
                              << std::setw(12) << res.pct.p99_us << std::fixed
                              << std::setprecision(1) << mbps;
                    std::cout.unsetf(std::ios_base::floatfield);
                }
                std::cout << "\n";
            };

            print_row("large-value", lv_res);
            print_row("overflow", ov_res);
            std::cout << std::flush;
        }
    }

    std::cout << std::string(70, '-') << "\n"
              << "  large-value path expected faster than overflow at all "
                 "concurrency levels; both are cold-cache reads.\n"
              << std::flush;

    store->Stop();
    CleanupStore(opts);
}

// ============================================================================
// Benchmark 2: batch-write throughput — IoStringBuffer vs std::string
// ============================================================================
//
// Sweep batch size M ∈ {1, 8, 32}: each BatchWriteRequest carries M
// (key, value) entries. Per (size, M) we run `nbatches = max(5, 64/M)`
// batches so total keys per config is ~64 for small M and at least 5
// samples for large M. Throughput = (nbatches * M * size) / total time.
// At M=1 the per-batch overhead (request alloc, queueing, UpdateMeta
// pass) dominates; larger M amortizes it.
//
// IoStringBuffer path uses io_uring write_fixed to segment files;
// std::string path writes overflow pages via regular io_uring writes.
// ============================================================================
TEST_CASE("Benchmark: batch-write throughput IoStringBuffer vs std::string",
          "[benchmark][write-throughput]")
{
    BenchHarness h;
    eloqstore::KvOptions opts = MakeBenchOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"bench_wt", 0};

    struct SizeCase
    {
        size_t bytes;
        const char *label;
    };
    const SizeCase cases[] = {
        {1 * 1024 * 1024, "1 MB"},
        {4 * 1024 * 1024, "4 MB"},
        {10 * 1024 * 1024, "10 MB"},
    };

    constexpr int batch_sizes[] = {1, 8, 32};

    std::cout << "\n=== Benchmark 2: batch-write throughput "
                 "(IoStringBuffer vs std::string) ===\n"
              << "  per (size, M) we run nbatches = max(5, 64/M) so the "
                 "total keys per config is ~64\n\n"
              << std::left << std::setw(8) << "Size" << std::setw(20) << "Path"
              << std::setw(6) << "M" << std::setw(10) << "nbatch"
              << std::setw(14) << "Total (ms)" << "Throughput\n"
              << std::string(72, '-') << "\n"
              << std::flush;

    uint64_t seed = 0x2000;
    uint64_t ts = 100;

    auto run_batches_lv = [&](size_t size, int M, int nbatches, int run_id)
    {
        auto t0 = Clock::now();
        for (int b = 0; b < nbatches; ++b)
        {
            eloqstore::BatchWriteRequest req;
            std::vector<eloqstore::WriteDataEntry> entries;
            entries.reserve(M);
            for (int i = 0; i < M; ++i)
            {
                std::string key = "wt_lv_r" + std::to_string(run_id) + "_b" +
                                  std::to_string(b) + "_" + std::to_string(i);
                entries.emplace_back(std::move(key),
                                     h.MakeBuf(size, seed++),
                                     ts++,
                                     eloqstore::WriteOp::Upsert);
            }
            std::sort(entries.begin(), entries.end());
            req.SetArgs(tbl, std::move(entries));
            store->ExecSync(&req);
            REQUIRE(req.Error() == eloqstore::KvError::NoError);
            h.RecycleBatch(req);
        }
        return chrono::duration_cast<chrono::milliseconds>(Clock::now() - t0)
            .count();
    };

    auto run_batches_ov = [&](size_t size, int M, int nbatches, int run_id)
    {
        auto t0 = Clock::now();
        for (int b = 0; b < nbatches; ++b)
        {
            eloqstore::BatchWriteRequest req;
            std::vector<eloqstore::WriteDataEntry> entries;
            entries.reserve(M);
            std::string filler(size, 'y');
            for (int i = 0; i < M; ++i)
            {
                std::string key = "wt_ov_r" + std::to_string(run_id) + "_b" +
                                  std::to_string(b) + "_" + std::to_string(i);
                entries.emplace_back(
                    std::move(key), filler, ts++, eloqstore::WriteOp::Upsert);
            }
            std::sort(entries.begin(), entries.end());
            req.SetArgs(tbl, std::move(entries));
            store->ExecSync(&req);
            REQUIRE(req.Error() == eloqstore::KvError::NoError);
        }
        return chrono::duration_cast<chrono::milliseconds>(Clock::now() - t0)
            .count();
    };

    int run_id = 0;
    for (const auto &c : cases)
    {
        for (int M : batch_sizes)
        {
            // Per-batch pool footprint = M * ceil(size/seg) segments.
            // The IoStringBuffer pool holds kBenchPool / seg = 1024 segs.
            // Cap M so the batch fits with headroom.
            size_t segs_per_value = (c.bytes + kBenchSeg - 1) / kBenchSeg;
            int max_M = static_cast<int>((kBenchPool / kBenchSeg) /
                                         std::max<size_t>(segs_per_value, 1));
            int effective_M = std::min(M, std::max(1, max_M - 1));
            if (effective_M < M)
            {
                // Skip M values that don't fit; print a notice instead.
                std::cout << std::left << std::setw(8) << c.label
                          << std::setw(20) << "(skipped)" << std::setw(6) << M
                          << std::setw(10) << "-" << std::setw(14) << "-"
                          << "M*K_segs exceeds pool\n";
                std::cout << std::flush;
                continue;
            }
            int nbatches = std::max(5, 64 / M);
            int64_t lv_ms = run_batches_lv(c.bytes, M, nbatches, run_id++);
            double lv_mbps =
                lv_ms > 0 ? static_cast<double>(static_cast<size_t>(nbatches) *
                                                M * c.bytes) /
                                1e6 / (static_cast<double>(lv_ms) / 1000.0)
                          : 0.0;

            int64_t ov_ms = run_batches_ov(c.bytes, M, nbatches, run_id++);
            double ov_mbps =
                ov_ms > 0 ? static_cast<double>(static_cast<size_t>(nbatches) *
                                                M * c.bytes) /
                                1e6 / (static_cast<double>(ov_ms) / 1000.0)
                          : 0.0;

            auto print_row = [&](const char *path, int64_t ms, double mbps)
            {
                std::cout << std::left << std::setw(8) << c.label
                          << std::setw(20) << path << std::setw(6) << M
                          << std::setw(10) << nbatches << std::setw(14) << ms
                          << std::fixed << std::setprecision(1) << mbps
                          << " MB/s\n";
                std::cout.unsetf(std::ios_base::floatfield);
            };

            print_row("IoStringBuffer", lv_ms, lv_mbps);
            print_row("std::string", ov_ms, ov_mbps);
            std::cout << std::flush;
        }
    }

    std::cout << std::string(72, '-') << "\n"
              << "  larger M amortizes per-batch overhead (request alloc, "
                 "queueing, UpdateMeta).\n"
              << std::flush;

    store->Stop();
    CleanupStore(opts);
}

// ============================================================================
// Benchmark 3: foreground read p99 during segment compaction
// ============================================================================
//
// Setup per yield_every run:
//   - Write 16 segment files × 8 segments × 256 KB = 32 MB total.
//   - Delete 6 of 8 keys per file → 2 live segments per file (25% live ratio).
//   - With seg_amp=0: DoCompactSegmentFile's skip condition is
//     `factor <= 0` which is always false, so all non-empty files are
//     rewritten. Auto compaction is also disabled (amplify_factor==0 path).
//   - Rewrite I/O: 32 live segments × 256 KB = 8 MB read + 8 MB write.
//
// Measurement:
//   1. Baseline: 30 sequential reads of live keys (no compaction running).
//   2. Submit CompactRequest via ExecAsyn; shard sets done_=false inside
//      SendRequest before returning, so IsDone() polls safely immediately.
//   3. Submit 50 reads via ExecSync. Each read waits in the shard queue until
//      the compaction coroutine yields (every yield_every segments). The
//      measured latency = wait-for-yield + actual read time.
//   4. Report baseline p99 vs during-compaction p99 and the overhead ratio.
//
// Expected: smaller yield_every → more frequent yields → lower p99 overhead.
// ============================================================================
TEST_CASE("Benchmark: foreground read p99 during segment compaction",
          "[benchmark][compact-overhead]")
{
    const uint32_t yield_values[] = {1, 8, 32};

    std::cout
        << "\n=== Benchmark 3: foreground read p99 during segment compaction "
           "===\n"
        << "  setup: 16 files × 8 segs × 256 KB; live: 2/file → 8 MB rewrite\n"
        << "  seg_amp=0: all non-empty files rewritten by CompactRequest\n\n"
        << std::left << std::setw(14) << "yield_every" << std::setw(18)
        << "baseline p99" << std::setw(22) << "during-compact p99"
        << "overhead\n"
        << std::string(70, '-') << "\n";

    for (uint32_t yield_every : yield_values)
    {
        BenchHarness h;
        eloqstore::KvOptions opts =
            MakeBenchOpts(h, /*seg_amp=*/0, yield_every);
        eloqstore::EloqStore *store = InitStore(opts);
        h.BindStore(store);

        eloqstore::TableIdent tbl{"bench_co", 0};

        // Fill 16 segment files (8 segments each); delete 6 per file.
        // segments_per_file_shift=3 → 8 segments per file, so writing 8 keys
        // back-to-back fills exactly one segment file.
        constexpr size_t kNumFiles = 16;
        constexpr size_t kSegPerFile = 8;
        constexpr size_t kKeepPerFile = 2;
        constexpr size_t kDropPerFile = kSegPerFile - kKeepPerFile;

        std::vector<std::string> live_keys;
        uint64_t seed = 0x3000;
        uint64_t ts = 1;

        for (size_t fi = 0; fi < kNumFiles; ++fi)
        {
            std::vector<std::string> file_keys;
            file_keys.reserve(kSegPerFile);
            for (size_t k = 0; k < kSegPerFile; ++k)
            {
                std::string key =
                    "co_f" + std::to_string(fi) + "_k" + std::to_string(k);
                WriteLarge(store, h, tbl, key, kBenchSeg, seed++, ts++);
                file_keys.push_back(key);
            }
            for (size_t k = 0; k < kDropPerFile; ++k)
                DeleteKey(store, tbl, file_keys[k], ts++);
            for (size_t k = kDropPerFile; k < kSegPerFile; ++k)
                live_keys.push_back(file_keys[k]);
        }

        // ---- Baseline: 30 reads with no compaction in progress ----
        constexpr int kBaseSamples = 30;
        std::vector<int64_t> base_ns;
        base_ns.reserve(kBaseSamples);
        for (int i = 0; i < kBaseSamples; ++i)
        {
            const std::string &key = live_keys[i % live_keys.size()];
            eloqstore::ReadRequest r;
            r.SetArgs(tbl, key);
            auto t0 = Clock::now();
            store->ExecSync(&r);
            auto t1 = Clock::now();
            if (r.Error() == eloqstore::KvError::NoError)
                base_ns.push_back(
                    chrono::duration_cast<chrono::nanoseconds>(t1 - t0)
                        .count());
            h.RecycleRead(r);
        }
        Pct base_pct = ComputePct(base_ns);

        // ---- During-compaction: submit CompactRequest then read ----
        // SendRequest sets done_=false before dispatching (eloq_store.cpp),
        // so IsDone() is safe to poll immediately after ExecAsyn returns.
        eloqstore::CompactRequest compact;
        compact.SetTableId(tbl);
        store->ExecAsyn(&compact);

        constexpr int kCompactSamples = 50;
        std::vector<int64_t> compact_ns;
        compact_ns.reserve(kCompactSamples);
        int overlapped = 0;

        for (int i = 0; i < kCompactSamples; ++i)
        {
            bool still_running = !compact.IsDone();
            const std::string &key = live_keys[i % live_keys.size()];
            eloqstore::ReadRequest r;
            r.SetArgs(tbl, key);
            auto t0 = Clock::now();
            store->ExecSync(&r);
            auto t1 = Clock::now();
            if (r.Error() == eloqstore::KvError::NoError)
                compact_ns.push_back(
                    chrono::duration_cast<chrono::nanoseconds>(t1 - t0)
                        .count());
            h.RecycleRead(r);
            if (still_running)
                ++overlapped;
        }

        compact.Wait();
        Pct compact_pct = ComputePct(compact_ns);

        std::string base_str = std::to_string(base_pct.p99_us) + " us";
        std::string comp_str;
        std::string ovhd_str;
        if (overlapped > 0)
        {
            comp_str = std::to_string(compact_pct.p99_us) + " us";
            if (base_pct.p99_us > 0)
            {
                double ratio = static_cast<double>(compact_pct.p99_us) /
                               static_cast<double>(base_pct.p99_us);
                char buf[48];
                std::snprintf(buf,
                              sizeof(buf),
                              "%.2fx (%d overlapped)",
                              ratio,
                              overlapped);
                ovhd_str = buf;
            }
            else
            {
                ovhd_str = "n/a";
            }
        }
        else
        {
            comp_str = "(compact too fast)";
            ovhd_str = "n/a";
        }

        std::cout << std::left << std::setw(14) << yield_every << std::setw(18)
                  << base_str << std::setw(22) << comp_str << ovhd_str << "\n";

        store->Stop();
        CleanupStore(opts);
    }

    std::cout << std::string(70, '-') << "\n"
              << "  smaller yield_every => more frequent yields => lower p99 "
                 "overhead.\n";
}

// ============================================================================
// Pinned-mode benchmarks (Phases 3, 4, 6, 7)
// ============================================================================
namespace
{
// 64 MiB pinned chunk: comfortably above the 10 MB working set and any
// scratch-fallback test fixture that places values flush against the
// chunk end. Pinned-mode harness owns one 4 KiB-aligned buffer registered
// via KvOptions::pinned_memory_chunks.
class PinnedBenchHarness
{
public:
    // 128 MiB: large enough to host (max_threads * max_K) = 8 disjoint
    // destination slots at the maximum value size (10 MiB) used by the
    // concurrent read benchmarks. 8 * 10 MiB = 80 MiB; the extra headroom
    // covers the write-phase source buffer (re-used by reads).
    static constexpr size_t kPinnedSize = 128ULL * 1024 * 1024;

    PinnedBenchHarness()
    {
        void *raw = nullptr;
        REQUIRE(posix_memalign(&raw, 4096, kPinnedSize) == 0);
        REQUIRE(raw != nullptr);
        std::memset(raw, 0, kPinnedSize);
        base_ = static_cast<char *>(raw);
    }

    ~PinnedBenchHarness()
    {
        std::free(base_);
    }

    PinnedBenchHarness(const PinnedBenchHarness &) = delete;
    PinnedBenchHarness &operator=(const PinnedBenchHarness &) = delete;

    char *Base() const
    {
        return base_;
    }
    size_t Size() const
    {
        return kPinnedSize;
    }
    uint32_t SegmentSize() const
    {
        return kBenchSeg;
    }

    std::vector<std::pair<char *, size_t>> Chunks() const
    {
        return {{base_, kPinnedSize}};
    }

    // Allocate `size` bytes from the cursor and pre-fill with a fixed
    // pattern. Cursor advances by ceil(size / seg) * seg so back-to-back
    // allocations stay segment-aligned; the rounded-up tail always stays
    // inside the chunk (fast path).
    std::pair<char *, size_t> AllocateSegmentAligned(size_t size, uint64_t seed)
    {
        const size_t k = (size + kBenchSeg - 1) / kBenchSeg;
        const size_t aligned = k * kBenchSeg;
        REQUIRE(cursor_ + aligned <= kPinnedSize);
        char *p = base_ + cursor_;
        cursor_ += aligned;
        FillPattern(p, size, seed);
        return {p, size};
    }

    // Allocate `size` bytes flush against the chunk end (no cursor
    // advance). The rounded-up tail extends past the chunk and forces
    // the Phase-7 scratch fallback. Multiple calls return the same
    // pointer -- caller is responsible for not letting concurrent writes
    // race on the source bytes.
    std::pair<char *, size_t> AllocateChunkEnd(size_t size, uint64_t seed)
    {
        REQUIRE(size > 0);
        REQUIRE(size <= kPinnedSize);
        const size_t k = (size + kBenchSeg - 1) / kBenchSeg;
        REQUIRE(k * kBenchSeg > size);
        char *p = base_ + (kPinnedSize - size);
        FillPattern(p, size, seed);
        return {p, size};
    }

    void ResetCursor()
    {
        cursor_ = 0;
    }

private:
    char *base_{nullptr};
    size_t cursor_{0};
};

eloqstore::KvOptions MakePinnedBenchOpts(PinnedBenchHarness &h)
{
    eloqstore::KvOptions opts;
    opts.num_threads = 1;
    opts.num_retained_archives = 0;
    opts.archive_interval_secs = 0;
    opts.file_amplify_factor = 0;
    opts.segment_file_amplify_factor = 0;
    opts.store_path = {test_path};
    opts.pages_per_file_shift = 8;
    opts.segment_size = h.SegmentSize();
    opts.segments_per_file_shift = 3;
    opts.data_append_mode = true;
    opts.buffer_pool_size = 16 * eloqstore::MB;
    opts.write_buffer_size = 0;
    opts.write_buffer_ratio = 0.0;
    opts.pinned_memory_chunks = h.Chunks();
    opts.gc_global_mem_size_per_shard = 32ULL * eloqstore::MB;
    opts.pinned_tail_scratch_slots = eloqstore::max_segments_batch;
    return opts;
}

bool WritePinned(eloqstore::EloqStore *store,
                 const eloqstore::TableIdent &tbl,
                 const std::string &key,
                 std::pair<char *, size_t> dst,
                 std::string metadata,
                 uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        key,
        std::move(metadata),
        std::make_pair(static_cast<const char *>(dst.first), dst.second),
        ts,
        eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    return req.Error() == eloqstore::KvError::NoError;
}

// Time n synchronous pinned writes of `size` bytes at `dst`. Each write
// uses a distinct key but reuses the same source buffer (the goal is
// the write path, not byte-uniqueness). Caller picks dst.
std::vector<int64_t> TimePinnedWrites(eloqstore::EloqStore *store,
                                      const eloqstore::TableIdent &tbl,
                                      const std::string &key_prefix,
                                      std::pair<char *, size_t> dst,
                                      const std::string &metadata,
                                      int n,
                                      uint64_t ts_base)
{
    std::vector<int64_t> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i)
    {
        std::string key = key_prefix + std::to_string(i);
        auto t0 = Clock::now();
        bool ok = WritePinned(store, tbl, key, dst, metadata, ts_base + i);
        auto t1 = Clock::now();
        if (ok)
            v.push_back(
                chrono::duration_cast<chrono::nanoseconds>(t1 - t0).count());
    }
    return v;
}

// Time n synchronous pinned reads via overload B (metadata + bytes) into
// a fresh aligned sub-range each time. Returns wall-clock nanoseconds per
// read; metadata destination is unused but allocated on every iteration
// so the cost is included in the measurement.
std::vector<int64_t> TimePinnedReadsOverloadB(eloqstore::EloqStore *store,
                                              PinnedBenchHarness &h,
                                              const eloqstore::TableIdent &tbl,
                                              const std::string &key,
                                              size_t value_size,
                                              int n)
{
    std::vector<int64_t> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i)
    {
        auto rb = h.AllocateSegmentAligned(value_size, /*seed=*/0);
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, key);
        r.large_value_dest_ = std::make_pair(rb.first, rb.second);
        auto t0 = Clock::now();
        store->ExecSync(&r);
        auto t1 = Clock::now();
        if (r.Error() == eloqstore::KvError::NoError)
            v.push_back(
                chrono::duration_cast<chrono::nanoseconds>(t1 - t0).count());
        // Reset the cursor every iteration so we don't run out of
        // pinned-buffer space across n iterations.
        h.ResetCursor();
    }
    return v;
}

// Same as above but uses overload C (pinned-only -- no metadata
// extraction).
std::vector<int64_t> TimePinnedReadsOverloadC(eloqstore::EloqStore *store,
                                              PinnedBenchHarness &h,
                                              const eloqstore::TableIdent &tbl,
                                              const std::string &key,
                                              size_t value_size,
                                              int n)
{
    std::vector<int64_t> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i)
    {
        auto rb = h.AllocateSegmentAligned(value_size, /*seed=*/0);
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, key);
        r.large_value_dest_ = std::make_pair(rb.first, rb.second);
        r.large_value_only_ = true;
        auto t0 = Clock::now();
        store->ExecSync(&r);
        auto t1 = Clock::now();
        if (r.Error() == eloqstore::KvError::NoError)
            v.push_back(
                chrono::duration_cast<chrono::nanoseconds>(t1 - t0).count());
        h.ResetCursor();
    }
    return v;
}

}  // namespace

// ============================================================================
// Benchmark 4: pinned-mode batch-write throughput vs IoStringBuffer
// ============================================================================
//
// Mirror of Benchmark 2 but compares pinned mode vs IoStringBuffer mode.
// Sweep batch size M ∈ {1, 8, 32}. Per (size, M) we run
// `nbatches = max(5, 64/M)` batches and measure total wall time.
//
// Pinned mode skips the staging copy from caller-side std::string /
// IoStringBuffer fragments into the io_uring registered pool because
// the bytes already live in caller-managed pinned memory. We reuse
// one pre-filled pinned region across all M entries in a batch
// (same on-disk content under M different keys) -- that's fine for
// throughput; the kernel just reads the same source range M times.
// ============================================================================
TEST_CASE("Benchmark: pinned-mode batch-write throughput",
          "[benchmark][write-throughput][pinned]")
{
    struct SizeCase
    {
        size_t bytes;
        const char *label;
    };
    const SizeCase cases[] = {
        {1 * 1024 * 1024, "1 MB"},
        {4 * 1024 * 1024, "4 MB"},
        {10 * 1024 * 1024, "10 MB"},
    };
    constexpr int batch_sizes[] = {1, 8, 32};

    std::cout << "\n=== Benchmark 4: pinned-mode batch-write throughput "
                 "(pinned vs IoStringBuffer) ===\n"
              << "  per (size, M) we run nbatches = max(5, 64/M)\n\n"
              << std::left << std::setw(8) << "Size" << std::setw(20) << "Path"
              << std::setw(6) << "M" << std::setw(10) << "nbatch"
              << std::setw(14) << "Total (ms)" << "Throughput\n"
              << std::string(72, '-') << "\n"
              << std::flush;

    // ---- IoStringBuffer path (baseline) ----
    std::vector<std::vector<std::pair<int64_t, double>>> lv_rows(
        std::size(cases),
        std::vector<std::pair<int64_t, double>>(std::size(batch_sizes)));
    {
        BenchHarness h;
        eloqstore::KvOptions opts = MakeBenchOpts(h);
        eloqstore::EloqStore *store = InitStore(opts);
        h.BindStore(store);
        eloqstore::TableIdent tbl{"bench_wt_lv", 0};

        uint64_t seed = 0x4000;
        uint64_t ts = 1;
        int run_id = 0;
        for (size_t ci = 0; ci < std::size(cases); ++ci)
        {
            const auto &c = cases[ci];
            size_t segs_per_value = (c.bytes + kBenchSeg - 1) / kBenchSeg;
            int max_M = static_cast<int>((kBenchPool / kBenchSeg) /
                                         std::max<size_t>(segs_per_value, 1));
            for (size_t mi = 0; mi < std::size(batch_sizes); ++mi)
            {
                int M = batch_sizes[mi];
                if (M > max_M - 1)
                {
                    lv_rows[ci][mi] = {-1, -1.0};
                    continue;
                }
                int nbatches = std::max(5, 64 / M);
                auto t0 = Clock::now();
                for (int b = 0; b < nbatches; ++b)
                {
                    eloqstore::BatchWriteRequest req;
                    std::vector<eloqstore::WriteDataEntry> entries;
                    entries.reserve(M);
                    for (int i = 0; i < M; ++i)
                    {
                        std::string key = "wt_lv_r" + std::to_string(run_id) +
                                          "_b" + std::to_string(b) + "_" +
                                          std::to_string(i);
                        entries.emplace_back(std::move(key),
                                             h.MakeBuf(c.bytes, seed++),
                                             ts++,
                                             eloqstore::WriteOp::Upsert);
                    }
                    std::sort(entries.begin(), entries.end());
                    req.SetArgs(tbl, std::move(entries));
                    store->ExecSync(&req);
                    REQUIRE(req.Error() == eloqstore::KvError::NoError);
                    h.RecycleBatch(req);
                }
                int64_t ms = chrono::duration_cast<chrono::milliseconds>(
                                 Clock::now() - t0)
                                 .count();
                double mbps =
                    ms > 0 ? static_cast<double>(static_cast<size_t>(nbatches) *
                                                 M * c.bytes) /
                                 1e6 / (static_cast<double>(ms) / 1000.0)
                           : 0.0;
                lv_rows[ci][mi] = {ms, mbps};
                ++run_id;
            }
        }
        store->Stop();
        CleanupStore(opts);
    }

    // ---- Pinned path ----
    std::vector<std::vector<std::pair<int64_t, double>>> p_rows(
        std::size(cases),
        std::vector<std::pair<int64_t, double>>(std::size(batch_sizes)));
    {
        PinnedBenchHarness h;
        eloqstore::KvOptions opts = MakePinnedBenchOpts(h);
        eloqstore::EloqStore *store = InitStore(opts);
        eloqstore::TableIdent tbl{"bench_wt_pinned", 0};

        uint64_t ts = 1;
        int run_id = 0;
        for (size_t ci = 0; ci < std::size(cases); ++ci)
        {
            const auto &c = cases[ci];
            // Pre-fill one shared source region; all M entries in a batch
            // point at it.
            h.ResetCursor();
            auto src = h.AllocateSegmentAligned(
                c.bytes, 0x5000 + static_cast<uint64_t>(ci));
            for (size_t mi = 0; mi < std::size(batch_sizes); ++mi)
            {
                int M = batch_sizes[mi];
                int nbatches = std::max(5, 64 / M);
                auto t0 = Clock::now();
                for (int b = 0; b < nbatches; ++b)
                {
                    eloqstore::BatchWriteRequest req;
                    std::vector<eloqstore::WriteDataEntry> entries;
                    entries.reserve(M);
                    for (int i = 0; i < M; ++i)
                    {
                        std::string key =
                            "wt_pinned_r" + std::to_string(run_id) + "_b" +
                            std::to_string(b) + "_" + std::to_string(i);
                        entries.emplace_back(
                            std::move(key),
                            /*metadata=*/std::string{},
                            std::make_pair(static_cast<const char *>(src.first),
                                           src.second),
                            ts++,
                            eloqstore::WriteOp::Upsert);
                    }
                    std::sort(entries.begin(), entries.end());
                    req.SetArgs(tbl, std::move(entries));
                    store->ExecSync(&req);
                    REQUIRE(req.Error() == eloqstore::KvError::NoError);
                }
                int64_t ms = chrono::duration_cast<chrono::milliseconds>(
                                 Clock::now() - t0)
                                 .count();
                double mbps =
                    ms > 0 ? static_cast<double>(static_cast<size_t>(nbatches) *
                                                 M * c.bytes) /
                                 1e6 / (static_cast<double>(ms) / 1000.0)
                           : 0.0;
                p_rows[ci][mi] = {ms, mbps};
                ++run_id;
            }
        }
        store->Stop();
        CleanupStore(opts);
    }

    // ---- Print interleaved by (size, M): IoStringBuffer row then pinned ----
    for (size_t ci = 0; ci < std::size(cases); ++ci)
    {
        const auto &c = cases[ci];
        for (size_t mi = 0; mi < std::size(batch_sizes); ++mi)
        {
            int M = batch_sizes[mi];
            int nbatches = std::max(5, 64 / M);
            auto row = [&](const char *path, std::pair<int64_t, double> r)
            {
                std::cout << std::left << std::setw(8) << c.label
                          << std::setw(20) << path << std::setw(6) << M
                          << std::setw(10) << nbatches << std::setw(14);
                if (r.first < 0)
                {
                    std::cout << "(skip: pool)" << "n/a\n";
                }
                else
                {
                    std::cout << r.first << std::fixed << std::setprecision(1)
                              << r.second << " MB/s\n";
                    std::cout.unsetf(std::ios_base::floatfield);
                }
            };
            row("IoStringBuffer", lv_rows[ci][mi]);
            row("pinned", p_rows[ci][mi]);
        }
        std::cout << std::flush;
    }

    std::cout << std::string(72, '-') << "\n"
              << "  pinned expected at parity or faster: no staging copy "
                 "from caller bytes to fragments.\n"
              << std::flush;
}

// ============================================================================
// Benchmark 5: pinned-write tail-scratch overhead
// ============================================================================
//
// Compare per-write latency for two pinned-write patterns:
//   - Fast path: value placed segment-aligned somewhere mid-chunk, so the
//     rounded-up [ptr, ptr + K*seg) stays inside the registered range.
//   - Scratch fallback: value placed flush against the chunk end, so the
//     rounded-up tail extends past the registered range and the Phase-7
//     scratch path runs (acquire slot, memcpy + memset, write_fixed from
//     the slot, release slot).
//
// Expected: scratch overhead is a small constant relative to total write
// time -- one memcpy / memset of at most segment_size bytes plus the
// scratch acquire/release.
// ============================================================================
TEST_CASE("Benchmark: pinned-write tail-scratch overhead",
          "[benchmark][write-latency][pinned][tail-scratch]")
{
    struct SizeCase
    {
        size_t bytes;
        const char *label;
    };
    // Mid-segment sizes -- ceil(bytes / seg) * seg > bytes for each, so the
    // chunk-end variant triggers scratch.
    const SizeCase cases[] = {
        {kBenchSeg + 4096, "1 seg + 4 KB"},
        {3 * kBenchSeg + 4096, "3 seg + 4 KB"},
        {7 * kBenchSeg + 4096, "7 seg + 4 KB"},
    };
    constexpr int kWarmup = 5;
    constexpr int kSamples = 30;

    PinnedBenchHarness h;
    eloqstore::KvOptions opts = MakePinnedBenchOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);
    eloqstore::TableIdent tbl{"bench_scratch", 0};

    std::cout << "\n=== Benchmark 5: pinned-write tail-scratch overhead ===\n"
              << "  samples=" << kSamples << " per path per size\n\n"
              << std::left << std::setw(15) << "Size" << std::setw(18) << "Path"
              << std::setw(14) << "p50 (us)" << "p99 (us)\n"
              << std::string(60, '-') << "\n";

    uint64_t ts = 1;
    for (const auto &c : cases)
    {
        // Fast path: cursor-allocated mid-chunk, rounded-up tail stays
        // inside.
        h.ResetCursor();
        auto fast_dst = h.AllocateSegmentAligned(c.bytes, 0xA000);
        // Warmup.
        for (int i = 0; i < kWarmup; ++i)
            WritePinned(store,
                        tbl,
                        "fast_warm_" + std::to_string(i),
                        fast_dst,
                        {},
                        ts++);
        auto fast_times = TimePinnedWrites(store,
                                           tbl,
                                           std::string{"fast_"} + c.label + "_",
                                           fast_dst,
                                           /*metadata=*/{},
                                           kSamples,
                                           ts);
        ts += kSamples;
        const size_t fast_acquires_before =
            store->TailScratchAcquireCount(/*shard_id=*/0);

        // Scratch fallback: flush against chunk end so every write spills
        // past the registered range.
        auto scratch_dst = h.AllocateChunkEnd(c.bytes, 0xB000);
        for (int i = 0; i < kWarmup; ++i)
            WritePinned(store,
                        tbl,
                        "scratch_warm_" + std::to_string(i),
                        scratch_dst,
                        {},
                        ts++);
        auto scratch_times =
            TimePinnedWrites(store,
                             tbl,
                             std::string{"scratch_"} + c.label + "_",
                             scratch_dst,
                             /*metadata=*/{},
                             kSamples,
                             ts);
        ts += kSamples;
        const size_t scratch_acquires_now =
            store->TailScratchAcquireCount(/*shard_id=*/0);

        // Sanity: fast path never touched scratch; scratch path acquired
        // at least once per non-warmup write.
        REQUIRE(scratch_acquires_now - fast_acquires_before >=
                static_cast<size_t>(kSamples));

        Pct fast_p = ComputePct(std::move(fast_times));
        Pct scratch_p = ComputePct(std::move(scratch_times));

        std::cout << std::left << std::setw(15) << c.label << std::setw(18)
                  << "fast-path" << std::setw(14) << fast_p.p50_us
                  << fast_p.p99_us << "\n"
                  << std::left << std::setw(15) << c.label << std::setw(18)
                  << "scratch-fallback" << std::setw(14) << scratch_p.p50_us
                  << scratch_p.p99_us << "\n";
    }

    std::cout << std::string(60, '-') << "\n"
              << "  scratch overhead = one segment-sized memcpy + memset + "
                 "slot acquire/release.\n";
    store->Stop();
    CleanupStore(opts);
}

// ============================================================================
// Benchmark 6: pinned-mode concurrent random reads
// ============================================================================
//
// Pinned-mode random reads on a dataset > OS page cache (evicted before
// each config). N_threads * K_inflight requests in flight, each thread
// issues `kPerThreadReads` reads via ExecAsyn / Wait (FIFO). Uses
// overload B (metadata + pinned bytes), the typical KV Cache call.
//
// Per-thread per-slot destination buffers live inside the harness's
// pinned chunk, at fixed offsets `(thread_id * K + slot) * value_size`.
// kPinnedSize (128 MiB) covers the worst case (4 * 2 * 10 MiB = 80 MiB).
//
// Reports per-request p50/p99 latency (combined across threads) and
// aggregate MB/s (total bytes / wall time).
// ============================================================================
TEST_CASE("Benchmark: pinned-mode concurrent random reads",
          "[benchmark][read-latency][pinned]")
{
    constexpr size_t kTargetDataset = 256ULL * 1024 * 1024;
    constexpr int kMinKeys = 256;
    constexpr int kMaxKeys = 2048;
    constexpr int kPerThreadReads = 100;
    const std::string metadata = "bench-meta";

    PinnedBenchHarness h;
    eloqstore::KvOptions opts = MakePinnedBenchOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);

    struct SizeCase
    {
        size_t bytes;
        const char *label;
    };
    const SizeCase cases[] = {
        {128 * 1024, "128 KB"},
        {1 * 1024 * 1024, "  1 MB"},
        {4 * 1024 * 1024, "  4 MB"},
        {10 * 1024 * 1024, " 10 MB"},
    };

    std::cout << "\n=== Benchmark 6: pinned-mode concurrent random reads ===\n"
              << "  target_dataset=" << (kTargetDataset / (1024 * 1024))
              << " MiB  reads/thread=" << kPerThreadReads
              << "  (OS page cache dropped before each (NxK) config)\n\n"
              << std::left << std::setw(9) << "Size" << std::setw(10)
              << "N keys" << std::setw(8) << "NxK" << std::setw(12)
              << "p50 (us)" << std::setw(12) << "p99 (us)" << "Aggregate MB/s\n"
              << std::string(66, '-') << "\n"
              << std::flush;

    uint64_t ts = 1;
    for (size_t ci = 0; ci < std::size(cases); ++ci)
    {
        const auto &c = cases[ci];
        int N = static_cast<int>(std::min<size_t>(
            kMaxKeys, std::max<size_t>(kMinKeys, kTargetDataset / c.bytes)));

        eloqstore::TableIdent tbl{"b6_" + std::to_string(ci), 0};

        // Reuse one pre-filled source buffer for all N writes (we measure
        // reads). Source lives at offset 0 of the pinned chunk and is
        // overwritten by read destinations -- safe because writes are
        // already on disk by the time the read phase starts.
        h.ResetCursor();
        auto src = h.AllocateSegmentAligned(c.bytes,
                                            0xDD00 + static_cast<uint64_t>(ci));
        for (int i = 0; i < N; ++i)
        {
            REQUIRE(WritePinned(
                store, tbl, MakeNumericKey("k_", i), src, metadata, ts++));
        }

        for (auto cfg : kConcurrencyConfigs)
        {
            EvictPageCache(opts, tbl);

            std::vector<std::mt19937_64> rngs(cfg.num_threads);
            for (int t = 0; t < cfg.num_threads; ++t)
                rngs[t].seed(0xFEEDC0DEull +
                             static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ull +
                             ci);
            std::uniform_int_distribution<int> pick(0, N - 1);

            auto prep =
                [&](eloqstore::ReadRequest &r, int t, int /*sample*/, int slot)
            {
                std::string key = MakeNumericKey("k_", pick(rngs[t]));
                r.SetArgs(tbl, key);
                // Per-thread per-slot destination buffer.
                size_t dst_off =
                    static_cast<size_t>(t * cfg.k_inflight + slot) * c.bytes;
                r.large_value_dest_ =
                    std::make_pair(h.Base() + dst_off, c.bytes);
            };
            auto recycle_noop = [&](eloqstore::ReadRequest &) {};

            auto res = RunConcurrentReads(store,
                                          cfg.num_threads,
                                          cfg.k_inflight,
                                          kPerThreadReads,
                                          c.bytes,
                                          prep,
                                          recycle_noop);

            char nk[12];
            std::snprintf(
                nk, sizeof(nk), "%dx%d", cfg.num_threads, cfg.k_inflight);
            double mbps = res.wall_us > 0
                              ? static_cast<double>(res.total_bytes) /
                                    static_cast<double>(res.wall_us)
                              : 0.0;
            std::cout << std::left << std::setw(9) << c.label << std::setw(10)
                      << N << std::setw(8) << nk << std::setw(12)
                      << res.pct.p50_us << std::setw(12) << res.pct.p99_us
                      << std::fixed << std::setprecision(1) << mbps << "\n";
            std::cout.unsetf(std::ios_base::floatfield);
            std::cout << std::flush;
        }
    }

    std::cout << std::string(66, '-') << "\n"
              << "  cold-cache concurrent random reads; total in-flight = "
                 "NxK per row.\n"
              << std::flush;
    store->Stop();
    CleanupStore(opts);
}

// ============================================================================
// Benchmark 7: pinned-mode concurrent windowed-locality reads (KV Cache)
// ============================================================================
//
// Simulates a KV Cache access pattern with concurrent clients: each of
// N_threads worker threads repeatedly reads a small window of W
// consecutive keys (close in the index -- adjacent positions in the
// same / neighboring data pages), then jumps to a different random
// window. Each thread has K_inflight requests outstanding via ExecAsyn.
//
// This is the canonical access shape for an inference server: each
// in-flight request loads its tokens' KV state from a contiguous key
// range; multiple requests are served in parallel.
//
// Same dataset and cold-cache treatment as Benchmark 6. The
// throughput delta vs Benchmark 6's pure-random pattern (at the same
// NxK) is the locality benefit -- the data-page-cache hit within a
// window saves the index navigation on the (W-1)/W subsequent reads.
// ============================================================================
TEST_CASE("Benchmark: pinned-mode concurrent windowed-locality reads",
          "[benchmark][read-latency][pinned]")
{
    constexpr size_t kTargetDataset = 256ULL * 1024 * 1024;
    constexpr int kMinKeys = 256;
    constexpr int kMaxKeys = 2048;
    constexpr int kWindowSize = 8;       // adjacent keys read per window
    constexpr int kPerThreadReads = 96;  // 12 windows of W=8
    static_assert(kPerThreadReads % kWindowSize == 0,
                  "per-thread reads must be a whole number of windows");
    const std::string metadata = "bench-meta";

    PinnedBenchHarness h;
    eloqstore::KvOptions opts = MakePinnedBenchOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);

    struct SizeCase
    {
        size_t bytes;
        const char *label;
    };
    const SizeCase cases[] = {
        {128 * 1024, "128 KB"},
        {1 * 1024 * 1024, "  1 MB"},
        {4 * 1024 * 1024, "  4 MB"},
        {10 * 1024 * 1024, " 10 MB"},
    };

    std::cout
        << "\n=== Benchmark 7: pinned-mode concurrent windowed-locality reads "
           "(KV Cache pattern) ===\n"
        << "  target_dataset=" << (kTargetDataset / (1024 * 1024))
        << " MiB  W=" << kWindowSize
        << " (keys per window)  reads/thread=" << kPerThreadReads << "\n\n"
        << std::left << std::setw(9) << "Size" << std::setw(10) << "N keys"
        << std::setw(8) << "NxK" << std::setw(12) << "p50 (us)" << std::setw(12)
        << "p99 (us)" << "Aggregate MB/s\n"
        << std::string(66, '-') << "\n"
        << std::flush;

    uint64_t ts = 1;
    for (size_t ci = 0; ci < std::size(cases); ++ci)
    {
        const auto &c = cases[ci];
        int N = static_cast<int>(std::min<size_t>(
            kMaxKeys, std::max<size_t>(kMinKeys, kTargetDataset / c.bytes)));
        if (N < kWindowSize)
            N = kWindowSize;

        eloqstore::TableIdent tbl{"b7_" + std::to_string(ci), 0};

        h.ResetCursor();
        auto src = h.AllocateSegmentAligned(c.bytes,
                                            0xEE00 + static_cast<uint64_t>(ci));
        for (int i = 0; i < N; ++i)
        {
            REQUIRE(WritePinned(
                store, tbl, MakeNumericKey("k_", i), src, metadata, ts++));
        }

        for (auto cfg : kConcurrencyConfigs)
        {
            EvictPageCache(opts, tbl);

            std::vector<std::mt19937_64> rngs(cfg.num_threads);
            for (int t = 0; t < cfg.num_threads; ++t)
                rngs[t].seed(0xBADC0FFEull +
                             static_cast<uint64_t>(t) * 0x9E3779B97F4A7C15ull +
                             ci);
            std::uniform_int_distribution<int> pick_start(0, N - kWindowSize);

            // Per-thread "current window start"; updated on entry to a new
            // window (sample_i % W == 0). Each thread mutates only its own
            // slot, so no atomics are needed.
            std::vector<int> thread_window_start(cfg.num_threads, 0);

            auto prep =
                [&](eloqstore::ReadRequest &r, int t, int sample, int slot)
            {
                int within = sample % kWindowSize;
                if (within == 0)
                    thread_window_start[t] = pick_start(rngs[t]);
                int key_idx = thread_window_start[t] + within;
                r.SetArgs(tbl, MakeNumericKey("k_", key_idx));
                size_t dst_off =
                    static_cast<size_t>(t * cfg.k_inflight + slot) * c.bytes;
                r.large_value_dest_ =
                    std::make_pair(h.Base() + dst_off, c.bytes);
            };
            auto recycle_noop = [&](eloqstore::ReadRequest &) {};

            auto res = RunConcurrentReads(store,
                                          cfg.num_threads,
                                          cfg.k_inflight,
                                          kPerThreadReads,
                                          c.bytes,
                                          prep,
                                          recycle_noop);

            char nk[12];
            std::snprintf(
                nk, sizeof(nk), "%dx%d", cfg.num_threads, cfg.k_inflight);
            double mbps = res.wall_us > 0
                              ? static_cast<double>(res.total_bytes) /
                                    static_cast<double>(res.wall_us)
                              : 0.0;
            std::cout << std::left << std::setw(9) << c.label << std::setw(10)
                      << N << std::setw(8) << nk << std::setw(12)
                      << res.pct.p50_us << std::setw(12) << res.pct.p99_us
                      << std::fixed << std::setprecision(1) << mbps << "\n";
            std::cout.unsetf(std::ios_base::floatfield);
            std::cout << std::flush;
        }
    }

    std::cout
        << std::string(66, '-') << "\n"
        << "  windowed-locality vs Benchmark 6's random: same NxK, but each "
           "thread's W="
        << kWindowSize << " consecutive reads share data-page navigation.\n"
        << std::flush;
    store->Stop();
    CleanupStore(opts);
}
