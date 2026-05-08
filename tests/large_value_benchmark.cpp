// tests/large_value_benchmark.cpp
//
// Informational benchmarks for the zero-copy large-value read/write paths.
// Excluded from the normal (non-benchmark) test run.
//
// Run with:
//   sudo prlimit --memlock=unlimited ./large_value_benchmark -t '[benchmark]'
//
// All results are informational; no assertions gate on specific latency values.

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
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
// Covers 10 MB reads (40 segments), compaction's max_segments_batch=8
// in-flight, warmup reads, and a safety margin.
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
            if (e.HasLargeValue())
                e.large_val_.Recycle(mem_.get(), base_);
    }

    void RecycleRead(eloqstore::ReadRequest &req)
    {
        if (!req.large_value_.Fragments().empty())
            req.large_value_.Recycle(mem_.get(), base_);
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

}  // namespace

// ============================================================================
// Benchmark 1: point-read latency — large-value path vs overflow-page path
// ============================================================================
//
// The large-value path stores values as fixed-size segments in dedicated
// segment files and reads them via io_uring write_fixed / read_fixed
// (zero-copy). The overflow-page path stores values as linked 4 KB pages in
// data files using regular io_uring reads.
//
// Expected: large-value is faster for sizes >= segment_size (256 KB) because
// it avoids multi-level page chaining and issues vectored fixed-buffer reads.
// ============================================================================
TEST_CASE("Benchmark: point-read latency large-value vs overflow-page path",
          "[benchmark][read-latency]")
{
    BenchHarness h;
    eloqstore::KvOptions opts = MakeBenchOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"bench_rd", 0};

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

    constexpr int kWarmup = 5;
    constexpr int kSamples = 30;

    std::cout << "\n=== Benchmark 1: point-read latency "
                 "(large-value vs overflow-page) ===\n"
              << "  segment_size=" << (h.SegmentSize() / 1024)
              << " KB  samples=" << kSamples << "\n\n"
              << std::left << std::setw(9) << "Size" << std::setw(16) << "Path"
              << std::setw(14) << "p50 (us)" << "p99 (us)\n"
              << std::string(51, '-') << "\n";

    uint64_t seed = 0x1000;
    uint64_t ts = 1;

    for (const auto &c : cases)
    {
        // ---- large-value path ----
        std::string lv_key = std::string("lv_") + c.label;
        WriteLarge(store, h, tbl, lv_key, c.bytes, seed++, ts++);

        for (int i = 0; i < kWarmup; ++i)
        {
            eloqstore::ReadRequest r;
            r.SetArgs(tbl, lv_key);
            store->ExecSync(&r);
            h.RecycleRead(r);
        }

        auto lv_pct =
            ComputePct(TimeLargeReads(store, h, tbl, lv_key, kSamples));

        // ---- overflow-page path (std::string of same logical size) ----
        std::string ov_key = std::string("ov_") + c.label;
        bool ov_ok =
            WriteStr(store, tbl, ov_key, std::string(c.bytes, 'x'), ts++);

        Pct ov_pct{};
        if (ov_ok)
        {
            for (int i = 0; i < kWarmup; ++i)
            {
                eloqstore::ReadRequest r;
                r.SetArgs(tbl, ov_key);
                store->ExecSync(&r);
            }
            ov_pct = ComputePct(TimeStrReads(store, tbl, ov_key, kSamples));
        }

        auto print_row = [&](const char *path, const Pct &p)
        {
            std::cout << std::left << std::setw(9) << c.label << std::setw(16)
                      << path;
            if (p.p50_us < 0)
                std::cout << std::setw(14) << "failed" << "failed";
            else
                std::cout << std::setw(14) << p.p50_us << p.p99_us;
            std::cout << "\n";
        };

        print_row("large-value", lv_pct);
        print_row(ov_ok ? "overflow" : "overflow(fail)", ov_pct);
    }

    std::cout << std::string(51, '-') << "\n"
              << "  large-value expected faster for sizes >= "
              << (h.SegmentSize() / 1024) << " KB.\n";

    store->Stop();
    CleanupStore(opts);
}

// ============================================================================
// Benchmark 2: batch-write throughput — IoStringBuffer vs std::string
// ============================================================================
//
// Both paths flush the same logical data volume. IoStringBuffer uses
// io_uring write_fixed to segment files; std::string writes overflow pages
// to data files via regular io_uring writes.
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

    constexpr int kWrites = 10;

    std::cout << "\n=== Benchmark 2: batch-write throughput "
                 "(IoStringBuffer vs std::string) ===\n"
              << "  writes=" << kWrites << " per path per size\n\n"
              << std::left << std::setw(8) << "Size" << std::setw(20) << "Path"
              << std::setw(14) << "Total (ms)" << "Throughput\n"
              << std::string(56, '-') << "\n";

    uint64_t seed = 0x2000;
    uint64_t ts = 100;

    for (const auto &c : cases)
    {
        // ---- IoStringBuffer ----
        auto t0 = Clock::now();
        for (int i = 0; i < kWrites; ++i)
        {
            std::string key = "wt_lv_" + std::to_string(i);
            WriteLarge(store, h, tbl, key, c.bytes, seed++, ts++);
        }
        int64_t lv_ms =
            chrono::duration_cast<chrono::milliseconds>(Clock::now() - t0)
                .count();
        double lv_mbps = lv_ms > 0
                             ? static_cast<double>(kWrites * c.bytes) / 1e6 /
                                   (static_cast<double>(lv_ms) / 1000.0)
                             : 0.0;

        // ---- std::string (overflow path) ----
        auto t1 = Clock::now();
        for (int i = 0; i < kWrites; ++i)
        {
            std::string key = "wt_ov_" + std::to_string(i);
            WriteStr(store, tbl, key, std::string(c.bytes, 'y'), ts++);
        }
        int64_t ov_ms =
            chrono::duration_cast<chrono::milliseconds>(Clock::now() - t1)
                .count();
        double ov_mbps = ov_ms > 0
                             ? static_cast<double>(kWrites * c.bytes) / 1e6 /
                                   (static_cast<double>(ov_ms) / 1000.0)
                             : 0.0;

        auto print_row = [&](const char *path, int64_t ms, double mbps)
        {
            std::cout << std::left << std::setw(8) << c.label << std::setw(20)
                      << path << std::setw(14) << ms << std::fixed
                      << std::setprecision(1) << mbps << " MB/s\n";
        };

        print_row("IoStringBuffer", lv_ms, lv_mbps);
        print_row("std::string", ov_ms, ov_mbps);
    }

    std::cout << std::string(56, '-') << "\n";

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
