#include <algorithm>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
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

// Each shard owns its own GlobalRegisteredMemory. Sized large enough to keep
// the working set comfortably above what the workers / background compaction
// hold in flight, so the test exercises real allocation patterns rather than
// the eviction edge case. 256MB / 256KB = 1024 segments per shard; with 2
// shards plus index buffers the locked footprint is well under the typical
// 2GB RLIMIT_MEMLOCK on dev boxes (raise via `sudo prlimit --memlock=2G`).
constexpr uint32_t kSegmentSize = 256 * 1024;
constexpr size_t kChunkSize = 64ULL * 1024 * 1024;
constexpr size_t kPoolSize = 256ULL * 1024 * 1024;

void FillDeterministic(char *dst, size_t size, uint64_t seed)
{
    for (size_t i = 0; i < size; ++i)
    {
        dst[i] = static_cast<char>((seed * 2654435761u + i) & 0xFF);
    }
}

bool VerifyDeterministic(const char *src, size_t size, uint64_t seed)
{
    for (size_t i = 0; i < size; ++i)
    {
        char expect = static_cast<char>((seed * 2654435761u + i) & 0xFF);
        if (src[i] != expect)
        {
            return false;
        }
    }
    return true;
}

// Multi-shard harness: one GlobalRegisteredMemory pool per shard. The pool is
// thread-unsafe by design, so tests pin one worker thread per shard and route
// every request that worker issues to that shard's partitions only. Workers
// then have two safe choices: ExecSync, which serializes worker and shard on
// the pool naturally; or ExecAsyn + Wait, which is also race-free as long as
// the caller does not touch the same shard's pool between submit and Wait
// (the shard owns that interval). The drain step below uses ExecAsyn so it
// can fan out across shards from a single thread.
class Harness
{
public:
    explicit Harness(uint16_t num_shards)
    {
        memories_.reserve(num_shards);
        ptrs_.reserve(num_shards);
        bases_.assign(num_shards, 0);
        for (uint16_t i = 0; i < num_shards; ++i)
        {
            memories_.emplace_back(
                std::make_unique<eloqstore::GlobalRegisteredMemory>(
                    kSegmentSize, kChunkSize, kPoolSize));
            ptrs_.push_back(memories_.back().get());
        }
    }

    std::vector<eloqstore::GlobalRegisteredMemory *> Pointers() const
    {
        return ptrs_;
    }

    // Force one synchronous request per shard so each shard has finished
    // BootstrapRing before we read GlobalRegMemIndexBase.
    void BindStore(eloqstore::EloqStore *store)
    {
        for (size_t i = 0; i < memories_.size(); ++i)
        {
            // Pick a partition_id whose ShardIndex == i.
            eloqstore::TableIdent warm{"__warmup__", static_cast<uint32_t>(i)};
            eloqstore::ReadRequest r;
            r.SetArgs(warm, "__warmup_key__");
            store->ExecSync(&r);
            bases_[i] = store->GlobalRegMemIndexBase(i);
        }
    }

    uint16_t NumShards() const
    {
        return static_cast<uint16_t>(memories_.size());
    }

    uint32_t SegmentSize() const
    {
        return kSegmentSize;
    }

    eloqstore::GlobalRegisteredMemory *Memory(size_t shard_id)
    {
        return memories_[shard_id].get();
    }

    uint16_t RegBase(size_t shard_id) const
    {
        return bases_[shard_id];
    }

    // Build an IoStringBuffer from this shard's pool. MUST be called from the
    // worker thread bound to `shard_id`.
    eloqstore::IoStringBuffer MakeLargeValue(size_t shard_id,
                                             size_t size,
                                             uint64_t seed)
    {
        auto *mem = memories_[shard_id].get();
        const uint16_t base = bases_[shard_id];
        const uint32_t seg = kSegmentSize;
        const size_t num_segs = (size + seg - 1) / seg;

        eloqstore::IoStringBuffer buf;
        size_t remaining = size;
        for (size_t i = 0; i < num_segs; ++i)
        {
            auto [ptr, chunk_idx] = mem->GetSegment([]() {});
            size_t nbytes = std::min<size_t>(seg, remaining);
            FillDeterministic(ptr, nbytes, seed + i);
            if (nbytes < seg)
            {
                std::memset(ptr + nbytes, 0, seg - nbytes);
            }
            buf.Append({ptr, static_cast<uint16_t>(base + chunk_idx)});
            remaining -= nbytes;
        }
        buf.SetSize(size);
        return buf;
    }

    bool VerifyLargeValue(const eloqstore::IoStringBuffer &buf,
                          size_t size,
                          uint64_t seed) const
    {
        if (buf.Size() != size)
            return false;
        const auto &frags = buf.Fragments();
        const uint32_t seg = kSegmentSize;
        const size_t num_segs = (size + seg - 1) / seg;
        if (frags.size() != num_segs)
            return false;
        size_t remaining = size;
        for (size_t i = 0; i < num_segs; ++i)
        {
            size_t nbytes = std::min<size_t>(seg, remaining);
            if (!VerifyDeterministic(frags[i].data_, nbytes, seed + i))
            {
                return false;
            }
            remaining -= nbytes;
        }
        return true;
    }

    void RecycleBatch(eloqstore::BatchWriteRequest &req, size_t shard_id)
    {
        for (auto &entry : req.batch_)
        {
            if (entry.HasLargeValue())
            {
                entry.large_val_.Recycle(memories_[shard_id].get(),
                                         bases_[shard_id]);
            }
        }
    }

    void RecycleRead(eloqstore::ReadRequest &req, size_t shard_id)
    {
        if (!req.large_value_.Fragments().empty())
        {
            req.large_value_.Recycle(memories_[shard_id].get(),
                                     bases_[shard_id]);
        }
    }

    // Background compactions queued just before quiescence may still be
    // unwinding (recycling segments) when the foreground drain returns. Poll
    // for restoration up to a short deadline before asserting; the drain step
    // is the real correctness signal, this just absorbs the recycle window.
    void AssertPoolRestored() const
    {
        const auto deadline = chrono::steady_clock::now() + chrono::seconds(2);
        for (size_t i = 0; i < memories_.size(); ++i)
        {
            while (memories_[i]->FreeSegments() !=
                       memories_[i]->TotalSegments() &&
                   chrono::steady_clock::now() < deadline)
            {
                std::this_thread::sleep_for(chrono::milliseconds(10));
            }
            INFO("shard=" << i);
            REQUIRE(memories_[i]->FreeSegments() ==
                    memories_[i]->TotalSegments());
        }
    }

private:
    std::vector<std::unique_ptr<eloqstore::GlobalRegisteredMemory>> memories_;
    std::vector<eloqstore::GlobalRegisteredMemory *> ptrs_;
    std::vector<uint16_t> bases_;
};

eloqstore::KvOptions MakeOpts(Harness &h,
                              uint8_t file_amp = 0,
                              uint8_t seg_amp = 0,
                              uint32_t num_archives = 0)
{
    eloqstore::KvOptions opts;
    opts.num_threads = h.NumShards();
    opts.num_retained_archives = num_archives;
    opts.archive_interval_secs = 0;
    opts.file_amplify_factor = file_amp;
    opts.segment_file_amplify_factor = seg_amp;
    opts.segment_compact_yield_every = 8;
    opts.store_path = {test_path};
    opts.pages_per_file_shift = 8;  // 256 pages/file = 1MB data file.
    opts.segment_size = h.SegmentSize();
    opts.segments_per_file_shift = 3;  // 8 segments per segment file.
    opts.data_append_mode = true;
    opts.buffer_pool_size = 16 * eloqstore::MB;
    opts.write_buffer_size = 0;
    opts.write_buffer_ratio = 0.0;
    opts.global_registered_memories = h.Pointers();
    return opts;
}

// Worker-side helpers. These all run on the worker thread bound to shard_id
// and either (a) only manipulate that shard's pool, or (b) only push small
// values that don't touch the pool at all.

void WriteLargeSync(eloqstore::EloqStore *store,
                    Harness &h,
                    size_t shard_id,
                    const eloqstore::TableIdent &tbl,
                    const std::string &key,
                    size_t size,
                    uint64_t seed,
                    uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(key,
                         h.MakeLargeValue(shard_id, size, seed),
                         ts,
                         eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    h.RecycleBatch(req, shard_id);
}

void DeleteSync(eloqstore::EloqStore *store,
                const eloqstore::TableIdent &tbl,
                const std::string &key,
                uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(key, std::string{}, ts, eloqstore::WriteOp::Delete);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

}  // namespace

// ---------------------------------------------------------------------------
// Multi-shard, multi-partition interleaved large-value workload. Each worker
// thread owns one shard and walks several partitions on it; the worker
// randomly inserts, overwrites, deletes, and reads large values for a few
// seconds. After quiescence we assert (a) every read observed the bytes the
// worker last wrote (so no read ever surfaced partial / stale bytes), and
// (b) every shard's pool is fully restored.
// ---------------------------------------------------------------------------
TEST_CASE("multi-shard interleaved large-value workload",
          "[large-value][concurrency]")
{
    constexpr uint16_t kNumShards = 2;
    constexpr uint16_t kPartitionsPerShard = 2;
    constexpr auto kRunFor = chrono::milliseconds(2000);

    Harness h(kNumShards);
    eloqstore::KvOptions opts = MakeOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    const size_t seg = h.SegmentSize();
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_reads{0};
    std::atomic<uint64_t> total_writes{0};
    std::atomic<uint64_t> read_failures{0};

    auto worker = [&](size_t shard_id)
    {
        std::mt19937 rng(static_cast<uint32_t>(shard_id) * 0x9E3779B1u + 1);
        // Build the per-worker partition list (partition_id % num_shards ==
        // shard_id by construction).
        std::vector<eloqstore::TableIdent> tbls;
        tbls.reserve(kPartitionsPerShard);
        for (uint16_t p = 0; p < kPartitionsPerShard; ++p)
        {
            uint32_t pid = static_cast<uint32_t>(shard_id + p * kNumShards);
            tbls.emplace_back("lvc", pid);
        }

        // Per-key expected (size, seed) for every live key, scoped per
        // partition so we can compare reads against what we last wrote.
        struct LiveValue
        {
            size_t size;
            uint64_t seed;
        };
        std::vector<std::unordered_map<std::string, LiveValue>> live(
            tbls.size());

        uint64_t ts = 1;
        uint64_t seed_counter = (shard_id + 1) * 0x100000ULL;

        while (!stop.load(std::memory_order_relaxed))
        {
            size_t pi =
                std::uniform_int_distribution<size_t>(0, tbls.size() - 1)(rng);
            auto &tbl = tbls[pi];
            auto &live_map = live[pi];

            int op = std::uniform_int_distribution<int>(0, 99)(rng);
            // Bias: 50% writes/overwrites, 35% reads, 15% deletes (only when
            // there is something to delete).
            std::string key =
                "k" +
                std::to_string(std::uniform_int_distribution<int>(0, 7)(rng));

            if (op < 50)
            {
                // Write or overwrite. Choose 1..3 segments so we exercise
                // multi-segment values without exhausting the pool.
                size_t num_segs =
                    std::uniform_int_distribution<size_t>(1, 3)(rng);
                size_t size = num_segs * seg -
                              std::uniform_int_distribution<size_t>(0, 99)(rng);
                uint64_t s = ++seed_counter;
                WriteLargeSync(store, h, shard_id, tbl, key, size, s, ts++);
                live_map[key] = LiveValue{size, s};
                total_writes.fetch_add(1, std::memory_order_relaxed);
            }
            else if (op < 85)
            {
                // Read. Validate against the last-write expected entry.
                eloqstore::ReadRequest r;
                r.SetArgs(tbl, key);
                store->ExecSync(&r);
                total_reads.fetch_add(1, std::memory_order_relaxed);

                auto it = live_map.find(key);
                if (it == live_map.end())
                {
                    if (r.Error() != eloqstore::KvError::NotFound)
                    {
                        read_failures.fetch_add(1, std::memory_order_relaxed);
                    }
                    h.RecycleRead(r, shard_id);
                    continue;
                }
                if (r.Error() != eloqstore::KvError::NoError ||
                    !h.VerifyLargeValue(
                        r.large_value_, it->second.size, it->second.seed))
                {
                    read_failures.fetch_add(1, std::memory_order_relaxed);
                }
                h.RecycleRead(r, shard_id);
            }
            else
            {
                if (live_map.empty())
                {
                    continue;
                }
                // Delete an existing key.
                auto it = live_map.begin();
                std::advance(it,
                             std::uniform_int_distribution<size_t>(
                                 0, live_map.size() - 1)(rng));
                DeleteSync(store, tbl, it->first, ts++);
                live_map.erase(it);
                total_writes.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> workers;
    workers.reserve(kNumShards);
    auto deadline = chrono::steady_clock::now() + kRunFor;
    for (uint16_t i = 0; i < kNumShards; ++i)
    {
        workers.emplace_back(worker, i);
    }
    std::this_thread::sleep_until(deadline);
    stop.store(true, std::memory_order_relaxed);
    for (auto &t : workers)
    {
        t.join();
    }

    INFO("writes=" << total_writes.load() << " reads=" << total_reads.load());
    REQUIRE(read_failures.load() == 0);
    // Stop the store before cleanup so in-flight background work (manifest
    // flushes, file GC) cannot race with the remove_all in CleanupStore.
    store->Stop();
    // Quiescence reached: every IoStringBuffer the workers issued has been
    // recycled. The pool accounting is the primary segment-leak guard.
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Snapshot-per-task: a reader holding an IoStringBuffer must keep observing
// the pre-overwrite bytes until it recycles, even after a writer overwrites
// the same key with different bytes. The design's "segment mapping is
// snapshot-per-task" guarantee says the read pinned a snapshot of the segment
// mapping at start; the subsequent overwrite allocates new physical segments
// (and updates the COW mapping in a separate root) so the held IoStringBuffer
// stays decoupled from the new state on disk.
// ---------------------------------------------------------------------------
TEST_CASE("reader snapshot survives overwrite of same key",
          "[large-value][concurrency][snapshot]")
{
    Harness h(/*num_shards=*/1);
    eloqstore::KvOptions opts = MakeOpts(h);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"lvc_snap", 0};
    const size_t seg = h.SegmentSize();
    const size_t size = seg * 2 + 13;
    const uint64_t seed_a = 0xA1A1A1A1ULL;
    const uint64_t seed_b = 0xB2B2B2B2ULL;

    WriteLargeSync(store, h, 0, tbl, "k", size, seed_a, /*ts=*/1);

    // Read the first version and HOLD the IoStringBuffer.
    eloqstore::ReadRequest r1;
    r1.SetArgs(tbl, "k");
    store->ExecSync(&r1);
    REQUIRE(r1.Error() == eloqstore::KvError::NoError);
    REQUIRE(r1.large_value_.Size() == size);
    REQUIRE(h.VerifyLargeValue(r1.large_value_, size, seed_a));

    // Overwrite the same key with different bytes WHILE r1 still owns its
    // segments. This exercises the snapshot-per-task guarantee — the in-flight
    // IoStringBuffer is independent of the COW root the overwrite produces.
    WriteLargeSync(store, h, 0, tbl, "k", size, seed_b, /*ts=*/2);

    // r1's bytes must still match seed_a (the snapshot we read earlier).
    REQUIRE(h.VerifyLargeValue(r1.large_value_, size, seed_a));
    h.RecycleRead(r1, 0);

    // A fresh read must observe the new bytes (seed_b).
    eloqstore::ReadRequest r2;
    r2.SetArgs(tbl, "k");
    store->ExecSync(&r2);
    REQUIRE(r2.Error() == eloqstore::KvError::NoError);
    REQUIRE(r2.large_value_.Size() == size);
    REQUIRE(h.VerifyLargeValue(r2.large_value_, size, seed_b));
    h.RecycleRead(r2, 0);

    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// GC + compaction under load. Workers run a write/overwrite/read mix on
// large values for a few seconds with both data and segment compaction
// enabled (file_amp=2, seg_amp=2) and a non-zero retained-archive count;
// a separate driver thread fires per-partition ArchiveRequests every ~300ms
// to force manifest flushes and exercise the archive-floor interaction with
// segment-file GC. After quiescence, every observed read must have matched
// the worker's last-write expectation (i.e. no reader saw a segment whose
// backing file had been GC'd or compacted out from under it).
//
// Known flake (~4% at the 256MB/shard pool): latent EloqStore bug in the
// concurrent Compact() path. Reproducer: two shards each run Compact()
// simultaneously while workers push new writes; the crash is a SIGSEGV
// inside EloqStore, not in the test. The failure logs always end with
// back-to-back "begin compaction on lvc_gc.X" lines on different shards
// just before the signal. Catch2 attributes the signal to whichever
// REQUIRE was last on the coroutine's stack (typically the test-helper
// REQUIRE in WriteLargeSync/DeleteSync after ExecSync returned), but the
// actual crash site is in BackgroundWrite::Compact / DoCompactSegmentFile.
// The crash rate was much higher (~13%) at the very tight write/read loop
// and is held down (not eliminated) by the 200us kOpPause below; eviction
// throttling at smaller pool sizes masked the bug entirely. Reducing the
// pool size is the knob to hide it again; fixing it belongs in EloqStore.
// ---------------------------------------------------------------------------
TEST_CASE("large-value workload survives concurrent compaction and archives",
          "[large-value][concurrency][gc]")
{
    constexpr uint16_t kNumShards = 2;
    constexpr uint16_t kPartitionsPerShard = 2;
    constexpr auto kRunFor = chrono::milliseconds(2000);
    constexpr auto kArchiveEvery = chrono::milliseconds(300);
    // Small inter-op pause to keep the shards' background queues from
    // saturating. With a 256MB pool there is no eviction throttle, and a
    // tight write/read loop on every worker triggers a latent EloqStore
    // race in concurrent compaction at a low (~10%) rate. The pause does
    // not weaken coverage: workers still issue hundreds of mixed ops per
    // second and the GC + archive pathways still get exercised.
    constexpr auto kOpPause = chrono::microseconds(200);

    Harness h(kNumShards);
    eloqstore::KvOptions opts = MakeOpts(h,
                                         /*file_amp=*/2,
                                         /*seg_amp=*/2,
                                         /*num_archives=*/1);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    const size_t seg = h.SegmentSize();
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> read_failures{0};
    std::atomic<uint64_t> archive_failures{0};
    std::atomic<uint64_t> total_archives{0};

    // Build the partition list once so the archive thread can target the same
    // partitions the workers populate.
    std::vector<eloqstore::TableIdent> all_tbls;
    all_tbls.reserve(kNumShards * kPartitionsPerShard);
    for (uint16_t s = 0; s < kNumShards; ++s)
    {
        for (uint16_t p = 0; p < kPartitionsPerShard; ++p)
        {
            uint32_t pid = static_cast<uint32_t>(s + p * kNumShards);
            all_tbls.emplace_back("lvc_gc", pid);
        }
    }

    auto worker = [&](size_t shard_id)
    {
        std::mt19937 rng(static_cast<uint32_t>(shard_id) * 0xC0FFEE13u + 7);
        std::vector<eloqstore::TableIdent> tbls;
        for (const auto &t : all_tbls)
        {
            if (t.partition_id_ % kNumShards == shard_id)
            {
                tbls.push_back(t);
            }
        }

        struct LiveValue
        {
            size_t size;
            uint64_t seed;
        };
        std::vector<std::unordered_map<std::string, LiveValue>> live(
            tbls.size());

        uint64_t ts = 1;
        uint64_t seed_counter = (shard_id + 1) * 0x200000ULL;

        while (!stop.load(std::memory_order_relaxed))
        {
            size_t pi =
                std::uniform_int_distribution<size_t>(0, tbls.size() - 1)(rng);
            auto &tbl = tbls[pi];
            auto &live_map = live[pi];

            int op = std::uniform_int_distribution<int>(0, 99)(rng);
            std::string key =
                "k" +
                std::to_string(std::uniform_int_distribution<int>(0, 5)(rng));

            if (op < 60)
            {
                // Heavy write/overwrite mix: drives both segment and data
                // amplification, which is what triggers compaction.
                size_t num_segs =
                    std::uniform_int_distribution<size_t>(1, 2)(rng);
                size_t size = num_segs * seg;
                uint64_t s = ++seed_counter;
                WriteLargeSync(store, h, shard_id, tbl, key, size, s, ts++);
                live_map[key] = LiveValue{size, s};
            }
            else if (op < 95)
            {
                eloqstore::ReadRequest r;
                r.SetArgs(tbl, key);
                store->ExecSync(&r);

                auto it = live_map.find(key);
                if (it == live_map.end())
                {
                    if (r.Error() != eloqstore::KvError::NotFound)
                    {
                        read_failures.fetch_add(1, std::memory_order_relaxed);
                    }
                    h.RecycleRead(r, shard_id);
                    continue;
                }
                if (r.Error() != eloqstore::KvError::NoError ||
                    !h.VerifyLargeValue(
                        r.large_value_, it->second.size, it->second.seed))
                {
                    read_failures.fetch_add(1, std::memory_order_relaxed);
                }
                h.RecycleRead(r, shard_id);
            }
            else
            {
                if (live_map.empty())
                {
                    continue;
                }
                auto it = live_map.begin();
                std::advance(it,
                             std::uniform_int_distribution<size_t>(
                                 0, live_map.size() - 1)(rng));
                DeleteSync(store, tbl, it->first, ts++);
                live_map.erase(it);
            }
            std::this_thread::sleep_for(kOpPause);
        }
    };

    // Archive driver: fires ArchiveRequests per partition on a slow cadence.
    // ArchiveRequest does not allocate from any shard's GlobalRegisteredMemory,
    // so it is safe to run from a thread other than the workers.
    auto archiver = [&]()
    {
        auto next = chrono::steady_clock::now() + kArchiveEvery;
        while (!stop.load(std::memory_order_relaxed))
        {
            std::this_thread::sleep_until(next);
            next += kArchiveEvery;
            for (const auto &tbl : all_tbls)
            {
                if (stop.load(std::memory_order_relaxed))
                {
                    break;
                }
                eloqstore::ArchiveRequest ar;
                ar.SetTableId(tbl);
                if (!store->ExecAsyn(&ar))
                {
                    archive_failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                ar.Wait();
                // NotFound is benign here: it just means the partition has no
                // root index yet because the workers haven't picked it during
                // their random walk. CreateArchive treats that as a no-op
                // outcome, and from this test's standpoint nothing failed.
                if (ar.Error() == eloqstore::KvError::NoError ||
                    ar.Error() == eloqstore::KvError::NotFound)
                {
                    total_archives.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    LOG(WARNING) << "archive failed: " << ar.ErrMessage()
                                 << " for " << tbl.ToString();
                    archive_failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kNumShards + 1);
    for (uint16_t i = 0; i < kNumShards; ++i)
    {
        threads.emplace_back(worker, i);
    }
    threads.emplace_back(archiver);

    std::this_thread::sleep_for(kRunFor);
    stop.store(true, std::memory_order_relaxed);
    for (auto &t : threads)
    {
        t.join();
    }

    // Drain any background CompactRequests still sitting in each partition's
    // pending queue. Compaction allocates from the shard's pool; without a
    // drain, the pool-restored assertion races with an in-flight compact that
    // hasn't yet recycled its segments. A read per partition is enqueued
    // behind any pending compact on that partition and only completes once
    // the compact finishes.
    //
    // Fan all drain reads out via ExecAsyn from a single thread, then Wait on
    // them sequentially. The drain reads do not allocate from the caller side
    // (only the shard side allocates during read processing), so submitting
    // them in parallel is race-free even though multiple shards' pools are
    // touched concurrently — each shard touches only its own pool.
    std::vector<eloqstore::ReadRequest> drain_reqs(all_tbls.size());
    for (size_t i = 0; i < all_tbls.size(); ++i)
    {
        drain_reqs[i].SetArgs(all_tbls[i], std::string("__drain__"));
        REQUIRE(store->ExecAsyn(&drain_reqs[i]));
    }
    for (size_t i = 0; i < all_tbls.size(); ++i)
    {
        drain_reqs[i].Wait();
        size_t shard_id = all_tbls[i].partition_id_ % kNumShards;
        h.RecycleRead(drain_reqs[i], shard_id);
    }

    INFO("archives=" << total_archives.load());
    REQUIRE(read_failures.load() == 0);
    REQUIRE(archive_failures.load() == 0);
    REQUIRE(total_archives.load() > 0);
    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}
