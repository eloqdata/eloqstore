#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../include/common.h"
#include "../include/eloq_store.h"
#include "../include/error.h"
#include "../include/global_registered_memory.h"
#include "../include/io_string_buffer.h"
#include "../include/kv_options.h"
#include "../include/types.h"
#include "common.h"

namespace fs = std::filesystem;
namespace chrono = std::chrono;

namespace
{
// Tests below configure small files so compaction is reachable quickly:
// - segment_size = 256KB, segments_per_file_shift = 3 -> 8 segments per file.
// - pages_per_file_shift = 8  -> 256 pages per data file (1MB).
// - global_registered_memories: one 256MB pool per shard (1024 segments),
//   well above any working set these tests touch so eviction is never on the
//   critical path.
constexpr uint32_t kSegmentSize = 256 * 1024;
constexpr size_t kChunkSize = 64ULL * 1024 * 1024;
constexpr size_t kTotalSize = 256ULL * 1024 * 1024;

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

class Harness
{
public:
    Harness()
    {
        mem_ = std::make_unique<eloqstore::GlobalRegisteredMemory>(
            kSegmentSize, kChunkSize, kTotalSize);
    }

    std::vector<eloqstore::GlobalRegisteredMemory *> Pointers() const
    {
        return {mem_.get()};
    }

    void BindStore(eloqstore::EloqStore *store)
    {
        // Force a round-trip through shard 0 so that BootstrapRing has
        // completed and GlobalRegMemIndexBase is valid.
        eloqstore::ReadRequest r;
        r.SetArgs(eloqstore::TableIdent{"__warmup__", 0}, "__warmup_key__");
        store->ExecSync(&r);
        base_ = store->GlobalRegMemIndexBase(0);
    }

    eloqstore::GlobalRegisteredMemory *Memory()
    {
        return mem_.get();
    }
    uint16_t RegBase() const
    {
        return base_;
    }
    uint32_t SegmentSize() const
    {
        return kSegmentSize;
    }

    eloqstore::IoStringBuffer MakeLargeValue(size_t size, uint64_t seed)
    {
        const uint32_t seg = kSegmentSize;
        const size_t num_segs = (size + seg - 1) / seg;
        eloqstore::IoStringBuffer buf;
        size_t remaining = size;
        for (size_t i = 0; i < num_segs; ++i)
        {
            auto [ptr, chunk_idx] = mem_->GetSegment([]() {});
            size_t nbytes = std::min<size_t>(seg, remaining);
            FillDeterministic(ptr, nbytes, seed + i);
            if (nbytes < seg)
            {
                std::memset(ptr + nbytes, 0, seg - nbytes);
            }
            buf.Append({ptr, static_cast<uint16_t>(base_ + chunk_idx)});
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

    void RecycleBatch(eloqstore::BatchWriteRequest &req)
    {
        for (auto &entry : req.batch_)
        {
            entry.RecycleLargeValue(mem_.get(), base_);
        }
    }

    void RecycleRead(eloqstore::ReadRequest &req)
    {
        if (auto *iosb =
                std::get_if<eloqstore::IoStringBuffer>(&req.large_value_dest_);
            iosb != nullptr && !iosb->Fragments().empty())
        {
            iosb->Recycle(mem_.get(), base_);
        }
    }

    void AssertPoolRestored() const
    {
        REQUIRE(mem_->FreeSegments() == mem_->TotalSegments());
    }

private:
    std::unique_ptr<eloqstore::GlobalRegisteredMemory> mem_;
    uint16_t base_{0};
};

eloqstore::KvOptions MakeOpts(Harness &h,
                              uint8_t file_amp = 2,
                              uint8_t seg_amp = 2,
                              uint32_t num_archives = 0)
{
    eloqstore::KvOptions opts;
    opts.num_threads = 1;
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

void WriteLarge(eloqstore::EloqStore *store,
                Harness &h,
                const eloqstore::TableIdent &tbl,
                const std::string &key,
                size_t size,
                uint64_t seed,
                uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        key, h.MakeLargeValue(size, seed), ts, eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    h.RecycleBatch(req);
}

void WriteLargeBatch(eloqstore::EloqStore *store,
                     Harness &h,
                     const eloqstore::TableIdent &tbl,
                     const std::vector<std::pair<std::string, size_t>> &keys,
                     uint64_t seed_base,
                     uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        entries.emplace_back(keys[i].first,
                             h.MakeLargeValue(keys[i].second, seed_base + i),
                             ts,
                             eloqstore::WriteOp::Upsert);
    }
    std::sort(entries.begin(), entries.end());
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    h.RecycleBatch(req);
}

void WriteSmall(eloqstore::EloqStore *store,
                const eloqstore::TableIdent &tbl,
                const std::string &key,
                const std::string &value,
                uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(key, value, ts, eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
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
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

struct SegmentFileInfo
{
    size_t count{0};
    eloqstore::FileId max_id{0};
    bool any{false};
};

SegmentFileInfo InspectSegmentFiles(const eloqstore::KvOptions &opts,
                                    const eloqstore::TableIdent &tbl)
{
    SegmentFileInfo info;
    if (opts.store_path.empty())
    {
        return info;
    }
    fs::path dir = fs::path(opts.store_path[0]) / tbl.ToString();
    if (!fs::exists(dir))
    {
        return info;
    }
    for (const auto &entry : fs::directory_iterator(dir))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        std::string fname = entry.path().filename().string();
        auto [type, suffix] = eloqstore::ParseFileName(fname);
        if (type != eloqstore::FileNameSegment)
        {
            continue;
        }
        eloqstore::FileId fid = 0;
        std::string_view branch_name;
        uint64_t term = 0;
        if (!eloqstore::ParseSegmentFileSuffix(suffix, fid, branch_name, term))
        {
            continue;
        }
        ++info.count;
        if (!info.any || fid > info.max_id)
        {
            info.max_id = fid;
        }
        info.any = true;
    }
    return info;
}

template <typename Pred>
bool WaitForCondition(chrono::milliseconds timeout,
                      chrono::milliseconds step,
                      Pred &&pred)
{
    auto deadline = chrono::steady_clock::now() + timeout;
    while (chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }
        std::this_thread::sleep_for(step);
    }
    return pred();
}

void ReadAndVerify(eloqstore::EloqStore *store,
                   Harness &h,
                   const eloqstore::TableIdent &tbl,
                   const std::string &key,
                   size_t size,
                   uint64_t seed)
{
    eloqstore::ReadRequest r;
    r.SetArgs(tbl, key);
    r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
    auto &iosb = std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
    store->ExecSync(&r);
    REQUIRE(r.Error() == eloqstore::KvError::NoError);
    REQUIRE(r.value_.empty());
    REQUIRE(iosb.Size() == size);
    REQUIRE(h.VerifyLargeValue(iosb, size, seed));
    h.RecycleRead(r);
}

}  // namespace

// ---------------------------------------------------------------------------
// Skip rule: segment_file_amplify_factor == 0 disables segment compaction.
// Intent: confirm that overwriting large values to the point where the
// segment mapping is heavily amplified does NOT cause segment-file rewrites
// when the knob is zero, even though the data allocator's compaction knob is
// still non-zero.
// ---------------------------------------------------------------------------
TEST_CASE("segment compaction is disabled when seg amplify factor is zero",
          "[segment-compact][skip-rules]")
{
    Harness h;
    // Data compaction stays on (file_amp=2) to show the two knobs are
    // independent; segment compaction is disabled via seg_amp=0.
    eloqstore::KvOptions opts = MakeOpts(h, /*file_amp=*/2, /*seg_amp=*/0);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"sc_skip_zero", 0};
    const size_t seg = h.SegmentSize();
    // 4 keys, one segment each -> all live segments fit in a single segment
    // file at a time. Overwrites cause the physical allocator to roll forward,
    // building up space amplification on the segment mapping.
    std::vector<std::pair<std::string, size_t>> keys = {
        {"k1", seg}, {"k2", seg}, {"k3", seg}, {"k4", seg}};
    WriteLargeBatch(store, h, tbl, keys, /*seed_base=*/0x1000, /*ts=*/1);

    // Drive enough overwrites on a subset that the segment allocator's
    // space / live ratio would exceed factor=2. Enough rounds to walk past
    // segments_per_file=8 so amplification is definitely detectable.
    for (int round = 0; round < 6; ++round)
    {
        WriteLargeBatch(store,
                        h,
                        tbl,
                        {{"k1", seg}, {"k2", seg}},
                        /*seed_base=*/0x2000 + round * 10,
                        /*ts=*/2 + round);
    }
    // Final batch to ensure any pending compaction signal would be picked up.
    WriteLargeBatch(store,
                    h,
                    tbl,
                    {{"k5", seg}},
                    /*seed_base=*/0x3000,
                    /*ts=*/100);
    // No wait: seg_amp=0 makes MapperExceedsAmplification short-circuit,
    // so CompactIfNeeded never schedules a CompactRequest. ExecSync has
    // already committed every write; on-disk segment file state is stable.

    // Every live value still reads back correctly.
    ReadAndVerify(store, h, tbl, "k1", seg, 0x2000 + 5 * 10);  // last overwrite
    ReadAndVerify(store, h, tbl, "k2", seg, 0x2000 + 5 * 10 + 1);
    ReadAndVerify(store, h, tbl, "k3", seg, 0x1002);
    ReadAndVerify(store, h, tbl, "k4", seg, 0x1003);
    ReadAndVerify(store, h, tbl, "k5", seg, 0x3000);

    // A segment-compaction rewrite would advance the max segment file ID well
    // beyond what plain appends produce (because Compact rewrites live
    // segments to a fresh tail). With seg_amp=0 the background never compacts
    // segments, so the on-disk max file ID should equal the allocator's tail
    // from monotonic appends alone.
    SegmentFileInfo info = InspectSegmentFiles(opts, tbl);
    REQUIRE(info.any);
    // Total segments allocated: 4 (batch1) + 2*6 (overwrites) + 1 (k5) = 17
    // segments -> files 0..2 (3 files, highest id = 2).
    REQUIRE(info.max_id == 2);

    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Skip rule: segment_mapper_ == nullptr (partition never wrote a large value).
// Intent: confirm Compact() is a safe no-op on partitions that never
// materialized a segment mapper, even when the data mapper triggers
// compaction via file_amplify_factor.
// ---------------------------------------------------------------------------
TEST_CASE("compact is safe on small-only partitions with no segment mapper",
          "[segment-compact][skip-rules]")
{
    Harness h;
    eloqstore::KvOptions opts = MakeOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    // Make each small value large enough to fill a data page so that
    // overwrites quickly exceed the data-mapper amplification factor and
    // trigger a compaction pass whose segment half must no-op.
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"sc_small_only", 0};
    std::string value(3500, 'x');  // near-full data page
    for (int i = 0; i < 32; ++i)
    {
        WriteSmall(store, tbl, "k" + std::to_string(i), value, /*ts=*/1);
    }
    // Overwrite enough to force the data mapper's space / live ratio > 2.
    for (int round = 0; round < 4; ++round)
    {
        for (int i = 0; i < 32; ++i)
        {
            WriteSmall(store,
                       tbl,
                       "k" + std::to_string(i),
                       value,
                       /*ts=*/2 + round);
        }
    }
    // No wait: the partition has no segment mapper, so segment compaction
    // can never run. Data-side compaction may run as a side effect of the
    // amplification check, but it does not affect the segment file
    // assertion below.

    // Reads must still succeed — segment path is never touched because the
    // segment mapper was never created.
    for (int i = 0; i < 32; ++i)
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k" + std::to_string(i));
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == value);
    }
    // No segment files should exist on disk for a small-only partition.
    SegmentFileInfo info = InspectSegmentFiles(opts, tbl);
    REQUIRE_FALSE(info.any);

    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Positive path: overwrite a working set enough to cross
// segment_file_amplify_factor, let the pending-compact dispatcher run, and
// verify (a) every live value still reads back byte-identically and (b) the
// on-disk max segment file id advances past the append-only tail (i.e. a
// tail rewrite happened).
// ---------------------------------------------------------------------------
TEST_CASE("segment compaction reclaims space and preserves bytes",
          "[segment-compact]")
{
    Harness h;
    eloqstore::KvOptions opts = MakeOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"sc_reclaim", 0};
    const size_t seg = h.SegmentSize();

    // Seed: four 1-segment keys fit into a single 8-segment file along with
    // four unused slots (next batches will fill those and then roll over).
    std::vector<std::pair<std::string, size_t>> seed_keys = {
        {"k1", seg}, {"k2", seg}, {"k3", seg}, {"k4", seg}};
    WriteLargeBatch(store, h, tbl, seed_keys, /*seed_base=*/0x4000, /*ts=*/1);

    // Overwrite k1+k2 repeatedly. Each round pushes two more physical
    // segments past the live frontier; eventually space_size / mapping_count
    // exceeds 2 and the file has a factor > 2 (live count shrinks). 10
    // rounds is a comfortable buffer beyond the crossover.
    uint64_t last_overwrite_seed = 0;
    for (int round = 0; round < 10; ++round)
    {
        uint64_t base_seed = 0x5000 + static_cast<uint64_t>(round) * 100;
        WriteLargeBatch(store,
                        h,
                        tbl,
                        {{"k1", seg}, {"k2", seg}},
                        base_seed,
                        /*ts=*/2 + round);
        last_overwrite_seed = base_seed;
    }
    // Final batch to pump the queue and ensure the pending-compact request,
    // scheduled in-flight by the last UpdateMeta, gets popped and executed.
    WriteLargeBatch(store,
                    h,
                    tbl,
                    {{"k5", seg}},
                    /*seed_base=*/0x6000,
                    /*ts=*/200);
    REQUIRE(WaitForCondition(
        chrono::seconds(3),
        chrono::milliseconds(20),
        [&]() { return InspectSegmentFiles(opts, tbl).count <= 2; }));

    // Every live value still reads back correctly.
    ReadAndVerify(store, h, tbl, "k1", seg, last_overwrite_seed + 0);
    ReadAndVerify(store, h, tbl, "k2", seg, last_overwrite_seed + 1);
    ReadAndVerify(store, h, tbl, "k3", seg, 0x4002);
    ReadAndVerify(store, h, tbl, "k4", seg, 0x4003);
    ReadAndVerify(store, h, tbl, "k5", seg, 0x6000);

    // Only 5 live segments remain (k1..k5). Without segment compaction those
    // stay scattered across whichever physical file their last allocation
    // happened to land in (see the seg_amp=0 sibling test — 3 distinct files
    // survive). With compaction active, live segments are packed into the
    // tail, leaving at most one or two files on disk.
    SegmentFileInfo info = InspectSegmentFiles(opts, tbl);
    REQUIRE(info.any);
    REQUIRE(info.count <= 2);

    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Empty-mapper / non-empty-space path: after every large value is deleted,
// the segment mapping is empty but the allocator still has non-zero space.
// MapperExceedsAmplification treats this as "exceeds" (mapping_cnt==0 and
// space_size>0) and Compact() should retire the empty segment files via
// UpdateStat + TriggerFileGC. Nothing is rewritten.
// ---------------------------------------------------------------------------
TEST_CASE("empty segment mapping retires all segment files",
          "[segment-compact][empty]")
{
    Harness h;
    eloqstore::KvOptions opts = MakeOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"sc_empty", 0};
    const size_t seg = h.SegmentSize();

    WriteLargeBatch(store,
                    h,
                    tbl,
                    {{"k1", seg}, {"k2", seg}, {"k3", seg}},
                    /*seed_base=*/0x7000,
                    /*ts=*/1);

    SegmentFileInfo before = InspectSegmentFiles(opts, tbl);
    REQUIRE(before.any);

    // Delete every large value, then force another BatchWrite so the
    // resulting UpdateMeta notices the segment mapper overflowed (empty
    // mapping, non-empty space) and schedules a compaction.
    for (const char *k : {"k1", "k2", "k3"})
    {
        DeleteKey(store, tbl, k, /*ts=*/10);
    }
    // Nudge a fresh BatchWrite through UpdateMeta to trigger CompactIfNeeded.
    WriteSmall(store, tbl, "nudge", "n", /*ts=*/20);
    REQUIRE(WaitForCondition(chrono::seconds(3),
                             chrono::milliseconds(20),
                             [&]()
                             { return !InspectSegmentFiles(opts, tbl).any; }));

    SegmentFileInfo after = InspectSegmentFiles(opts, tbl);
    REQUIRE_FALSE(after.any);

    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// CreateArchive gating: when neither mapper exceeds its amplification
// threshold, the archive path must NOT invoke Compact(). Observed via the
// max segment file id on disk — a compaction rewrite moves the tail forward
// beyond what plain appends produce, so if the archive side fired an
// uncalled-for Compact we would see a higher max id than the monotonic
// tail. (num_retained_archives > 0 so the archive actually runs; seg_amp=2
// so the threshold is well above what a single clean batch produces.)
// ---------------------------------------------------------------------------
TEST_CASE("create archive does not compact below threshold",
          "[segment-compact][archive-gate]")
{
    Harness h;
    eloqstore::KvOptions opts = MakeOpts(h,
                                         /*file_amp=*/2,
                                         /*seg_amp=*/2,
                                         /*num_archives=*/1);
    eloqstore::EloqStore *store = InitStore(opts);
    h.BindStore(store);

    eloqstore::TableIdent tbl{"sc_archive_gate", 0};
    const size_t seg = h.SegmentSize();

    // A single clean batch: 3 live segments, 3 allocations -> factor 1.
    WriteLargeBatch(store,
                    h,
                    tbl,
                    {{"k1", seg}, {"k2", seg}, {"k3", seg}},
                    /*seed_base=*/0x8000,
                    /*ts=*/1);

    SegmentFileInfo before = InspectSegmentFiles(opts, tbl);
    REQUIRE(before.any);

    eloqstore::ArchiveRequest ar;
    ar.SetTableId(tbl);
    REQUIRE(store->ExecAsyn(&ar));
    ar.Wait();
    REQUIRE(ar.Error() == eloqstore::KvError::NoError);
    // No wait: ar.Wait() returned, so CreateArchive is fully committed.
    // factor=1 < threshold=2 means CreateArchive's needs_compact check is
    // false and no inline Compact ran. Nothing else should change.

    SegmentFileInfo after = InspectSegmentFiles(opts, tbl);
    // Archive must not tail-rewrite the one live segment file. Max segment
    // file id must stay pinned at the pre-archive value. Also the file set
    // is pinned by the archive floor, so nothing disappears.
    REQUIRE(after.any);
    REQUIRE(after.max_id == before.max_id);
    REQUIRE(after.count == before.count);

    // Bytes still read back.
    ReadAndVerify(store, h, tbl, "k1", seg, 0x8000);
    ReadAndVerify(store, h, tbl, "k2", seg, 0x8001);
    ReadAndVerify(store, h, tbl, "k3", seg, 0x8002);

    store->Stop();
    h.AssertPoolRestored();
    CleanupStore(opts);
}

// ===========================================================================
// Pinned-mode compaction tests
// ===========================================================================
namespace
{
// Harness for KV Cache pinned-memory mode. One 4 KiB-aligned backing buffer
// is registered with EloqStore via KvOptions::pinned_memory_chunks. Writes
// allocate sub-ranges by advancing a cursor.
class PinnedHarness
{
public:
    // 64 MiB gives the test room for several seeds + 10+ overwrite rounds
    // (each at seg*2) plus per-key read buffers, with headroom for a
    // sentinel region in the back half.
    static constexpr size_t kPinnedSize = 64ULL * 1024 * 1024;

    PinnedHarness()
    {
        void *raw = nullptr;
        REQUIRE(posix_memalign(&raw, 4096, kPinnedSize) == 0);
        REQUIRE(raw != nullptr);
        std::memset(raw, 0, kPinnedSize);
        base_ = static_cast<char *>(raw);
    }

    ~PinnedHarness()
    {
        std::free(base_);
    }

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
        return kSegmentSize;
    }

    // Allocate `size` bytes at the current cursor; cursor advances by
    // ceil(size/seg)*seg so back-to-back allocations stay segment-aligned.
    std::pair<char *, size_t> AllocateSegmentAligned(size_t size)
    {
        const size_t k = (size + kSegmentSize - 1) / kSegmentSize;
        const size_t aligned = k * kSegmentSize;
        REQUIRE(cursor_ + aligned <= cursor_limit_);
        char *p = base_ + cursor_;
        cursor_ += aligned;
        return {p, size};
    }

    // Cap allocations to the front [0, limit). Used by the sentinel test
    // to keep writes from straying into the protected back region.
    void SetCursorLimit(size_t limit)
    {
        REQUIRE(limit <= kPinnedSize);
        cursor_limit_ = limit;
    }

    std::vector<std::pair<char *, size_t>> Chunks() const
    {
        return {{base_, kPinnedSize}};
    }

private:
    char *base_{nullptr};
    size_t cursor_{0};
    size_t cursor_limit_{kPinnedSize};
};

eloqstore::KvOptions MakePinnedOpts(PinnedHarness &h,
                                    uint8_t file_amp = 2,
                                    uint8_t seg_amp = 2,
                                    uint32_t num_archives = 0)
{
    eloqstore::KvOptions opts;
    opts.num_threads = 1;
    opts.num_retained_archives = num_archives;
    opts.archive_interval_secs = 0;
    opts.file_amplify_factor = file_amp;
    opts.segment_file_amplify_factor = seg_amp;
    opts.segment_compact_yield_every = 8;
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

// One-shot pinned write of a single key. Fills the destination with
// deterministic bytes seeded by `seed` and submits a BatchWriteRequest.
void WritePinned(eloqstore::EloqStore *store,
                 const eloqstore::TableIdent &tbl,
                 const std::string &key,
                 std::pair<char *, size_t> dst,
                 uint64_t seed,
                 std::string metadata = {},
                 uint64_t ts = 1)
{
    FillDeterministic(dst.first, dst.second, seed);
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
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

struct PinnedBatchEntry
{
    std::string key;
    std::pair<char *, size_t> dst;
    uint64_t seed;
    std::string metadata;
};

// Multi-entry pinned write: groups several keys into a single
// BatchWriteRequest so that UpdateMeta (and any subsequent
// CompactIfNeeded) fires once for the whole group. Matches the access
// pattern of the GlobalRegisteredMemory-mode tests' WriteLargeBatch and
// avoids fragmenting tail rewrites across many small batches.
void WritePinnedBatch(eloqstore::EloqStore *store,
                      const eloqstore::TableIdent &tbl,
                      std::vector<PinnedBatchEntry> entries_in,
                      uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.reserve(entries_in.size());
    for (auto &e : entries_in)
    {
        FillDeterministic(e.dst.first, e.dst.second, e.seed);
        entries.emplace_back(
            e.key,
            std::move(e.metadata),
            std::make_pair(static_cast<const char *>(e.dst.first),
                           e.dst.second),
            ts,
            eloqstore::WriteOp::Upsert);
    }
    std::sort(entries.begin(), entries.end());
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

// Read via overload B (metadata + pinned bytes) and check both halves.
void ReadAndVerifyPinned(eloqstore::EloqStore *store,
                         PinnedHarness &h,
                         const eloqstore::TableIdent &tbl,
                         const std::string &key,
                         size_t size,
                         uint64_t expected_seed,
                         const std::string &expected_metadata)
{
    auto rb = h.AllocateSegmentAligned(size);
    std::memset(rb.first, 0, rb.second);
    eloqstore::ReadRequest r;
    r.SetArgs(tbl, key);
    r.large_value_dest_ = std::make_pair(rb.first, rb.second);
    store->ExecSync(&r);
    REQUIRE(r.Error() == eloqstore::KvError::NoError);
    REQUIRE(r.value_ == expected_metadata);
    REQUIRE(VerifyDeterministic(rb.first, size, expected_seed));
}

}  // namespace

// ---------------------------------------------------------------------------
// Sentinel test: compaction in pinned mode must route reads/writes through
// the private gc GlobalRegisteredMemory pool, not the caller's pinned
// chunks. We fill the back half of the pinned chunk with a recognizable
// pattern that no write touches; if compaction issues fixed I/O into the
// pinned chunks by mistake, the kernel's write_fixed will scribble somewhere
// in that range and the sentinel bytes will change.
// ---------------------------------------------------------------------------
TEST_CASE("segment compaction in pinned mode leaves caller's chunk untouched",
          "[segment-compact][pinned]")
{
    PinnedHarness h;
    eloqstore::KvOptions opts =
        MakePinnedOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    eloqstore::EloqStore *store = InitStore(opts);

    eloqstore::TableIdent tbl{"sc_pinned_sentinel", 0};
    const size_t seg = h.SegmentSize();

    // Back half is sentinel; front half holds all write/read allocations.
    constexpr size_t kHalf = PinnedHarness::kPinnedSize / 2;
    char *sentinel_base = h.Base() + kHalf;
    constexpr unsigned char kSentinel = 0xCA;
    std::memset(sentinel_base, kSentinel, kHalf);
    h.SetCursorLimit(kHalf);

    // 4 seed keys + 10 rounds of overwrites on k1+k2. Each value spans 2
    // segments. The seed and per-round overwrites are issued as single
    // batches so UpdateMeta (and CompactIfNeeded) fires once per round --
    // matches the access pattern of the GlobalRegisteredMemory-mode reclaim
    // test and lets compaction collapse the tail into 1-2 files.
    const size_t value_segs = 2;
    const size_t value_size = seg * value_segs;
    {
        std::vector<PinnedBatchEntry> seed_entries;
        for (size_t i = 0; i < 4; ++i)
        {
            seed_entries.push_back({"k" + std::to_string(i + 1),
                                    h.AllocateSegmentAligned(value_size),
                                    0xA000 + i,
                                    {}});
        }
        WritePinnedBatch(store, tbl, std::move(seed_entries), /*ts=*/1);
    }

    uint64_t last_k1 = 0;
    uint64_t last_k2 = 0;
    for (int round = 0; round < 10; ++round)
    {
        last_k1 = 0xB000 + static_cast<uint64_t>(round) * 100;
        last_k2 = 0xB000 + static_cast<uint64_t>(round) * 100 + 1;
        std::vector<PinnedBatchEntry> round_entries = {
            {"k1", h.AllocateSegmentAligned(value_size), last_k1, {}},
            {"k2", h.AllocateSegmentAligned(value_size), last_k2, {}}};
        WritePinnedBatch(
            store, tbl, std::move(round_entries), /*ts=*/2 + round);
    }
    REQUIRE(WaitForCondition(
        chrono::seconds(3),
        chrono::milliseconds(20),
        [&]() { return InspectSegmentFiles(opts, tbl).count <= 2; }));

    // Sentinel region is byte-for-byte intact.
    for (size_t i = 0; i < kHalf; ++i)
    {
        if (static_cast<unsigned char>(sentinel_base[i]) != kSentinel)
        {
            INFO("sentinel byte at offset " << i << " changed");
            REQUIRE(false);
        }
    }

    // And every live value reads back correctly via overload B.
    ReadAndVerifyPinned(store,
                        h,
                        tbl,
                        "k1",
                        value_size,
                        last_k1,
                        /*metadata=*/{});
    ReadAndVerifyPinned(store,
                        h,
                        tbl,
                        "k2",
                        value_size,
                        last_k2,
                        /*metadata=*/{});
    ReadAndVerifyPinned(store,
                        h,
                        tbl,
                        "k3",
                        value_size,
                        0xA000 + 2,
                        /*metadata=*/{});
    ReadAndVerifyPinned(store,
                        h,
                        tbl,
                        "k4",
                        value_size,
                        0xA000 + 3,
                        /*metadata=*/{});

    // Compaction actually ran: the live mapping has only 4 keys * 2 = 8
    // segments, so the collapsed on-disk file count must be small.
    SegmentFileInfo info = InspectSegmentFiles(opts, tbl);
    REQUIRE(info.any);
    REQUIRE(info.count <= 2);

    store->Stop();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Bound check: KV Cache mode requires `max_segments_batch * segment_size <=
// gc_global_mem_size_per_shard` so compaction's worst-case batch always fits
// in the private pool. Validate the boundary value is accepted; the
// less-than-boundary case is rejected by ValidateOptions and reaches the
// caller as a LOG(FATAL) from the EloqStore constructor, which cannot be
// exercised from inside a single Catch2 process (no fork-based death tests
// here).
// ---------------------------------------------------------------------------
TEST_CASE(
    "pinned-mode boundary gc_global_mem_size_per_shard is accepted by Start",
    "[segment-compact][pinned]")
{
    PinnedHarness h;
    eloqstore::KvOptions opts = MakePinnedOpts(h);
    // Exactly max_segments_batch * segment_size: the minimum the bound check
    // allows. Anything smaller would trip the FATAL.
    opts.gc_global_mem_size_per_shard =
        static_cast<size_t>(eloqstore::max_segments_batch) * opts.segment_size;
    CleanupStore(opts);

    auto store = std::make_unique<eloqstore::EloqStore>(opts);
    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    store->Stop();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Compaction in pinned mode must preserve the metadata trailer stored in the
// data page. Write metadata-bearing large values, overwrite them past the
// segment-amplification threshold, wait for compaction, then verify each key
// reads back with both the latest bytes AND the original metadata blob.
// ---------------------------------------------------------------------------
TEST_CASE("segment compaction in pinned mode preserves metadata trailer",
          "[segment-compact][pinned]")
{
    PinnedHarness h;
    eloqstore::KvOptions opts =
        MakePinnedOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    eloqstore::EloqStore *store = InitStore(opts);

    eloqstore::TableIdent tbl{"sc_pinned_meta", 0};
    const size_t seg = h.SegmentSize();
    const std::string meta_k1 = "tensor[seg*2,bf16,key=k1]";
    const std::string meta_k2 = "tensor[seg*2,fp32,key=k2,longer-blob]";

    // Seed: 2 metadata-bearing values, 2 segments each.
    {
        auto b1 = h.AllocateSegmentAligned(seg * 2);
        WritePinned(store, tbl, "k1", b1, 0xC100, meta_k1, /*ts=*/1);
        auto b2 = h.AllocateSegmentAligned(seg * 2);
        WritePinned(store, tbl, "k2", b2, 0xC200, meta_k2, /*ts=*/1);
    }

    // Overwrite both keys (same metadata) until segment amplification
    // exceeds the factor=2 threshold and compaction rewrites the tail.
    uint64_t last_k1 = 0;
    uint64_t last_k2 = 0;
    for (int round = 0; round < 10; ++round)
    {
        auto bk1 = h.AllocateSegmentAligned(seg * 2);
        last_k1 = 0xD000 + static_cast<uint64_t>(round) * 100;
        WritePinned(store, tbl, "k1", bk1, last_k1, meta_k1, /*ts=*/2 + round);

        auto bk2 = h.AllocateSegmentAligned(seg * 2);
        last_k2 = 0xD000 + static_cast<uint64_t>(round) * 100 + 1;
        WritePinned(store, tbl, "k2", bk2, last_k2, meta_k2, /*ts=*/2 + round);
    }
    REQUIRE(WaitForCondition(
        chrono::seconds(3),
        chrono::milliseconds(20),
        [&]() { return InspectSegmentFiles(opts, tbl).count <= 2; }));

    // Both metadata and bytes round-trip through compaction.
    ReadAndVerifyPinned(store, h, tbl, "k1", seg * 2, last_k1, meta_k1);
    ReadAndVerifyPinned(store, h, tbl, "k2", seg * 2, last_k2, meta_k2);

    // And compaction did run (live mapping = 4 segments collapse to ~1 file).
    SegmentFileInfo info = InspectSegmentFiles(opts, tbl);
    REQUIRE(info.any);
    REQUIRE(info.count <= 2);

    store->Stop();
    CleanupStore(opts);
}
