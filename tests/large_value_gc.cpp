// Tests focused on GC of segment files referenced by metadata-bearing large
// values (KV Cache pinned mode).
//
// Compaction-side preservation of the metadata trailer is exercised in
// tests/segment_compact.cpp. The angle here is GC's accounting of which
// segment files are live: the encoded trailer (stored inline in the data
// page next to the segment-mapping pointers) must NOT keep stale segment
// files alive. Segments are referenced solely via the segment mapping.

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
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
#include "../include/kv_options.h"
#include "../include/types.h"
#include "common.h"

namespace fs = std::filesystem;
namespace chrono = std::chrono;

namespace
{
constexpr uint32_t kSegmentSize = 256 * 1024;

void FillDeterministic(char *dst, size_t n, uint64_t seed)
{
    for (size_t i = 0; i < n; ++i)
        dst[i] = static_cast<char>((seed * 2654435761u + i) & 0xFF);
}

bool VerifyDeterministic(const char *src, size_t n, uint64_t seed)
{
    for (size_t i = 0; i < n; ++i)
    {
        if (src[i] != static_cast<char>((seed * 2654435761u + i) & 0xFF))
            return false;
    }
    return true;
}

// KV Cache pinned-memory harness. One 4 KiB-aligned backing buffer is
// registered with EloqStore via KvOptions::pinned_memory_chunks; writes
// allocate sub-ranges by advancing a cursor.
class PinnedHarness
{
public:
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
    uint32_t SegmentSize() const
    {
        return kSegmentSize;
    }

    std::pair<char *, size_t> AllocateSegmentAligned(size_t size)
    {
        const size_t k = (size + kSegmentSize - 1) / kSegmentSize;
        const size_t aligned = k * kSegmentSize;
        REQUIRE(cursor_ + aligned <= kPinnedSize);
        char *p = base_ + cursor_;
        cursor_ += aligned;
        return {p, size};
    }

    std::vector<std::pair<char *, size_t>> Chunks() const
    {
        return {{base_, kPinnedSize}};
    }

private:
    char *base_{nullptr};
    size_t cursor_{0};
};

eloqstore::KvOptions MakePinnedOpts(PinnedHarness &h,
                                    uint8_t file_amp = 2,
                                    uint8_t seg_amp = 2)
{
    eloqstore::KvOptions opts;
    opts.num_threads = 1;
    opts.num_retained_archives = 0;
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

struct PinnedBatchEntry
{
    std::string key;
    std::pair<char *, size_t> dst;
    uint64_t seed;
    std::string metadata;
};

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

void DeleteBatch(eloqstore::EloqStore *store,
                 const eloqstore::TableIdent &tbl,
                 const std::vector<std::string> &keys,
                 uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.reserve(keys.size());
    for (const auto &k : keys)
    {
        entries.emplace_back(k, std::string{}, ts, eloqstore::WriteOp::Delete);
    }
    std::sort(entries.begin(), entries.end());
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

void WriteSmall(eloqstore::EloqStore *store,
                const eloqstore::TableIdent &tbl,
                const std::string &key,
                const std::string &value,
                uint64_t ts)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(key, value, ts, eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

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

struct SegmentFileInfo
{
    size_t count{0};
    bool any{false};
};

SegmentFileInfo InspectSegmentFiles(const eloqstore::KvOptions &opts,
                                    const eloqstore::TableIdent &tbl)
{
    SegmentFileInfo info;
    if (opts.store_path.empty())
        return info;
    fs::path dir = fs::path(opts.store_path[0]) / tbl.ToString();
    if (!fs::exists(dir))
        return info;
    for (const auto &entry : fs::directory_iterator(dir))
    {
        if (!entry.is_regular_file())
            continue;
        std::string fname = entry.path().filename().string();
        auto [type, suffix] = eloqstore::ParseFileName(fname);
        if (type != eloqstore::FileNameSegment)
            continue;
        eloqstore::FileId fid = 0;
        std::string_view branch_name;
        uint64_t term = 0;
        if (!eloqstore::ParseSegmentFileSuffix(suffix, fid, branch_name, term))
            continue;
        ++info.count;
        info.any = true;
    }
    return info;
}

void WaitForGc(int ms = 800)
{
    std::this_thread::sleep_for(chrono::milliseconds(ms));
}

}  // namespace

// ---------------------------------------------------------------------------
// After deleting every metadata-bearing large value in a table, the
// segment-mapping is empty but the on-disk segment files still hold space.
// MapperExceedsAmplification's mapping_count==0 branch triggers compaction,
// which schedules a TriggerFileGC that retires all unreferenced segment
// files. The encoded metadata trailer (stored inline in the data page next
// to the segment-pointer encoding) must NOT keep any of those files alive.
// ---------------------------------------------------------------------------
TEST_CASE(
    "deleting all metadata-bearing pinned values retires every segment file",
    "[large-value-gc][pinned]")
{
    PinnedHarness h;
    eloqstore::KvOptions opts =
        MakePinnedOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    eloqstore::EloqStore *store = InitStore(opts);

    eloqstore::TableIdent tbl{"lvgc_delete_all", 0};
    const size_t seg = h.SegmentSize();
    const std::string meta_a = "tensor[seg*2,bf16,key=a]";
    const std::string meta_b = "tensor[seg*2,fp32,key=b,extra=blob]";
    const std::string meta_c = "tensor[seg*3,fp16,key=c]";
    const std::string meta_d = "tensor[seg*1,int8,key=d]";

    // Seed: four metadata-bearing values of varying sizes so the segment
    // mapping is non-trivial.
    {
        std::vector<PinnedBatchEntry> entries;
        entries.push_back(
            {"k_a", h.AllocateSegmentAligned(seg * 2), 0x1010, meta_a});
        entries.push_back(
            {"k_b", h.AllocateSegmentAligned(seg * 2), 0x1020, meta_b});
        entries.push_back(
            {"k_c", h.AllocateSegmentAligned(seg * 3), 0x1030, meta_c});
        entries.push_back(
            {"k_d", h.AllocateSegmentAligned(seg * 1), 0x1040, meta_d});
        WritePinnedBatch(store, tbl, std::move(entries), /*ts=*/1);
    }

    SegmentFileInfo before = InspectSegmentFiles(opts, tbl);
    REQUIRE(before.any);

    // Delete every key in one batch. The trailer is encoded inline in the
    // data page; if the GC bookkeeping (mistakenly) tracked segment files
    // via the trailer rather than the segment mapping, this delete would
    // not be enough to free them.
    DeleteBatch(store, tbl, {"k_a", "k_b", "k_c", "k_d"}, /*ts=*/2);

    // Nudge an unrelated batch so UpdateMeta runs again and the empty
    // mapper crosses the compaction threshold (mapping_count==0,
    // space_size>0).
    WriteSmall(store, tbl, "nudge", "x", /*ts=*/3);
    WaitForGc();

    SegmentFileInfo after = InspectSegmentFiles(opts, tbl);
    REQUIRE_FALSE(after.any);

    store->Stop();
    CleanupStore(opts);
}

// ---------------------------------------------------------------------------
// Overwrite metadata-bearing pinned values many times. GC must reclaim the
// dead segment files (those whose segments are no longer referenced by the
// live mapping) and leave only files holding live mapping entries.
// Independent check from segment_compact's metadata-preservation test: this
// one asserts on file-count BOUNDS rather than read-back bytes, ensuring the
// trailer doesn't pin extra files alive.
// ---------------------------------------------------------------------------
TEST_CASE(
    "overwritten metadata-bearing pinned values free their old segment "
    "files",
    "[large-value-gc][pinned]")
{
    PinnedHarness h;
    eloqstore::KvOptions opts =
        MakePinnedOpts(h, /*file_amp=*/2, /*seg_amp=*/2);
    eloqstore::EloqStore *store = InitStore(opts);

    eloqstore::TableIdent tbl{"lvgc_overwrite", 0};
    const size_t seg = h.SegmentSize();
    const std::string meta_x = "meta-X-blob";
    const std::string meta_y = "meta-Y-longer-blob-for-padding";

    // Seed two metadata-bearing values, then overwrite each 10 times so the
    // segment allocator runs ahead of the live mapping by a factor > 2.
    {
        std::vector<PinnedBatchEntry> seed;
        seed.push_back(
            {"k_x", h.AllocateSegmentAligned(seg * 2), 0x2100, meta_x});
        seed.push_back(
            {"k_y", h.AllocateSegmentAligned(seg * 2), 0x2200, meta_y});
        WritePinnedBatch(store, tbl, std::move(seed), /*ts=*/1);
    }

    uint64_t last_x = 0;
    uint64_t last_y = 0;
    for (int round = 0; round < 10; ++round)
    {
        last_x = 0x3000 + static_cast<uint64_t>(round) * 100;
        last_y = 0x3000 + static_cast<uint64_t>(round) * 100 + 1;
        std::vector<PinnedBatchEntry> entries = {
            {"k_x", h.AllocateSegmentAligned(seg * 2), last_x, meta_x},
            {"k_y", h.AllocateSegmentAligned(seg * 2), last_y, meta_y},
        };
        WritePinnedBatch(store, tbl, std::move(entries), /*ts=*/2 + round);
    }
    WaitForGc();

    // Live mapping: 2 keys * 2 segments = 4 segments. After compaction's
    // tail rewrite + TriggerFileGC, those 4 segments occupy at most 2
    // segment files (8 per file). Without GC reclaiming dead files the
    // count would be 22/8 = 3 files at minimum.
    SegmentFileInfo info = InspectSegmentFiles(opts, tbl);
    REQUIRE(info.any);
    REQUIRE(info.count <= 2);

    // Latest bytes + original metadata round-trip cleanly.
    ReadAndVerifyPinned(store, h, tbl, "k_x", seg * 2, last_x, meta_x);
    ReadAndVerifyPinned(store, h, tbl, "k_y", seg * 2, last_y, meta_y);

    store->Stop();
    CleanupStore(opts);
}
