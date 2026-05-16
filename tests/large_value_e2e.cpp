#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
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
// Default harness sizing: 256KB segments, 64MB chunks, 256MB total per shard
// (1024 segments). Comfortably above the working set of any single test
// (largest case is a 10MB value at 40 segments) so failures here are real
// rather than evict-induced. Locked memory stays well under the typical 2GB
// RLIMIT_MEMLOCK on dev boxes.
constexpr uint32_t kDefaultSegmentSize = 256 * 1024;
constexpr size_t kDefaultChunkSize = 64ULL * 1024 * 1024;
constexpr size_t kDefaultTotalSize = 256ULL * 1024 * 1024;

// Deterministically fill a buffer with bytes derived from (seed, index).
// Used so we can assert exact byte equality after a round trip.
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

class LargeValueHarness
{
public:
    LargeValueHarness(uint16_t num_shards = 1,
                      uint32_t segment_size = kDefaultSegmentSize,
                      size_t chunk_size = kDefaultChunkSize,
                      size_t total_size = kDefaultTotalSize)
        : segment_size_(segment_size)
    {
        memories_.reserve(num_shards);
        mem_ptrs_.reserve(num_shards);
        bases_.assign(num_shards, 0);
        for (uint16_t i = 0; i < num_shards; ++i)
        {
            memories_.emplace_back(
                std::make_unique<eloqstore::GlobalRegisteredMemory>(
                    segment_size, chunk_size, total_size));
            mem_ptrs_.push_back(memories_.back().get());
        }
    }

    const std::vector<eloqstore::GlobalRegisteredMemory *> &Pointers() const
    {
        return mem_ptrs_;
    }

    // Call once the store is running so we can pick up the io_uring
    // registered-buffer base for each shard. Start() returns before the shard
    // thread has finished BootstrapRing, so we force a round-trip via a
    // synchronous read per shard: ExecSync blocks until the shard processes
    // the request, and the shard can only process requests after
    // InitIoMgrAndPagePool (and thus BootstrapRing) has run.
    void BindStore(eloqstore::EloqStore *store)
    {
        eloqstore::TableIdent warmup_tbl{"__warmup__", 0};
        for (size_t i = 0; i < memories_.size(); ++i)
        {
            eloqstore::ReadRequest r;
            // Route to the i-th shard by picking a key that hashes to it.
            // A simple approach: use any key and rely on single-shard tests;
            // for multi-shard harnesses we probe each shard explicitly by
            // polling the manager's init flag instead.
            r.SetArgs(warmup_tbl, "__warmup_key__");
            store->ExecSync(&r);
            bases_[i] = store->GlobalRegMemIndexBase(i);
        }
    }

    uint32_t SegmentSize() const
    {
        return segment_size_;
    }

    uint16_t RegBase(size_t shard_id) const
    {
        return bases_[shard_id];
    }

    eloqstore::GlobalRegisteredMemory *Memory(size_t shard_id)
    {
        return memories_[shard_id].get();
    }

    // Build an IoStringBuffer filled with deterministic bytes of the given
    // size. Segments are drawn from the per-shard GlobalRegisteredMemory and
    // buf_index_ values are set to the shard's registered-buffer base so the
    // shard's io_uring can submit writes with IOSQE_BUFFER_SELECT /
    // write_fixed.
    eloqstore::IoStringBuffer MakeLargeValue(size_t size,
                                             uint64_t seed,
                                             size_t shard_id = 0)
    {
        auto *mem = memories_[shard_id].get();
        const uint16_t base = bases_[shard_id];
        const uint32_t seg_size = segment_size_;
        const size_t num_segments = (size + seg_size - 1) / seg_size;

        eloqstore::IoStringBuffer buf;
        size_t bytes_remaining = size;
        for (size_t i = 0; i < num_segments; ++i)
        {
            auto [ptr, chunk_idx] = mem->GetSegment([]() {});
            // Pattern seed varies per fragment so we catch reordered
            // segments in the round-trip.
            uint64_t frag_seed = seed + i;
            size_t seg_bytes = std::min<size_t>(seg_size, bytes_remaining);
            FillDeterministic(ptr, seg_bytes, frag_seed);
            // Zero unused tail for hygiene (large values write a full segment
            // to disk regardless of actual_length).
            if (seg_bytes < seg_size)
            {
                std::memset(ptr + seg_bytes, 0, seg_size - seg_bytes);
            }
            buf.Append({ptr, static_cast<uint16_t>(base + chunk_idx)});
            bytes_remaining -= seg_bytes;
        }
        buf.SetSize(size);
        return buf;
    }

    // Verify the fragments hold the deterministic pattern for a value of
    // 'size' bytes created with 'seed'.
    bool VerifyLargeValue(const eloqstore::IoStringBuffer &buf,
                          size_t size,
                          uint64_t seed) const
    {
        if (buf.Size() != size)
            return false;
        const auto &frags = buf.Fragments();
        const uint32_t seg_size = segment_size_;
        const size_t num_segments = (size + seg_size - 1) / seg_size;
        if (frags.size() != num_segments)
            return false;
        size_t remaining = size;
        for (size_t i = 0; i < num_segments; ++i)
        {
            size_t seg_bytes = std::min<size_t>(seg_size, remaining);
            if (!VerifyDeterministic(frags[i].data_, seg_bytes, seed + i))
            {
                return false;
            }
            remaining -= seg_bytes;
        }
        return true;
    }

    // Recycle all fragments carried by a batch-write request (caller holds the
    // request until after ExecSync completes, so this is safe). Also clears
    // each WriteDataEntry's IoStringBuffer.
    void RecycleBatch(eloqstore::BatchWriteRequest &req, size_t shard_id = 0)
    {
        for (auto &entry : req.batch_)
        {
            entry.RecycleLargeValue(memories_[shard_id].get(),
                                    bases_[shard_id]);
        }
    }

    void RecycleReadValue(eloqstore::ReadRequest &req, size_t shard_id = 0)
    {
        if (auto *iosb =
                std::get_if<eloqstore::IoStringBuffer>(&req.large_value_dest_);
            iosb != nullptr && !iosb->Fragments().empty())
        {
            iosb->Recycle(memories_[shard_id].get(), bases_[shard_id]);
        }
    }

    void AssertFreePoolRestored() const
    {
        for (size_t i = 0; i < memories_.size(); ++i)
        {
            INFO("shard=" << i);
            REQUIRE(memories_[i]->FreeSegments() ==
                    memories_[i]->TotalSegments());
        }
    }

private:
    uint32_t segment_size_;
    std::vector<std::unique_ptr<eloqstore::GlobalRegisteredMemory>> memories_;
    std::vector<eloqstore::GlobalRegisteredMemory *> mem_ptrs_;
    std::vector<uint16_t> bases_;
};

// Build KvOptions wired to the harness.
eloqstore::KvOptions MakeOpts(LargeValueHarness &harness)
{
    eloqstore::KvOptions opts = append_opts;
    opts.num_threads = 1;
    opts.segment_size = harness.SegmentSize();
    opts.segments_per_file_shift = 3;  // 8 segments per segment file.
    opts.buffer_pool_size = 16 * eloqstore::MB;
    // Suppress the append-mode write buffer; nothing in the e2e tests
    // exercises it, and leaving it disabled keeps the locked footprint
    // predictable.
    opts.write_buffer_size = 0;
    opts.write_buffer_ratio = 0.0;
    opts.global_registered_memories = harness.Pointers();
    return opts;
}

// Drive a one-shot write of a single (key, large-value) pair. Recycles the
// input buffer before returning so the caller does not need to.
void WriteLarge(eloqstore::EloqStore *store,
                LargeValueHarness &harness,
                const eloqstore::TableIdent &tbl,
                std::string key,
                size_t size,
                uint64_t seed,
                uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(std::move(key),
                         harness.MakeLargeValue(size, seed),
                         ts,
                         eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    harness.RecycleBatch(req);
}

void WriteSmall(eloqstore::EloqStore *store,
                const eloqstore::TableIdent &tbl,
                std::string key,
                std::string value,
                uint64_t ts = 1)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        std::move(key), std::move(value), ts, eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

void DeleteKey(eloqstore::EloqStore *store,
               const eloqstore::TableIdent &tbl,
               std::string key,
               uint64_t ts = 2)
{
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        std::move(key), std::string{}, ts, eloqstore::WriteOp::Delete);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

}  // namespace

TEST_CASE("EloqStore round-trips large values across canonical sizes",
          "[large-value-e2e]")
{
    LargeValueHarness harness(/*num_shards=*/1);
    eloqstore::KvOptions opts = MakeOpts(harness);
    eloqstore::EloqStore *store = InitStore(opts);
    harness.BindStore(store);

    eloqstore::TableIdent tbl{"lv", 0};

    // Each entry writes one large value of that size. Choose sizes around
    // segment boundaries to exercise single-segment, exact-multiple, and
    // odd-tail cases.
    const uint32_t seg = harness.SegmentSize();
    struct Case
    {
        size_t size;
        uint64_t seed;
        std::string key;
    };
    std::vector<Case> cases = {
        {size_t{128} * 1024, 0x11, "k_128k"},             // < 1 segment
        {size_t{seg}, 0x22, "k_seg"},                     // exact 1 segment
        {size_t{1} * 1024 * 1024, 0x33, "k_1m"},          // 4 segments
        {size_t{4} * 1024 * 1024, 0x44, "k_4m"},          // 16 segments
        {size_t{10} * 1024 * 1024 + 123, 0x55, "k_10m"},  // 41 segments + tail
    };

    for (const auto &c : cases)
    {
        WriteLarge(store, harness, tbl, c.key, c.size, c.seed);
    }

    // Point-read each key and verify fragments / bytes / large-path dispatch.
    for (const auto &c : cases)
    {
        eloqstore::ReadRequest req;
        req.SetArgs(tbl, c.key);
        req.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        auto &iosb = std::get<eloqstore::IoStringBuffer>(req.large_value_dest_);
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);
        // Large-value path does not populate the small-value output field
        // beyond the metadata trailer (empty for these no-metadata writes).
        REQUIRE(req.value_.empty());
        REQUIRE(iosb.Size() == c.size);
        const size_t expected_frags = (c.size + seg - 1) / seg;
        REQUIRE(iosb.Fragments().size() == expected_frags);
        for (const auto &frag : iosb.Fragments())
        {
            // buf_index_ must lie inside the registered range owned by the
            // harness (base + [0, num_chunks)).
            const uint16_t base = harness.RegBase(0);
            const size_t num_chunks = harness.Memory(0)->MemChunks().size();
            REQUIRE(frag.buf_index_ >= base);
            REQUIRE(frag.buf_index_ < base + num_chunks);
        }
        REQUIRE(harness.VerifyLargeValue(iosb, c.size, c.seed));
        harness.RecycleReadValue(req);
    }

    store->Stop();
    harness.AssertFreePoolRestored();
    CleanupStore(opts);
}

TEST_CASE("EloqStore routes short, overflow, and large values in one batch",
          "[large-value-e2e]")
{
    LargeValueHarness harness(1);
    eloqstore::KvOptions opts = MakeOpts(harness);
    eloqstore::EloqStore *store = InitStore(opts);
    harness.BindStore(store);

    eloqstore::TableIdent tbl{"mix", 0};
    const size_t seg = harness.SegmentSize();

    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back("k_short", "hello", 1, eloqstore::WriteOp::Upsert);
    // Overflow value (larger than a data page but below the large-value
    // threshold), exercised via the normal string-ctor path.
    entries.emplace_back("k_overflow",
                         std::string(8 * 1024, 'o'),
                         1,
                         eloqstore::WriteOp::Upsert);
    entries.emplace_back("k_large",
                         harness.MakeLargeValue(seg * 3 + 17, 0x7a),
                         1,
                         eloqstore::WriteOp::Upsert);
    std::sort(entries.begin(), entries.end());
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
    harness.RecycleBatch(req);

    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k_short");
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == "hello");
    }
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k_overflow");
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == std::string(8 * 1024, 'o'));
    }
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k_large");
        r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        auto &iosb = std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_.empty());
        REQUIRE(iosb.Size() == seg * 3 + 17);
        REQUIRE(harness.VerifyLargeValue(iosb, seg * 3 + 17, 0x7a));
        harness.RecycleReadValue(r);
    }

    store->Stop();
    harness.AssertFreePoolRestored();
    CleanupStore(opts);
}

TEST_CASE("EloqStore overwrite semantics across large/small transitions",
          "[large-value-e2e]")
{
    LargeValueHarness harness(1);
    eloqstore::KvOptions opts = MakeOpts(harness);
    eloqstore::EloqStore *store = InitStore(opts);
    harness.BindStore(store);

    eloqstore::TableIdent tbl{"ow", 0};
    const size_t seg = harness.SegmentSize();

    // (a) large -> short
    WriteLarge(store, harness, tbl, "k1", seg * 2, 0xa1, /*ts=*/10);
    WriteSmall(store, tbl, "k1", "tiny", /*ts=*/20);
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k1");
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == "tiny");
    }

    // (b) short -> large
    WriteSmall(store, tbl, "k2", "seed", /*ts=*/10);
    WriteLarge(store, harness, tbl, "k2", seg * 3, 0xa2, /*ts=*/20);
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k2");
        r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        auto &iosb = std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_.empty());
        REQUIRE(iosb.Size() == seg * 3);
        REQUIRE(harness.VerifyLargeValue(iosb, seg * 3, 0xa2));
        harness.RecycleReadValue(r);
    }

    // (c) large -> large with different segment count.
    WriteLarge(store, harness, tbl, "k3", seg * 2 + 7, 0xa3, /*ts=*/10);
    WriteLarge(store, harness, tbl, "k3", seg * 5, 0xa4, /*ts=*/20);
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k3");
        r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        auto &iosb = std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(iosb.Size() == seg * 5);
        REQUIRE(iosb.Fragments().size() == 5);
        REQUIRE(harness.VerifyLargeValue(iosb, seg * 5, 0xa4));
        harness.RecycleReadValue(r);
    }

    store->Stop();
    harness.AssertFreePoolRestored();
    CleanupStore(opts);
}

TEST_CASE(
    "EloqStore deleting a large value returns NotFound on subsequent read",
    "[large-value-e2e]")
{
    LargeValueHarness harness(1);
    eloqstore::KvOptions opts = MakeOpts(harness);
    eloqstore::EloqStore *store = InitStore(opts);
    harness.BindStore(store);

    eloqstore::TableIdent tbl{"del", 0};
    WriteLarge(store,
               harness,
               tbl,
               "gone",
               size_t{2} * 1024 * 1024,
               0xdd,
               /*ts=*/1);
    DeleteKey(store, tbl, "gone", /*ts=*/2);

    eloqstore::ReadRequest r;
    r.SetArgs(tbl, "gone");
    store->ExecSync(&r);
    REQUIRE(r.Error() == eloqstore::KvError::NotFound);
    REQUIRE(r.value_.empty());

    store->Stop();
    harness.AssertFreePoolRestored();
    CleanupStore(opts);
}

TEST_CASE(
    "EloqStore small-only partition never leaks segments (lazy mapper path)",
    "[large-value-e2e]")
{
    // If a partition never writes a large value the segment mapper should
    // stay unallocated. We can't crack RootMeta open from here, but we can
    // drive a pure short-value workload and assert that (a) writes/reads work
    // and (b) not a single segment was ever taken from the pool.
    LargeValueHarness harness(1);
    eloqstore::KvOptions opts = MakeOpts(harness);
    eloqstore::EloqStore *store = InitStore(opts);
    harness.BindStore(store);

    eloqstore::TableIdent tbl{"small_only", 0};
    for (int i = 0; i < 32; ++i)
    {
        WriteSmall(
            store, tbl, "k" + std::to_string(i), "v" + std::to_string(i));
    }
    for (int i = 0; i < 32; ++i)
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k" + std::to_string(i));
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == "v" + std::to_string(i));
    }
    // Full pool = no segment was ever fetched for this partition.
    REQUIRE(harness.Memory(0)->FreeSegments() ==
            harness.Memory(0)->TotalSegments());

    store->Stop();
    harness.AssertFreePoolRestored();
    CleanupStore(opts);
}

namespace
{
// Harness for KV Cache pinned-memory mode. Owns one large 4 KiB-aligned
// backing buffer and hands out segment-aligned sub-ranges per key. The
// buffer is registered with EloqStore via KvOptions::pinned_memory_chunks
// (one chunk per shard's ring).
class PinnedHarness
{
public:
    static constexpr size_t kPinnedSize = 32ULL * 1024 * 1024;  // 32 MiB

    PinnedHarness(uint32_t segment_size = kDefaultSegmentSize)
        : segment_size_(segment_size)
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
        return segment_size_;
    }

    // Allocate `size` bytes from the head of the pinned buffer. Sub-ranges
    // are not segment-aligned by themselves; the caller picks `size` to
    // match the test's segment count. Cursor advances by ceil(size/seg) *
    // seg bytes so back-to-back allocations stay segment-aligned (so a
    // mid-segment `size` leaves the rest of the segment slack inside the
    // chunk -- the fast path).
    std::pair<char *, size_t> AllocateSegmentAligned(size_t size)
    {
        const size_t k = (size + segment_size_ - 1) / segment_size_;
        const size_t aligned = k * segment_size_;
        REQUIRE(cursor_ + aligned <= kPinnedSize);
        char *p = base_ + cursor_;
        cursor_ += aligned;
        return {p, size};
    }

    // Allocate `size` bytes flush against the end of the chunk: the
    // resulting [ptr, ptr + ceil(size/seg)*seg) extends past the chunk
    // and forces Phase 7's scratch fallback. Cursor is *not* advanced
    // (this is a terminal allocation).
    std::pair<char *, size_t> AllocateChunkEnd(size_t size)
    {
        // Place the sub-range at chunk_end - size, so the *meaningful*
        // bytes end exactly at chunk_end but the rounded-up K*seg range
        // extends past it.
        REQUIRE(size <= kPinnedSize);
        REQUIRE(size > 0);
        const size_t k = (size + segment_size_ - 1) / segment_size_;
        REQUIRE(k * segment_size_ > size);  // must round up to trigger fallback
        char *p = base_ + (kPinnedSize - size);
        return {p, size};
    }

    void ResetCursor()
    {
        cursor_ = 0;
    }

    std::vector<std::pair<char *, size_t>> Chunks() const
    {
        return {{base_, kPinnedSize}};
    }

private:
    uint32_t segment_size_;
    char *base_{nullptr};
    size_t cursor_{0};
};

// Build KvOptions wired for pinned mode.
eloqstore::KvOptions MakePinnedOpts(PinnedHarness &harness,
                                    uint16_t num_threads = 1)
{
    eloqstore::KvOptions opts = append_opts;
    opts.num_threads = num_threads;
    opts.segment_size = harness.SegmentSize();
    opts.segments_per_file_shift = 3;
    opts.buffer_pool_size = 16 * eloqstore::MB;
    opts.write_buffer_size = 0;
    opts.write_buffer_ratio = 0.0;
    opts.pinned_memory_chunks = harness.Chunks();
    opts.gc_global_mem_size_per_shard = 32ULL * eloqstore::MB;
    opts.pinned_tail_scratch_slots = eloqstore::max_segments_batch;
    return opts;
}

// One-shot pinned write of a single (key, value) pair with optional
// metadata. Fills `[ptr, ptr+size)` with deterministic bytes and submits
// a BatchWriteRequest.
void WritePinned(eloqstore::EloqStore *store,
                 PinnedHarness &harness,
                 const eloqstore::TableIdent &tbl,
                 std::string key,
                 std::pair<char *, size_t> dst,
                 uint64_t seed,
                 std::string metadata = {},
                 uint64_t ts = 1)
{
    FillDeterministic(dst.first, dst.second, seed);
    eloqstore::BatchWriteRequest req;
    std::vector<eloqstore::WriteDataEntry> entries;
    entries.emplace_back(
        std::move(key),
        std::move(metadata),
        std::make_pair(static_cast<const char *>(dst.first), dst.second),
        ts,
        eloqstore::WriteOp::Upsert);
    req.SetArgs(tbl, std::move(entries));
    store->ExecSync(&req);
    (void) harness;
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}
}  // namespace

TEST_CASE("EloqStore pinned write + read round-trips with metadata",
          "[large-value-e2e][pinned]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    auto *store = InitStore(opts);

    eloqstore::TableIdent tbl{"pinned", 0};
    const size_t seg = harness.SegmentSize();
    const size_t value_size = seg * 3;  // exactly 3 segments
    const uint64_t seed = 0xb1u;
    const std::string metadata = "tensor[3*seg,bf16,key=pinned]";

    auto write_buf = harness.AllocateSegmentAligned(value_size);
    WritePinned(store, harness, tbl, "k", write_buf, seed, metadata);

    // Overload A metadata-only: leave `large_value_` null so the dispatch
    // skips the segment fetch. `value_` receives the metadata blob.
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k");
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == metadata);
    }

    // Overload B: value_ <- metadata, segments <- pinned buffer.
    auto read_buf = harness.AllocateSegmentAligned(value_size);
    std::memset(read_buf.first, 0, value_size);
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k");
        r.large_value_dest_ = std::make_pair(read_buf.first, read_buf.second);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == metadata);
        REQUIRE(VerifyDeterministic(read_buf.first, value_size, seed));
    }

    store->Stop();
    CleanupStore(opts);
}

TEST_CASE("EloqStore mixed metadata / no-metadata large values via overload A",
          "[large-value-e2e][pinned]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    auto *store = InitStore(opts);

    eloqstore::TableIdent tbl{"mixed_meta", 0};
    const size_t seg = harness.SegmentSize();

    struct Case
    {
        std::string key;
        size_t size;
        uint64_t seed;
        std::string metadata;
    };
    std::vector<Case> cases = {
        {"with_meta", seg * 2, 0xc1, "meta-A"},
        {"no_meta", seg * 2, 0xc2, ""},
        {"with_meta_3seg", seg * 3, 0xc3, "meta-C-longer"},
        {"no_meta_3seg", seg * 3, 0xc4, ""},
    };

    for (const auto &c : cases)
    {
        auto buf = harness.AllocateSegmentAligned(c.size);
        WritePinned(store, harness, tbl, c.key, buf, c.seed, c.metadata);
    }

    // Read back via overload A (metadata-only).
    for (const auto &c : cases)
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, c.key);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == c.metadata);
    }

    // Read back via overload B (metadata + pinned).
    for (const auto &c : cases)
    {
        auto read_buf = harness.AllocateSegmentAligned(c.size);
        std::memset(read_buf.first, 0, c.size);
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, c.key);
        r.large_value_dest_ = std::make_pair(read_buf.first, read_buf.second);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(r.value_ == c.metadata);
        REQUIRE(VerifyDeterministic(read_buf.first, c.size, c.seed));
    }

    store->Stop();
    CleanupStore(opts);
}

TEST_CASE("EloqStore overload C (pinned-only) reads",
          "[large-value-e2e][pinned]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    auto *store = InitStore(opts);

    eloqstore::TableIdent tbl{"overload_c", 0};
    const size_t seg = harness.SegmentSize();

    // Set up: write a large value with metadata.
    const size_t value_size = seg * 4;
    const uint64_t seed = 0xc5u;
    const std::string metadata = "C-only-test";
    auto write_buf = harness.AllocateSegmentAligned(value_size);
    WritePinned(store, harness, tbl, "k_large", write_buf, seed, metadata);

    // A short value to exercise the "not a large value" error path.
    WriteSmall(store, tbl, "k_short", "tiny");

    // Overload C on the large value: fresh pinned buffer, value-only.
    auto read_buf = harness.AllocateSegmentAligned(value_size);
    std::memset(read_buf.first, 0, value_size);
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k_large");
        r.large_value_dest_ = std::make_pair(read_buf.first, read_buf.second);
        r.large_value_only_ = true;
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        // value-only mode does NOT touch value_; it remains empty.
        REQUIRE(r.value_.empty());
        REQUIRE(VerifyDeterministic(read_buf.first, value_size, seed));
    }

    // Value-only read on a non-large entry -> InvalidArgs.
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k_short");
        r.large_value_dest_ = std::make_pair(read_buf.first, read_buf.second);
        r.large_value_only_ = true;
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::InvalidArgs);
    }

    store->Stop();
    CleanupStore(opts);
}

TEST_CASE("EloqStore pinned-write tail scratch fast path and fallback",
          "[large-value-e2e][pinned][tail-scratch]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    auto *store = InitStore(opts);

    eloqstore::TableIdent tbl{"tail_scratch", 0};
    const size_t seg = harness.SegmentSize();

    // Fast path 1: segment-aligned size, sub-range inside the chunk.
    const uint64_t seed_a = 0xaa;
    auto buf_a = harness.AllocateSegmentAligned(seg * 2);
    WritePinned(store, harness, tbl, "aligned", buf_a, seed_a);

    // Fast path 2: mid-segment size, but the sub-range has slack to the
    // chunk end (chunk size is much larger than allocations so far).
    const uint64_t seed_b = 0xbb;
    auto buf_b = harness.AllocateSegmentAligned(seg * 2 + 4096);
    WritePinned(store, harness, tbl, "slack", buf_b, seed_b);

    // At this point, both writes took the fast path; the scratch pool
    // should not have been acquired yet.
    REQUIRE(store->TailScratchAcquireCount(/*shard_id=*/0) == 0);

    // Fallback: mid-segment size flush against the chunk end. The
    // rounded-up tail extends past the chunk -- scratch is acquired. Size
    // is 4 KiB-aligned so the reverse-read via overload C can use the
    // same buffer size (`GetLargeValueContiguous` requires 4 KiB-aligned
    // dst_size).
    const uint64_t seed_c = 0xcc;
    auto buf_c = harness.AllocateChunkEnd(seg * 2 + 4096);
    WritePinned(store, harness, tbl, "tail_fallback", buf_c, seed_c);
    REQUIRE(store->TailScratchAcquireCount(/*shard_id=*/0) == 1);

    // Read back: all three should round-trip cleanly.
    auto verify =
        [&](const char *key, std::pair<char *, size_t> orig, uint64_t seed)
    {
        INFO("verify key=" << key << " size=" << orig.second);
        auto read_buf = harness.AllocateSegmentAligned(orig.second);
        std::memset(read_buf.first, 0, orig.second);
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, key);
        r.large_value_dest_ = std::make_pair(read_buf.first, read_buf.second);
        r.large_value_only_ = true;
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(VerifyDeterministic(read_buf.first, orig.second, seed));
    };
    verify("aligned", buf_a, seed_a);
    verify("slack", buf_b, seed_b);
    verify("tail_fallback", buf_c, seed_c);

    store->Stop();
    CleanupStore(opts);
}

TEST_CASE("EloqStore pinned-write zero-slot scratch rejects cross-boundary",
          "[large-value-e2e][pinned][tail-scratch]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    opts.pinned_tail_scratch_slots = 0;  // disable fallback
    auto *store = InitStore(opts);

    eloqstore::TableIdent tbl{"zero_slot", 0};
    const size_t seg = harness.SegmentSize();

    // Segment-aligned write succeeds without scratch.
    auto aligned_buf = harness.AllocateSegmentAligned(seg * 2);
    WritePinned(store, harness, tbl, "ok", aligned_buf, 0xab);

    // Cross-boundary write must fail with InvalidArgs.
    {
        auto bad_buf = harness.AllocateChunkEnd(seg + 4096);
        FillDeterministic(bad_buf.first, bad_buf.second, 0xff);
        eloqstore::BatchWriteRequest req;
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.emplace_back(
            "fail",
            std::string{},
            std::make_pair(static_cast<const char *>(bad_buf.first),
                           bad_buf.second),
            /*ts=*/1,
            eloqstore::WriteOp::Upsert);
        req.SetArgs(tbl, std::move(entries));
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::InvalidArgs);
    }

    store->Stop();
    CleanupStore(opts);
}

// In KV Cache pinned mode the internal GlobalRegisteredMemory is reserved
// for GC / compaction. Reads must use a caller-owned destination
// (overload B or C); reads with `large_value_dest_ = IoStringBuffer{}`
// would force fragments out of the internal pool with no way for the
// caller to recycle them. The dispatch in src/storage/shard.cpp rejects
// this combination at request-execute time with KvError::InvalidArgs.
TEST_CASE("EloqStore pinned mode rejects IoStringBuffer read destination",
          "[large-value-e2e][pinned]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    auto *store = InitStore(opts);

    eloqstore::TableIdent tbl{"pinned_reject_iosb", 0};
    const size_t seg = harness.SegmentSize();
    const size_t value_size = seg * 2;
    auto write_buf = harness.AllocateSegmentAligned(value_size);
    WritePinned(store,
                harness,
                tbl,
                "k",
                write_buf,
                /*seed=*/0xe5,
                /*metadata=*/{});

    // Wrong arm: IoStringBuffer destination on a pinned-mode store.
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k");
        r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::InvalidArgs);
        // The dispatch refused before reaching GetLargeValue, so the
        // IoStringBuffer inside the variant stays empty (no segments
        // allocated from the internal pool).
        auto &iosb = std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
        REQUIRE(iosb.Fragments().empty());
    }

    // Sanity: the correct pinned arm still works on the same key.
    auto read_buf = harness.AllocateSegmentAligned(value_size);
    std::memset(read_buf.first, 0, value_size);
    {
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "k");
        r.large_value_dest_ = std::make_pair(read_buf.first, read_buf.second);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(VerifyDeterministic(read_buf.first, value_size, 0xe5));
    }

    store->Stop();
    CleanupStore(opts);
}

TEST_CASE("EloqStore persists metadata-bearing pinned writes across restart",
          "[large-value-e2e][pinned][restart]")
{
    PinnedHarness harness;
    auto opts = MakePinnedOpts(harness);
    CleanupStore(opts);

    eloqstore::TableIdent tbl{"pinned_restart", 0};
    const size_t seg = harness.SegmentSize();

    struct Case
    {
        std::string key;
        size_t size;
        uint64_t seed;
        std::string metadata;
    };
    std::vector<Case> cases = {
        {"r1", seg * 2, 0xd1, "meta-r1"},
        {"r2", seg * 3, 0xd2, ""},
        {"r3", seg * 4, 0xd3, "meta-r3-bigger-blob"},
    };

    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        for (const auto &c : cases)
        {
            auto buf = harness.AllocateSegmentAligned(c.size);
            WritePinned(
                store.get(), harness, tbl, c.key, buf, c.seed, c.metadata);
        }
        // Archive flushes the segment mapping into the manifest.
        eloqstore::ArchiveRequest ar;
        ar.SetTableId(tbl);
        store->ExecAsyn(&ar);
        ar.Wait();
        REQUIRE(ar.Error() == eloqstore::KvError::NoError);
        store->Stop();
    }

    // Restart and read everything back.
    harness.ResetCursor();
    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);

        for (const auto &c : cases)
        {
            // Metadata-only read (leave large_value_ null).
            eloqstore::ReadRequest r1;
            r1.SetArgs(tbl, c.key);
            store->ExecSync(&r1);
            REQUIRE(r1.Error() == eloqstore::KvError::NoError);
            REQUIRE(r1.value_ == c.metadata);

            // Full-value read via overload B.
            auto read_buf = harness.AllocateSegmentAligned(c.size);
            std::memset(read_buf.first, 0, c.size);
            eloqstore::ReadRequest r2;
            r2.SetArgs(tbl, c.key);
            r2.large_value_dest_ =
                std::make_pair(read_buf.first, read_buf.second);
            store->ExecSync(&r2);
            REQUIRE(r2.Error() == eloqstore::KvError::NoError);
            REQUIRE(r2.value_ == c.metadata);
            REQUIRE(VerifyDeterministic(read_buf.first, c.size, c.seed));
        }
        store->Stop();
    }

    CleanupStore(opts);
}

TEST_CASE("EloqStore persists large values across restart",
          "[large-value-e2e][restart]")
{
    // Don't route through InitStore for this test: InitStore wipes the
    // on-disk store before every invocation, which would defeat restart.
    LargeValueHarness harness(1);
    eloqstore::KvOptions opts = MakeOpts(harness);
    CleanupStore(opts);

    eloqstore::TableIdent tbl{"restart", 0};
    const size_t seg = harness.SegmentSize();
    struct Case
    {
        size_t size;
        uint64_t seed;
        std::string key;
    };
    std::vector<Case> cases = {
        {size_t{128} * 1024, 0xe1, "r_small"},
        {seg * 3 + 100, 0xe2, "r_three"},
        {size_t{3} * 1024 * 1024, 0xe3, "r_threemb"},
    };

    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());
        for (const auto &c : cases)
        {
            WriteLarge(store.get(), harness, tbl, c.key, c.size, c.seed);
        }
        // Archive flushes segment mapping to manifest so the next boot
        // restores segment_mapper_ and the segment allocator state.
        eloqstore::ArchiveRequest ar;
        ar.SetTableId(tbl);
        store->ExecAsyn(&ar);
        ar.Wait();
        REQUIRE(ar.Error() == eloqstore::KvError::NoError);
        store->Stop();
        harness.AssertFreePoolRestored();
    }

    {
        auto store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        harness.BindStore(store.get());

        for (const auto &c : cases)
        {
            eloqstore::ReadRequest r;
            r.SetArgs(tbl, c.key);
            r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
            auto &iosb =
                std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
            store->ExecSync(&r);
            REQUIRE(r.Error() == eloqstore::KvError::NoError);
            REQUIRE(r.value_.empty());
            REQUIRE(iosb.Size() == c.size);
            REQUIRE(harness.VerifyLargeValue(iosb, c.size, c.seed));
            harness.RecycleReadValue(r);
        }

        // A fresh write after restart must still succeed — this exercises the
        // AppendAllocator restored from the persisted manifest.
        WriteLarge(
            store.get(), harness, tbl, "r_post_restart", seg * 2 + 9, 0xf0);
        eloqstore::ReadRequest r;
        r.SetArgs(tbl, "r_post_restart");
        r.large_value_dest_.emplace<eloqstore::IoStringBuffer>();
        auto &iosb = std::get<eloqstore::IoStringBuffer>(r.large_value_dest_);
        store->ExecSync(&r);
        REQUIRE(r.Error() == eloqstore::KvError::NoError);
        REQUIRE(harness.VerifyLargeValue(iosb, seg * 2 + 9, 0xf0));
        harness.RecycleReadValue(r);

        store->Stop();
        harness.AssertFreePoolRestored();
    }

    CleanupStore(opts);
}
