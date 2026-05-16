// Unit tests for the pinned-write tail-scratch pool's intrusive free list.
// The pool is normally populated in IouringMgr::BootstrapRing, which also
// initializes io_uring and registers iovecs with the kernel. These tests
// bypass that machinery: we construct a bare IouringMgr (its constructor
// only stores opts + fd_limit), then populate `tail_scratch_buf_` and the
// free-list head directly, mirroring the layout that BootstrapRing would
// produce. This isolates the intrusive-list logic from io_uring setup and
// from coroutine context (only the blocking acquire path needs ThdTask()
// / WaitingZone, which is covered by the Phase 8 e2e tests).

#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "async_io_manager.h"
#include "kv_options.h"

namespace
{
// Set up a `slot_count`-slot tail-scratch pool on `mgr` using a freshly
// allocated, 4 KiB-aligned backing buffer. The buffer is owned by `mgr`'s
// `tail_scratch_buf_` and torn down when `mgr` is destroyed.
void PopulateTailScratchPool(eloqstore::IouringMgr &mgr,
                             size_t slot_size,
                             uint16_t slot_count,
                             uint16_t buf_idx)
{
    const size_t pool_bytes = slot_size * slot_count;
    void *raw_ptr = nullptr;
    REQUIRE(posix_memalign(&raw_ptr, 4096, pool_bytes) == 0);
    REQUIRE(raw_ptr != nullptr);
    mgr.tail_scratch_buf_.reset(static_cast<char *>(raw_ptr));
    mgr.tail_scratch_buf_idx_ = buf_idx;

    // Forward-chain matches BootstrapRing: free-list head ends up at slot N-1.
    mgr.tail_scratch_free_ = nullptr;
    for (uint16_t i = 0; i < slot_count; ++i)
    {
        char *slot =
            mgr.tail_scratch_buf_.get() + static_cast<size_t>(i) * slot_size;
        std::memcpy(slot, &mgr.tail_scratch_free_, sizeof(char *));
        mgr.tail_scratch_free_ = slot;
    }
}
}  // namespace

TEST_CASE("AcquireTailScratch returns nullptr when pool is unallocated",
          "[tail-scratch]")
{
    eloqstore::KvOptions opts;
    opts.segment_size = 64 * 1024;
    opts.pinned_tail_scratch_slots = 4;
    eloqstore::IouringMgr mgr(&opts, /*fd_limit=*/1);

    // tail_scratch_buf_ is null until BootstrapRing populates it. Acquire
    // must return nullptr immediately (no blocking, no coroutine needed).
    uint16_t buf_index = 0;
    REQUIRE(mgr.AcquireTailScratch(buf_index) == nullptr);
    REQUIRE(buf_index == std::numeric_limits<uint16_t>::max());

    // Releasing nothing is a no-op (defensive: a caller shouldn't normally
    // do this, but the function must not crash when the pool is empty).
    mgr.ReleaseTailScratch(nullptr);
}

TEST_CASE("AcquireTailScratch returns N distinct, segment-aligned slots",
          "[tail-scratch]")
{
    constexpr size_t kSlotSize = 64 * 1024;
    constexpr uint16_t kSlotCount = 4;
    constexpr uint16_t kBufIdx = 17;

    eloqstore::KvOptions opts;
    opts.segment_size = kSlotSize;
    opts.pinned_tail_scratch_slots = kSlotCount;
    eloqstore::IouringMgr mgr(&opts, /*fd_limit=*/1);
    PopulateTailScratchPool(mgr, kSlotSize, kSlotCount, kBufIdx);

    char *const pool_base = mgr.tail_scratch_buf_.get();
    std::vector<char *> acquired;
    for (uint16_t i = 0; i < kSlotCount; ++i)
    {
        uint16_t buf_index = 0;
        char *slot = mgr.AcquireTailScratch(buf_index);
        REQUIRE(slot != nullptr);
        // All slots share the same buf_index (single iovec covers the pool).
        REQUIRE(buf_index == kBufIdx);
        // The slot must be inside the backing buffer and slot-aligned.
        REQUIRE(slot >= pool_base);
        REQUIRE(slot < pool_base + kSlotSize * kSlotCount);
        const ptrdiff_t off = slot - pool_base;
        REQUIRE(off % static_cast<ptrdiff_t>(kSlotSize) == 0);
        acquired.push_back(slot);
    }

    // All four slots must be distinct.
    for (size_t i = 0; i < acquired.size(); ++i)
    {
        for (size_t j = i + 1; j < acquired.size(); ++j)
        {
            REQUIRE(acquired[i] != acquired[j]);
        }
    }

    // Pool is drained: the free-list head is null.
    REQUIRE(mgr.tail_scratch_free_ == nullptr);
}

TEST_CASE("AcquireTailScratch / ReleaseTailScratch is LIFO", "[tail-scratch]")
{
    constexpr size_t kSlotSize = 64 * 1024;
    constexpr uint16_t kSlotCount = 3;

    eloqstore::KvOptions opts;
    opts.segment_size = kSlotSize;
    opts.pinned_tail_scratch_slots = kSlotCount;
    eloqstore::IouringMgr mgr(&opts, /*fd_limit=*/1);
    PopulateTailScratchPool(mgr, kSlotSize, kSlotCount, /*buf_idx=*/0);

    // Drain the pool.
    uint16_t unused = 0;
    char *a = mgr.AcquireTailScratch(unused);
    char *b = mgr.AcquireTailScratch(unused);
    char *c = mgr.AcquireTailScratch(unused);
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE(c != nullptr);
    // Pool is drained: free-list head is null. A further Acquire on a
    // populated-but-drained pool would block on `tail_scratch_waiting_`,
    // which requires `ThdTask()`; that path is covered in the Phase 8
    // e2e suite, not here.
    REQUIRE(mgr.tail_scratch_free_ == nullptr);

    // Release in order a, b, c -> free list head becomes c (LIFO push).
    mgr.ReleaseTailScratch(a);
    mgr.ReleaseTailScratch(b);
    mgr.ReleaseTailScratch(c);

    // Subsequent acquires return in reverse-release order: c, b, a.
    REQUIRE(mgr.AcquireTailScratch(unused) == c);
    REQUIRE(mgr.AcquireTailScratch(unused) == b);
    REQUIRE(mgr.AcquireTailScratch(unused) == a);
    REQUIRE(mgr.tail_scratch_free_ == nullptr);
}

TEST_CASE("N acquires followed by N releases restore the pool invariant",
          "[tail-scratch]")
{
    constexpr size_t kSlotSize = 64 * 1024;
    constexpr uint16_t kSlotCount = 8;

    eloqstore::KvOptions opts;
    opts.segment_size = kSlotSize;
    opts.pinned_tail_scratch_slots = kSlotCount;
    eloqstore::IouringMgr mgr(&opts, /*fd_limit=*/1);
    PopulateTailScratchPool(mgr, kSlotSize, kSlotCount, /*buf_idx=*/0);

    // Walk the free list before any acquire and record the count.
    auto count_free = [&]
    {
        size_t n = 0;
        char *p = mgr.tail_scratch_free_;
        while (p != nullptr)
        {
            char *next = nullptr;
            std::memcpy(&next, p, sizeof(char *));
            p = next;
            ++n;
        }
        return n;
    };
    REQUIRE(count_free() == kSlotCount);

    std::vector<char *> taken;
    uint16_t unused = 0;
    for (uint16_t i = 0; i < kSlotCount; ++i)
    {
        char *slot = mgr.AcquireTailScratch(unused);
        REQUIRE(slot != nullptr);
        taken.push_back(slot);
    }
    REQUIRE(count_free() == 0);

    // Release in arbitrary order (forward); the free list should grow by
    // one per release, with each released slot becoming the new head.
    for (size_t i = 0; i < taken.size(); ++i)
    {
        mgr.ReleaseTailScratch(taken[i]);
        REQUIRE(count_free() == i + 1);
    }
    REQUIRE(count_free() == kSlotCount);
}

TEST_CASE("ReleaseTailScratch is a no-op when pool is unallocated",
          "[tail-scratch]")
{
    eloqstore::KvOptions opts;
    opts.segment_size = 64 * 1024;
    opts.pinned_tail_scratch_slots = 0;
    eloqstore::IouringMgr mgr(&opts, /*fd_limit=*/1);

    // No backing buffer. Release with any pointer must not crash and must
    // not modify the (null) head.
    char fake;
    mgr.ReleaseTailScratch(&fake);
    REQUIRE(mgr.tail_scratch_free_ == nullptr);
}

TEST_CASE("IouringMgr::ReadSegments validates tail_size",
          "[tail-scratch][read-segments]")
{
    // ReadSegments runs the tail_size validation before any io_uring work,
    // so an empty input + an invalid tail_size triggers InvalidArgs without
    // touching the kernel. Empty inputs satisfy the size-match asserts.
    constexpr uint32_t kSegSize = 256 * 1024;
    eloqstore::KvOptions opts;
    opts.segment_size = kSegSize;
    eloqstore::IouringMgr mgr(&opts, /*fd_limit=*/1);

    const eloqstore::TableIdent tbl{"unused", 0};
    std::span<const eloqstore::FilePageId> seg_ids{};
    std::span<char *> dst_ptrs{};
    std::span<const uint16_t> buf_indices{};

    // tail_size == 0 (default) is accepted; empty input -> NoError.
    REQUIRE(mgr.ReadSegments(tbl, seg_ids, dst_ptrs, buf_indices, /*tail=*/0) ==
            eloqstore::KvError::NoError);

    // Not 4 KiB aligned -> InvalidArgs.
    REQUIRE(mgr.ReadSegments(tbl, seg_ids, dst_ptrs, buf_indices, 4097) ==
            eloqstore::KvError::InvalidArgs);
    REQUIRE(mgr.ReadSegments(tbl, seg_ids, dst_ptrs, buf_indices, 1) ==
            eloqstore::KvError::InvalidArgs);

    // > segment_size -> InvalidArgs.
    REQUIRE(mgr.ReadSegments(
                tbl, seg_ids, dst_ptrs, buf_indices, kSegSize + 4096) ==
            eloqstore::KvError::InvalidArgs);

    // == segment_size and 4 KiB-aligned values < segment_size are accepted.
    REQUIRE(mgr.ReadSegments(tbl, seg_ids, dst_ptrs, buf_indices, kSegSize) ==
            eloqstore::KvError::NoError);
    REQUIRE(mgr.ReadSegments(tbl, seg_ids, dst_ptrs, buf_indices, 4096) ==
            eloqstore::KvError::NoError);
}
