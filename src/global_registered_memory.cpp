#include "global_registered_memory.h"

#include <atomic>
#include <cassert>
#include <cstdlib>

namespace eloqstore
{

static constexpr size_t kPageAlign = 4096;

GlobalRegisteredMemory::GlobalRegisteredMemory(uint32_t segment_size,
                                               size_t chunk_size,
                                               size_t total_size)
    : segment_size_(segment_size),
      segments_per_chunk_(chunk_size / segment_size),
      total_segments_(total_size / segment_size),
      free_count_(0),
      head_(EncodeHead(0, kInvalidIdx))
{
    assert(segment_size >= kPageAlign);
    assert(segment_size % kPageAlign == 0);
    assert(chunk_size % segment_size == 0);
    assert(total_size % chunk_size == 0);
    assert(total_segments_ > 0);
    // The 32-bit seg-index field in head_ must cover every segment plus the
    // kInvalidIdx sentinel.
    assert(total_segments_ < static_cast<size_t>(kInvalidIdx));

    const size_t num_chunks = total_size / chunk_size;
    chunks_.reserve(num_chunks);

    // One next-pointer slot per segment. successors_[i] is the next free-list
    // index when segment i is on the free list. Stored separately from
    // segment data so caller-owned I/O writes can't corrupt the metadata.
    successors_ = std::make_unique<std::atomic<uint32_t>[]>(total_segments_);

    // Build the free list. Push every segment; order doesn't affect
    // correctness (stack semantics).
    uint32_t current_head_idx = kInvalidIdx;
    for (size_t c = 0; c < num_chunks; ++c)
    {
        char *base =
            static_cast<char *>(std::aligned_alloc(kPageAlign, chunk_size));
        assert(base != nullptr);
        chunks_.emplace_back(base, chunk_size);

        const uint32_t chunk_idx = static_cast<uint32_t>(c);
        const uint32_t base_idx =
            chunk_idx * static_cast<uint32_t>(segments_per_chunk_);
        for (size_t i = 0; i < segments_per_chunk_; ++i)
        {
            const uint32_t seg_idx = base_idx + static_cast<uint32_t>(i);
            successors_[seg_idx].store(current_head_idx,
                                       std::memory_order_relaxed);
            current_head_idx = seg_idx;
        }
    }
    // Initial version 0; any starting value is fine -- the version only has
    // to be monotonically incremented from here.
    head_.store(EncodeHead(0, current_head_idx), std::memory_order_relaxed);
    free_count_.store(total_segments_, std::memory_order_relaxed);
}

GlobalRegisteredMemory::~GlobalRegisteredMemory()
{
    for (auto &chunk : chunks_)
    {
        std::free(chunk.base_);
    }
}

std::span<const MemChunk> GlobalRegisteredMemory::MemChunks() const
{
    return chunks_;
}

std::pair<char *, uint32_t> GlobalRegisteredMemory::TryGetSegment()
{
    // ABA-tagged Treiber pop. The version field in head_ ensures that a CAS
    // observing a stale (top, ver) tuple always fails, even if `top` has
    // been popped and pushed back in the interim.
    uint64_t cur = head_.load(std::memory_order_acquire);
    while (true)
    {
        const uint32_t cur_idx = DecodeIdx(cur);
        if (cur_idx == kInvalidIdx)
        {
            return {nullptr, 0};
        }
        // The acquire-load on head_ synchronizes with the release-CAS in the
        // Recycle that put cur_idx at the top, so the successors_[cur_idx]
        // store from that Recycle is visible here under relaxed.
        const uint32_t next_idx =
            successors_[cur_idx].load(std::memory_order_relaxed);
        const uint64_t new_head = EncodeHead(DecodeVer(cur) + 1, next_idx);
        if (head_.compare_exchange_weak(cur,
                                        new_head,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire))
        {
            free_count_.fetch_sub(1, std::memory_order_relaxed);
            return {SegmentPtr(cur_idx), ChunkIdxOf(cur_idx)};
        }
        // CAS failed; `cur` now holds the current head. Loop with the new
        // value -- the failure path's acquire keeps the next iteration's
        // successors_ load consistent.
    }
}

void GlobalRegisteredMemory::Recycle(char *segment, uint32_t chunk_index)
{
    assert(segment != nullptr);
    const uint32_t seg_idx = SegmentIndex(segment, chunk_index);

    uint64_t cur = head_.load(std::memory_order_relaxed);
    while (true)
    {
        // Publish the new next-pointer for seg_idx before swinging head_.
        // The release on head_'s CAS below orders this store; any later
        // acquire-load of head_ that observes the new (ver+1, seg_idx)
        // value also sees this successors_ store.
        successors_[seg_idx].store(DecodeIdx(cur), std::memory_order_relaxed);
        const uint64_t new_head = EncodeHead(DecodeVer(cur) + 1, seg_idx);
        if (head_.compare_exchange_weak(cur,
                                        new_head,
                                        std::memory_order_release,
                                        std::memory_order_relaxed))
        {
            free_count_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        // `cur` updated by CAS to the current head; retry.
    }
}

void GlobalRegisteredMemory::SetEvictFunc(std::function<void()> func)
{
    evict_func_ = std::move(func);
}

}  // namespace eloqstore
