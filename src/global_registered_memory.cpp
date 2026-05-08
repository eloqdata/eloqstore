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
      head_(0)
{
    assert(segment_size >= kPageAlign);
    assert(segment_size % kPageAlign == 0);
    assert(chunk_size % segment_size == 0);
    assert(total_size % chunk_size == 0);
    assert(total_segments_ > 0);

    const size_t num_chunks = total_size / chunk_size;
    chunks_.reserve(num_chunks);

    // Allocate the separate successor-pointer array. Storing successors here
    // (rather than in the first 8 bytes of each segment) prevents caller I/O
    // writes from corrupting free-list metadata and causing ABA.
    successors_ = std::make_unique<std::atomic<uintptr_t>[]>(total_segments_);

    // Build the free list. successors_[i] stores the encoded successor for
    // segment i (Encode(next_ptr, next_chunk_idx, /*mark=*/false)).
    uintptr_t current_head = 0;  // encoded nullptr
    for (size_t c = 0; c < num_chunks; ++c)
    {
        char *base =
            static_cast<char *>(std::aligned_alloc(kPageAlign, chunk_size));
        assert(base != nullptr);
        chunks_.emplace_back(base, chunk_size);

        uint32_t chunk_idx = static_cast<uint32_t>(c);
        size_t meta_base = c * segments_per_chunk_;
        for (size_t i = 0; i < segments_per_chunk_; ++i)
        {
            char *segment = base + i * segment_size;
            successors_[meta_base + i].store(current_head,
                                             std::memory_order_relaxed);
            current_head = Encode(segment, chunk_idx, false);
        }
    }
    head_.store(current_head, std::memory_order_relaxed);
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
    uintptr_t enc = head_.load(std::memory_order_acquire);

    while (true)
    {
        char *seg = DecodePtr(enc);
        if (seg == nullptr)
        {
            return {nullptr, 0};
        }
        const uint32_t seg_chunk_idx = DecodeIdx(enc);

        // Read seg's successor from the metadata array, never from the segment
        // data itself (which may be overwritten by caller I/O).
        std::atomic<uintptr_t> &next_ref =
            successors_[SegmentIndex(seg, seg_chunk_idx)];
        uintptr_t next_enc = next_ref.load(std::memory_order_acquire);

        if (DecodeMark(next_enc))
        {
            // seg is already claimed by another thread. Help swing head to
            // seg's successor, then retry.
            const uintptr_t clean_next = next_enc & ~uintptr_t{1};
            if (head_.compare_exchange_weak(enc,
                                            clean_next,
                                            std::memory_order_release,
                                            std::memory_order_acquire))
            {
                enc = clean_next;
            }
            // On CAS failure enc is updated to the current head; retry either
            // way.
            continue;
        }

        // Try to mark seg's successor pointer to claim exclusive ownership of
        // seg.
        if (!next_ref.compare_exchange_weak(next_enc,
                                            next_enc | uintptr_t{1},
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire))
        {
            // Another thread modified seg's successor. Reload head and retry.
            enc = head_.load(std::memory_order_acquire);
            continue;
        }

        // We own seg. Attempt to swing head to seg's successor. Even if a
        // helper thread beats us, ownership belongs to whoever won the mark
        // CAS.
        head_.compare_exchange_weak(enc,
                                    next_enc & ~uintptr_t{1},
                                    std::memory_order_release,
                                    std::memory_order_acquire);

        free_count_.fetch_sub(1, std::memory_order_relaxed);
        return {seg, seg_chunk_idx};
    }
}

void GlobalRegisteredMemory::Recycle(char *segment, uint32_t chunk_index)
{
    assert(segment != nullptr);
    const uintptr_t seg_enc = Encode(segment, chunk_index, false);

    uintptr_t old_head = head_.load(std::memory_order_relaxed);
    std::atomic<uintptr_t> &meta =
        successors_[SegmentIndex(segment, chunk_index)];
    while (true)
    {
        // Publish old_head as segment's successor before swinging head.
        meta.store(old_head, std::memory_order_release);

        if (head_.compare_exchange_weak(old_head,
                                        seg_enc,
                                        std::memory_order_release,
                                        std::memory_order_relaxed))
        {
            break;
        }
        // old_head is updated by the CAS to the current head; retry.
    }
    free_count_.fetch_add(1, std::memory_order_relaxed);
}

void GlobalRegisteredMemory::SetEvictFunc(std::function<void()> func)
{
    evict_func_ = std::move(func);
}

}  // namespace eloqstore
