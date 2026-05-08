#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace eloqstore
{

struct MemChunk
{
    char *base_;
    size_t size_;
};

/**
 * @brief A global registered memory area shared across networking, in-memory
 * cache and the storage engine.
 *
 * This class represents a per-core memory area registered in io urings of both
 * the networking layer and EloqStore. It stores segments for very large string
 * values, enabling zero-copy I/O across modules.
 *
 * GetSegment() and Recycle() are thread-safe and lock-free. The free list is
 * a singly-linked stack using a Harris-style mark-then-CAS algorithm.
 *
 * Successor pointers are stored in a separate successors_ array (one
 * std::atomic<uintptr_t> per segment), NOT in the segment's own memory.
 * This is essential: once a segment is returned to a caller, the caller may
 * write arbitrary data into it (e.g. I/O buffers). If the successor were
 * embedded in the first 8 bytes of the segment, a stale thread could read
 * that corrupted data as a successor, pass the Harris mark-CAS on a lucky
 * coincidence, and write garbage into head_, crashing the next pop. The
 * separate successors_ array is only ever written by TryGetSegment and Recycle,
 * so it always contains a valid encoded value.
 *
 * Each entry in successors_ (and head_) is encoded as:
 *
 *   bits 63..16  — successor pointer shifted left 16 (x86-64 user-space
 *                  addresses fit in 48 bits, so the upper 16 bits are free)
 *   bits 15..1   — chunk_idx of the successor segment
 *   bit  0       — Harris mark bit (1 = this node is being popped)
 *
 * head_ uses the same layout with the mark bit always 0.
 */
class GlobalRegisteredMemory
{
public:
    /**
     * @brief Constructs the global registered memory.
     * @param segment_size Size of each segment in bytes (128KB-512KB). Must be
     *        4KB aligned.
     * @param chunk_size Size of each memory chunk in bytes. Must be a multiple
     *        of segment_size and 4KB aligned.
     * @param total_size Total memory to allocate in bytes. Must be a multiple
     *        of chunk_size.
     */
    GlobalRegisteredMemory(uint32_t segment_size,
                           size_t chunk_size,
                           size_t total_size);

    ~GlobalRegisteredMemory();

    GlobalRegisteredMemory(const GlobalRegisteredMemory &) = delete;
    GlobalRegisteredMemory &operator=(const GlobalRegisteredMemory &) = delete;
    GlobalRegisteredMemory(GlobalRegisteredMemory &&) = delete;
    GlobalRegisteredMemory &operator=(GlobalRegisteredMemory &&) = delete;

    /**
     * @brief Exposes memory chunks for io_uring buffer registration.
     *
     * The networking module and EloqStore register these chunks in their
     * respective io urings at initialization.
     */
    std::span<const MemChunk> MemChunks() const;

    /**
     * @brief Returns one segment of memory from the free list.
     *
     * The segment cannot be returned to other callers until it is explicitly
     * recycled via Recycle(). If no segment is available, calls evict_func_()
     * and the provided yield function, retrying until a segment is freed.
     *
     * @tparam YieldFunc A callable that yields the current coroutine.
     * @param yield The yield function to call when no segment is available.
     * @return A pair of (segment pointer, chunk index within chunks_). The
     *         caller computes the io_uring buffer index by adding its own
     *         registered buffer start index.
     */
    template <typename YieldFunc>
    std::pair<char *, uint32_t> GetSegment(YieldFunc &&yield)
    {
        while (true)
        {
            auto result = TryGetSegment();
            if (result.first != nullptr)
            {
                return result;
            }
            if (evict_func_)
            {
                evict_func_();
            }
            yield();
        }
    }

    /**
     * @brief Recycles a previously allocated segment, returning it to the free
     * list.
     * @param segment Pointer to the segment to recycle. Must have been
     *        previously returned by GetSegment().
     * @param chunk_index The chunk index returned by GetSegment().
     */
    void Recycle(char *segment, uint32_t chunk_index);

    /**
     * @brief Sets the eviction function called when no segments are available.
     *
     * Typically set by the in-memory cache module to evict cached large values
     * and recycle their segments.
     */
    void SetEvictFunc(std::function<void()> func);

    /**
     * @brief Returns the configured segment size in bytes.
     */
    uint32_t SegmentSize() const
    {
        return segment_size_;
    }

    /**
     * @brief Returns the total number of segments.
     */
    size_t TotalSegments() const
    {
        return total_segments_;
    }

    /**
     * @brief Returns the approximate number of free segments.
     */
    size_t FreeSegments() const
    {
        return free_count_.load(std::memory_order_relaxed);
    }

private:
    /**
     * @brief Tries to pop one segment from the free list without blocking.
     * @return A pair of (segment pointer, chunk index), or (nullptr, 0) if
     *         the free list is empty.
     */
    std::pair<char *, uint32_t> TryGetSegment();

    // Returns the flat index into successors_ for a given segment.
    size_t SegmentIndex(char *seg, uint32_t chunk_idx) const
    {
        return static_cast<size_t>(chunk_idx) * segments_per_chunk_ +
               static_cast<size_t>(seg - chunks_[chunk_idx].base_) /
                   segment_size_;
    }

    // Encode a free-list node: pointer in bits 63..16, chunk_idx in bits
    // 15..1, Harris mark bit in bit 0.
    static uintptr_t Encode(char *ptr, uint32_t chunk_idx, bool mark)
    {
        return (reinterpret_cast<uintptr_t>(ptr) << 16) |
               (static_cast<uintptr_t>(chunk_idx) << 1) |
               static_cast<uintptr_t>(mark ? 1 : 0);
    }
    static char *DecodePtr(uintptr_t v)
    {
        return reinterpret_cast<char *>(v >> 16);
    }
    static uint32_t DecodeIdx(uintptr_t v)
    {
        return static_cast<uint32_t>((v >> 1) & 0x7FFFu);
    }
    static bool DecodeMark(uintptr_t v)
    {
        return v & 1u;
    }

    uint32_t segment_size_;
    size_t segments_per_chunk_;
    size_t total_segments_;
    std::atomic<size_t> free_count_;
    // Free-list head. 0 means the list is empty.
    std::atomic<uintptr_t> head_;
    // Per-segment successor pointers, stored separately from segment data.
    // successors_[SegmentIndex(seg, chunk_idx)] holds the encoded successor of
    // seg.
    std::unique_ptr<std::atomic<uintptr_t>[]> successors_;
    std::function<void()> evict_func_;
    std::vector<MemChunk> chunks_;
};

}  // namespace eloqstore
