#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
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
 * This class represents a per-core memory area registered in io urings of
 * both the networking layer and EloqStore. It stores segments for very
 * large string values, enabling zero-copy I/O across modules.
 *
 * GetSegment() and Recycle() are thread-safe and lock-free. The free list
 * is an ABA-tagged Treiber stack:
 *
 *   - Every segment has a stable global index in [0, total_segments_).
 *     The index encodes both chunk_idx and offset-within-chunk and is
 *     fully recoverable to a (char*, chunk_idx) pair via SegmentPtr() /
 *     ChunkIdxOf().
 *   - `head_` is a 64-bit atomic carrying `(version : 32, seg_idx : 32)`.
 *     `seg_idx == kInvalidIdx` (== UINT32_MAX) means the stack is empty.
 *     Every successful push/pop bumps the version, so any CAS that loaded
 *     a stale `head_` will fail. This eliminates the classic ABA hazard
 *     where a segment is popped, handed to a caller, and recycled before
 *     a concurrent popper finishes its CAS.
 *   - `successors_[i]` holds the next free-list seg_idx when i is on the
 *     free list. It is mutated only by the Push (Recycle) that makes i
 *     the new top, before that Push's release-CAS on `head_`; reads are
 *     performed by the Pop (TryGetSegment) that observes i as the top.
 *
 * Storing successors in a separate array (rather than in the segment's own
 * memory) is still important: the caller may write arbitrary data into a
 * segment it owns, and that data must never be reinterpreted as free-list
 * metadata.
 *
 * Memory ordering: Recycle stores successors_[i] then release-CASes head_;
 * TryGetSegment acquire-loads head_ and then loads successors_[top] under
 * the established release-acquire synchronization. The CAS pair on head_
 * is the only barrier the readers need.
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

    // Sentinel "no successor" / "empty stack" value. uint32_t indices range
    // [0, total_segments_); the max value is reserved as the sentinel.
    static constexpr uint32_t kInvalidIdx =
        std::numeric_limits<uint32_t>::max();

    // (version, seg_idx) packing for head_.
    static uint64_t EncodeHead(uint32_t ver, uint32_t idx)
    {
        return (static_cast<uint64_t>(ver) << 32) | static_cast<uint64_t>(idx);
    }
    static uint32_t DecodeVer(uint64_t enc)
    {
        return static_cast<uint32_t>(enc >> 32);
    }
    static uint32_t DecodeIdx(uint64_t enc)
    {
        return static_cast<uint32_t>(enc & 0xFFFFFFFFu);
    }

    // (char*, chunk_idx) -> stable global segment index.
    uint32_t SegmentIndex(char *seg, uint32_t chunk_idx) const
    {
        return chunk_idx * static_cast<uint32_t>(segments_per_chunk_) +
               static_cast<uint32_t>((seg - chunks_[chunk_idx].base_) /
                                     segment_size_);
    }
    // Inverse of SegmentIndex.
    char *SegmentPtr(uint32_t idx) const
    {
        const uint32_t chunk_idx =
            idx / static_cast<uint32_t>(segments_per_chunk_);
        const uint32_t in_chunk =
            idx % static_cast<uint32_t>(segments_per_chunk_);
        return chunks_[chunk_idx].base_ +
               static_cast<size_t>(in_chunk) * segment_size_;
    }
    uint32_t ChunkIdxOf(uint32_t idx) const
    {
        return idx / static_cast<uint32_t>(segments_per_chunk_);
    }

    uint32_t segment_size_;
    size_t segments_per_chunk_;
    size_t total_segments_;
    std::atomic<size_t> free_count_;
    // Free-list head: high 32 bits = version, low 32 bits = top seg index
    // (kInvalidIdx when empty). Every push/pop bumps the version, so a
    // stale-head CAS always fails.
    std::atomic<uint64_t> head_;
    // Per-segment next-pointer. successors_[i] is the seg index that
    // follows i on the free list. Written by Push only (Recycle) before
    // its release-CAS on head_; read by Pop (TryGetSegment) when i is
    // observed as the top under acquire on head_.
    std::unique_ptr<std::atomic<uint32_t>[]> successors_;
    std::function<void()> evict_func_;
    std::vector<MemChunk> chunks_;
};

}  // namespace eloqstore
