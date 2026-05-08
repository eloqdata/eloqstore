#pragma once

#include <cstddef>
#include <vector>

#include "io_buffer_ref.h"

namespace eloqstore
{

class GlobalRegisteredMemory;

/**
 * @brief Represents a big string comprising of a list of memory chunks.
 *
 * The memory chunks are references, pointing to the buffers in the storage
 * engine, i.e., IO uring registered buffers. This is to ensure zero copy when
 * reading the value from the storage engine, returning to the caller, caching
 * it in the upper tx service and sending the value to the remote client.
 */
class IoStringBuffer
{
public:
    IoStringBuffer();

    IoStringBuffer(const IoStringBuffer &) = delete;
    IoStringBuffer &operator=(const IoStringBuffer &) = delete;

    IoStringBuffer(IoStringBuffer &&other) noexcept;
    IoStringBuffer &operator=(IoStringBuffer &&other) noexcept;

    ~IoStringBuffer() = default;

    void Append(IoBufferRef data);

    /**
     * @brief Returns the actual byte size of the value.
     */
    size_t Size() const;

    void SetSize(size_t size);

    const std::vector<IoBufferRef> &Fragments() const;

    /**
     * @brief Recycles all fragments back to the global registered memory and
     * resets this buffer to the empty state.
     * @param mem The global registered memory these fragments were obtained
     *        from.
     * @param reg_mem_index_base The io_uring buffer index base that was added
     *        to the chunk index when GetSegment() was called. The chunk index
     *        passed to Recycle() is recovered as buf_index_ -
     * reg_mem_index_base.
     */
    void Recycle(GlobalRegisteredMemory *mem,
                 unsigned short reg_mem_index_base);

private:
    std::vector<IoBufferRef> fragments_;
    size_t size_{0};
};

}  // namespace eloqstore