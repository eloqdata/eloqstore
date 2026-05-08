#include "io_string_buffer.h"

#include "global_registered_memory.h"

namespace eloqstore
{

IoStringBuffer::IoStringBuffer()
{
    fragments_.reserve(16);
}

IoStringBuffer::IoStringBuffer(IoStringBuffer &&other) noexcept
    : fragments_(std::move(other.fragments_)), size_(other.size_)
{
    other.size_ = 0;
}

IoStringBuffer &IoStringBuffer::operator=(IoStringBuffer &&other) noexcept
{
    if (this != &other)
    {
        fragments_ = std::move(other.fragments_);
        size_ = other.size_;
        other.size_ = 0;
    }
    return *this;
}

void IoStringBuffer::Append(IoBufferRef data)
{
    fragments_.emplace_back(data);
}

size_t IoStringBuffer::Size() const
{
    return size_;
}

void IoStringBuffer::SetSize(size_t size)
{
    size_ = size;
}

const std::vector<IoBufferRef> &IoStringBuffer::Fragments() const
{
    return fragments_;
}

void IoStringBuffer::Recycle(GlobalRegisteredMemory *mem,
                             unsigned short reg_mem_index_base)
{
    for (IoBufferRef &frag : fragments_)
    {
        uint32_t chunk_index =
            static_cast<uint32_t>(frag.buf_index_ - reg_mem_index_base);
        mem->Recycle(frag.data_, chunk_index);
    }
    fragments_.clear();
    size_ = 0;
}

}  // namespace eloqstore
