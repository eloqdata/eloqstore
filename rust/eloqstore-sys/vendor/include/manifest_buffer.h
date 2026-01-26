#pragma once

#include <cstring>
#include <string_view>

#include "coding.h"
#include "direct_io_buffer.h"

namespace eloqstore
{

class ManifestBuffer : public DirectIoBuffer
{
public:
    ManifestBuffer() = default;
    ManifestBuffer(ManifestBuffer &&) noexcept = default;
    ManifestBuffer &operator=(ManifestBuffer &&) noexcept = default;
    ManifestBuffer(const ManifestBuffer &) = delete;
    ManifestBuffer &operator=(const ManifestBuffer &) = delete;
    ~ManifestBuffer() = default;

    using DirectIoBuffer::append;
    using DirectIoBuffer::assign;
    using DirectIoBuffer::clear;
    using DirectIoBuffer::data;
    using DirectIoBuffer::padded_size;
    using DirectIoBuffer::reserve;
    using DirectIoBuffer::resize;
    using DirectIoBuffer::size;
    using DirectIoBuffer::view;

    void AppendVarint32(uint32_t value)
    {
        char buf[5];
        char *end = EncodeVarint32(buf, value);
        append(buf, static_cast<size_t>(end - buf));
    }

    void AppendVarint64(uint64_t value)
    {
        char buf[10];
        char *end = EncodeVarint64(buf, value);
        append(buf, static_cast<size_t>(end - buf));
    }

    void AlignTo(uint64_t alignment)
    {
        size_ = (size_ + alignment - 1) & ~(alignment - 1);
    }

    std::string_view View() const
    {
        return view();
    }
};

}  // namespace eloqstore
