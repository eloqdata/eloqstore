#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <new>

#include "coding.h"

namespace eloqstore
{

class ManifestBuffer
{
public:
    ManifestBuffer() = default;
    ManifestBuffer(ManifestBuffer &&other) noexcept
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_)
    {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }
    ManifestBuffer &operator=(ManifestBuffer &&other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }
        std::free(data_);
        data_ = other.data_;
        size_ = other.size_;
        capacity_ = other.capacity_;
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
        return *this;
    }
    ManifestBuffer(const ManifestBuffer &) = delete;
    ManifestBuffer &operator=(const ManifestBuffer &) = delete;
    ~ManifestBuffer()
    {
        std::free(data_);
    }

    void Reserve(size_t bytes)
    {
        EnsureCapacity(bytes);
    }

    void Resize(size_t new_size)
    {
        EnsureCapacity(new_size);
        size_ = new_size;
    }

    void Reset()
    {
        size_ = 0;
    }

    void Append(const char *src, size_t len)
    {
        if (len == 0)
        {
            return;
        }
        EnsureCapacity(size_ + len);
        std::memcpy(data_ + size_, src, len);
        size_ += len;
    }

    void AppendVarint32(uint32_t value)
    {
        char buf[5];
        char *end = EncodeVarint32(buf, value);
        Append(buf, static_cast<size_t>(end - buf));
    }

    void AppendVarint64(uint64_t value)
    {
        char buf[10];
        char *end = EncodeVarint64(buf, value);
        Append(buf, static_cast<size_t>(end - buf));
    }

    char *Data()
    {
        return data_;
    }

    const char *Data() const
    {
        return data_;
    }

    size_t Size() const
    {
        return size_;
    }

    std::string_view View() const
    {
        return {data_, size_};
    }

private:
    static constexpr size_t kAlignment = 4096;

    static size_t AlignSize(size_t size)
    {
        if (size == 0)
        {
            return kAlignment;
        }
        const size_t remainder = size % kAlignment;
        if (remainder == 0)
        {
            return size;
        }
        return size + (kAlignment - remainder);
    }

    void EnsureCapacity(size_t min_capacity)
    {
        if (min_capacity <= capacity_)
        {
            return;
        }
        size_t grow = capacity_ == 0 ? kAlignment : capacity_ * 2;
        size_t new_capacity = std::max(AlignSize(min_capacity), AlignSize(grow));
        char *new_data = static_cast<char *>(
            std::aligned_alloc(kAlignment, new_capacity));
        if (new_data == nullptr)
        {
            throw std::bad_alloc();
        }
        if (data_ != nullptr)
        {
            std::memcpy(new_data, data_, size_);
            std::free(data_);
        }
        data_ = new_data;
        capacity_ = new_capacity;
    }

    char *data_{nullptr};
    size_t size_{0};
    size_t capacity_{0};
};

}  // namespace eloqstore
