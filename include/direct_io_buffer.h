#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>
#include <utility>

#include "storage/page.h"

namespace eloqstore
{

class DirectIoBuffer
{
public:
    DirectIoBuffer() = default;
    DirectIoBuffer(DirectIoBuffer &&other) noexcept
        : data_(std::exchange(other.data_, nullptr)),
          size_(std::exchange(other.size_, 0)),
          capacity_(std::exchange(other.capacity_, 0))
    {
    }
    DirectIoBuffer &operator=(DirectIoBuffer &&other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }
        std::free(data_);
        data_ = std::exchange(other.data_, nullptr);
        size_ = std::exchange(other.size_, 0);
        capacity_ = std::exchange(other.capacity_, 0);
        return *this;
    }
    DirectIoBuffer(const DirectIoBuffer &) = delete;
    DirectIoBuffer &operator=(const DirectIoBuffer &) = delete;
    ~DirectIoBuffer()
    {
        std::free(data_);
    }

    void reserve(size_t bytes)
    {
        EnsureCapacity(bytes);
    }

    void resize(size_t new_size)
    {
        EnsureCapacity(new_size);
        size_ = new_size;
    }

    void clear()
    {
        size_ = 0;
    }

    void assign(std::string_view src)
    {
        if (src.empty())
        {
            clear();
            return;
        }
        EnsureCapacity(src.size());
        size_ = src.size();
        std::memcpy(data_, src.data(), src.size());
    }

    void append(const char *src, size_t len)
    {
        if (len == 0)
        {
            return;
        }
        EnsureCapacity(size_ + len);
        std::memcpy(data_ + size_, src, len);
        size_ += len;
    }

    void append(std::string_view src)
    {
        append(src.data(), src.size());
    }

    static void UpdateDefaultReserve(size_t bytes)
    {
        default_reserve_bytes_.store(bytes);
    }

    void EnsureDefaultReserve()
    {
        size_t reserve = default_reserve_bytes_.load(std::memory_order_relaxed);
        if (reserve == 0 || capacity_ >= reserve)
        {
            return;
        }
        EnsureCapacity(reserve);
        if (size_ > reserve)
        {
            size_ = reserve;
        }
    }

    char *data()
    {
        return data_;
    }

    const char *data() const
    {
        return data_;
    }

    size_t size() const
    {
        return size_;
    }

    bool empty() const
    {
        return size_ == 0;
    }

    size_t capacity() const
    {
        return capacity_;
    }

    size_t alignment() const
    {
        return page_align;
    }

    size_t padded_size() const
    {
        if (data_ == nullptr || size_ == 0)
        {
            return 0;
        }
        return AlignSize(size_);
    }

    std::string_view view() const
    {
        return {data_, size_};
    }

protected:
    static size_t AlignSize(size_t size)
    {
        return (size + page_align - 1) & ~(page_align - 1);
    }

    void EnsureCapacity(size_t min_capacity)
    {
        if (min_capacity <= capacity_)
        {
            return;
        }
        const size_t alignment = page_align;
        size_t grow = capacity_ == 0 ? alignment : capacity_ * 2;
        size_t new_capacity =
            std::max(AlignSize(min_capacity), AlignSize(grow));
        char *new_data =
            static_cast<char *>(std::aligned_alloc(alignment, new_capacity));
        if (new_data == nullptr)
        {
            throw std::bad_alloc();
        }
        std::memset(new_data, 0, new_capacity);
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
    static inline std::atomic<size_t> default_reserve_bytes_{0};
};

}  // namespace eloqstore
