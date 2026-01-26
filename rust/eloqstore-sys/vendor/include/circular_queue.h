#pragma once

#include <memory>
#include <stdexcept>
#include <utility>

namespace eloqstore
{

static void ValidateCapacity(size_t capacity)
{
    if (capacity == 0 || (capacity & (capacity - 1)) != 0)
    {
        throw std::invalid_argument(
            "CircularQueue capacity must be a power of two");
    }
}

template <typename T>
class CircularQueue
{
public:
    explicit CircularQueue(size_t capacity = 8) : head_(0), cnt_(0)
    {
        ValidateCapacity(capacity);
        capacity_ = capacity;
        mask_ = capacity_ - 1;
        vec_ = std::make_unique<T[]>(capacity_);
    }

    CircularQueue(CircularQueue &&rhs) noexcept
    {
        head_ = rhs.head_;
        cnt_ = rhs.cnt_;
        capacity_ = rhs.capacity_;
        mask_ = rhs.mask_;
        vec_ = std::move(rhs.vec_);
    }

    CircularQueue &operator=(CircularQueue &&rhs) noexcept
    {
        if (this != &rhs)
        {
            head_ = rhs.head_;
            cnt_ = rhs.cnt_;
            capacity_ = rhs.capacity_;
            mask_ = rhs.mask_;
            vec_ = std::move(rhs.vec_);
        }
        return *this;
    }

    CircularQueue(const CircularQueue &rhs) = delete;
    CircularQueue &operator=(const CircularQueue &rhs) = delete;

    ~CircularQueue() = default;

    void Reset(size_t new_cap)
    {
        head_ = 0;
        cnt_ = 0;
        ValidateCapacity(new_cap);
        capacity_ = new_cap;
        mask_ = capacity_ - 1;
        vec_ = std::make_unique<T[]>(capacity_);
    }

    void Enqueue(const T &item)
    {
        if (__builtin_expect(cnt_ != capacity_, 1))
        {
            size_t tail = (head_ + cnt_) & mask_;
            vec_[tail] = item;
            ++cnt_;
            return;
        }

        size_t new_capacity = capacity_ << 1;
        std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

        if (capacity_ > 0)
        {
            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            std::copy(
                vec_.get() + head_, vec_.get() + capacity_, new_vec.get());

            size_t half_cnt = capacity_ - head_;
            // Copy 0 --> Tail
            std::copy(vec_.get(), vec_.get() + head_, new_vec.get() + half_cnt);
        }

        capacity_ = new_capacity;
        mask_ = capacity_ - 1;
        vec_ = std::move(new_vec);
        head_ = 0;

        size_t tail = (head_ + cnt_) & mask_;
        vec_[tail] = item;
        ++cnt_;
    }

    void Enqueue(T &&item)
    {
        if (__builtin_expect(cnt_ != capacity_, 1))
        {
            size_t tail = (head_ + cnt_) & mask_;
            vec_[tail] = std::move(item);
            ++cnt_;
            return;
        }

        size_t new_capacity = capacity_ << 1;
        std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

        if (capacity_ > 0)
        {
            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            size_t end = 0;
            for (size_t idx = head_; idx < capacity_; ++idx, ++end)
            {
                new_vec[end] = std::move(vec_[idx]);
            }

            // Copy 0 --> Tail
            for (size_t idx = 0; idx < head_; ++idx, ++end)
            {
                new_vec[end] = std::move(vec_[idx]);
            }
        }

        capacity_ = new_capacity;
        mask_ = capacity_ - 1;
        head_ = 0;
        vec_ = std::move(new_vec);

        size_t tail = (head_ + cnt_) & mask_;
        vec_[tail] = std::move(item);
        ++cnt_;
    }

    void EnqueueAsFirst(const T &item)
    {
        if (cnt_ == 0)
        {
            vec_[0] = item;
            head_ = 0;
            cnt_ = 1;
        }
        else if (cnt_ == capacity_)
        {
            size_t new_capacity = capacity_ << 1;
            std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            std::copy(
                vec_.get() + head_, vec_.get() + capacity_, new_vec.get());

            size_t half_cnt = capacity_ - head_;
            // Copy 0 --> Tail
            std::copy(vec_.get(), vec_.get() + head_, new_vec.get() + half_cnt);

            new_vec[new_capacity - 1] = item;
            head_ = new_capacity - 1;
            cnt_ = capacity_ + 1;
            capacity_ = new_capacity;
            mask_ = capacity_ - 1;
            vec_ = std::move(new_vec);
        }
        else
        {
            if (head_ != 0)
            {
                vec_[head_ - 1] = item;
                --head_;
            }
            else
            {
                vec_[capacity_ - 1] = item;
                head_ = capacity_ - 1;
            }
            ++cnt_;
        }
    }

    void Dequeue()
    {
        assert(cnt_ > 0);
        head_ = (head_ + 1) & mask_;
        --cnt_;
    }

    T &Peek()
    {
        return vec_[head_];
    }

    size_t Size() const
    {
        return cnt_;
    }

    size_t Capacity() const
    {
        return capacity_;
    }

    size_t MemUsage() const
    {
        return sizeof(CircularQueue) + capacity_ * sizeof(T);
    }

    T &Get(size_t index) const
    {
        return vec_[(head_ + index) & mask_];
    }

    void Erase(size_t index)
    {
        while (index < cnt_ - 1)
        {
            vec_[(head_ + index) & mask_] = vec_[(head_ + index + 1) & mask_];
            index++;
        }
        cnt_--;
        if (cnt_ == 0)
        {
            head_ = 0;
        }
    }

private:
    std::unique_ptr<T[]> vec_;
    size_t head_;
    size_t cnt_;
    size_t capacity_;
    size_t mask_;
};
}  // namespace eloqstore
