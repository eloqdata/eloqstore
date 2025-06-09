#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <memory>
#include <tuple>
#include <vector>

namespace eloq
{
struct TailMeta
{
    std::atomic<uint16_t> tail_idx_{0};
    uint16_t head_cache_{0};
    char padding_[64 - sizeof(std::atomic<uint16_t>) - sizeof(uint16_t)];
};

template <typename T>
class ShardQueue
{
public:
    ShardQueue(size_t capacity, size_t core_cnt)
        : capacity_(capacity), core_cnt_(core_cnt)
    {
        data_ = std::make_unique<T[]>(capacity * core_cnt);
        head_idx_array_ = std::make_unique<std::atomic<uint16_t>[]>(core_cnt);
        tail_array_ = std::make_unique<TailMeta[]>(core_cnt);
        tail_snapshots_ = std::make_unique<uint16_t[]>(core_cnt);

        for (size_t cid = 0; cid < core_cnt; ++cid)
        {
            head_idx_array_[cid].store(0, std::memory_order_relaxed);
            tail_array_[cid].tail_idx_.store(0, std::memory_order_relaxed);
            tail_array_[cid].head_cache_ = 0;
        }
    }

    ShardQueue(const ShardQueue &) = delete;

    ShardQueue(ShardQueue &&rhs)
        : data_(std::move(rhs.data_)),
          head_idx_array_(std::move(rhs.head_idx_array_)),
          tail_array_(std::move(rhs.tail_array_)),
          tail_snapshots_(std::move(rhs.tail_snapshots_)),
          capacity_(rhs.capacity_),
          core_cnt_(rhs.core_cnt_)
    {
    }

    ~ShardQueue() = default;

    ShardQueue &operator=(ShardQueue &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        data_ = std::move(rhs.data_);
        head_idx_array_ = std::move(rhs.head_idx_array_);
        tail_array_ = std::move(rhs.tail_array_);
        tail_snapshots_ = std::move(rhs.tail_snapshots_);
        capacity_ = rhs.capacity_;
        core_cnt_ = rhs.core_cnt_;

        return *this;
    }

    bool TryEnqueue(T &&element,
                    uint16_t prod_core_id,
                    uint16_t *tail_rec = nullptr,
                    uint16_t con_core_id = UINT16_MAX)
    {
        assert(prod_core_id < core_cnt_);

        TailMeta &tail = tail_array_[prod_core_id];
        uint16_t tail_idx = tail.tail_idx_.load(std::memory_order_relaxed);
        uint16_t next_tail_idx = AdvanceLocalIdx(tail_idx);

        // The queue is full according to the cached head. Updates the head to
        // the newest value to see if it is really full.
        if (next_tail_idx == tail.head_cache_)
        {
            tail.head_cache_ =
                head_idx_array_[prod_core_id].load(std::memory_order_acquire);
            if (next_tail_idx == tail.head_cache_)
            {
                if (tail_rec != nullptr)
                {
                    *tail_rec = UINT16_MAX;
                }
                return false;
            }
        }

        size_t g_idx = GetGlobaIdx(tail_idx, prod_core_id);
        data_[g_idx] = std::move(element);
        if (tail_rec != nullptr)
        {
            *tail_rec = next_tail_idx;
        }
        tail.tail_idx_.store(next_tail_idx, std::memory_order_release);

        return true;
    }

    template <typename It>
    uint16_t TryEnqueueBulk(It itemFirst, uint16_t size, uint16_t prod_core_id)
    {
        assert(prod_core_id < core_cnt_);

        TailMeta &tail = tail_array_[prod_core_id];
        uint16_t tail_idx = tail.tail_idx_.load(std::memory_order_relaxed);
        uint16_t next_tail_idx = tail_idx;

        uint16_t cnt = 0;
        while (cnt < size)
        {
            next_tail_idx = AdvanceLocalIdx(tail_idx);
            // The ring buffer is full according to the cached head. Updates the
            // head cache to see if it is really full.
            if (next_tail_idx == tail.head_cache_)
            {
                tail.head_cache_ = head_idx_array_[prod_core_id].load(
                    std::memory_order_acquire);
                // The ring buffer is full.
                if (next_tail_idx == tail.head_cache_)
                {
                    // Rollbacks the tail index to last one.
                    next_tail_idx = tail_idx;
                    break;
                }
            }

            size_t g_idx = GetGlobaIdx(tail_idx, prod_core_id);
            data_[g_idx] = std::move(*itemFirst);

            tail_idx = next_tail_idx;
            itemFirst++;
            ++cnt;
        }

        tail.tail_idx_.store(next_tail_idx, std::memory_order_release);
        return cnt;
    }

    template <typename It>
    uint16_t TryDequeueBulk(It itemFirst,
                            uint16_t max,
                            std::vector<uint16_t> *head_rec = nullptr)
    {
        for (uint16_t cid = 0; cid < core_cnt_; ++cid)
        {
            TailMeta &tail = tail_array_[cid];
            tail_snapshots_[cid] =
                tail.tail_idx_.load(std::memory_order_acquire);

            if (head_rec != nullptr)
            {
                (*head_rec)[cid] = tail_snapshots_[cid];
            }
        }

        uint16_t total = 0;
        for (uint16_t cid = 0; cid < core_cnt_ && max > 0; ++cid)
        {
            uint16_t head_idx =
                head_idx_array_[cid].load(std::memory_order_relaxed);

            if (tail_snapshots_[cid] == head_idx)
            {
                continue;
            }

            uint16_t cnt = TryDequeueBulk(
                itemFirst, max, head_idx, tail_snapshots_[cid], cid);
            max -= cnt;
            total += cnt;
            itemFirst += cnt;
        }

        return total;
    }

private:
    uint16_t AdvanceLocalIdx(uint16_t idx) const
    {
        uint16_t next_idx = idx + 1;
        if (next_idx == capacity_)
        {
            next_idx = 0;
        }
        return next_idx;
    }

    size_t GetGlobaIdx(uint16_t idx, uint16_t core_id) const
    {
        return core_id * capacity_ + idx;
    }

    void RecordDeque(uint16_t core_id,
                     uint16_t begin,
                     uint16_t end1,
                     uint16_t end2)
    {
        if (core_id >= last_deque_.size())
        {
            last_deque_.resize(core_id + 1);
        }

        auto &deque_history = last_deque_[core_id];
        if (deque_history.size() >= 2048)
        {
            deque_history.erase(deque_history.begin(),
                                deque_history.begin() + 1024);
        }

        deque_history.emplace_back(begin, end1, end2);
    }

    template <typename It>
    uint16_t TryDequeueBulk(It it,
                            uint16_t max,
                            uint16_t head_idx,
                            uint16_t tail_idx,
                            uint16_t core_id)
    {
        uint16_t total = 0;
        uint16_t new_head;

        if (head_idx < tail_idx)
        {
            // Copies elements in [head_idx, tail_idx)
            uint16_t len = tail_idx - head_idx;
            total = std::min(len, max);
            size_t begin = GetGlobaIdx(head_idx, core_id);
            size_t end = GetGlobaIdx(head_idx + total, core_id);
            std::move(data_.get() + begin, data_.get() + end, it);
            new_head = head_idx + total;

            // RecordDeque(core_id, head_idx, head_idx + total, UINT16_MAX);
        }
        else
        {
            // Copies elements in [head_idx, capacity_)
            uint16_t seg1_len = capacity_ - head_idx;
            uint16_t size_1 = std::min(seg1_len, max);
            size_t begin1 = GetGlobaIdx(head_idx, core_id);
            size_t end1 = GetGlobaIdx(head_idx + size_1, core_id);
            std::move(data_.get() + begin1, data_.get() + end1, it);
            it += size_1;
            total = size_1;

            if (max > seg1_len)
            {
                // Copies elements in [0, tail)
                uint16_t seg2_len = tail_idx;
                uint16_t max_remain = max - seg1_len;
                uint16_t size_2 = std::min(seg2_len, max_remain);

                size_t begin2 = GetGlobaIdx(0, core_id);
                size_t end2 = GetGlobaIdx(size_2, core_id);
                std::move(data_.get() + begin2, data_.get() + end2, it);

                total += size_2;
                new_head = size_2;

                // RecordDeque(core_id, head_idx, head_idx + size_1, size_2);
            }
            else
            {
                new_head = head_idx + size_1;
                if (new_head == capacity_)
                {
                    new_head = 0;
                }

                // RecordDeque(core_id, head_idx, head_idx + size_1,
                // UINT16_MAX);
            }
        }

        head_idx_array_[core_id].store(new_head, std::memory_order_release);
        return total;
    }

    std::unique_ptr<T[]> data_;
    std::unique_ptr<std::atomic<uint16_t>[]> head_idx_array_;
    std::unique_ptr<TailMeta[]> tail_array_;
    std::unique_ptr<uint16_t[]> tail_snapshots_;

    const uint16_t capacity_;
    const uint16_t core_cnt_;

    std::vector<std::vector<std::tuple<uint16_t, uint16_t, uint16_t>>>
        last_deque_;
};
}  // namespace eloq