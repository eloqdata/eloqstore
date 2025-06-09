#pragma once

#include <vector>

// #include "SPSCQueue.h"
#include "eloq_queue.h"

template <typename T>
class ReqQueue
{
public:
    ReqQueue() = delete;

    ReqQueue(size_t shard_id, size_t sub_queue_id)
        : queue_(1024), shard_id_(shard_id), sub_queue_id_(sub_queue_id)
    {
    }

    ReqQueue(ReqQueue &&rhs) = delete;
    ReqQueue(const ReqQueue &rhs) = delete;

    bool TryEnqueue(const T &element)
    {
        bool success = queue_.TryEnqueue(element);
        return success;
    }

    // bool TryDequeue(T &element)
    // {
    //     bool success = queue_.try_dequeue(element);
    //     if (success)
    //     {
    //         cnt_.fetch_sub(1, std::memory_order_relaxed);
    //     }
    //     return success;
    // }

    template <typename It>
    size_t TryDequeueBulk(It itemFirst, size_t max)
    {
        return queue_.TryDequeueBulk(itemFirst, max);
    }

private:
    eloq::SpscQueue<T> queue_;
    size_t shard_id_;
    size_t sub_queue_id_;
};

template <typename T>
class ShardQueue
{
public:
    ShardQueue(size_t core_num, size_t sid) : shard_id_(sid)
    {
        queues_.reserve(core_num);
        for (size_t sub_id = 0; sub_id < core_num; ++sub_id)
        {
            queues_.emplace_back(
                std::make_unique<ReqQueue<T>>(shard_id_, sub_id));
        }
    }

    ShardQueue(const ShardQueue &) = delete;

    ShardQueue(ShardQueue &&rhs)
        : queues_(std::move(rhs.queues_)), shard_id_(rhs.shard_id_)
    {
    }

    void Enqueue(const T &element, size_t core_id)
    {
        queues_[core_id]->TryEnqueue(element);
    }

    template <typename It>
    size_t TryDequeueBulk(It itemFirst, size_t max)
    {
        size_t total = 0;
        for (auto &req_queue : queues_)
        {
            size_t num = req_queue->TryDequeueBulk(itemFirst, max);
            max -= num;
            total += num;
            itemFirst += num;
        }

        return total;
    }

private:
    std::vector<std::unique_ptr<ReqQueue<T>>> queues_;
    size_t shard_id_;
};
