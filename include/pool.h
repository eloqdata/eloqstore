#pragma once

#include <utility>
#include <vector>
#include <glog/logging.h>

namespace eloqstore
{

template <typename T>
class Pool
{
public:
    explicit Pool(size_t max_cached = 0) : max_cached_(max_cached)
    {
        pool_.reserve(max_cached_);
    }

    T Acquire()
    {
        if (pool_.empty())
        {
            LOG(INFO) << "Pool Acquire " << typeid(*this).name();
            return T();
        }
        T value = std::move(pool_.back());
        pool_.pop_back();
        value.clear();
        return value;
    }

    void Release(T &&value)
    {
        if (max_cached_ != 0 && pool_.size() >= max_cached_)
        {
            LOG(INFO) << "Pool Release";
            return;
        }
        pool_.push_back(std::move(value));
    }

private:
    size_t max_cached_;
    std::deque<T> pool_;
};

}  // namespace eloqstore
