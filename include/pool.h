#pragma once

#include <deque>
#include <type_traits>
#include <utility>

namespace eloqstore
{

template <typename T>
class Pool
{
public:
    explicit Pool(size_t max_cached = 0, bool preallocate = false)
        : max_cached_(max_cached)
    {
        if (preallocate)
        {
            Preallocate();
        }
    };

    T Acquire()
    {
        if (pool_.empty())
        {
            T value;
            ReserveHelper<T>::Call(value);
            return value;
        }
        T value = std::move(pool_.back());
        pool_.pop_back();
        value.clear();
        ReserveHelper<T>::Call(value);
        return value;
    }

    void Release(T &&value)
    {
        if (max_cached_ != 0 && pool_.size() >= max_cached_)
        {
            return;
        }
        pool_.push_back(std::move(value));
    }

private:
    size_t max_cached_;
    std::deque<T> pool_;
    void Preallocate()
    {
        if (max_cached_ == 0)
        {
            return;
        }
        for (size_t i = pool_.size(); i < max_cached_; ++i)
        {
            T value;
            ReserveHelper<T>::Call(value);
            pool_.push_back(std::move(value));
        }
    }

    template <typename U, typename = void>
    struct ReserveHelper
    {
        static void Call(U &)
        {
        }
    };

    template <typename U>
    struct ReserveHelper<
        U,
        std::void_t<decltype(std::declval<U &>().EnsureDefaultReserve())>>
    {
        static void Call(U &value)
        {
            value.EnsureDefaultReserve();
        }
    };
};

}  // namespace eloqstore
