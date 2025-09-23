#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>

namespace chrono = std::chrono;

namespace utils
{
template <typename T>
inline T UnsetLowBits(T num, uint8_t n)
{
    assert(n < (sizeof(T) * 8));
    return num & (~((uint64_t(1) << n) - 1));
}

template <typename T>
uint64_t UnixTs()
{
    auto dur = chrono::steady_clock::now().time_since_epoch();
    return chrono::duration_cast<T>(dur).count();
}

static size_t DirEntryCount(std::filesystem::path path)
{
    return std::distance(std::filesystem::directory_iterator(path),
                         std::filesystem::directory_iterator{});
}

[[maybe_unused]] static size_t CountUsedFD()
{
    return DirEntryCount("/proc/self/fd");
}

template <typename F>
struct YCombinator
{
    F f;
    template <typename... Args>
    decltype(auto) operator()(Args &&...args) const
    {
        return f(*this, std::forward<Args>(args)...);
    }
};

template <typename F>
YCombinator<std::decay_t<F>> MakeYCombinator(F &&f)
{
    return {std::forward<F>(f)};
}

}  // namespace utils