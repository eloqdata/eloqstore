#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace chrono = std::chrono;

namespace utils
{
struct CloudObjectInfo
{
    std::string name;
    std::string path;
    uint64_t size{0};
    bool is_dir{false};
    std::string mod_time;
    std::string continuation_token;  // For pagination
};

template <typename T>
inline T UnsetLowBits(T num, uint8_t n)
{
    assert(n < (sizeof(T) * 8));
    return num & (~((uint64_t(1) << n) - 1));
}

template <typename T>
uint64_t UnixTs()
{
    auto dur = chrono::system_clock::now().time_since_epoch();
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

inline int RandomInt(int n)
{
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, n - 1);
    return dist(gen);
}

}  // namespace utils

namespace eloqstore
{
constexpr size_t kDefaultStorePathLutEntries = 1 << 20;

inline std::vector<uint32_t> ComputeStorePathLut(
    const std::vector<uint64_t> &weights,
    size_t max_entries = kDefaultStorePathLutEntries)
{
    std::vector<uint32_t> lut;
    if (weights.empty() || max_entries == 0)
    {
        return lut;
    }

    std::vector<uint64_t> normalized(weights.begin(), weights.end());
    for (uint64_t &weight : normalized)
    {
        if (weight == 0)
        {
            weight = 1;
        }
    }

    uint64_t gcd_value = normalized.front();
    for (size_t i = 1; i < normalized.size(); ++i)
    {
        gcd_value = std::gcd(gcd_value, normalized[i]);
    }
    if (gcd_value > 1)
    {
        for (uint64_t &weight : normalized)
        {
            weight /= gcd_value;
        }
    }

    size_t total_slots = 0;
    for (uint64_t weight : normalized)
    {
        total_slots += weight;
    }
    if (total_slots == 0)
    {
        total_slots = normalized.size();
        for (uint64_t &weight : normalized)
        {
            weight = 1;
        }
    }

    if (total_slots > max_entries)
    {
        size_t scale = (total_slots + max_entries - 1) / max_entries;
        total_slots = 0;
        for (uint64_t &weight : normalized)
        {
            weight = std::max<uint64_t>(1, weight / scale);
            total_slots += weight;
        }
    }

    lut.reserve(total_slots);
    for (size_t idx = 0; idx < normalized.size(); ++idx)
    {
        for (uint64_t slot = 0; slot < normalized[idx]; ++slot)
        {
            lut.push_back(static_cast<uint32_t>(idx));
        }
    }
    return lut;
}
}  // namespace eloqstore
