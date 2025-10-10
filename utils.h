#pragma once

#include <jsoncpp/json/reader.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

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

inline int RandomInt(int n)
{
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, n - 1);
    return dist(gen);
}

inline bool ParseRCloneListObjectsResponse(std::string &response_data,
                                           std::vector<std::string> &objects)
{
    Json::Value response;
    Json::Reader reader;
    if (reader.parse(response_data, response))
    {
        if (response.isMember("list") && response["list"].isArray())
        {
            objects.clear();
            for (const auto &item : response["list"])
            {
                if (item.isMember("Name") && item["Name"].isString())
                {
                    objects.push_back(item["Name"].asString());
                }
            }
        }
        return true;
    }
    return false;
}

}  // namespace utils