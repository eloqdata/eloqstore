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
struct CloudObjectInfo
{
    std::string name;
    std::string path;
    uint64_t size{0};
    bool is_dir{false};
    std::string mod_time;
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

inline bool ParseRCloneListObjectsResponse(
    std::string &response_data,
    std::vector<std::string> *objects,
    std::vector<CloudObjectInfo> *object_infos = nullptr)
{
    Json::Value response;
    Json::Reader reader;
    if (reader.parse(response_data, response))
    {
        if (response.isMember("list") && response["list"].isArray())
        {
            if (objects != nullptr)
            {
                objects->clear();
            }
            if (object_infos != nullptr)
            {
                object_infos->clear();
            }
            for (const auto &item : response["list"])
            {
                if (objects != nullptr && item.isMember("Name") &&
                    item["Name"].isString())
                {
                    objects->push_back(item["Name"].asString());
                }
                if (object_infos != nullptr)
                {
                    CloudObjectInfo info;
                    if (item.isMember("Name") && item["Name"].isString())
                    {
                        info.name = item["Name"].asString();
                    }
                    if (item.isMember("Path") && item["Path"].isString())
                    {
                        info.path = item["Path"].asString();
                    }
                    if (item.isMember("ModTime") && item["ModTime"].isString())
                    {
                        info.mod_time = item["ModTime"].asString();
                    }
                    if (item.isMember("Size"))
                    {
                        const Json::Value &size_value = item["Size"];
                        if (size_value.isUInt64())
                        {
                            info.size = size_value.asUInt64();
                        }
                        else if (size_value.isString())
                        {
                            try
                            {
                                info.size =
                                    std::stoull(size_value.asString(), nullptr);
                            }
                            catch (const std::exception &)
                            {
                                info.size = 0;
                            }
                        }
                    }
                    if (item.isMember("IsDir") && item["IsDir"].isBool())
                    {
                        info.is_dir = item["IsDir"].asBool();
                    }
                    object_infos->push_back(std::move(info));
                }
            }
        }
        return true;
    }
    return false;
}

}  // namespace utils
