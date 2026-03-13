#pragma once

#include <algorithm>
#include <cassert>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
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

inline std::string TrimAsciiWhitespace(std::string value)
{
    auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };
    auto begin = std::find_if_not(value.begin(), value.end(), is_space);
    auto end = std::find_if_not(value.rbegin(), value.rend(), is_space).base();
    if (begin >= end)
    {
        return "";
    }
    return std::string(begin, end);
}

inline void ParseCsvStringList(std::string_view input,
                               std::vector<std::string> &out)
{
    out.clear();
    std::string token;
    std::istringstream token_stream{std::string(input)};
    while (std::getline(token_stream, token, ','))
    {
        token = TrimAsciiWhitespace(token);
        if (!token.empty())
        {
            out.emplace_back(std::move(token));
        }
    }
}

inline bool ParseStorePathListWithWeights(std::string_view input,
                                          std::vector<std::string> &paths,
                                          std::vector<uint64_t> &weights,
                                          std::string *error_message = nullptr)
{
    paths.clear();
    weights.clear();

    std::string value = TrimAsciiWhitespace(std::string(input));
    if (value.empty())
    {
        return true;
    }

    std::string_view path_part = value;
    std::string_view weight_part;
    size_t colon_pos = value.find(':');
    if (colon_pos != std::string::npos)
    {
        path_part = std::string_view(value.data(), colon_pos);
        weight_part = std::string_view(value.data() + colon_pos + 1,
                                       value.size() - colon_pos - 1);
    }

    ParseCsvStringList(path_part, paths);
    if (paths.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "empty path list";
        }
        return false;
    }
    if (weight_part.empty())
    {
        return true;
    }

    std::string token;
    std::istringstream token_stream{std::string(weight_part)};
    while (std::getline(token_stream, token, ','))
    {
        token = TrimAsciiWhitespace(token);
        if (token.empty())
        {
            continue;
        }
        uint64_t weight = 0;
        auto [ptr, ec] =
            std::from_chars(token.data(), token.data() + token.size(), weight);
        if (ec != std::errc() || ptr != token.data() + token.size())
        {
            if (error_message != nullptr)
            {
                *error_message = "invalid weight: " + token;
            }
            paths.clear();
            weights.clear();
            return false;
        }
        weights.push_back(weight);
    }
    if (weights.size() != paths.size())
    {
        if (error_message != nullptr)
        {
            *error_message = "weight count does not match path count";
        }
        paths.clear();
        weights.clear();
        return false;
    }
    return true;
}

inline std::string BuildStorePathListWithWeights(
    const std::vector<std::string> &paths, const std::vector<uint64_t> &weights)
{
    std::ostringstream oss;
    for (size_t i = 0; i < paths.size(); ++i)
    {
        if (i > 0)
        {
            oss << ",";
        }
        oss << paths[i];
    }

    if (!weights.empty())
    {
        oss << ":";
        for (size_t i = 0; i < weights.size(); ++i)
        {
            if (i > 0)
            {
                oss << ",";
            }
            oss << weights[i];
        }
    }
    return oss.str();
}

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
