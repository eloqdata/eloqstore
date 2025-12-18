#pragma once

#include <cpuid.h>
#include <x86intrin.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <thread>
#include <string>
#include <vector>

namespace chrono = std::chrono;

namespace eloqstore
{
class Clock
{
public:
    Clock()
    {
        if (!init_tsc())
        {
            std::abort();
        }
    }

    uint64_t now_ns() const
    {
        uint64_t c = __rdtsc();
        return static_cast<uint64_t>((c - tsc_base_) * ns_per_cycle_);
    }

private:
    uint64_t tsc_base_{0};
    double ns_per_cycle_{0.0};

    bool init_tsc()
    {
        double ns_per_cycle = 0.0;
        if (!(query_tsc_freq_cpuid(ns_per_cycle) || calibrate_tsc(ns_per_cycle)))
        {
            return false;
        }

        ns_per_cycle_ = ns_per_cycle;
        tsc_base_ = __rdtsc();
        return true;
    }

    static bool query_tsc_freq_cpuid(double &ns_per_cycle)
    {
        uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;

        // ---------- CPUID 0x15 ----------
        if (__get_cpuid_max(0, nullptr) >= 0x15)
        {
            if (__get_cpuid(0x15, &eax, &ebx, &ecx, &edx))
            {
                if (eax != 0 && ebx != 0 && ecx != 0)
                {
                    double tsc_hz = (double) ecx * (double) ebx / (double) eax;
                    if (tsc_hz > 0)
                    {
                        ns_per_cycle = 1e9 / tsc_hz;
                        return true;
                    }
                }
            }
        }

        // ---------- fallback: CPUID 0x16 ----------
        if (__get_cpuid_max(0, nullptr) >= 0x16)
        {
            if (__get_cpuid(0x16, &eax, &ebx, &ecx, &edx))
            {
                if (eax != 0)
                {
                    double tsc_hz = (double) eax * 1e6;
                    if (tsc_hz > 0)
                    {
                        ns_per_cycle = 1e9 / tsc_hz;
                        return true;
                    }
                }
            }
        }

        return false;
    }

    static bool calibrate_tsc(double &ns_per_cycle)
    {
        using namespace std::chrono_literals;
        constexpr auto kCalibrationDuration = 50ms;

        auto start_time = chrono::steady_clock::now();
        uint64_t start_tsc = __rdtsc();

        std::this_thread::sleep_for(kCalibrationDuration);

        auto end_time = chrono::steady_clock::now();
        uint64_t end_tsc = __rdtsc();

        auto elapsed_ns = chrono::duration_cast<chrono::nanoseconds>(end_time - start_time).count();
        uint64_t delta_cycles = end_tsc - start_tsc;
        if (elapsed_ns <= 0 || delta_cycles == 0)
        {
            return false;
        }

        ns_per_cycle = static_cast<double>(elapsed_ns) / static_cast<double>(delta_cycles);
        return true;
    }
};
}  // namespace eloqstore

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

}  // namespace utils
