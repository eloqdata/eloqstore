#pragma once

#include <atomic>
#include <cstring>

// Test-only error injection. Unlike KillPoint (kill_point.h), which SIGTERMs
// the process to exercise crash-recovery paths, FailPoint perturbs a code path
// in-process (for example, returning an error or forcing a scheduler yield).
//
// Usage: ArmOnce auto-disarms after a matching TEST_FAIL_POINT_RETURN or
// TEST_FAIL_POINT_ACTION; ArmPersistent fires every match until Disarm.
// Compiled out entirely in release (NDEBUG) builds, like the kill-point macros.
#ifndef NDEBUG
#define TEST_FAIL_POINT_ACTION(name, action)                        \
    do                                                              \
    {                                                               \
        if (::eloqstore::FailPoint::GetInstance().ShouldFail(name)) \
        {                                                           \
            action;                                                 \
        }                                                           \
    } while (0)
#define TEST_FAIL_POINT_RETURN(name, err)                           \
    do                                                              \
    {                                                               \
        if (::eloqstore::FailPoint::GetInstance().ShouldFail(name)) \
        {                                                           \
            return (err);                                           \
        }                                                           \
    } while (0)
#else
#define TEST_FAIL_POINT_ACTION(name, action) \
    do                                       \
    {                                        \
    } while (0)
#define TEST_FAIL_POINT_RETURN(name, err) \
    do                                    \
    {                                     \
    } while (0)
#endif

namespace eloqstore
{
// Trivially destructible (no global/static dtor work): point names are string
// literals with static storage duration, so a const char* is sufficient.
class FailPoint
{
public:
    static FailPoint &GetInstance()
    {
        static FailPoint instance;
        return instance;
    }

    // Arm so the next ShouldFail(name) returns true once, then auto-disarms.
    // @p name must be a string literal (stored by pointer, not copied).
    void ArmOnce(const char *name)
    {
        persistent_.store(false, std::memory_order_relaxed);
        armed_.store(name, std::memory_order_release);
    }

    // Arm until Disarm. Used by scheduler regressions that must perturb every
    // matching wake throughout a bounded observation window.
    void ArmPersistent(const char *name)
    {
        persistent_.store(true, std::memory_order_relaxed);
        armed_.store(name, std::memory_order_release);
    }

    void Disarm()
    {
        armed_.store(nullptr, std::memory_order_release);
    }

    bool ShouldFail(const char *name)
    {
        const char *armed = armed_.load(std::memory_order_acquire);
        if (armed == nullptr || std::strcmp(armed, name) != 0)
        {
            return false;
        }
        if (persistent_.load(std::memory_order_relaxed))
        {
            return true;
        }
        return armed_.compare_exchange_strong(armed,
                                              nullptr,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire);
    }

private:
    std::atomic<const char *> armed_{nullptr};
    std::atomic<bool> persistent_{false};
};
}  // namespace eloqstore
