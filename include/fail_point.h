#pragma once

#include <cstring>

// Test-only error injection. Unlike KillPoint (kill_point.h), which SIGTERMs
// the process to exercise crash-recovery paths, FailPoint makes a code path
// return an error so tests can drive in-process error/abort handling (e.g.
// verifying WriteTask::Abort rolls back the BranchFileMapping high-water
// marks).
//
// Usage: arm a named point from the test, then a TEST_FAIL_POINT_RETURN(name,
// err) embedded in engine code returns `err` exactly once before
// auto-disarming. Compiled out entirely in release (NDEBUG) builds, like the
// kill-point macros.
#ifndef NDEBUG
#define TEST_FAIL_POINT_RETURN(name, err)                           \
    do                                                              \
    {                                                               \
        if (::eloqstore::FailPoint::GetInstance().ShouldFail(name)) \
        {                                                           \
            return (err);                                           \
        }                                                           \
    } while (0)
#else
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
        armed_ = name;
    }

    void Disarm()
    {
        armed_ = nullptr;
    }

    bool ShouldFail(const char *name)
    {
        if (armed_ == nullptr || std::strcmp(armed_, name) != 0)
        {
            return false;
        }
        armed_ = nullptr;  // fire once
        return true;
    }

private:
    const char *armed_{nullptr};
};
}  // namespace eloqstore
