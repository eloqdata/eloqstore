// Custom Catch2 entry point for large-value tests.
//
// These tests use GlobalRegisteredMemory (256MB per shard) with io_uring
// fixed-buffer registration.  io_uring pins the pages in RAM, which counts
// against RLIMIT_MEMLOCK.  The kernel default (often 64KB) is far too small;
// bump it to 2GB before any test fixture allocates memory.

#include <sys/resource.h>

#include <catch2/catch_session.hpp>

int main(int argc, char *argv[])
{
    struct rlimit lim;
    getrlimit(RLIMIT_MEMLOCK, &lim);
    constexpr rlim_t kTwoGB = 2ULL * 1024 * 1024 * 1024;
    if (lim.rlim_cur < kTwoGB)
    {
        lim.rlim_cur = kTwoGB;
        if (lim.rlim_max != RLIM_INFINITY && lim.rlim_max < kTwoGB)
        {
            lim.rlim_max = kTwoGB;
        }
        setrlimit(RLIMIT_MEMLOCK, &lim);
    }
    return Catch::Session().run(argc, argv);
}
