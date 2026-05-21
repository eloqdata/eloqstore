// Custom Catch2 entry point for large-value tests.
//
// These tests use GlobalRegisteredMemory (256MB per shard) with io_uring
// fixed-buffer registration.  io_uring pins the pages in RAM, which counts
// against RLIMIT_MEMLOCK.  The kernel default (often 64KB) is far too small;
// bump it to 2GB before any test fixture allocates memory.
//
// Preferred launch under WSL2:
//   systemd-run --user --pipe --wait --property=LimitMEMLOCK=2G ./large_value_*
// (ulimit/prlimit lie about MEMLOCK under WSL2.)

#include <sys/resource.h>

#include <catch2/catch_session.hpp>
#include <cerrno>
#include <cstdio>
#include <cstring>

int main(int argc, char *argv[])
{
    // Best-effort bump. Build-time test discovery (catch_discover_tests) and
    // unprivileged ad-hoc runs may not be able to raise the hard limit; log
    // and continue so test-listing still works. Actual test runs that need
    // the locked pages will fail loudly later on io_uring registration if
    // the launch wasn't via the recommended systemd-run wrapper above.
    struct rlimit lim{};
    if (getrlimit(RLIMIT_MEMLOCK, &lim) != 0)
    {
        std::fprintf(stderr,
                     "large_value_main: getrlimit(RLIMIT_MEMLOCK) failed: "
                     "%s; continuing\n",
                     std::strerror(errno));
    }
    else
    {
        constexpr rlim_t kTwoGB = 2ULL * 1024 * 1024 * 1024;
        if (lim.rlim_cur < kTwoGB)
        {
            lim.rlim_cur = kTwoGB;
            if (lim.rlim_max != RLIM_INFINITY && lim.rlim_max < kTwoGB)
            {
                lim.rlim_max = kTwoGB;
            }
            if (setrlimit(RLIMIT_MEMLOCK, &lim) != 0)
            {
                std::fprintf(
                    stderr,
                    "large_value_main: setrlimit(RLIMIT_MEMLOCK=2G) failed: "
                    "%s; tests may fail at io_uring buffer registration.\n"
                    "Recommended: systemd-run --user --pipe --wait "
                    "--property=LimitMEMLOCK=2G %s\n",
                    std::strerror(errno),
                    argv[0]);
            }
        }
    }
    return Catch::Session().run(argc, argv);
}
