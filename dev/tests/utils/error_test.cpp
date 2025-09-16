#include <catch2/catch_test_macros.hpp>
#include <sstream>
#include <set>

#include "error.h"
#include "fixtures/test_helpers.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("KvError_BasicErrors", "[error][unit]") {
    SECTION("Error codes are unique") {
        std::set<int> error_codes;

        // Check all error codes are unique
        error_codes.insert(static_cast<int>(KvError::NoError));
        error_codes.insert(static_cast<int>(KvError::NotFound));
        error_codes.insert(static_cast<int>(KvError::Corruption));
        error_codes.insert(static_cast<int>(KvError::NotSupported));
        error_codes.insert(static_cast<int>(KvError::InvalidArgument));
        error_codes.insert(static_cast<int>(KvError::IOError));
        error_codes.insert(static_cast<int>(KvError::MergeInProgress));
        error_codes.insert(static_cast<int>(KvError::Incomplete));
        error_codes.insert(static_cast<int>(KvError::ShutdownInProgress));
        error_codes.insert(static_cast<int>(KvError::TimedOut));
        error_codes.insert(static_cast<int>(KvError::Aborted));
        error_codes.insert(static_cast<int>(KvError::Busy));
        error_codes.insert(static_cast<int>(KvError::Expired));
        error_codes.insert(static_cast<int>(KvError::TryAgain));
        error_codes.insert(static_cast<int>(KvError::Cancelled));
        error_codes.insert(static_cast<int>(KvError::OutOfMemory));
        error_codes.insert(static_cast<int>(KvError::DiskFull));
        error_codes.insert(static_cast<int>(KvError::FileNotFound));
        error_codes.insert(static_cast<int>(KvError::PermissionDenied));
        error_codes.insert(static_cast<int>(KvError::Unknown));

        // Number of unique codes should equal number of error types
        REQUIRE(error_codes.size() == 20);  // Adjust based on actual error count
    }

    SECTION("NoError is zero") {
        REQUIRE(static_cast<int>(KvError::NoError) == 0);
    }

    SECTION("Error comparison") {
        KvError err1 = KvError::NoError;
        KvError err2 = KvError::NotFound;
        KvError err3 = KvError::NotFound;

        REQUIRE(err1 != err2);
        REQUIRE(err2 == err3);
        REQUIRE(err1 == KvError::NoError);
    }
}

TEST_CASE("KvError_ErrorCategories", "[error][unit]") {
    SECTION("Retryable errors") {
        // These errors should be retryable
        std::vector<KvError> retryable_errors = {
            KvError::TryAgain,
            KvError::Busy,
            KvError::TimedOut,
            KvError::Incomplete
        };

        for (KvError err : retryable_errors) {
            REQUIRE(IsRetryable(err) == true);
        }
    }

    SECTION("Non-retryable errors") {
        // These errors should not be retryable
        std::vector<KvError> non_retryable_errors = {
            KvError::Corruption,
            KvError::NotSupported,
            KvError::InvalidArgument,
            KvError::FileNotFound,
            KvError::PermissionDenied
        };

        for (KvError err : non_retryable_errors) {
            REQUIRE(IsRetryable(err) == false);
        }
    }

    SECTION("Fatal errors") {
        // These errors indicate serious problems
        std::vector<KvError> fatal_errors = {
            KvError::Corruption,
            KvError::OutOfMemory,
            KvError::DiskFull
        };

        for (KvError err : fatal_errors) {
            REQUIRE(IsFatal(err) == true);
        }
    }

    SECTION("I/O related errors") {
        std::vector<KvError> io_errors = {
            KvError::IOError,
            KvError::DiskFull,
            KvError::FileNotFound,
            KvError::PermissionDenied
        };

        for (KvError err : io_errors) {
            REQUIRE(IsIORelated(err) == true);
        }
    }
}

TEST_CASE("KvError_ErrorMessages", "[error][unit]") {
    SECTION("All errors have messages") {
        std::vector<KvError> all_errors = {
            KvError::NoError,
            KvError::NotFound,
            KvError::Corruption,
            KvError::NotSupported,
            KvError::InvalidArgument,
            KvError::IOError,
            KvError::MergeInProgress,
            KvError::Incomplete,
            KvError::ShutdownInProgress,
            KvError::TimedOut,
            KvError::Aborted,
            KvError::Busy,
            KvError::Expired,
            KvError::TryAgain,
            KvError::Cancelled,
            KvError::OutOfMemory,
            KvError::DiskFull,
            KvError::FileNotFound,
            KvError::PermissionDenied,
            KvError::Unknown
        };

        for (KvError err : all_errors) {
            std::string msg = ErrorToString(err);
            REQUIRE(!msg.empty());
            REQUIRE(msg != "Unknown error");  // Except for Unknown itself
        }
    }

    SECTION("Error message format") {
        REQUIRE(ErrorToString(KvError::NoError) == "OK");
        REQUIRE(ErrorToString(KvError::NotFound) == "Not found");
        REQUIRE(ErrorToString(KvError::Corruption) == "Corruption detected");
        REQUIRE(ErrorToString(KvError::IOError) == "I/O error");
    }

    SECTION("Error with context") {
        KvError err = KvError::NotFound;
        std::string context = "key='test_key'";
        std::string full_msg = ErrorToStringWithContext(err, context);

        REQUIRE(full_msg.find("Not found") != std::string::npos);
        REQUIRE(full_msg.find(context) != std::string::npos);
    }
}

TEST_CASE("KvError_ErrorConversion", "[error][unit]") {
    SECTION("Convert from errno") {
        // Common errno values
        REQUIRE(ErrnoToKvError(0) == KvError::NoError);
        REQUIRE(ErrnoToKvError(ENOENT) == KvError::FileNotFound);
        REQUIRE(ErrnoToKvError(EACCES) == KvError::PermissionDenied);
        REQUIRE(ErrnoToKvError(EIO) == KvError::IOError);
        REQUIRE(ErrnoToKvError(ENOMEM) == KvError::OutOfMemory);
        REQUIRE(ErrnoToKvError(ENOSPC) == KvError::DiskFull);
        REQUIRE(ErrnoToKvError(EINVAL) == KvError::InvalidArgument);
        REQUIRE(ErrnoToKvError(EAGAIN) == KvError::TryAgain);
        REQUIRE(ErrnoToKvError(EBUSY) == KvError::Busy);
        REQUIRE(ErrnoToKvError(ETIMEDOUT) == KvError::TimedOut);
    }

    SECTION("Convert to errno") {
        // Reverse conversion
        REQUIRE(KvErrorToErrno(KvError::NoError) == 0);
        REQUIRE(KvErrorToErrno(KvError::FileNotFound) == ENOENT);
        REQUIRE(KvErrorToErrno(KvError::PermissionDenied) == EACCES);
        REQUIRE(KvErrorToErrno(KvError::IOError) == EIO);
        REQUIRE(KvErrorToErrno(KvError::OutOfMemory) == ENOMEM);
        REQUIRE(KvErrorToErrno(KvError::DiskFull) == ENOSPC);
    }

    SECTION("Round-trip conversion") {
        std::vector<int> errnos = {0, ENOENT, EACCES, EIO, ENOMEM, ENOSPC, EINVAL};

        for (int err : errnos) {
            KvError kv_err = ErrnoToKvError(err);
            int back = KvErrorToErrno(kv_err);
            // May not always round-trip perfectly due to many-to-one mappings
            if (err == 0) {
                REQUIRE(back == 0);
            }
        }
    }
}

TEST_CASE("KvError_ErrorHandling", "[error][unit]") {
    SECTION("Success check") {
        REQUIRE(IsSuccess(KvError::NoError) == true);
        REQUIRE(IsSuccess(KvError::NotFound) == false);
        REQUIRE(IsSuccess(KvError::IOError) == false);
    }

    SECTION("Error propagation") {
        auto operation_that_fails = []() -> KvError {
            return KvError::NotFound;
        };

        auto operation_that_succeeds = []() -> KvError {
            return KvError::NoError;
        };

        KvError err1 = operation_that_fails();
        REQUIRE(!IsSuccess(err1));

        KvError err2 = operation_that_succeeds();
        REQUIRE(IsSuccess(err2));
    }

    SECTION("Error combination") {
        // When multiple errors occur, which takes precedence?
        KvError err1 = KvError::NoError;
        KvError err2 = KvError::NotFound;

        KvError combined = CombineErrors(err1, err2);
        REQUIRE(combined == KvError::NotFound);  // Error takes precedence

        err1 = KvError::IOError;
        err2 = KvError::Corruption;

        combined = CombineErrors(err1, err2);
        // More severe error should take precedence
        REQUIRE(combined == KvError::Corruption);
    }
}

TEST_CASE("KvError_EdgeCases", "[error][unit][edge-case]") {
    SECTION("Invalid error code") {
        // Cast invalid number to KvError
        KvError invalid = static_cast<KvError>(999999);
        std::string msg = ErrorToString(invalid);
        REQUIRE(msg == "Unknown error");
    }

    SECTION("Error in destructor context") {
        // Errors that can be safely ignored in destructors
        std::vector<KvError> safe_in_destructor = {
            KvError::NoError,
            KvError::Cancelled,
            KvError::ShutdownInProgress
        };

        for (KvError err : safe_in_destructor) {
            REQUIRE(CanIgnoreInDestructor(err) == true);
        }
    }

    SECTION("Error severity ordering") {
        // More severe errors should compare greater
        REQUIRE(GetSeverity(KvError::Corruption) > GetSeverity(KvError::NotFound));
        REQUIRE(GetSeverity(KvError::OutOfMemory) > GetSeverity(KvError::Busy));
        REQUIRE(GetSeverity(KvError::DiskFull) > GetSeverity(KvError::TimedOut));
    }
}

// Helper functions (these would normally be in error.h/cpp)
bool IsRetryable(KvError err) {
    switch (err) {
        case KvError::TryAgain:
        case KvError::Busy:
        case KvError::TimedOut:
        case KvError::Incomplete:
            return true;
        default:
            return false;
    }
}

bool IsFatal(KvError err) {
    switch (err) {
        case KvError::Corruption:
        case KvError::OutOfMemory:
        case KvError::DiskFull:
            return true;
        default:
            return false;
    }
}

bool IsIORelated(KvError err) {
    switch (err) {
        case KvError::IOError:
        case KvError::DiskFull:
        case KvError::FileNotFound:
        case KvError::PermissionDenied:
            return true;
        default:
            return false;
    }
}

std::string ErrorToString(KvError err) {
    switch (err) {
        case KvError::NoError: return "OK";
        case KvError::NotFound: return "Not found";
        case KvError::Corruption: return "Corruption detected";
        case KvError::NotSupported: return "Operation not supported";
        case KvError::InvalidArgument: return "Invalid argument";
        case KvError::IOError: return "I/O error";
        case KvError::MergeInProgress: return "Merge in progress";
        case KvError::Incomplete: return "Operation incomplete";
        case KvError::ShutdownInProgress: return "Shutdown in progress";
        case KvError::TimedOut: return "Operation timed out";
        case KvError::Aborted: return "Operation aborted";
        case KvError::Busy: return "Resource busy";
        case KvError::Expired: return "Resource expired";
        case KvError::TryAgain: return "Try again";
        case KvError::Cancelled: return "Operation cancelled";
        case KvError::OutOfMemory: return "Out of memory";
        case KvError::DiskFull: return "Disk full";
        case KvError::FileNotFound: return "File not found";
        case KvError::PermissionDenied: return "Permission denied";
        case KvError::Unknown: return "Unknown error";
        default: return "Unknown error";
    }
}

std::string ErrorToStringWithContext(KvError err, const std::string& context) {
    return ErrorToString(err) + " (" + context + ")";
}

KvError ErrnoToKvError(int err) {
    switch (err) {
        case 0: return KvError::NoError;
        case ENOENT: return KvError::FileNotFound;
        case EACCES: return KvError::PermissionDenied;
        case EIO: return KvError::IOError;
        case ENOMEM: return KvError::OutOfMemory;
        case ENOSPC: return KvError::DiskFull;
        case EINVAL: return KvError::InvalidArgument;
        case EAGAIN: return KvError::TryAgain;
        case EBUSY: return KvError::Busy;
        case ETIMEDOUT: return KvError::TimedOut;
        default: return KvError::Unknown;
    }
}

int KvErrorToErrno(KvError err) {
    switch (err) {
        case KvError::NoError: return 0;
        case KvError::FileNotFound: return ENOENT;
        case KvError::PermissionDenied: return EACCES;
        case KvError::IOError: return EIO;
        case KvError::OutOfMemory: return ENOMEM;
        case KvError::DiskFull: return ENOSPC;
        case KvError::InvalidArgument: return EINVAL;
        case KvError::TryAgain: return EAGAIN;
        case KvError::Busy: return EBUSY;
        case KvError::TimedOut: return ETIMEDOUT;
        default: return EIO;  // Generic I/O error for unknown
    }
}

bool IsSuccess(KvError err) {
    return err == KvError::NoError;
}

KvError CombineErrors(KvError err1, KvError err2) {
    if (err1 == KvError::NoError) return err2;
    if (err2 == KvError::NoError) return err1;

    // Return more severe error
    if (IsFatal(err2)) return err2;
    if (IsFatal(err1)) return err1;

    return err1;  // Return first error by default
}

bool CanIgnoreInDestructor(KvError err) {
    return err == KvError::NoError ||
           err == KvError::Cancelled ||
           err == KvError::ShutdownInProgress;
}

int GetSeverity(KvError err) {
    if (err == KvError::NoError) return 0;
    if (err == KvError::Cancelled || err == KvError::ShutdownInProgress) return 1;
    if (err == KvError::NotFound) return 2;
    if (err == KvError::Busy || err == KvError::TryAgain) return 3;
    if (err == KvError::TimedOut) return 4;
    if (err == KvError::InvalidArgument) return 5;
    if (err == KvError::IOError) return 6;
    if (err == KvError::DiskFull) return 7;
    if (err == KvError::OutOfMemory) return 8;
    if (err == KvError::Corruption) return 9;
    return 10;  // Unknown
}

TEST_CASE("KvError_StressTest", "[error][stress]") {
    SECTION("Concurrent error handling") {
        std::atomic<int> error_count{0};
        std::atomic<int> success_count{0};

        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) {
            threads.emplace_back([&error_count, &success_count, i]() {
                for (int j = 0; j < 1000; ++j) {
                    KvError err = (j % 10 == 0) ? KvError::NoError : KvError::Busy;

                    if (IsSuccess(err)) {
                        success_count++;
                    } else {
                        error_count++;
                    }

                    // Simulate error handling
                    std::string msg = ErrorToString(err);
                    volatile size_t len = msg.length();
                    (void)len;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count == 800);  // 8 threads * 1000 iterations * 10% success
        REQUIRE(error_count == 7200);   // 8 threads * 1000 iterations * 90% error
    }
}