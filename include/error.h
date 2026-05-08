#pragma once

#include <cstdint>

#define CHECK_KV_ERR(err)          \
    if ((err) != KvError::NoError) \
    {                              \
        return err;                \
    }

namespace eloqstore
{
enum struct KvError : uint8_t
{
    NoError = 0,      // Success.
    InvalidArgs,      // Invalid inputs/options (e.g., bad path/key).
    NotFound,         // Resource not found.
    ResourceMissing,  // Expected data file/manifest/term file/cloud object is
                      // missing unexpectedly.
    NotRunning,       // Store not started or already stopping.
    Corrupted,        // Corrupted manifest/term data or checksum mismatch.
    EndOfFile,        // Manifest read hit EOF or truncated padding.
    OutOfSpace,       // Disk/cache space exhausted (ENOSPC or cache limit).
    OutOfMem,         // Memory allocation failure or cache eviction failed.
    OpenFileLimit,    // Too many open files or io_uring fd slots.
    TryAgain,         // Retryable condition (EAGAIN/EINTR/ENOBUFS).
    Busy,             // Resource/device busy (EBUSY).
    Timeout,          // Cloud HTTP/CURL timeout or retryable status.
    NoPermission,     // Permission denied (EPERM).
    CloudErr,         // Cloud service error (non-timeout HTTP/CURL).
    IoFail,           // Unclassified local I/O error.
    ExpiredTerm,      // Cloud term file indicates stale process term.
    OssInsufficientStorage,  // Object storage out of capacity (HTTP 507).
    AlreadyExists,          // Branch or table already exists (e.g., HTTP 409 or
                            // EEXIST).
    LargeValueUnsupported,  // Operation does not support very large values.
};

constexpr const char *ErrorString(KvError err)
{
    switch (err)
    {
    case KvError::NoError:
        return "Succeed";
    case KvError::InvalidArgs:
        return "Invalid arguments";
    case KvError::NotFound:
        return "Resource not found";
    case KvError::ResourceMissing:
        return "Expected resource missing";
    case KvError::NotRunning:
        return "EloqStore is not running";
    case KvError::EndOfFile:
        return "End of file";
    case KvError::OutOfSpace:
        return "Out of disk space";
    case KvError::OutOfMem:
        return "Out of memory";
    case KvError::Corrupted:
        return "Disk data corrupted";
    case KvError::OpenFileLimit:
        return "Too many opened files";
    case KvError::TryAgain:
        return "Try again later";
    case KvError::Busy:
        return "Device or resource busy";
    case KvError::IoFail:
        return "I/O failure";
    case KvError::CloudErr:
        return "Cloud service is unavailable";
    case KvError::Timeout:
        return "Operation timeout";
    case KvError::NoPermission:
        return "Operation not permitted";
    case KvError::ExpiredTerm:
        return "Expired term";
    case KvError::OssInsufficientStorage:
        return "Object storage insufficient storage";
    case KvError::AlreadyExists:
        return "Resource already exists";
    case KvError::LargeValueUnsupported:
        return "Operation does not support very large values";
    }
    return "Unknown error";
}

constexpr bool IsRetryableErr(KvError err)
{
    switch (err)
    {
    case KvError::OpenFileLimit:
    case KvError::Busy:
    case KvError::TryAgain:
        return true;
    default:
        return false;
    }
}

}  // namespace eloqstore
