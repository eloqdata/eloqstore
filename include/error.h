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
    NoError = 0,
    InvalidArgs,
    NotFound,
    NotRunning,
    Corrupted,
    EndOfFile,
    OutOfSpace,
    OutOfMem,
    OpenFileLimit,
    TryAgain,
    Busy,
    Timeout,
    NoPermission,
    CloudErr,
    IoFail,
    ExpiredTerm,

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
