#pragma once

#include <cstdint>

#define CHECK_KV_ERR(err)          \
    if ((err) != KvError::NoError) \
    {                              \
        return err;                \
    }

namespace kvstore
{
enum struct KvError : uint8_t
{
    NoError = 0,
    InvalidArgs,
    NotFound,
    NotRunning,
    EndOfFile,
    OutOfSpace,
    OutOfMem,
    Corrupted,
    OpenFileLimit,
    TryAgain,
    Busy,
    IoFail,
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
    }
    return "Unknown error";
}

}  // namespace kvstore