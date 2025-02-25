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
    NotFound,
    Failed,
    EndOfFile,
    OutOfSpace,
    OutOfMem,
    Corrupted,
    BadOption,
    BadDir,
    WriteConflict,
    OpenFileLimit,
    TryAgain,
    Busy,
    IoFail
};
}  // namespace kvstore