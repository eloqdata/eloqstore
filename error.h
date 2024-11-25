#pragma once

#include <cstdint>

namespace kvstore
{
enum struct KvError : uint8_t
{
    NoError = 0,
    NotFound,
    Failed,
    EndOfFile,
    OutOfSpace
};
}  // namespace kvstore