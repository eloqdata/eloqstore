#pragma once

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <string_view>

#include "coding.h"
#include "eloq_store.h"

static constexpr kvstore::TableIdent test_tbl_id = {"t1", 1};
inline std::unique_ptr<kvstore::EloqStore> memstore = nullptr;

void InitMemStore();

inline std::string_view ConvertIntKey(char *ptr, uint64_t key)
{
    uint64_t big_endian = kvstore::ToBigEndian(key);
    kvstore::EncodeFixed64(ptr, big_endian);
    return {ptr, sizeof(uint64_t)};
}

inline uint64_t ConvertIntKey(std::string_view key)
{
    uint64_t big_endian = kvstore::DecodeFixed64(key.data());
    return __builtin_bswap64(big_endian);
}
