#pragma once

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <map>
#include <string_view>

#include "coding.h"
#include "scan_task.h"

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

void InitEnv();
void InitData(const std::string &tbl_name,
              uint32_t partition_id,
              size_t data_size,
              uint64_t data_ts);

class MapVerifier
{
public:
    MapVerifier(kvstore::TableIdent tid);
    void Upsert(uint64_t begin, uint64_t end);
    void Delete(uint64_t begin, uint64_t end);
    void Read(uint64_t k);
    void Scan(uint64_t begin, uint64_t end);
    void ScanAll();

private:
    std::string key(uint64_t k);
    const kvstore::TableIdent tid_;
    uint64_t ts_;
    std::map<std::string, kvstore::Tuple> answer_;
};