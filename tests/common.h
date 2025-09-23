#pragma once

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <string_view>

#include "../common.h"
#include "coding.h"
#include "eloq_store.h"

constexpr char test_path[] = "/tmp/eloqstore";
static const eloqstore::TableIdent test_tbl_id = {"t0", 0};
const eloqstore::KvOptions mem_store_opts = {};
const eloqstore::KvOptions default_opts = {
    .store_path = {test_path},
};
const eloqstore::KvOptions archive_opts = {
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // send archive request immediately
    .file_amplify_factor = 2,
    .store_path = {test_path},
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};

eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts);

inline std::string_view ConvertIntKey(char *ptr, uint64_t key)
{
    uint64_t big_endian = eloqstore::ToBigEndian(key);
    eloqstore::EncodeFixed64(ptr, big_endian);
    return {ptr, sizeof(uint64_t)};
}

inline uint64_t ConvertIntKey(std::string_view key)
{
    uint64_t big_endian = eloqstore::DecodeFixed64(key.data());
    return __builtin_bswap64(big_endian);
}
inline void CleanupStore(eloqstore::KvOptions opts)
{
    for (const std::string &db_path : opts.store_path)
    {
        std::filesystem::remove_all(db_path);
    }
    if (!opts.cloud_store_path.empty())
    {
        std::string command = "rclone delete ";
        command.append(opts.cloud_store_path);
        int res = system(command.c_str());
    }
}