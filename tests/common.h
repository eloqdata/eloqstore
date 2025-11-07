#pragma once

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <optional>
#include <string_view>

#include "../common.h"
#include "coding.h"
#include "eloq_store.h"
#include "kv_options.h"

constexpr char test_path[] = "/tmp/eloqstore";
static const eloqstore::TableIdent test_tbl_id = {"t0", 0};
const eloqstore::KvOptions mem_store_opts = {};
const eloqstore::KvOptions default_opts = {
    .store_path = {test_path},
};

const eloqstore::KvOptions append_opts = {
    .store_path = {test_path},
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};
const eloqstore::KvOptions archive_opts = {
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // send archive request immediately
    .file_amplify_factor = 2,
    .store_path = {test_path},
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};
const eloqstore::KvOptions cloud_options = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .local_space_limit = 200 << 20,  // 100MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "docker-minio:eloqstore/unit-test",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

const eloqstore::KvOptions cloud_archive_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // send archive request immediately
    .file_amplify_factor = 2,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "docker-minio:eloqstore/unit-test",
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};
eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts);

bool ValidateFileSizes(const eloqstore::KvOptions &opts);

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

inline void CleanupLocalStore(eloqstore::KvOptions opts)
{
    for (const std::string &db_path : opts.store_path)
    {
        std::filesystem::remove_all(db_path);
    }
}

inline void CleanupStore(eloqstore::KvOptions opts)
{
    CleanupLocalStore(opts);
    if (!opts.cloud_store_path.empty())
    {
        std::string command = "rclone delete ";
        command.append(opts.cloud_store_path);
        int res = system(command.c_str());
    }
}

// Helper function to send HTTP request to rclone server
inline bool SendRcloneRequest(const std::string &daemon_url,
                              const std::string &operation,
                              const std::string &json_data)
{
    std::string command =
        "curl -s -X POST -H 'Content-Type: application/json' -d '";
    command += json_data;
    command += "' " + daemon_url + "/" + operation;

    int result = std::system(command.c_str());
    return result == 0;
}

// Helper function to move cloud file using rclone server
inline bool MoveCloudFile(const std::string &daemon_url,
                          const std::string &cloud_path,
                          const std::string &src_file,
                          const std::string &dst_file)
{
    std::string src_path = cloud_path + "/" + src_file;
    std::string dst_path = cloud_path + "/" + dst_file;

    std::string json_data = "{\"srcFs\":\"" + cloud_path +
                            "\",\"srcRemote\":\"" + src_file +
                            "\",\"dstFs\":\"" + cloud_path +
                            "\",\"dstRemote\":\"" + dst_file + "\"}";

    return SendRcloneRequest(daemon_url, "operations/movefile", json_data);
}

// Helper function to list cloud files using rclone server
inline std::vector<std::string> ListCloudFiles(
    const std::string &daemon_url,
    const std::string &cloud_path,
    const std::string &remote_path = "")
{
    std::vector<std::string> files;

    // Construct JSON request similar to object_store.cpp SetupListRequest
    std::string json_data =
        "{\"fs\":\"" + cloud_path + "\",\"remote\":\"" + remote_path +
        "\",\"opt\":{\"recurse\":false,\"showHash\":false}}";

    // Send request to rclone daemon
    std::string command =
        "curl -s -X POST -H 'Content-Type: application/json' -d '" + json_data +
        "' " + daemon_url + "/operations/list";

    FILE *pipe = popen(command.c_str(), "r");
    if (!pipe)
    {
        return files;
    }

    std::string response;
    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr)
    {
        response += buffer;
    }
    pclose(pipe);

    // Parse JSON response similar to file_gc.cpp
    try
    {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(response, root))
        {
            if (root.isMember("list") && root["list"].isArray())
            {
                for (const auto &item : root["list"])
                {
                    if (item.isMember("Name") && item["Name"].isString())
                    {
                        files.push_back(item["Name"].asString());
                    }
                }
            }
        }
    }

    catch (const std::exception &e)
    {
        // Return empty vector on parse error
        files.clear();
    }

    return files;
}

inline std::optional<uint64_t> GetCloudSize(const std::string &daemon_url,
                                            const std::string &cloud_path)
{
    std::string json_data = "{\"fs\":\"" + cloud_path + "\"}";
    std::string command =
        "curl -s -X POST -H 'Content-Type: application/json' -d '" + json_data +
        "' " + daemon_url + "/operations/size";

    FILE *pipe = popen(command.c_str(), "r");
    if (!pipe)
    {
        return std::nullopt;
    }

    std::string response;
    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr)
    {
        response += buffer;
    }
    pclose(pipe);

    try
    {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(response, root) && root.isMember("bytes"))
        {
            return root["bytes"].asUInt64();
        }
    }
    catch (const std::exception &)
    {
        // ignore parse errors
    }
    return std::nullopt;
}

inline uint64_t DirectorySize(const std::filesystem::path &path)
{
    std::error_code ec;
    if (!std::filesystem::exists(path, ec))
    {
        return 0;
    }
    uint64_t total = 0;
    for (std::filesystem::recursive_directory_iterator it(path, ec), end;
         it != end && !ec;
         it.increment(ec))
    {
        std::error_code file_ec;
        if (it->is_regular_file(file_ec) && !file_ec)
        {
            total += it->file_size(file_ec);
        }
        if (ec)
        {
            break;
        }
    }
    return total;
}