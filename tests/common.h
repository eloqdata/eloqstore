#pragma once

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <glog/logging.h>

#include <algorithm>
#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "../include/common.h"
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
    .archive_interval_secs = 0,  // disable automatic archive scheduling
    .file_amplify_factor = 2,
    .store_path = {test_path},
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};
const eloqstore::KvOptions cloud_options = {
    .manifest_limit = 1 << 20,
    .fd_limit = 100 + eloqstore::num_reserved_fd,
    .local_space_limit = 200 << 20,  // 100MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "eloqstore/unit-test",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

const eloqstore::KvOptions cloud_archive_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // disable automatic archive scheduling
    .file_amplify_factor = 2,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "eloqstore/unit-test",
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

namespace
{
constexpr std::string_view kDefaultTestAwsEndpoint = "http://127.0.0.1:9900";
constexpr std::string_view kDefaultTestAwsRegion = "us-east-1";

struct ParsedCloudPath
{
    std::string bucket;
    std::string prefix;
};

inline Aws::SDKOptions &TestAwsOptions()
{
    static Aws::SDKOptions options;
    return options;
}

inline void EnsureAwsSdkInitialized()
{
    static std::once_flag once;
    std::call_once(
        once,
        []
        {
            Aws::InitAPI(TestAwsOptions());
            std::atexit([]() { Aws::ShutdownAPI(TestAwsOptions()); });
        });
}

inline std::string TrimSlashes(std::string_view value, bool trim_front)
{
    std::string_view out = value;
    if (trim_front)
    {
        while (!out.empty() && out.front() == '/')
        {
            out.remove_prefix(1);
        }
    }
    else
    {
        while (!out.empty() && out.back() == '/')
        {
            out.remove_suffix(1);
        }
    }
    return std::string(out);
}

inline ParsedCloudPath ParseCloudPathSpec(std::string spec)
{
    ParsedCloudPath path;
    if (spec.empty())
    {
        return path;
    }
    auto colon = spec.find(':');
    if (colon != std::string::npos)
    {
        LOG(FATAL) << "cloud_store_path should be 'bucket[/prefix]', got "
                   << spec;
    }
    spec = TrimSlashes(spec, true);
    if (spec.empty())
    {
        return path;
    }
    auto slash = spec.find('/');
    if (slash == std::string::npos)
    {
        path.bucket = spec;
        return path;
    }
    path.bucket = spec.substr(0, slash);
    std::string rest = spec.substr(slash + 1);
    rest = TrimSlashes(rest, true);
    rest = TrimSlashes(rest, false);
    path.prefix = rest;
    return path;
}

inline std::string AppendRemoteComponent(std::string base,
                                         std::string_view extra)
{
    if (extra.empty())
    {
        return base;
    }
    if (!base.empty() && base.back() != '/')
    {
        base.push_back('/');
    }
    std::string_view trimmed = extra;
    while (!trimmed.empty() && trimmed.front() == '/')
    {
        trimmed.remove_prefix(1);
    }
    base.append(trimmed.data(), trimmed.size());
    return base;
}

class S3TestClient
{
public:
    explicit S3TestClient(const eloqstore::KvOptions &opts)
    {
        EnsureAwsSdkInitialized();
        Aws::Client::ClientConfiguration config;
        std::string endpoint = opts.cloud_endpoint.empty()
                                   ? std::string(kDefaultTestAwsEndpoint)
                                   : opts.cloud_endpoint;
        if (!endpoint.empty())
        {
            config.endpointOverride = endpoint.c_str();
            if (endpoint.rfind("https://", 0) == 0)
            {
                config.scheme = Aws::Http::Scheme::HTTPS;
            }
            else
            {
                config.scheme = Aws::Http::Scheme::HTTP;
            }
        }
        std::string region = opts.cloud_region.empty()
                                 ? std::string(kDefaultTestAwsRegion)
                                 : opts.cloud_region;
        config.region = region.c_str();
        bool verify_ssl =
            opts.cloud_endpoint.empty() ? false : opts.cloud_verify_ssl;
        config.verifySSL = verify_ssl;
        Aws::Auth::AWSCredentials credentials(opts.cloud_access_key.c_str(),
                                              opts.cloud_secret_key.c_str());
        client_ = std::make_unique<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
    }

    ~S3TestClient() = default;

    Aws::S3::S3Client &Client()
    {
        return *client_;
    }

private:
    std::unique_ptr<Aws::S3::S3Client> client_;
};

struct ObjectListResult
{
    std::vector<std::string> keys;
    uint64_t total_size{0};
};

inline bool ListObjects(const eloqstore::KvOptions &opts,
                        const ParsedCloudPath &path,
                        ObjectListResult &out,
                        bool raw_keys)
{
    if (path.bucket.empty())
    {
        return false;
    }
    S3TestClient client(opts);
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(path.bucket.c_str());
    std::string request_prefix = path.prefix;
    if (!request_prefix.empty() && request_prefix.back() != '/')
    {
        request_prefix.push_back('/');
    }
    if (!request_prefix.empty())
    {
        request.SetPrefix(request_prefix.c_str());
    }

    Aws::String continuation;
    do
    {
        if (!continuation.empty())
        {
            request.SetContinuationToken(continuation);
        }
        auto outcome = client.Client().ListObjectsV2(request);
        if (!outcome.IsSuccess())
        {
            LOG(ERROR) << "ListObjectsV2 failed: "
                       << outcome.GetError().GetMessage();
            return false;
        }
        const auto &result = outcome.GetResult();
        for (const auto &object : result.GetContents())
        {
            out.total_size += static_cast<uint64_t>(object.GetSize());
            std::string key = object.GetKey().c_str();
            if (!raw_keys && !request_prefix.empty() &&
                key.rfind(request_prefix, 0) == 0)
            {
                key.erase(0, request_prefix.size());
            }
            out.keys.push_back(std::move(key));
        }
        continuation = result.GetNextContinuationToken();
    } while (!continuation.empty());

    return true;
}

inline bool DeleteObjects(const eloqstore::KvOptions &opts,
                          const ParsedCloudPath &path)
{
    ObjectListResult objects;
    if (!ListObjects(opts, path, objects, true))
    {
        return false;
    }
    if (objects.keys.empty())
    {
        return true;
    }
    S3TestClient client(opts);
    for (const auto &key : objects.keys)
    {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(path.bucket.c_str());
        request.SetKey(key.c_str());
        auto outcome = client.Client().DeleteObject(request);
        if (!outcome.IsSuccess())
        {
            LOG(ERROR) << "DeleteObject failed for " << key << ": "
                       << outcome.GetError().GetMessage();
            return false;
        }
    }
    return true;
}

inline std::string ComposeObjectKey(const ParsedCloudPath &path,
                                    const std::string &filename)
{
    if (path.bucket.empty())
    {
        return {};
    }
    std::string key = path.prefix;
    if (!key.empty() && key.back() != '/')
    {
        key.push_back('/');
    }
    key.append(filename);
    return key;
}

inline std::vector<std::string> ListCloudFilesInternal(
    const eloqstore::KvOptions &opts, const std::string &cloud_path)
{
    ParsedCloudPath path = ParseCloudPathSpec(cloud_path);
    ObjectListResult result;
    if (!ListObjects(opts, path, result, false))
    {
        return {};
    }
    return result.keys;
}

}  // namespace

inline void CleanupStore(eloqstore::KvOptions opts)
{
    CleanupLocalStore(opts);
    if (!opts.cloud_store_path.empty())
    {
        ParsedCloudPath path = ParseCloudPathSpec(opts.cloud_store_path);
        if (!path.bucket.empty())
        {
            DeleteObjects(opts, path);
        }
    }
}

inline bool MoveCloudFile(const eloqstore::KvOptions &opts,
                          const std::string &cloud_path,
                          const std::string &src_file,
                          const std::string &dst_file)
{
    ParsedCloudPath path = ParseCloudPathSpec(cloud_path);
    if (path.bucket.empty())
    {
        return false;
    }
    S3TestClient client(opts);
    std::string src_key = ComposeObjectKey(path, src_file);
    std::string dst_key = ComposeObjectKey(path, dst_file);

    Aws::S3::Model::CopyObjectRequest copy_request;
    copy_request.SetBucket(path.bucket.c_str());
    copy_request.SetKey(dst_key.c_str());
    copy_request.SetCopySource((path.bucket + "/" + src_key).c_str());

    auto copy_outcome = client.Client().CopyObject(copy_request);
    if (!copy_outcome.IsSuccess())
    {
        LOG(ERROR) << "CopyObject failed: "
                   << copy_outcome.GetError().GetMessage();
        return false;
    }

    Aws::S3::Model::DeleteObjectRequest delete_request;
    delete_request.SetBucket(path.bucket.c_str());
    delete_request.SetKey(src_key.c_str());
    auto delete_outcome = client.Client().DeleteObject(delete_request);
    if (!delete_outcome.IsSuccess())
    {
        LOG(ERROR) << "DeleteObject failed: "
                   << delete_outcome.GetError().GetMessage();
        return false;
    }
    return true;
}

inline std::vector<std::string> ListCloudFiles(
    const eloqstore::KvOptions &opts,
    const std::string &cloud_path,
    const std::string &remote_path = "")
{
    std::string combined = cloud_path;
    combined = AppendRemoteComponent(combined, remote_path);
    return ListCloudFilesInternal(opts, combined);
}

inline std::optional<uint64_t> GetCloudSize(const eloqstore::KvOptions &opts,
                                            const std::string &cloud_path)
{
    ParsedCloudPath path = ParseCloudPathSpec(cloud_path);
    ObjectListResult result;
    if (!ListObjects(opts, path, result, true))
    {
        return std::nullopt;
    }
    return result.total_size;
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
