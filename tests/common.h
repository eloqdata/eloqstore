#pragma once

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/xml/XmlSerializer.h>
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
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
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
    .cloud_endpoint = "http://127.0.0.1:9900",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

const eloqstore::KvOptions cloud_archive_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 100 + eloqstore::num_reserved_fd,
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // disable automatic archive scheduling
    .file_amplify_factor = 2,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "eloqstore/unit-test",
    .cloud_endpoint = "http://127.0.0.1:9900",
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};
/**
 * Create (or replace) the per-binary test store. Stops and destroys any
 * previously created store first, so at most one EloqStore instance is ever
 * started in the process — required by the process-global Options() pointer.
 * All test store creation must go through here; do not construct EloqStore
 * directly in tests. Pass cleanup = false to keep existing local/cloud state
 * (warm-restart and cache-reuse tests).
 */
eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts,
                                bool cleanup = true);

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

// Minimal S3 REST client over signed plain HTTP; only needs aws-cpp-sdk-core.
class S3TestClient
{
public:
    explicit S3TestClient(const eloqstore::KvOptions &opts)
    {
        EnsureAwsSdkInitialized();
        auto settings = ResolveSettings(opts);
        std::lock_guard<std::mutex> lock(ClientMutex());
        auto &shared_client = SharedClient();
        auto &shared_settings = SharedSettings();
        if (!shared_client)
        {
            shared_client = BuildClient(settings);
            shared_settings = std::move(settings);
        }
        else if (!(shared_settings == settings))
        {
            LOG(FATAL) << "S3TestClient is process-global and was already "
                          "initialized with different settings";
        }
        client_ = shared_client;
        EnsureBucketCreated(ParseCloudPathSpec(opts.cloud_store_path).bucket);
    }

    ~S3TestClient() = default;

    // Appends one page of keys to `keys` and object bytes to `total_size`;
    // updates `continuation` (empty when the listing is exhausted).
    bool ListObjectsPage(const std::string &bucket,
                         const std::string &prefix,
                         std::string &continuation,
                         std::vector<std::string> &keys,
                         uint64_t &total_size)
    {
        Aws::Http::URI uri = MakeUri(bucket, "");
        uri.AddQueryStringParameter("list-type", "2");
        if (!prefix.empty())
        {
            uri.AddQueryStringParameter("prefix", prefix.c_str());
        }
        if (!continuation.empty())
        {
            uri.AddQueryStringParameter("continuation-token",
                                        continuation.c_str());
        }
        auto response = Execute(Aws::Http::HttpMethod::HTTP_GET, uri);
        if (!IsCode(response, {Aws::Http::HttpResponseCode::OK}))
        {
            LOG(ERROR) << "ListObjectsV2 failed: " << Describe(response);
            return false;
        }
        auto doc = Aws::Utils::Xml::XmlDocument::CreateFromXmlStream(
            response->GetResponseBody());
        if (!doc.WasParseSuccessful())
        {
            LOG(ERROR) << "ListObjectsV2 returned unparsable XML: "
                       << doc.GetErrorMessage();
            return false;
        }
        Aws::Utils::Xml::XmlNode root = doc.GetRootElement();
        for (Aws::Utils::Xml::XmlNode node = root.FirstChild("Contents");
             !node.IsNull();
             node = node.NextNode("Contents"))
        {
            Aws::Utils::Xml::XmlNode key_node = node.FirstChild("Key");
            if (key_node.IsNull())
            {
                continue;
            }
            keys.emplace_back(key_node.GetText().c_str());
            Aws::Utils::Xml::XmlNode size_node = node.FirstChild("Size");
            if (!size_node.IsNull())
            {
                total_size +=
                    std::strtoull(size_node.GetText().c_str(), nullptr, 10);
            }
        }
        Aws::Utils::Xml::XmlNode next =
            root.FirstChild("NextContinuationToken");
        continuation = next.IsNull() ? "" : next.GetText().c_str();
        return true;
    }

    bool DeleteObject(const std::string &bucket, const std::string &key)
    {
        auto response =
            Execute(Aws::Http::HttpMethod::HTTP_DELETE, MakeUri(bucket, key));
        if (!IsCode(response,
                    {Aws::Http::HttpResponseCode::NO_CONTENT,
                     Aws::Http::HttpResponseCode::OK}))
        {
            LOG(ERROR) << "DeleteObject failed for " << key << ": "
                       << Describe(response);
            return false;
        }
        return true;
    }

    bool CopyObject(const std::string &bucket,
                    const std::string &src_key,
                    const std::string &dst_key)
    {
        std::string copy_source = bucket + "/" + src_key;
        auto response = Execute(Aws::Http::HttpMethod::HTTP_PUT,
                                MakeUri(bucket, dst_key),
                                copy_source.c_str());
        if (!IsCode(response, {Aws::Http::HttpResponseCode::OK}))
        {
            LOG(ERROR) << "CopyObject failed: " << Describe(response);
            return false;
        }
        return true;
    }

private:
    struct HttpContext
    {
        std::shared_ptr<Aws::Http::HttpClient> http_client;
        std::shared_ptr<Aws::Client::AWSAuthV4Signer> signer;
        std::string endpoint;
    };
    struct ClientSettings
    {
        std::string endpoint;
        std::string region;
        bool verify_ssl{false};
        bool requested_auto_credentials{false};
        bool auto_credentials{false};
        std::string access_key;
        std::string secret_key;

        bool operator==(const ClientSettings &other) const = default;
    };

    static ClientSettings ResolveSettings(const eloqstore::KvOptions &opts)
    {
        ClientSettings settings;
        settings.endpoint = opts.cloud_endpoint.empty()
                                ? std::string(kDefaultTestAwsEndpoint)
                                : opts.cloud_endpoint;
        settings.region = opts.cloud_region.empty()
                              ? std::string(kDefaultTestAwsRegion)
                              : opts.cloud_region;
        settings.verify_ssl =
            opts.cloud_endpoint.empty() ? false : opts.cloud_verify_ssl;
        settings.requested_auto_credentials = opts.cloud_auto_credentials;
        settings.auto_credentials =
            opts.cloud_auto_credentials && opts.cloud_secret_key.empty();
        settings.access_key = opts.cloud_access_key;
        settings.secret_key = opts.cloud_secret_key;
        return settings;
    }

    static std::shared_ptr<HttpContext> BuildClient(
        const ClientSettings &settings)
    {
        Aws::Client::ClientConfiguration config;
        config.region = settings.region.c_str();
        config.verifySSL = settings.verify_ssl;
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider;
        if (settings.auto_credentials)
        {
            provider =
                Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(
                    "eloqstore");
        }
        else
        {
            if (settings.requested_auto_credentials &&
                !settings.secret_key.empty())
            {
                LOG(INFO)
                    << "cloud_secret_key is set; disabling auto credentials";
            }
            provider = Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
                "eloqstore",
                Aws::String(settings.access_key.c_str()),
                Aws::String(settings.secret_key.c_str()));
        }
        auto ctx = std::make_shared<HttpContext>();
        ctx->http_client = Aws::Http::CreateHttpClient(config);
        ctx->signer = std::make_shared<Aws::Client::AWSAuthV4Signer>(
            provider,
            "s3",
            config.region,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
        ctx->endpoint = settings.endpoint;
        return ctx;
    }

    Aws::Http::URI MakeUri(const std::string &bucket,
                           const std::string &key) const
    {
        Aws::Http::URI uri(client_->endpoint.c_str());
        uri.AddPathSegment(bucket);
        if (!key.empty())
        {
            uri.AddPathSegments(key);
        }
        return uri;
    }

    std::shared_ptr<Aws::Http::HttpResponse> Execute(
        Aws::Http::HttpMethod method,
        const Aws::Http::URI &uri,
        const char *copy_source = nullptr)
    {
        auto request = Aws::Http::CreateHttpRequest(
            uri,
            method,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
        if (copy_source != nullptr)
        {
            request->SetHeaderValue("x-amz-copy-source", copy_source);
        }
        if (!client_->signer->SignRequest(*request))
        {
            return nullptr;
        }
        return client_->http_client->MakeRequest(request);
    }

    static bool IsCode(
        const std::shared_ptr<Aws::Http::HttpResponse> &response,
        std::initializer_list<Aws::Http::HttpResponseCode> accepted)
    {
        if (!response)
        {
            return false;
        }
        Aws::Http::HttpResponseCode code = response->GetResponseCode();
        return std::find(accepted.begin(), accepted.end(), code) !=
               accepted.end();
    }

    static std::string Describe(
        const std::shared_ptr<Aws::Http::HttpResponse> &response)
    {
        if (!response)
        {
            return "request could not be signed or sent";
        }
        std::ostringstream oss;
        oss << "http status " << static_cast<int>(response->GetResponseCode())
            << ": ";
        oss << response->GetResponseBody().rdbuf();
        return oss.str();
    }

    void EnsureBucketCreated(const std::string &bucket)
    {
        if (bucket.empty())
        {
            return;
        }
        auto &created = CreatedBucket();
        auto &created_bucket = CreatedBucketName();
        if (created)
        {
            if (created_bucket != bucket)
            {
                LOG(FATAL) << "S3 test bucket changed from '" << created_bucket
                           << "' to '" << bucket << "'";
            }
            return;
        }

        auto response =
            Execute(Aws::Http::HttpMethod::HTTP_PUT, MakeUri(bucket, ""));
        if (!IsCode(response, {Aws::Http::HttpResponseCode::OK}))
        {
            std::string detail = Describe(response);
            if (detail.find("BucketAlreadyOwnedByYou") == std::string::npos &&
                detail.find("BucketAlreadyExists") == std::string::npos)
            {
                LOG(FATAL) << "CreateBucket failed for " << bucket << ": "
                           << detail;
            }
        }
        created = true;
        created_bucket = bucket;
    }

    static std::mutex &ClientMutex()
    {
        static std::mutex mutex;
        return mutex;
    }

    static std::shared_ptr<HttpContext> &SharedClient()
    {
        static std::shared_ptr<HttpContext> client;
        return client;
    }

    static ClientSettings &SharedSettings()
    {
        static ClientSettings settings;
        return settings;
    }

    static bool &CreatedBucket()
    {
        static bool created = false;
        return created;
    }

    static std::string &CreatedBucketName()
    {
        static std::string bucket;
        return bucket;
    }

    std::shared_ptr<HttpContext> client_;
};

struct ObjectListResult
{
    std::vector<std::string> keys;
    uint64_t total_size{0};
};

inline bool ListObjects(S3TestClient &client,
                        const ParsedCloudPath &path,
                        ObjectListResult &out,
                        bool raw_keys)
{
    if (path.bucket.empty())
    {
        return false;
    }
    std::string request_prefix = path.prefix;
    if (!request_prefix.empty() && request_prefix.back() != '/')
    {
        request_prefix.push_back('/');
    }

    std::string continuation;
    do
    {
        std::vector<std::string> page_keys;
        if (!client.ListObjectsPage(path.bucket,
                                    request_prefix,
                                    continuation,
                                    page_keys,
                                    out.total_size))
        {
            return false;
        }
        for (std::string &key : page_keys)
        {
            if (!raw_keys && !request_prefix.empty() &&
                key.rfind(request_prefix, 0) == 0)
            {
                key.erase(0, request_prefix.size());
            }
            out.keys.push_back(std::move(key));
        }
    } while (!continuation.empty());

    return true;
}

inline bool ListObjects(const eloqstore::KvOptions &opts,
                        const ParsedCloudPath &path,
                        ObjectListResult &out,
                        bool raw_keys)
{
    S3TestClient client(opts);
    return ListObjects(client, path, out, raw_keys);
}

inline bool DeleteObjects(S3TestClient &client, const ParsedCloudPath &path)
{
    ObjectListResult objects;
    if (!ListObjects(client, path, objects, true))
    {
        return false;
    }
    if (objects.keys.empty())
    {
        return true;
    }
    for (const auto &key : objects.keys)
    {
        if (!client.DeleteObject(path.bucket, key))
        {
            return false;
        }
    }
    return true;
}

inline bool DeleteObjects(const eloqstore::KvOptions &opts,
                          const ParsedCloudPath &path)
{
    S3TestClient client(opts);
    return DeleteObjects(client, path);
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

    if (!client.CopyObject(path.bucket, src_key, dst_key))
    {
        return false;
    }
    return client.DeleteObject(path.bucket, src_key);
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
