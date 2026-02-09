#include "storage/object_store.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <ios>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "cloud_storage_service.h"
#include "storage/shard.h"
#include "tasks/task.h"
#include "utils.h"

namespace eloqstore
{
namespace fs = std::filesystem;
namespace
{
std::mutex g_aws_mutex;
size_t g_aws_refcount = 0;
bool g_aws_initialized = false;
Aws::SDKOptions g_sdk_options;
bool g_aws_cleanup_registered = false;

constexpr std::string_view kDefaultAwsEndpoint = "http://127.0.0.1:9900";
constexpr std::string_view kDefaultAwsRegion = "us-east-1";
constexpr std::string_view kDefaultGcsEndpoint =
    "https://storage.googleapis.com";
constexpr std::string_view kDefaultGcsRegion = "auto";

size_t AppendToString(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t total = size * nmemb;
    if (!userp || total == 0)
    {
        return total;
    }
    auto *buffer = static_cast<std::string *>(userp);
    buffer->append(static_cast<char *>(contents), total);
    return total;
}

bool ParseJsonListResponse(std::string_view payload,
                           std::vector<std::string> *objects,
                           std::vector<utils::CloudObjectInfo> *infos)
{
    Json::Value response;
    Json::Reader reader;
    if (!reader.parse(
            payload.data(), payload.data() + payload.size(), response, false))
    {
        return false;
    }
    if (!response.isMember("list") || !response["list"].isArray())
    {
        return false;
    }
    if (objects)
    {
        objects->clear();
    }
    if (infos)
    {
        infos->clear();
    }
    for (const auto &item : response["list"])
    {
        std::string name;
        if (item.isMember("Name") && item["Name"].isString())
        {
            name = item["Name"].asString();
        }
        std::string path;
        if (item.isMember("Path") && item["Path"].isString())
        {
            path = item["Path"].asString();
        }
        bool is_dir = item.isMember("IsDir") && item["IsDir"].isBool()
                          ? item["IsDir"].asBool()
                          : false;
        uint64_t size = 0;
        if (item.isMember("Size"))
        {
            const Json::Value &size_value = item["Size"];
            if (size_value.isUInt64())
            {
                size = size_value.asUInt64();
            }
            else if (size_value.isString())
            {
                try
                {
                    size = std::stoull(size_value.asString());
                }
                catch (const std::exception &)
                {
                    size = 0;
                }
            }
        }
        std::string mod_time;
        if (item.isMember("ModTime") && item["ModTime"].isString())
        {
            mod_time = item["ModTime"].asString();
        }

        if (objects && !name.empty())
        {
            objects->push_back(name);
        }
        if (infos)
        {
            utils::CloudObjectInfo info;
            info.name = name;
            info.path = path;
            info.size = size;
            info.is_dir = is_dir;
            info.mod_time = mod_time;
            infos->push_back(std::move(info));
        }
    }
    return true;
}

std::string DecodeXmlEntities(std::string_view value)
{
    std::string result;
    result.reserve(value.size());
    for (size_t i = 0; i < value.size();)
    {
        if (value[i] != '&')
        {
            result.push_back(value[i]);
            ++i;
            continue;
        }

        if (value.substr(i, 5) == "&amp;")
        {
            result.push_back('&');
            i += 5;
        }
        else if (value.substr(i, 4) == "&lt;")
        {
            result.push_back('<');
            i += 4;
        }
        else if (value.substr(i, 4) == "&gt;")
        {
            result.push_back('>');
            i += 4;
        }
        else if (value.substr(i, 6) == "&quot;")
        {
            result.push_back('"');
            i += 6;
        }
        else if (value.substr(i, 6) == "&apos;")
        {
            result.push_back(static_cast<char>(39));
            i += 6;
        }
        else
        {
            result.push_back(value[i]);
            ++i;
        }
    }
    return result;
}

std::string ExtractTagValue(std::string_view block, std::string_view tag)
{
    std::string open = "<" + std::string(tag) + ">";
    std::string close = "</" + std::string(tag) + ">";
    size_t start = block.find(open);
    if (start == std::string::npos)
    {
        return {};
    }
    start += open.size();
    size_t end = block.find(close, start);
    if (end == std::string::npos)
    {
        return {};
    }
    return std::string(block.substr(start, end - start));
}

std::string UrlEncode(std::string_view value)
{
    static constexpr char kHexDigits[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (unsigned char c : value)
    {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
        {
            encoded.push_back(static_cast<char>(c));
        }
        else
        {
            encoded.push_back('%');
            encoded.push_back(kHexDigits[(c >> 4) & 0xF]);
            encoded.push_back(kHexDigits[c & 0xF]);
        }
    }
    return encoded;
}

std::string StripPrefixFromKey(std::string key, const std::string &strip_prefix)
{
    if (!strip_prefix.empty() && key.rfind(strip_prefix, 0) == 0)
    {
        key.erase(0, strip_prefix.size());
    }
    else if (!strip_prefix.empty())
    {
        std::string without_slash = strip_prefix;
        if (!without_slash.empty() && without_slash.back() == '/')
        {
            without_slash.pop_back();
        }
        if (!without_slash.empty() && key.rfind(without_slash, 0) == 0)
        {
            key.erase(0, without_slash.size());
        }
    }
    while (!key.empty() && key.front() == '/')
    {
        key.erase(key.begin());
    }
    return key;
}

void AppendParsedEntry(const std::string &decoded_key,
                       const std::string &strip_prefix,
                       bool is_dir,
                       uint64_t size,
                       const std::string &mod_time,
                       std::vector<std::string> *objects,
                       std::vector<utils::CloudObjectInfo> *infos)
{
    std::string relative = StripPrefixFromKey(decoded_key, strip_prefix);
    if (relative.empty())
    {
        return;
    }
    if (is_dir && !relative.empty() && relative.back() == '/')
    {
        relative.pop_back();
    }

    std::string name = relative;
    size_t pos = name.find_last_of('/');
    if (pos != std::string::npos)
    {
        name.erase(0, pos + 1);
    }

    if (objects && !name.empty())
    {
        objects->push_back(name);
    }
    if (infos)
    {
        utils::CloudObjectInfo info;
        info.name = name;
        info.path = relative;
        info.size = size;
        info.is_dir = is_dir;
        info.mod_time = mod_time;
        infos->push_back(std::move(info));
    }
}

bool ParseS3XmlListResponse(std::string_view payload,
                            const std::string &strip_prefix,
                            std::vector<std::string> *objects,
                            std::vector<utils::CloudObjectInfo> *infos,
                            std::string *next_continuation_token)
{
    if (objects)
    {
        objects->clear();
    }
    if (infos)
    {
        infos->clear();
    }

    std::string normalized_strip = strip_prefix;
    if (!normalized_strip.empty() && normalized_strip.back() != '/')
    {
        normalized_strip.push_back('/');
    }

    size_t pos = 0;
    const std::string contents_start = "<Contents>";
    const std::string contents_end = "</Contents>";
    while (true)
    {
        size_t start = payload.find(contents_start, pos);
        if (start == std::string::npos)
        {
            break;
        }
        size_t end = payload.find(contents_end, start);
        if (end == std::string::npos)
        {
            return false;
        }
        size_t inner_begin = start + contents_start.size();
        std::string block(payload.substr(inner_begin, end - inner_begin));
        pos = end + contents_end.size();

        std::string key = ExtractTagValue(block, "Key");
        if (key.empty())
        {
            continue;
        }
        std::string decoded_key = DecodeXmlEntities(key);
        std::string size_str = ExtractTagValue(block, "Size");
        uint64_t size = 0;
        if (!size_str.empty())
        {
            try
            {
                size = std::stoull(size_str);
            }
            catch (const std::exception &)
            {
                size = 0;
            }
        }
        std::string mod_time =
            DecodeXmlEntities(ExtractTagValue(block, "LastModified"));
        AppendParsedEntry(decoded_key,
                          normalized_strip,
                          false,
                          size,
                          mod_time,
                          objects,
                          infos);
    }

    pos = 0;
    const std::string prefix_start = "<CommonPrefixes>";
    const std::string prefix_end = "</CommonPrefixes>";
    while (true)
    {
        size_t start = payload.find(prefix_start, pos);
        if (start == std::string::npos)
        {
            break;
        }
        size_t end = payload.find(prefix_end, start);
        if (end == std::string::npos)
        {
            return false;
        }
        size_t inner_begin = start + prefix_start.size();
        std::string block(payload.substr(inner_begin, end - inner_begin));
        pos = end + prefix_end.size();
        std::string prefix_value = ExtractTagValue(block, "Prefix");
        if (prefix_value.empty())
        {
            continue;
        }
        std::string decoded_prefix = DecodeXmlEntities(prefix_value);
        AppendParsedEntry(decoded_prefix,
                          normalized_strip,
                          true,
                          0,
                          std::string{},
                          objects,
                          infos);
    }

    // Extract pagination info
    if (next_continuation_token)
    {
        std::string is_truncated = ExtractTagValue(payload, "IsTruncated");
        if (is_truncated == "true")
        {
            *next_continuation_token =
                ExtractTagValue(payload, "NextContinuationToken");
        }
        else
        {
            next_continuation_token->clear();
        }
    }

    return true;
}

void CleanupAws()
{
    std::lock_guard lk(g_aws_mutex);
    if (g_aws_initialized)
    {
        Aws::ShutdownAPI(g_sdk_options);
        g_aws_initialized = false;
    }
}

CloudPathInfo ParseCloudPath(const std::string &spec)
{
    CloudPathInfo info;
    std::string_view path(spec);
    auto colon = path.find(':');
    if (colon != std::string::npos)
    {
        LOG(FATAL) << "cloud_store_path no longer supports prefixes like '"
                   << spec << "'";
    }
    while (!path.empty() && path.front() == '/')
    {
        path.remove_prefix(1);
    }
    size_t slash = path.find('/');
    if (slash == std::string::npos)
    {
        info.bucket.assign(path.data(), path.size());
    }
    else
    {
        info.bucket.assign(path.substr(0, slash).data(), slash);
        std::string_view prefix = path.substr(slash + 1);
        while (!prefix.empty() && prefix.front() == '/')
        {
            prefix.remove_prefix(1);
        }
        while (!prefix.empty() && prefix.back() == '/')
        {
            prefix.remove_suffix(1);
        }
        info.prefix.assign(prefix.data(), prefix.size());
    }
    if (info.bucket.empty())
    {
        LOG(FATAL) << "Invalid cloud_store_path: missing bucket name";
    }
    return info;
}

enum class BackendType
{
    kAws,
    kGcs
};

BackendType DetermineBackendType(const KvOptions *options)
{
    std::string provider = options->cloud_provider;
    std::string lowered;
    lowered.reserve(provider.size());
    for (char c : provider)
    {
        lowered.push_back(
            static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    if (lowered == "gcs" || lowered == "google" || lowered == "google-cloud")
    {
        return BackendType::kGcs;
    }
    return BackendType::kAws;
}

class AwsCloudBackend : public CloudBackend
{
public:
    AwsCloudBackend(const KvOptions *options, CloudPathInfo path)
        : options_(options), cloud_path_(std::move(path))
    {
        credentials_provider_ = AwsCloudBackend::BuildCredentialsProvider();
        const auto &client_config = GetClientConfig();
        s3_client_ = std::make_unique<Aws::S3::S3Client>(
            credentials_provider_,
            client_config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            AwsCloudBackend::UseVirtualAddressing());
        signer_ = std::make_unique<Aws::Client::AWSAuthV4Signer>(
            credentials_provider_,
            "s3",
            client_config.region,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
        bucket_url_ = BuildBucketUrl();
        if (bucket_url_.empty())
        {
            LOG(ERROR) << "Failed to build bucket URL for listing";
        }
    }

    std::string CreateSignedUrl(CloudHttpMethod method,
                                const std::string &key) override
    {
        Aws::Http::HttpMethod aws_method = ToAwsMethod(method);
        Aws::String url = s3_client_->GeneratePresignedUrl(
            cloud_path_.bucket, key, aws_method, 3600);
        return {url};
    }

    bool ParseListObjectsResponse(
        std::string_view payload,
        const std::string &strip_prefix,
        std::vector<std::string> *objects,
        std::vector<utils::CloudObjectInfo> *infos,
        std::string *next_continuation_token) const override
    {
        if (ParseJsonListResponse(payload, objects, infos))
        {
            if (next_continuation_token)
            {
                next_continuation_token->clear();  // JSON format not paginated
            }
            return true;
        }
        return ParseS3XmlListResponse(
            payload, strip_prefix, objects, infos, next_continuation_token);
    }

protected:
    virtual Aws::Client::ClientConfiguration BuildClientConfig() const
    {
        Aws::Client::ClientConfiguration config;
        std::string endpoint = options_->cloud_endpoint.empty()
                                   ? DefaultEndpoint()
                                   : options_->cloud_endpoint;
        if (!endpoint.empty())
        {
            config.endpointOverride =
                Aws::String(endpoint.c_str(), endpoint.size());
            if (endpoint.rfind("https://", 0) == 0)
            {
                config.scheme = Aws::Http::Scheme::HTTPS;
            }
            else
            {
                config.scheme = Aws::Http::Scheme::HTTP;
            }
        }
        std::string region = options_->cloud_region.empty()
                                 ? DefaultRegion()
                                 : options_->cloud_region;
        config.region = Aws::String(region.c_str(), region.size());
        bool verify_ssl = options_->cloud_endpoint.empty()
                              ? DefaultVerifySsl()
                              : options_->cloud_verify_ssl;
        config.verifySSL = verify_ssl;
        return config;
    }

    virtual std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
    BuildCredentialsProvider() const
    {
        return Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
            "eloqstore",
            options_->cloud_access_key.c_str(),
            options_->cloud_secret_key.c_str());
    }

    virtual std::string DefaultEndpoint() const
    {
        return std::string(kDefaultAwsEndpoint);
    }

    virtual std::string DefaultRegion() const
    {
        return std::string(kDefaultAwsRegion);
    }

    virtual bool DefaultVerifySsl() const
    {
        return false;
    }

    virtual bool UseVirtualAddressing() const
    {
        return false;
    }

    bool BuildListRequest(const std::string &prefix,
                          bool recursive,
                          const std::string &continuation,
                          SignedRequestInfo *request) const override
    {
        if (!request)
        {
            return false;
        }
        request->url.clear();
        request->headers.clear();
        request->body.clear();
        if (bucket_url_.empty() || !signer_)
        {
            return false;
        }

        std::string target_url =
            ComposeListUrl(prefix, recursive, continuation);
        if (target_url.empty())
        {
            return false;
        }

        Aws::Http::URI uri(target_url.c_str());
        auto list_request =
            Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
                "eloqstore", uri, Aws::Http::HttpMethod::HTTP_GET);
        if (!list_request)
        {
            return false;
        }
        if (!signer_->SignRequest(*list_request))
        {
            return false;
        }

        request->url = list_request->GetUri().GetURIString().c_str();
        for (const auto &header : list_request->GetHeaders())
        {
            request->headers.emplace_back(std::string(header.first.c_str()) +
                                          ": " +
                                          std::string(header.second.c_str()));
        }
        return true;
    }

    bool BuildCreateBucketRequest(SignedRequestInfo *request) const override
    {
        if (!request)
        {
            return false;
        }
        request->url.clear();
        request->headers.clear();
        request->body.clear();
        if (bucket_url_.empty() || !signer_)
        {
            return false;
        }

        Aws::Http::URI uri(bucket_url_.c_str());
        auto create_request =
            Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
                "eloqstore", uri, Aws::Http::HttpMethod::HTTP_PUT);
        if (!create_request)
        {
            return false;
        }

        std::string default_region = DefaultRegion();
        std::string region = options_->cloud_region.empty()
                                 ? default_region
                                 : options_->cloud_region;
        if (!region.empty() && region != default_region)
        {
            std::string xml_body =
                "<CreateBucketConfiguration "
                "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
            xml_body.append("<LocationConstraint>");
            xml_body.append(region);
            xml_body.append(
                "</LocationConstraint></CreateBucketConfiguration>");

            auto body_stream = Aws::MakeShared<Aws::StringStream>("eloqstore");
            if (!body_stream)
            {
                return false;
            }
            *body_stream << xml_body;
            body_stream->flush();
            body_stream->seekg(0, std::ios_base::beg);
            create_request->AddContentBody(body_stream);
            create_request->SetHeaderValue(Aws::Http::CONTENT_TYPE_HEADER,
                                           "application/xml");
            std::string body_length = std::to_string(xml_body.size());
            create_request->SetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER,
                                           Aws::String(body_length.c_str()));
            request->body = std::move(xml_body);
        }
        if (!signer_->SignRequest(*create_request))
        {
            return false;
        }

        request->url = create_request->GetUri().GetURIString().c_str();
        for (const auto &header : create_request->GetHeaders())
        {
            request->headers.emplace_back(std::string(header.first.c_str()) +
                                          ": " +
                                          std::string(header.second.c_str()));
        }
        return true;
    }

    std::string BuildBucketUrl() const
    {
        std::string endpoint = options_->cloud_endpoint.empty()
                                   ? DefaultEndpoint()
                                   : options_->cloud_endpoint;
        if (endpoint.empty())
        {
            return {};
        }
        while (!endpoint.empty() && endpoint.back() == '/')
        {
            endpoint.pop_back();
        }
        if (UseVirtualAddressing())
        {
            return endpoint;
        }
        endpoint.push_back('/');
        endpoint.append(cloud_path_.bucket);
        return endpoint;
    }

    std::string ComposeListUrl(const std::string &prefix,
                               bool recursive,
                               const std::string &continuation) const
    {
        if (bucket_url_.empty())
        {
            return {};
        }
        std::string query = "list-type=2";
        if (!prefix.empty())
        {
            query.append("&prefix=");
            query.append(UrlEncode(prefix));
        }
        if (!recursive)
        {
            query.append("&delimiter=%2F");
        }
        if (!continuation.empty())
        {
            query.append("&continuation-token=");
            query.append(UrlEncode(continuation));
        }
        std::string url = bucket_url_;
        url.push_back('?');
        url.append(query);
        return url;
    }

    static Aws::Http::HttpMethod ToAwsMethod(CloudHttpMethod method)
    {
        switch (method)
        {
        case CloudHttpMethod::kPut:
            return Aws::Http::HttpMethod::HTTP_PUT;
        case CloudHttpMethod::kDelete:
            return Aws::Http::HttpMethod::HTTP_DELETE;
        case CloudHttpMethod::kGet:
        default:
            return Aws::Http::HttpMethod::HTTP_GET;
        }
    }

    const Aws::Client::ClientConfiguration &GetClientConfig() const
    {
        std::call_once(
            client_config_once_,
            [this]
            {
                client_config_ =
                    std::make_unique<Aws::Client::ClientConfiguration>(
                        BuildClientConfig());
            });
        return *client_config_;
    }

    const KvOptions *options_;
    CloudPathInfo cloud_path_;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider_;
    std::unique_ptr<Aws::S3::S3Client> s3_client_;
    std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer_;
    std::string bucket_url_;

    inline static std::once_flag client_config_once_;
    inline static std::unique_ptr<Aws::Client::ClientConfiguration>
        client_config_;
};
class GcsCloudBackend : public AwsCloudBackend
{
public:
    GcsCloudBackend(const KvOptions *options, CloudPathInfo path)
        : AwsCloudBackend(options, std::move(path))
    {
    }

    bool ParseListObjectsResponse(
        std::string_view payload,
        const std::string &strip_prefix,
        std::vector<std::string> *objects,
        std::vector<utils::CloudObjectInfo> *infos,
        std::string *next_continuation_token) const override
    {
        if (ParseJsonListResponse(payload, objects, infos))
        {
            if (next_continuation_token)
            {
                next_continuation_token->clear();
            }
            return true;
        }
        return ParseS3XmlListResponse(
            payload, strip_prefix, objects, infos, next_continuation_token);
    }

protected:
    std::string DefaultEndpoint() const override
    {
        return std::string(kDefaultGcsEndpoint);
    }

    std::string DefaultRegion() const override
    {
        return std::string(kDefaultGcsRegion);
    }

    bool DefaultVerifySsl() const override
    {
        return true;
    }
};

std::unique_ptr<CloudBackend> CreateBackend(const KvOptions *options,
                                            const CloudPathInfo &path)
{
    switch (DetermineBackendType(options))
    {
    case BackendType::kGcs:
        return std::make_unique<GcsCloudBackend>(options, path);
    case BackendType::kAws:
    default:
        return std::make_unique<AwsCloudBackend>(options, path);
    }
}

}  // namespace

ObjectStore::ObjectStore(const KvOptions *options, CloudStorageService *service)
    : cloud_service_(service)
{
    CHECK(cloud_service_ != nullptr);
    curl_global_init(CURL_GLOBAL_DEFAULT);
    {
        std::lock_guard lk(g_aws_mutex);
        if (g_aws_refcount == 0 && !g_aws_initialized)
        {
            Aws::InitAPI(g_sdk_options);
            g_aws_initialized = true;
        }
        g_aws_refcount++;
        if (!g_aws_cleanup_registered)
        {
            std::atexit(CleanupAws);
            g_aws_cleanup_registered = true;
        }
    }
    async_http_mgr_ = std::make_unique<AsyncHttpManager>(options, service);
}

ObjectStore::~ObjectStore()
{
    async_http_mgr_.reset();
    {
        std::lock_guard lk(g_aws_mutex);
        if (g_aws_refcount > 0)
        {
            g_aws_refcount--;
        }
    }
    curl_global_cleanup();
}

bool ObjectStore::ParseListObjectsResponse(
    std::string_view payload,
    const std::string &strip_prefix,
    std::vector<std::string> *objects,
    std::vector<utils::CloudObjectInfo> *infos,
    std::string *next_continuation_token) const
{
    return async_http_mgr_->ParseListObjectsResponse(
        payload, strip_prefix, objects, infos, next_continuation_token);
}

KvError ObjectStore::EnsureBucketExists()
{
    if (!async_http_mgr_)
    {
        return KvError::CloudErr;
    }
    return async_http_mgr_->EnsureBucketExists();
}

void ObjectStore::SubmitTask(ObjectStore::Task *task, Shard *owner_shard)
{
    CHECK(task != nullptr);
    CHECK(owner_shard != nullptr);
    task->SetOwnerShard(owner_shard);
    CHECK(task->kv_task_ != nullptr);
    task->kv_task_->inflight_io_++;
    cloud_service_->Submit(this, task);
}

void ObjectStore::StartHttpRequest(ObjectStore::Task *task)
{
    CHECK(async_http_mgr_ != nullptr);
    async_http_mgr_->SubmitRequest(task);
}

void ObjectStore::RunHttpWork()
{
    async_http_mgr_->PerformRequests();
    async_http_mgr_->ProcessCompletedRequests();
}

bool ObjectStore::HttpWorkIdle() const
{
    return async_http_mgr_->IsIdle();
}

AsyncHttpManager::AsyncHttpManager(const KvOptions *option,
                                   CloudStorageService *service)
    : cloud_service_(service)
{
    if (option->cloud_store_path.empty())
    {
        LOG(FATAL) << "cloud_store_path must be set when using cloud store";
    }
    cloud_path_ = ParseCloudPath(option->cloud_store_path);

    backend_ = CreateBackend(option, cloud_path_);
    CHECK(backend_) << "Failed to initialize cloud backend";

    multi_handle_ = curl_multi_init();
    if (!multi_handle_)
    {
        LOG(FATAL) << "Failed to initialize cURL multi handle";
    }
    curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, 200L);
}

AsyncHttpManager::~AsyncHttpManager()
{
    Cleanup();
    if (multi_handle_)
    {
        curl_multi_cleanup(multi_handle_);
    }
}

bool AsyncHttpManager::ParseListObjectsResponse(
    std::string_view payload,
    const std::string &strip_prefix,
    std::vector<std::string> *objects,
    std::vector<utils::CloudObjectInfo> *infos,
    std::string *next_continuation_token) const
{
    if (!backend_)
    {
        return false;
    }
    return backend_->ParseListObjectsResponse(
        payload, strip_prefix, objects, infos, next_continuation_token);
}

KvError AsyncHttpManager::EnsureBucketExists()
{
    if (!backend_)
    {
        LOG(ERROR) << "Cloud backend is not initialized";
        return KvError::CloudErr;
    }

    SignedRequestInfo request_info;
    if (!backend_->BuildCreateBucketRequest(&request_info) ||
        request_info.url.empty())
    {
        LOG(ERROR) << "Failed to build bucket creation request";
        return KvError::CloudErr;
    }

    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR)
            << "Failed to initialize cURL easy handle for bucket creation";
        return KvError::CloudErr;
    }

    std::string response_body;
    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, AppendToString);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    const char *body_ptr =
        request_info.body.empty() ? "" : request_info.body.c_str();
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body_ptr);
    curl_easy_setopt(easy,
                     CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<int64_t>(request_info.body.size()));

    curl_slist *headers = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        headers = curl_slist_append(headers, header_line.c_str());
    }
    if (headers)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
    }

    CURLcode res = curl_easy_perform(easy);
    int64_t response_code = 0;
    if (res == CURLE_OK)
    {
        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
    }

    KvError result = KvError::NoError;
    if (res != CURLE_OK)
    {
        LOG(ERROR) << "Bucket creation request failed: "
                   << curl_easy_strerror(res);
        result = ClassifyCurlError(res);
    }
    else if ((response_code >= 200 && response_code < 300) ||
             response_code == 409)
    {
        if (response_code == 409)
        {
            LOG(INFO) << "Cloud bucket '" << cloud_path_.bucket
                      << "' already exists";
        }
        else
        {
            LOG(INFO) << "Created cloud bucket '" << cloud_path_.bucket
                      << "' (HTTP " << response_code << ")";
        }
        result = KvError::NoError;
    }
    else
    {
        LOG(ERROR) << "Bucket creation failed with HTTP " << response_code
                   << ": " << response_body;
        result = ClassifyHttpError(response_code);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(easy);
    return result;
}

void AsyncHttpManager::PerformRequests()
{
    ProcessPendingRetries();
    if (!multi_handle_ || active_requests_.empty())
    {
        return;
    }
    curl_multi_perform(multi_handle_, &running_handles_);
}

void AsyncHttpManager::SubmitRequest(ObjectStore::Task *task)
{
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    task->waiting_retry_ = false;

    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR) << "Failed to initialize cURL easy handle";
        task->error_ = KvError::CloudErr;
        OnTaskFinished(task);
        return;
    }

    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, task);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, HeaderCallback);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, task);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, task);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, 300L);

    bool setup_ok = false;
    switch (task->TaskType())
    {
    case ObjectStore::Task::Type::AsyncDownload:
        setup_ok = SetupDownloadRequest(
            static_cast<ObjectStore::DownloadTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncUpload:
        setup_ok = SetupUploadRequest(
            static_cast<ObjectStore::UploadTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncDelete:
        setup_ok = SetupDeleteRequest(
            static_cast<ObjectStore::DeleteTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncList:
        setup_ok =
            SetupListRequest(static_cast<ObjectStore::ListTask *>(task), easy);
        break;
    default:
        LOG(ERROR) << "Unknown async task type";
        task->error_ = KvError::CloudErr;
        break;
    }

    if (!setup_ok)
    {
        CleanupTaskResources(task);
        curl_easy_cleanup(easy);
        OnTaskFinished(task);
        return;
    }

    CURLMcode mres = curl_multi_add_handle(multi_handle_, easy);
    if (mres != CURLM_OK)
    {
        LOG(ERROR) << "Failed to add handle to multi: "
                   << curl_multi_strerror(mres);
        curl_easy_cleanup(easy);
        task->error_ = KvError::CloudErr;
        OnTaskFinished(task);
        return;
    }

    active_requests_[easy] = task;
}

std::string AsyncHttpManager::ComposeKey(const TableIdent *tbl_id,
                                         std::string_view filename) const
{
    std::string key;
    auto append_component = [&](std::string_view part)
    {
        if (part.empty())
        {
            return;
        }
        if (!key.empty())
        {
            key.push_back('/');
        }
        key.append(part.data(), part.size());
    };

    append_component(cloud_path_.prefix);
    std::string tbl = tbl_id->ToString();
    append_component(tbl);
    append_component(filename);
    return key;
}

std::string AsyncHttpManager::ComposeKeyFromRemote(
    std::string_view remote_path, bool ensure_trailing_slash) const
{
    std::string key;
    auto append_component = [&](std::string_view part)
    {
        if (part.empty())
        {
            return;
        }
        if (!key.empty() && key.back() != '/')
        {
            key.push_back('/');
        }
        key.append(part.data(), part.size());
    };

    append_component(cloud_path_.prefix);

    std::string_view trimmed = remote_path;
    while (!trimmed.empty() && trimmed.front() == '/')
    {
        trimmed.remove_prefix(1);
    }
    while (!trimmed.empty() && trimmed.back() == '/')
    {
        trimmed.remove_suffix(1);
    }
    append_component(trimmed);

    if (ensure_trailing_slash && !key.empty() && key.back() != '/')
    {
        key.push_back('/');
    }
    return key;
}

bool AsyncHttpManager::SetupDownloadRequest(ObjectStore::DownloadTask *task,
                                            CURL *easy)
{
    std::string key = ComposeKey(task->tbl_id_, task->filename_);
    task->json_data_ = backend_->CreateSignedUrl(CloudHttpMethod::kGet, key);
    if (task->json_data_.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    curl_easy_setopt(easy, CURLOPT_URL, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    task->headers_ = nullptr;
    return true;
}

bool AsyncHttpManager::SetupUploadRequest(ObjectStore::UploadTask *task,
                                          CURL *easy)
{
    const std::string &filename = task->filename_;
    // Inline full-buffer upload only.
    // When file_size_ is not prefilled by caller, infer it from buffer size.
    if (task->file_size_ == 0)
    {
        task->file_size_ = task->data_buffer_.size();
    }
    if (task->data_buffer_.empty() && task->file_size_ > 0)
    {
        LOG(ERROR) << "UploadTask missing inline upload buffer for file "
                   << filename << " file_size=" << task->file_size_;
        task->error_ = KvError::InvalidArgs;
        return false;
    }
    if (task->data_buffer_.size() != task->file_size_)
    {
        LOG(ERROR) << "UploadTask buffer size mismatch for file " << filename
                   << " buffer_size=" << task->data_buffer_.size()
                   << " file_size=" << task->file_size_;
        task->error_ = KvError::InvalidArgs;
        return false;
    }

    std::string key = ComposeKey(task->tbl_id_, filename);
    task->json_data_ = backend_->CreateSignedUrl(CloudHttpMethod::kPut, key);
    if (task->json_data_.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    curl_easy_setopt(easy, CURLOPT_URL, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(easy, CURLOPT_UPLOAD, 0L);
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->data_buffer_.data());
    curl_easy_setopt(easy,
                     CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<curl_off_t>(task->file_size_));
    task->headers_ = nullptr;
    task->headers_ = curl_slist_append(task->headers_, "Expect:");

    // Add conditional headers if provided
    if (!task->if_match_.empty())
    {
        std::string header = "If-Match: " + task->if_match_;
        task->headers_ = curl_slist_append(task->headers_, header.c_str());
    }
    else if (!task->if_none_match_.empty())
    {
        std::string header = "If-None-Match: " + task->if_none_match_;
        task->headers_ = curl_slist_append(task->headers_, header.c_str());
    }

    if (task->headers_)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers_);
    }
    return true;
}

bool AsyncHttpManager::SetupDeleteRequest(ObjectStore::DeleteTask *task,
                                          CURL *easy)
{
    std::string key = ComposeKeyFromRemote(task->remote_path_, false);
    if (key.empty())
    {
        task->error_ = KvError::InvalidArgs;
        return false;
    }
    task->json_data_ = backend_->CreateSignedUrl(CloudHttpMethod::kDelete, key);
    if (task->json_data_.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    curl_easy_setopt(easy, CURLOPT_URL, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "DELETE");
    task->headers_ = nullptr;
    return true;
}

bool AsyncHttpManager::SetupListRequest(ObjectStore::ListTask *task, CURL *easy)
{
    std::string strip_prefix =
        ComposeKeyFromRemote(task->remote_path_, task->ensure_trailing_slash_);

    task->json_data_ = strip_prefix;

    SignedRequestInfo request_info;
    if (!backend_->BuildListRequest(strip_prefix,
                                    task->Recursive(),
                                    task->continuation_token_,
                                    &request_info))
    {
        task->error_ = KvError::CloudErr;
        return false;
    }
    if (request_info.url.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    task->headers_ = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        task->headers_ = curl_slist_append(task->headers_, header_line.c_str());
    }
    if (task->headers_)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers_);
    }
    return true;
}

size_t AsyncHttpManager::WriteCallback(void *contents,
                                       size_t size,
                                       size_t nmemb,
                                       void *userp)
{
    size_t total = size * nmemb;
    if (userp == nullptr || total == 0)
    {
        return total;
    }
    auto *task = static_cast<ObjectStore::Task *>(userp);
    task->response_data_.append(static_cast<char *>(contents), total);
    return total;
}

size_t AsyncHttpManager::HeaderCallback(char *buffer,
                                        size_t size,
                                        size_t nitems,
                                        void *userdata)
{
    ObjectStore::Task *task = static_cast<ObjectStore::Task *>(userdata);
    if (!task)
    {
        return size * nitems;
    }

    // Extract ETag header: "ETag: "value"\r\n"
    std::string_view header_line(buffer, size * nitems);
    constexpr std::string_view etag_prefix = "ETag:";
    if (header_line.size() >= etag_prefix.size() &&
        header_line.substr(0, etag_prefix.size()) == etag_prefix)
    {
        // Find the value after "ETag: "
        size_t value_start = etag_prefix.size();
        while (value_start < header_line.size() &&
               (header_line[value_start] == ' ' ||
                header_line[value_start] == '\t'))
        {
            ++value_start;
        }

        // Extract value (may be quoted)
        size_t value_end = value_start;
        while (value_end < header_line.size() &&
               header_line[value_end] != '\r' && header_line[value_end] != '\n')
        {
            ++value_end;
        }

        if (value_end > value_start)
        {
            std::string_view etag_value =
                header_line.substr(value_start, value_end - value_start);
            // Remove quotes if present
            if (etag_value.size() >= 2 && etag_value.front() == '"' &&
                etag_value.back() == '"')
            {
                etag_value = etag_value.substr(1, etag_value.size() - 2);
            }
            task->etag_ = std::string(etag_value);
        }
    }

    return size * nitems;
}

void AsyncHttpManager::ProcessCompletedRequests()
{
    if (IsIdle())
    {
        return;
    }

    CURLMsg *msg;
    int msgs_left;
    while ((msg = curl_multi_info_read(multi_handle_, &msgs_left)))
    {
        if (msg->msg == CURLMSG_DONE)
        {
            CURL *easy = msg->easy_handle;
            ObjectStore::Task *task;
            curl_easy_getinfo(
                easy, CURLINFO_PRIVATE, reinterpret_cast<void **>(&task));

            if (!task)
            {
                LOG(ERROR) << "Task is null in ProcessCompletedRequests";
                curl_multi_remove_handle(multi_handle_, easy);
                curl_easy_cleanup(easy);
                continue;
            }

            int64_t response_code = 0;
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
            task->response_code_ = response_code;  // Store response code for
                                                   // CAS conflict detection

            bool schedule_retry = false;
            uint32_t retry_delay_ms = 0;

            if (msg->data.result == CURLE_OK)
            {
                if (response_code >= 200 && response_code < 300)
                {
                    task->error_ = KvError::NoError;
                }
                else if (response_code == 404)
                {
                    task->error_ = KvError::NotFound;
                }
                else if (IsHttpRetryable(response_code) &&
                         task->retry_count_ < task->max_retries_)
                {
                    task->retry_count_++;
                    retry_delay_ms = ComputeBackoffMs(task->retry_count_);
                    schedule_retry = true;
                    LOG(WARNING)
                        << "HTTP error " << response_code
                        << ", scheduling retry " << unsigned(task->retry_count_)
                        << "/" << unsigned(task->max_retries_) << " in "
                        << retry_delay_ms << " ms" << ", task=" << task->Info();
                }
                else
                {
                    LOG(ERROR) << "HTTP error: " << response_code;
                    task->error_ = ClassifyHttpError(response_code);
                }
            }
            else
            {
                if (IsCurlRetryable(msg->data.result) &&
                    task->retry_count_ < task->max_retries_)
                {
                    task->retry_count_++;
                    retry_delay_ms = ComputeBackoffMs(task->retry_count_);
                    schedule_retry = true;
                    LOG(WARNING)
                        << "cURL transport error: "
                        << curl_easy_strerror(msg->data.result)
                        << ", scheduling retry " << unsigned(task->retry_count_)
                        << "/" << unsigned(task->max_retries_) << " in "
                        << retry_delay_ms << " ms" << ", task=" << task->Info();
                }
                else
                {
                    LOG(ERROR) << "cURL error: "
                               << curl_easy_strerror(msg->data.result);
                    task->error_ = ClassifyCurlError(msg->data.result);
                }
            }

            curl_multi_remove_handle(multi_handle_, easy);
            curl_easy_cleanup(easy);
            active_requests_.erase(easy);
            CleanupTaskResources(task);

            if (schedule_retry)
            {
                ScheduleRetry(task, std::chrono::milliseconds(retry_delay_ms));
                continue;
            }

            if (task->retry_count_ > 0)
            {
                LOG(INFO) << "Retry succeeded after "
                          << static_cast<unsigned>(task->retry_count_)
                          << " attempts: " << task->Info();
                task->retry_count_ = 0;
            }

            OnTaskFinished(task);
        }
    }
}

void AsyncHttpManager::CleanupTaskResources(ObjectStore::Task *task)
{
    if (task == nullptr)
    {
        return;
    }
    curl_slist_free_all(task->headers_);
    task->headers_ = nullptr;
}

void AsyncHttpManager::Cleanup()
{
    for (auto &[easy, task] : active_requests_)
    {
        curl_multi_remove_handle(multi_handle_, easy);
        CleanupTaskResources(task);
        curl_easy_cleanup(easy);

        if (task->kv_task_->inflight_io_ > 0)
        {
            task->error_ = KvError::CloudErr;
            OnTaskFinished(task);
        }
    }
    active_requests_.clear();
}

void AsyncHttpManager::OnTaskFinished(ObjectStore::Task *task)
{
    cloud_service_->NotifyTaskFinished(task);
}

void AsyncHttpManager::ProcessPendingRetries()
{
    if (pending_retries_.empty())
    {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    auto it = pending_retries_.begin();
    while (it != pending_retries_.end() && it->first <= now)
    {
        ObjectStore::Task *task = it->second;
        it = pending_retries_.erase(it);
        LOG(INFO) << "Retrying task after backoff (attempt "
                  << unsigned(task->retry_count_) << "/"
                  << unsigned(task->max_retries_) << "): " << task->Info();
        // SubmitRequest will handle rescheduling if slot acquisition fails
        SubmitRequest(task);
    }
}

void AsyncHttpManager::ScheduleRetry(ObjectStore::Task *task,
                                     std::chrono::steady_clock::duration delay)
{
    task->waiting_retry_ = true;
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    for (auto it = pending_retries_.begin(); it != pending_retries_.end();)
    {
        if (it->second == task)
        {
            it = pending_retries_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    auto deadline = std::chrono::steady_clock::now() + delay;
    pending_retries_.emplace(deadline, task);
}

uint32_t AsyncHttpManager::ComputeBackoffMs(uint8_t attempt)
{
    if (attempt == 0)
    {
        return kInitialRetryDelayMs;
    }

    uint64_t delay = static_cast<uint64_t>(kInitialRetryDelayMs)
                     << (attempt - 1);
    delay = std::min<uint64_t>(delay, kMaxRetryDelayMs);
    return static_cast<uint32_t>(delay);
}

bool AsyncHttpManager::IsCurlRetryable(CURLcode code)
{
    switch (code)
    {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_GOT_NOTHING:
    case CURLE_HTTP2_STREAM:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_PARTIAL_FILE:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
        return true;
    default:
        return false;
    }
}

bool AsyncHttpManager::IsHttpRetryable(int64_t response_code)
{
    switch (response_code)
    {
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
        return true;
    default:
        return false;
    }
}

KvError AsyncHttpManager::ClassifyHttpError(int64_t response_code)
{
    switch (response_code)
    {
    case 400:
    case 401:
    case 403:
    case 409:
        return KvError::CloudErr;
    case 404:
        return KvError::NotFound;
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
    case 505:
        return KvError::Timeout;
    case 507:
        return KvError::OssInsufficientStorage;
    default:
        return KvError::CloudErr;
    }
}

KvError AsyncHttpManager::ClassifyCurlError(CURLcode code)
{
    switch (code)
    {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_GOT_NOTHING:
        return KvError::Timeout;
    default:
        return KvError::CloudErr;
    }
}
}  // namespace eloqstore
