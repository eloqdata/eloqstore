#include "storage/cloud_backend.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <curl/curl.h>
#include <glog/logging.h>
#include <jsoncpp/json/json.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "kv_options.h"
#include "utils.h"

namespace eloqstore
{
namespace
{
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
        uint64_t size_value_u64 = 0;
        if (item.isMember("Size"))
        {
            const Json::Value &size_value = item["Size"];
            if (size_value.isUInt64())
            {
                size_value_u64 = size_value.asUInt64();
            }
            else if (size_value.isString())
            {
                try
                {
                    size_value_u64 = std::stoull(size_value.asString());
                }
                catch (const std::exception &)
                {
                    size_value_u64 = 0;
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
            info.size = size_value_u64;
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
        if (value[i] == '&')
        {
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
                result.push_back('\'');
                i += 6;
            }
            else
            {
                result.push_back(value[i]);
                ++i;
            }
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

std::string UrlEncodePreservingSlash(std::string_view value)
{
    static constexpr char kHexDigits[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (unsigned char c : value)
    {
        if (c == '/')
        {
            encoded.push_back('/');
            continue;
        }
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
    if (objects)
    {
        objects->push_back(relative);
    }
    if (infos)
    {
        utils::CloudObjectInfo info;
        info.name = relative;
        info.path = decoded_key;
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

    size_t pos = 0;
    const std::string member_start = "<Contents>";
    const std::string member_end = "</Contents>";
    while (true)
    {
        size_t start = payload.find(member_start, pos);
        if (start == std::string::npos)
        {
            break;
        }
        size_t end = payload.find(member_end, start);
        if (end == std::string::npos)
        {
            return false;
        }
        size_t inner_begin = start + member_start.size();
        std::string block(payload.substr(inner_begin, end - inner_begin));
        pos = end + member_end.size();
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
        AppendParsedEntry(
            decoded_key, strip_prefix, false, size, mod_time, objects, infos);
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
                          strip_prefix,
                          true,
                          0,
                          std::string{},
                          objects,
                          infos);
    }

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

constexpr std::string_view kDefaultAwsEndpoint = "http://127.0.0.1:9900";
constexpr std::string_view kDefaultAwsRegion = "us-east-1";
constexpr std::string_view kDefaultGcsEndpoint =
    "https://storage.googleapis.com";
constexpr std::string_view kDefaultGcsRegion = "auto";
constexpr std::string_view kDefaultGcsScope =
    "https://www.googleapis.com/auth/devstorage.read_write";
constexpr std::string_view kDefaultGcsTokenUri =
    "https://oauth2.googleapis.com/token";

struct GcsServiceAccountCredentials
{
    std::string client_email;
    std::string private_key;
    std::string token_uri;
    std::string project_id;
};

std::optional<std::string> ReadFileContents(const std::string &path)
{
    std::ifstream file(path);
    if (!file)
    {
        LOG(ERROR) << "Failed to open credentials file: " << path;
        return std::nullopt;
    }
    std::ostringstream oss;
    oss << file.rdbuf();
    return oss.str();
}

std::optional<GcsServiceAccountCredentials> ParseServiceAccountJson(
    std::string_view json_content)
{
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value root;
    std::string errs;
    if (!reader->parse(json_content.data(),
                       json_content.data() + json_content.size(),
                       &root,
                       &errs))
    {
        LOG(ERROR) << "Failed to parse service account JSON: " << errs;
        return std::nullopt;
    }

    GcsServiceAccountCredentials creds;
    creds.client_email = root.get("client_email", "").asString();
    creds.private_key = root.get("private_key", "").asString();
    creds.token_uri = root.get("token_uri", "").asString();
    creds.project_id = root.get("project_id", "").asString();
    if (creds.client_email.empty() || creds.private_key.empty())
    {
        LOG(ERROR) << "Service account credentials missing client_email or "
                      "private_key";
        return std::nullopt;
    }
    if (creds.token_uri.empty())
    {
        creds.token_uri = std::string(kDefaultGcsTokenUri);
    }
    return creds;
}

std::optional<GcsServiceAccountCredentials>
LoadServiceAccountCredentialsFromEnv()
{
    const char *json_env = std::getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON");
    if (json_env && *json_env)
    {
        if (auto creds = ParseServiceAccountJson(json_env))
        {
            return creds;
        }
    }

    const char *path_env = std::getenv("GOOGLE_APPLICATION_CREDENTIALS");
    if (path_env && *path_env)
    {
        auto content = ReadFileContents(path_env);
        if (content)
        {
            return ParseServiceAccountJson(*content);
        }
    }
    return std::nullopt;
}

struct GcsMetadataCredentials
{
    std::string token_uri;
    std::string project_id;
};

constexpr std::string_view kMetadataServerHost =
    "http://metadata.google.internal";
constexpr std::string_view kMetadataTokenPath =
    "/computeMetadata/v1/instance/service-accounts/default/token";
constexpr std::string_view kMetadataProjectIdPath =
    "/computeMetadata/v1/project/project-id";
constexpr std::string_view kMetadataFlavorHeader = "Metadata-Flavor: Google";

bool PerformMetadataRequest(const std::string &url,
                            std::string *response,
                            long timeout_seconds)
{
    CURL *easy = curl_easy_init();
    if (!easy)
    {
        return false;
    }

    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, kMetadataFlavorHeader.data());

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, AppendToString);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, response);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, timeout_seconds);
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(easy);
    long http_code = 0;
    if (res == CURLE_OK)
    {
        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(easy);

    if (res != CURLE_OK)
    {
        LOG(INFO) << "Metadata request failed: " << curl_easy_strerror(res);
        return false;
    }
    if (http_code < 200 || http_code >= 300)
    {
        LOG(INFO) << "Metadata request returned HTTP " << http_code;
        return false;
    }
    return true;
}

std::optional<std::string> FetchMetadataValue(std::string_view path)
{
    std::string url = std::string(kMetadataServerHost);
    url.append(path.data(), path.size());
    std::string response;
    if (!PerformMetadataRequest(url, &response, 5L))
    {
        return std::nullopt;
    }
    while (!response.empty() &&
           (response.back() == '\n' || response.back() == '\r'))
    {
        response.pop_back();
    }
    return response;
}

std::optional<GcsMetadataCredentials> LoadMetadataCredentials()
{
    auto project_id = FetchMetadataValue(kMetadataProjectIdPath);
    if (!project_id)
    {
        LOG(INFO) << "Metadata server unavailable or project id not accessible "
                     "for GCS auto credentials";
        return std::nullopt;
    }
    GcsMetadataCredentials creds;
    creds.project_id = *project_id;
    std::string token_url = std::string(kMetadataServerHost);
    token_url.append(kMetadataTokenPath.data(), kMetadataTokenPath.size());
    token_url.append("?scopes=");
    token_url.append(UrlEncode(std::string(kDefaultGcsScope)));
    creds.token_uri = std::move(token_url);
    return creds;
}

std::string Base64UrlEncode(std::string_view input)
{
    BIO *b64 = BIO_new(BIO_f_base64());
    if (!b64)
    {
        return {};
    }
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    BIO *mem = BIO_new(BIO_s_mem());
    if (!mem)
    {
        BIO_free_all(b64);
        return {};
    }
    b64 = BIO_push(b64, mem);
    BIO_write(b64, input.data(), static_cast<int>(input.size()));
    BIO_flush(b64);
    BUF_MEM *buffer_ptr = nullptr;
    BIO_get_mem_ptr(b64, &buffer_ptr);
    std::string encoded(buffer_ptr->data, buffer_ptr->length);
    BIO_free_all(b64);
    for (char &c : encoded)
    {
        if (c == '+')
        {
            c = '-';
        }
        else if (c == '/')
        {
            c = '_';
        }
    }
    while (!encoded.empty() && encoded.back() == '=')
    {
        encoded.pop_back();
    }
    return encoded;
}

std::string JsonCompactString(const Json::Value &value)
{
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}

class GcsAccessTokenProvider
{
public:
    static std::unique_ptr<GcsAccessTokenProvider> Create()
    {
        if (auto creds = LoadServiceAccountCredentialsFromEnv())
        {
            return std::unique_ptr<GcsAccessTokenProvider>(
                new GcsAccessTokenProvider(std::move(*creds)));
        }
        if (auto metadata = LoadMetadataCredentials())
        {
            return std::unique_ptr<GcsAccessTokenProvider>(
                new GcsAccessTokenProvider(std::move(*metadata)));
        }
        LOG(ERROR) << "Failed to initialize GCS auto credentials from "
                      "environment or metadata";
        return nullptr;
    }

    std::string GetAccessToken()
    {
        std::lock_guard lk(mutex_);
        const auto now = std::chrono::system_clock::now();
        if (!access_token_.empty() && now + std::chrono::seconds(60) < expiry_)
        {
            return access_token_;
        }
        if (!RefreshLocked(now))
        {
            return {};
        }
        return access_token_;
    }

    const std::string &ProjectId() const
    {
        if (mode_ == CredentialMode::kMetadata)
        {
            return metadata_creds_.project_id;
        }
        return credentials_.project_id;
    }

private:
    enum class CredentialMode
    {
        kServiceAccount,
        kMetadata
    };

    explicit GcsAccessTokenProvider(GcsServiceAccountCredentials creds)
        : mode_(CredentialMode::kServiceAccount), credentials_(std::move(creds))
    {
    }

    explicit GcsAccessTokenProvider(GcsMetadataCredentials metadata)
        : mode_(CredentialMode::kMetadata), metadata_creds_(std::move(metadata))
    {
    }

    bool RefreshLocked(std::chrono::system_clock::time_point now)
    {
        std::string token;
        std::chrono::system_clock::time_point expiry;
        if (!FetchAccessToken(now, &token, &expiry))
        {
            LOG(ERROR) << "Failed to refresh GCS access token";
            return false;
        }
        access_token_ = std::move(token);
        expiry_ = expiry;
        return true;
    }

    bool FetchAccessToken(std::chrono::system_clock::time_point now,
                          std::string *token,
                          std::chrono::system_clock::time_point *expiry)
    {
        if (mode_ == CredentialMode::kMetadata)
        {
            return FetchMetadataAccessToken(now, token, expiry);
        }
        return FetchServiceAccountAccessToken(now, token, expiry);
    }

    bool FetchServiceAccountAccessToken(
        std::chrono::system_clock::time_point now,
        std::string *token,
        std::chrono::system_clock::time_point *expiry)
    {
        std::string assertion = CreateJwtAssertion(now);
        if (assertion.empty())
        {
            LOG(ERROR) << "Failed to build JWT assertion for GCS token";
            return false;
        }
        std::string post_fields =
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&"
            "assertion=" +
            UrlEncode(assertion);

        CURL *easy = curl_easy_init();
        if (!easy)
        {
            LOG(ERROR) << "Failed to init curl for GCS token";
            return false;
        }

        std::string response;
        curl_easy_setopt(easy, CURLOPT_URL, credentials_.token_uri.c_str());
        curl_easy_setopt(easy, CURLOPT_PROXY, "");
        curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
        curl_easy_setopt(easy, CURLOPT_POST, 1L);
        curl_easy_setopt(easy, CURLOPT_POSTFIELDS, post_fields.c_str());
        curl_easy_setopt(easy,
                         CURLOPT_POSTFIELDSIZE_LARGE,
                         static_cast<curl_off_t>(post_fields.size()));
        curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, AppendToString);
        curl_easy_setopt(easy, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(easy, CURLOPT_TIMEOUT, 30L);

        CURLcode res = curl_easy_perform(easy);
        long http_code = 0;
        if (res == CURLE_OK)
        {
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);
        }
        curl_easy_cleanup(easy);

        if (res != CURLE_OK)
        {
            LOG(ERROR) << "Token request failed: " << curl_easy_strerror(res);
            return false;
        }
        if (http_code < 200 || http_code >= 300)
        {
            LOG(ERROR) << "Token endpoint returned HTTP " << http_code << ": "
                       << response;
            return false;
        }

        Json::Value resp_json;
        Json::Reader json_reader;
        if (!json_reader.parse(response, resp_json, false))
        {
            LOG(ERROR) << "Failed to parse token response";
            return false;
        }
        std::string token_value = resp_json.get("access_token", "").asString();
        int expires_in = resp_json.get("expires_in", 0).asInt();
        if (token_value.empty() || expires_in <= 0)
        {
            LOG(ERROR) << "Token response missing access_token or expires_in";
            return false;
        }

        *token = std::move(token_value);
        auto lifetime = std::chrono::seconds(std::max(1, expires_in - 60));
        *expiry = now + lifetime;
        return true;
    }

    bool FetchMetadataAccessToken(std::chrono::system_clock::time_point now,
                                  std::string *token,
                                  std::chrono::system_clock::time_point *expiry)
    {
        std::string response;
        if (!PerformMetadataRequest(metadata_creds_.token_uri, &response, 10L))
        {
            LOG(ERROR) << "Failed to fetch access token from metadata server";
            return false;
        }

        Json::Value resp_json;
        Json::Reader json_reader;
        if (!json_reader.parse(response, resp_json, false))
        {
            LOG(ERROR) << "Failed to parse metadata token response";
            return false;
        }
        std::string token_value = resp_json.get("access_token", "").asString();
        int expires_in = resp_json.get("expires_in", 0).asInt();
        if (token_value.empty() || expires_in <= 0)
        {
            LOG(ERROR)
                << "Metadata token response missing access_token or expires_in";
            return false;
        }

        *token = std::move(token_value);
        auto lifetime = std::chrono::seconds(std::max(1, expires_in - 60));
        *expiry = now + lifetime;
        return true;
    }

    bool SignAssertion(const std::string &input, std::string *signature) const
    {
        BIO *bio =
            BIO_new_mem_buf(credentials_.private_key.data(),
                            static_cast<int>(credentials_.private_key.size()));
        if (!bio)
        {
            LOG(ERROR) << "Failed to load GCS private key";
            return false;
        }
        EVP_PKEY *pkey =
            PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
        BIO_free(bio);
        if (!pkey)
        {
            LOG(ERROR) << "Failed to parse GCS private key";
            return false;
        }

        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (!ctx)
        {
            EVP_PKEY_free(pkey);
            LOG(ERROR) << "Failed to init digest context";
            return false;
        }

        bool ok = true;
        if (EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) != 1)
        {
            ok = false;
        }
        else if (EVP_DigestSignUpdate(ctx, input.data(), input.size()) != 1)
        {
            ok = false;
        }
        else
        {
            size_t sig_len = 0;
            if (EVP_DigestSignFinal(ctx, nullptr, &sig_len) != 1)
            {
                ok = false;
            }
            else
            {
                std::string sig(sig_len, '\0');
                if (EVP_DigestSignFinal(
                        ctx,
                        reinterpret_cast<unsigned char *>(sig.data()),
                        &sig_len) != 1)
                {
                    ok = false;
                }
                else
                {
                    sig.resize(sig_len);
                    *signature = std::move(sig);
                }
            }
        }

        EVP_MD_CTX_free(ctx);
        EVP_PKEY_free(pkey);
        if (!ok)
        {
            LOG(ERROR) << "Failed to sign JWT for GCS";
        }
        return ok;
    }

    std::string CreateJwtAssertion(std::chrono::system_clock::time_point now)
    {
        const int64_t iat = std::chrono::duration_cast<std::chrono::seconds>(
                                now.time_since_epoch())
                                .count();
        const int64_t exp = iat + 3600;
        Json::Value payload;
        payload["iss"] = credentials_.client_email;
        payload["scope"] = std::string(kDefaultGcsScope);
        payload["aud"] = credentials_.token_uri;
        payload["iat"] = static_cast<Json::Value::Int64>(iat);
        payload["exp"] = static_cast<Json::Value::Int64>(exp);

        const std::string header = R"({"alg":"RS256","typ":"JWT"})";
        const std::string header_b64 = Base64UrlEncode(header);
        const std::string payload_b64 =
            Base64UrlEncode(JsonCompactString(payload));
        if (header_b64.empty() || payload_b64.empty())
        {
            return {};
        }
        std::string input = header_b64 + "." + payload_b64;
        std::string signature;
        if (!SignAssertion(input, &signature))
        {
            return {};
        }
        return input + "." + Base64UrlEncode(signature);
    }

    CredentialMode mode_{CredentialMode::kServiceAccount};
    GcsServiceAccountCredentials credentials_;
    GcsMetadataCredentials metadata_creds_;
    std::mutex mutex_;
    std::string access_token_;
    std::chrono::system_clock::time_point expiry_{};
};

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

    bool BuildObjectRequest(CloudHttpMethod method,
                            const std::string &key,
                            SignedRequestInfo *request) const override
    {
        if (!request)
        {
            return false;
        }
        request->url.clear();
        request->headers.clear();
        request->body.clear();
        Aws::Http::HttpMethod aws_method = ToAwsMethod(method);
        Aws::String url = s3_client_->GeneratePresignedUrl(
            cloud_path_.bucket, key, aws_method, 3600);
        request->url.assign(url.c_str(), url.size());
        return !request->url.empty();
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
        request->method = "PUT";
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
        if (options_ && options_->cloud_auto_credentials)
        {
            if (!options_->cloud_secret_key.empty())
            {
                LOG(INFO)
                    << "cloud_secret_key is set; disabling auto credentials";
            }
            else
            {
                return Aws::MakeShared<
                    Aws::Auth::DefaultAWSCredentialsProviderChain>("eloqstore");
            }
        }
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

    std::string ComposeObjectUrl(const std::string &key) const
    {
        if (bucket_url_.empty())
        {
            return {};
        }
        std::string url = bucket_url_;
        if (!key.empty())
        {
            if (!url.empty() && url.back() != '/')
            {
                url.push_back('/');
            }
            url.append(UrlEncodePreservingSlash(key));
        }
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
        if (options && options->cloud_auto_credentials)
        {
            token_provider_ = GcsAccessTokenProvider::Create();
            if (!token_provider_)
            {
                LOG(FATAL) << "Failed to initialize GCS auto credentials from "
                              "environment";
            }
        }
    }

    bool BuildObjectRequest(CloudHttpMethod method,
                            const std::string &key,
                            SignedRequestInfo *request) const override
    {
        if (!token_provider_)
        {
            return AwsCloudBackend::BuildObjectRequest(method, key, request);
        }
        if (!request)
        {
            return false;
        }
        request->url = ComposeObjectUrl(key);
        if (request->url.empty())
        {
            return false;
        }
        request->headers.clear();
        request->body.clear();
        return AppendAuthHeader(request);
    }

    bool BuildListRequest(const std::string &prefix,
                          bool recursive,
                          const std::string &continuation,
                          SignedRequestInfo *request) const override
    {
        if (!token_provider_)
        {
            return AwsCloudBackend::BuildListRequest(
                prefix, recursive, continuation, request);
        }
        if (!request)
        {
            return false;
        }
        request->headers.clear();
        request->body.clear();
        request->url = ComposeListUrl(prefix, recursive, continuation);
        if (request->url.empty())
        {
            return false;
        }
        return AppendAuthHeader(request);
    }

    bool BuildCreateBucketRequest(SignedRequestInfo *request) const override
    {
        if (!token_provider_)
        {
            return AwsCloudBackend::BuildCreateBucketRequest(request);
        }
        if (!request)
        {
            return false;
        }
        request->headers.clear();
        request->body.clear();
        request->method = "POST";

        const std::string &project_id = token_provider_->ProjectId();
        if (project_id.empty())
        {
            LOG(ERROR) << "GCS auto credentials missing project id for bucket "
                          "creation";
            return false;
        }

        std::string endpoint = options_->cloud_endpoint.empty()
                                   ? std::string(kDefaultGcsEndpoint)
                                   : options_->cloud_endpoint;
        while (!endpoint.empty() && endpoint.back() == '/')
        {
            endpoint.pop_back();
        }
        if (endpoint.empty())
        {
            LOG(ERROR) << "Invalid GCS endpoint for bucket creation";
            return false;
        }

        request->url = endpoint;
        request->url.append("/storage/v1/b?project=");
        request->url.append(UrlEncode(project_id));
        if (request->url.empty())
        {
            return false;
        }

        Json::Value body_json;
        body_json["name"] = cloud_path_.bucket;
        std::string region = options_->cloud_region.empty()
                                 ? std::string(kDefaultGcsRegion)
                                 : options_->cloud_region;
        std::string lowered;
        lowered.reserve(region.size());
        for (char c : region)
        {
            lowered.push_back(
                static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
        if (!region.empty() && lowered != "auto")
        {
            body_json["location"] = region;
        }

        Json::StreamWriterBuilder builder;
        builder["indentation"] = "";
        request->body = Json::writeString(builder, body_json);

        request->headers.emplace_back("Content-Type: application/json");
        if (!AppendAuthHeader(request))
        {
            return false;
        }
        return true;
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

private:
    bool AppendAuthHeader(SignedRequestInfo *request) const
    {
        if (!request)
        {
            return false;
        }
        std::string header = BuildAuthHeader();
        if (header.empty())
        {
            return false;
        }
        request->headers.emplace_back(std::move(header));
        return true;
    }

    std::string BuildAuthHeader() const
    {
        if (!token_provider_)
        {
            return {};
        }
        std::string token = token_provider_->GetAccessToken();
        if (token.empty())
        {
            LOG(ERROR) << "Failed to acquire GCS access token";
            return {};
        }
        return "Authorization: Bearer " + token;
    }

    void AppendProjectHeader(SignedRequestInfo *request) const
    {
        if (!request || !token_provider_)
        {
            return;
        }
        const std::string &project_id = token_provider_->ProjectId();
        if (!project_id.empty())
        {
            request->headers.emplace_back("x-goog-project-id: " + project_id);
        }
    }

    std::unique_ptr<GcsAccessTokenProvider> token_provider_;
};

}  // namespace

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

}  // namespace eloqstore
