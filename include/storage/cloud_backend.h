#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace utils
{
struct CloudObjectInfo;
}

namespace eloqstore
{
struct KvOptions;

struct CloudPathInfo
{
    std::string bucket;
    std::string prefix;
};

enum class CloudHttpMethod : uint8_t
{
    kGet = 0,
    kPut,
    kDelete
};

struct SignedRequestInfo
{
    std::string url;
    std::vector<std::string> headers;
    std::string body;
    std::string method;
};

class CloudBackend
{
public:
    virtual ~CloudBackend() = default;

    virtual bool BuildObjectRequest(CloudHttpMethod method,
                                    const std::string &key,
                                    SignedRequestInfo *request) const = 0;
    virtual bool BuildListRequest(const std::string &prefix,
                                  bool recursive,
                                  const std::string &continuation,
                                  SignedRequestInfo *request) const = 0;
    virtual bool BuildCreateBucketRequest(SignedRequestInfo *request) const = 0;
    virtual bool ParseListObjectsResponse(
        std::string_view payload,
        const std::string &strip_prefix,
        std::vector<std::string> *objects,
        std::vector<utils::CloudObjectInfo> *infos,
        std::string *next_continuation_token = nullptr) const = 0;
};

std::unique_ptr<CloudBackend> CreateBackend(const KvOptions *options,
                                            const CloudPathInfo &path);

}  // namespace eloqstore
