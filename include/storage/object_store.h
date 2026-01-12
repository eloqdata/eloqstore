#pragma once

#include <curl/curl.h>
#include <jsoncpp/json/json.h>

#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "direct_io_buffer.h"
#include "error.h"
#include "kv_options.h"
#include "pool.h"
#include "tasks/task.h"
#include "types.h"

namespace utils
{
struct CloudObjectInfo;
}

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE

namespace eloqstore
{
class KvTask;
class CloudStoreMgr;
class AsyncHttpManager;
class AsyncIoManager;

using DirectIoBufferPool = Pool<DirectIoBuffer>;
class ObjectStore
{
public:
    explicit ObjectStore(const KvOptions *options);
    ~ObjectStore();

    AsyncHttpManager *GetHttpManager()
    {
        return async_http_mgr_.get();
    }

    KvError EnsureBucketExists();

    bool ParseListObjectsResponse(
        std::string_view payload,
        const std::string &strip_prefix,
        std::vector<std::string> *objects,
        std::vector<utils::CloudObjectInfo> *infos,
        std::string *next_continuation_token = nullptr) const;

    class Task
    {
    public:
        Task() = default;
        Task(Task &&) noexcept = default;
        Task &operator=(Task &&) noexcept = default;
        Task(const Task &) = delete;
        Task &operator=(const Task &) = delete;
        virtual ~Task() = default;
        enum class Type : uint8_t
        {
            AsyncDownload = 0,
            AsyncUpload,
            AsyncList,
            AsyncDelete
        };
        virtual Type TaskType() = 0;

        KvError error_{KvError::NoError};
        DirectIoBuffer response_data_;
        std::string json_data_{};
        curl_slist *headers_{nullptr};
        bool cloud_slot_acquired_{false};

        uint8_t retry_count_ = 0;
        uint8_t max_retries_ = 5;
        bool waiting_retry_{false};

        // KvTask pointer for direct task resumption
        KvTask *kv_task_{nullptr};
        void SetKvTask(KvTask *task)
        {
            kv_task_ = task;
        }

    protected:
        friend class ObjectStore;
        friend class AsyncHttpManager;
    };

    class DownloadTask : public Task
    {
    public:
        DownloadTask(const TableIdent *tbl_id, std::string_view filename)
            : tbl_id_(tbl_id), filename_(filename)
        {
        }
        Type TaskType() override
        {
            return Type::AsyncDownload;
        }
        const TableIdent *tbl_id_;
        std::string_view filename_;
    };

    class UploadTask : public Task
    {
    public:
        UploadTask(const TableIdent *tbl_id, std::string filename)
            : tbl_id_(tbl_id), filename_(std::move(filename))
        {
        }
        Type TaskType() override
        {
            return Type::AsyncUpload;
        }

        const TableIdent *tbl_id_;
        std::string filename_;
        size_t file_size_{0};
        DirectIoBuffer data_buffer_;
        size_t buffer_offset_{0};
    };

    class ListTask : public Task
    {
    public:
        explicit ListTask(std::string_view remote_path)
            : remote_path_(remote_path)
        {
        }
        void SetRecursive(bool recurse)
        {
            recurse_ = recurse;
        }
        bool Recursive() const
        {
            return recurse_;
        }
        void SetContinuationToken(std::string token)
        {
            continuation_token_ = std::move(token);
        }
        Type TaskType() override
        {
            return Type::AsyncList;
        }
        std::string remote_path_;
        bool recurse_{false};
        std::string continuation_token_;
    };

    class DeleteTask : public Task
    {
    public:
        explicit DeleteTask(std::string remote_path)
            : remote_path_(std::move(remote_path))
        {
        }
        Type TaskType() override
        {
            return Type::AsyncDelete;
        }

        std::string remote_path_;
    };

private:
    std::unique_ptr<AsyncHttpManager> async_http_mgr_;
};

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
};

class CloudBackend
{
public:
    virtual ~CloudBackend() = default;

    virtual std::string CreateSignedUrl(CloudHttpMethod method,
                                        const std::string &key) = 0;
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

class AsyncHttpManager
{
public:
    AsyncHttpManager();
    ~AsyncHttpManager();

    void SubmitRequest(ObjectStore::Task *task);
    void PerformRequests();
    void ProcessCompletedRequests();

    KvError EnsureBucketExists();

    void Cleanup();
    bool IsIdle() const
    {
        return active_requests_.empty();
    }
    size_t NumActiveRequests() const
    {
        return active_requests_.size();
    }

    bool ParseListObjectsResponse(
        std::string_view payload,
        const std::string &strip_prefix,
        std::vector<std::string> *objects,
        std::vector<utils::CloudObjectInfo> *infos,
        std::string *next_continuation_token = nullptr) const;

private:
    void CleanupTaskResources(ObjectStore::Task *task);
    bool SetupUploadRequest(ObjectStore::UploadTask *task, CURL *easy);
    bool SetupDownloadRequest(ObjectStore::DownloadTask *task, CURL *easy);
    bool SetupDeleteRequest(ObjectStore::DeleteTask *task, CURL *easy);
    bool SetupListRequest(ObjectStore::ListTask *task, CURL *easy);
    void ProcessPendingRetries();
    void ScheduleRetry(ObjectStore::Task *task,
                       std::chrono::steady_clock::duration delay);
    uint32_t ComputeBackoffMs(uint8_t attempt) const;
    bool IsCurlRetryable(CURLcode code) const;
    bool IsHttpRetryable(int64_t response_code) const;
    KvError ClassifyHttpError(int64_t response_code) const;
    KvError ClassifyCurlError(CURLcode code) const;

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                void *userp);

    static constexpr uint32_t kInitialRetryDelayMs = 10'000;
    static constexpr uint32_t kMaxRetryDelayMs = 40'000;
    static constexpr uint32_t kCloudConcurrencyLimit = 20;

    CURLM *multi_handle_{nullptr};
    std::unordered_map<CURL *, ObjectStore::Task *> active_requests_;
    std::multimap<std::chrono::steady_clock::time_point, ObjectStore::Task *>
        pending_retries_;
    int running_handles_{0};

    CloudPathInfo cloud_path_;
    std::unique_ptr<CloudBackend> backend_;
    uint32_t cloud_inflight_{0};
    WaitingZone cloud_waiting_;

    std::string ComposeKey(const TableIdent *tbl_id,
                           std::string_view filename) const;
    std::string ComposeKeyFromRemote(std::string_view remote_path,
                                     bool ensure_trailing_slash) const;
    bool AcquireCloudSlot(KvTask *task,
                          ObjectStore::Task *store_task,
                          bool is_retry);
    void ReleaseCloudSlot(ObjectStore::Task *store_task);
};

}  // namespace eloqstore
