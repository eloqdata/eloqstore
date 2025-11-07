#pragma once

#include <curl/curl.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "types.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE

namespace eloqstore
{
class KvTask;
class CloudStoreMgr;
class AsyncHttpManager;
class AsyncIoManager;

class ObjectStore
{
public:
    explicit ObjectStore(const KvOptions *options);
    ~ObjectStore();

    AsyncHttpManager *GetHttpManager()
    {
        return async_http_mgr_.get();
    }

    class Task
    {
    public:
        Task() = default;
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
        std::string response_data_{};
        std::string json_data_{};
        curl_slist *headers_{nullptr};

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
            : tbl_id_(tbl_id), filename_(filename) {};
        Type TaskType() override
        {
            return Type::AsyncDownload;
        };
        const TableIdent *tbl_id_;
        std::string_view filename_;
    };

    class UploadTask : public Task
    {
    public:
        UploadTask(const TableIdent *tbl_id, std::vector<std::string> filenames)
            : tbl_id_(tbl_id), filenames_(std::move(filenames)) {};
        Type TaskType() override
        {
            return Type::AsyncUpload;
        }

        const TableIdent *tbl_id_;
        std::vector<std::string> filenames_;

        // cURL related members
        curl_mime *mime_{nullptr};
    };

    class ListTask : public Task
    {
    public:
        explicit ListTask(std::string_view remote_path)
            : remote_path_(remote_path) {};
        void SetRecursive(bool recurse)
        {
            recurse_ = recurse;
        }
        bool Recursive() const
        {
            return recurse_;
        }
        Type TaskType() override
        {
            return Type::AsyncList;
        }
        std::string remote_path_;
        bool recurse_{false};
    };

    class DeleteTask : public Task
    {
    public:
        explicit DeleteTask(std::string remote_path, bool is_dir = false)
            : remote_path_(std::move(remote_path)), is_dir_(is_dir) {};
        Type TaskType() override
        {
            return Type::AsyncDelete;
        }

        std::string remote_path_;
        bool is_dir_{false};
    };

private:
    std::unique_ptr<AsyncHttpManager> async_http_mgr_;
};

class AsyncHttpManager
{
public:
    explicit AsyncHttpManager(const KvOptions *options);
    ~AsyncHttpManager();

    void SubmitRequest(ObjectStore::Task *task);
    void PerformRequests();
    void ProcessCompletedRequests();

    void Cleanup();
    bool IsIdle() const
    {
        return active_requests_.empty();
    }
    size_t NumActiveRequests() const
    {
        return active_requests_.size();
    }

private:
    void CleanupTaskResources(ObjectStore::Task *task);
    void SetupMultipartUpload(ObjectStore::UploadTask *task, CURL *easy);
    void SetupDownloadRequest(ObjectStore::DownloadTask *task, CURL *easy);
    void SetupListRequest(ObjectStore::ListTask *task, CURL *easy);
    void SetupDeleteRequest(ObjectStore::DeleteTask *task, CURL *easy);
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
                                std::string *userp)
    {
        return userp->append(static_cast<char *>(contents), size * nmemb),
               size * nmemb;
    }

    static constexpr uint32_t kInitialRetryDelayMs = 10'000;
    static constexpr uint32_t kMaxRetryDelayMs = 40'000;

    CURLM *multi_handle_{nullptr};
    std::unordered_map<CURL *, ObjectStore::Task *> active_requests_;
    std::multimap<std::chrono::steady_clock::time_point, ObjectStore::Task *>
        pending_retries_;
    const std::string daemon_url_;
    const std::string daemon_upload_url_;
    const std::string daemon_download_url_;
    const std::string daemon_list_url_;
    const std::string daemon_delete_url_;
    const std::string daemon_purge_url_;
    const KvOptions *options_;
    int running_handles_{0};
};

}  // namespace eloqstore
