#pragma once

#include <curl/curl.h>
#include <jsoncpp/json/json.h>

#include <atomic>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_map>

#include "error.h"
#include "kv_options.h"
#include "types.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace eloqstore
{
class KvTask;
class CloudStoreMgr;
class AsyncHttpManager;

class ObjectStore
{
public:
    ObjectStore(const KvOptions *options);
    ~ObjectStore();

    AsyncHttpManager *GetHttpManager()
    {
        return async_http_mgr_.get();
    }

    class Task
    {
    public:
        Task() : request_id_(next_request_id_++){};
        virtual ~Task() = default;
        enum class Type : uint8_t
        {
            AsyncDownload = 0,
            AsyncUpload,
            AsyncList,
            AsyncDelete,
            Stop
        };
        virtual Type TaskType() = 0;

        KvError error_{KvError::NoError};
        uint64_t request_id_;
        std::string response_data_;

        uint8_t retry_count_ = 0;
        const uint8_t max_retries_ = 3;

        using CompletionCallback = std::function<void(Task *)>;
        CompletionCallback callback_;

    protected:
        static std::atomic<uint64_t> next_request_id_;
        friend class ObjectStore;
        friend class AsyncHttpManager;
    };

    class DownloadTask : public Task
    {
    public:
        DownloadTask(CloudStoreMgr *io_mgr,
                     const TableIdent *tbl_id,
                     std::string_view filename,
                     CompletionCallback callback)
            : tbl_id_(tbl_id), io_mgr_(io_mgr), filename_(filename)
        {
            callback_ = std::move(callback);
        };
        Type TaskType() override
        {
            return Type::AsyncDownload;
        };
        const TableIdent *tbl_id_;
        CloudStoreMgr *io_mgr_;
        std::string filename_;
        struct curl_slist *headers_{nullptr};
        std::string json_data_;
    };

    class UploadTask : public Task
    {
    public:
        UploadTask(CloudStoreMgr *io_mgr,
                   const TableIdent *tbl_id,
                   std::vector<std::string> filenames,
                   CompletionCallback callback)
            : tbl_id_(tbl_id), io_mgr_(io_mgr), filenames_(std::move(filenames))
        {
            callback_ = std::move(callback);
        };
        Type TaskType() override
        {
            return Type::AsyncUpload;
        };

        const TableIdent *tbl_id_;
        CloudStoreMgr *io_mgr_;
        std::vector<std::string> filenames_;

        // cURL related members
        curl_mime *mime_{nullptr};
        struct curl_slist *headers_{nullptr};
    };

    class ListTask : public Task
    {
    public:
        ListTask(const KvOptions *options,
                 std::string_view remote_path,
                 std::vector<std::string> *result,
                 CompletionCallback callback)
            : options_(options), remote_path_(remote_path), result_(result)
        {
            callback_ = std::move(callback);
        };
        Type TaskType() override
        {
            return Type::AsyncList;
        };
        const KvOptions *options_;
        std::string remote_path_;
        std::vector<std::string> *result_;
        struct curl_slist *headers_{nullptr};
        std::string json_data_;
    };

    class DeleteTask : public Task
    {
    public:
        DeleteTask(const KvOptions *options,
                   std::string_view file_path,
                   CompletionCallback callback)
            : options_(options), file_path_(file_path)
        {
            callback_ = std::move(callback);
        };
        Type TaskType() override
        {
            return Type::AsyncDelete;
        };
        const KvOptions *options_;
        std::string file_path_;
        struct curl_slist *headers_{nullptr};
        std::string json_data_;
    };
    const KvOptions *options_;
    moodycamel::BlockingConcurrentQueue<Task *> submit_q_;

    std::unique_ptr<AsyncHttpManager> async_http_mgr_;
};

class AsyncHttpManager
{
public:
    AsyncHttpManager(const std::string &daemon_url);
    ~AsyncHttpManager();

    void SubmitRequest(ObjectStore::Task *task);
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

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                std::string *userp)
    {
        return ((std::string *) userp)->append((char *) contents, size * nmemb),
               size * nmemb;
    }
    struct ActiveRequest
    {
        ObjectStore::Task *task;
        CURL *easy_handle;
    };
    CURLM *multi_handle_{nullptr};
    std::unordered_map<uint64_t, ActiveRequest> active_requests_;
    std::string daemon_url_;
};

}  // namespace eloqstore