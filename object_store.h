#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_map>

#include "error.h"
#include "kv_options.h"
#include "types.h"
#ifdef USE_RCLONE_HTTP_API
#include <curl/curl.h>
#include <jsoncpp/json/json.h>
#endif

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace eloqstore
{
class KvTask;
class CloudStoreMgr;

class ObjectStore
{
public:
    ObjectStore(const KvOptions *options);
    ~ObjectStore();
    void Start();
    void Stop();
    bool IsRunning() const;

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

        KvTask *kv_task_;
        KvError error_{KvError::NoError};
        uint64_t request_id_;
        std::string response_data_;

        uint8_t retry_count_ = 0;
        const uint8_t max_retries_ = 3;

        using CompletionCallback = std::function<void(Task *)>;
        CompletionCallback callback_;

    protected:
        // const TableIdent *tbl_id_;
        // CloudStoreMgr *io_mgr_{nullptr};
        static std::atomic<uint64_t> next_request_id_;
        friend class ObjectStore;
        friend class AsyncHttpManager;
    };

    class DownloadTask : public Task
    {
    public:
        DownloadTask(CloudStoreMgr *io_mgr,
                     KvTask *kv_task,
                     const TableIdent *tbl_id,
                     std::string_view filename,
                     CompletionCallback callback)
            : tbl_id_(tbl_id), io_mgr_(io_mgr), filename_(filename)
        {
            kv_task_ = kv_task;
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
                   KvTask *kv_task,
                   const TableIdent *tbl_id,
                   std::vector<std::string> filenames,
                   CompletionCallback callback)
            : tbl_id_(tbl_id), io_mgr_(io_mgr), filenames_(std::move(filenames))
        {
            kv_task_ = kv_task;
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

private:
    class StopSignal : public Task
    {
    public:
        StopSignal() : Task(){};
        Type TaskType() override
        {
            return Type::Stop;
        };
    };
    class AsyncHttpManager
    {
    public:
        AsyncHttpManager(const std::string &daemon_url);
        ~AsyncHttpManager();

        void SubmitRequest(Task *task);
        void ProcessCompletedRequests();
        void WaitForNetworkEvents(int timeout_ms);
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
        void CleanupTaskResources(Task *task);
        void SetupMultipartUpload(UploadTask *task, CURL *easy);
        void SetupDownloadRequest(DownloadTask *task, CURL *easy);
        void SetupListRequest(ListTask *task, CURL *easy);
        void SetupDeleteRequest(DeleteTask *task, CURL *easy);

        static size_t WriteCallback(void *contents,
                                    size_t size,
                                    size_t nmemb,
                                    std::string *userp)
        {
            userp->append((char *) contents, size * nmemb);
            return size * nmemb;
        }
        struct ActiveRequest
        {
            Task *task;
            CURL *easy_handle;
        };
        CURLM *multi_handle_{nullptr};
        std::unordered_map<uint64_t, ActiveRequest> active_requests_;
        std::string daemon_url_;
    };

    std::unique_ptr<AsyncHttpManager> async_http_mgr_;
    void WorkLoop();

    std::vector<std::thread> workers_;
};
}  // namespace eloqstore