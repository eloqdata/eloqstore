#pragma once

#include <curl/curl.h>

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
#include "concurrentqueue/blockingconcurrentqueue.h"

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

        uint8_t retry_count_ = 0;
        const uint8_t max_retries_ = 3;

        // KvTask pointer for direct task resumption
        KvTask *kv_task_{nullptr};
        // Inflight IO counter for handling multiple async operations
        int inflight_io_{0};

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
        curl_slist *headers_{nullptr};
        std::string json_data_;
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
        curl_slist *headers_{nullptr};
    };

    class ListTask : public Task
    {
    public:
        explicit ListTask(std::string_view remote_path)
            : remote_path_(remote_path) {};
        Type TaskType() override
        {
            return Type::AsyncList;
        }
        std::string remote_path_;
        curl_slist *headers_{nullptr};
        std::string json_data_;
    };

    class DeleteTask : public Task
    {
    public:
        explicit DeleteTask(std::vector<std::string> file_paths,
                            bool is_dir = false)
            : file_paths_(std::move(file_paths)),
              current_index_(0),
              is_dir_(is_dir)
        {
            headers_list_.resize(file_paths_.size(), nullptr);
            json_data_list_.resize(file_paths_.size());
        }
        Type TaskType() override
        {
            return Type::AsyncDelete;
        }

        // Check if this batch is for directories
        bool IsDir() const
        {
            return is_dir_;
        }

        // Set whether this batch is for directories
        void SetIsDir(bool is_dir)
        {
            is_dir_ = is_dir;
        }

        std::vector<std::string> file_paths_;  // Support batch delete
        size_t current_index_;  // Current index being processed in file_paths_

        std::vector<struct curl_slist *> headers_list_;
        std::vector<std::string> json_data_list_;
        bool is_dir_{false};  // Track whether this batch is for directories
                              // (default: false for files)

        bool has_error_{false};
        KvError first_error_{KvError::NoError};
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

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                std::string *userp)
    {
        return userp->append(static_cast<char *>(contents), size * nmemb),
               size * nmemb;
    }

    CURLM *multi_handle_{nullptr};
    std::unordered_map<CURL *, ObjectStore::Task *> active_requests_;
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