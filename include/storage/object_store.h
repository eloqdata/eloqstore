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
/**
 * @brief Represents a contiguous byte range for segment-based file uploads.
 *
 * In cloud append mode, files are uploaded using segments that may be:
 * 1. In-memory buffers captured via OnFileRangeWritten (for recent writes)
 * 2. Disk reads (for file prefixes not covered by in-memory segments)
 * 3. Zero-filled buffers (for data file tails that haven't been written yet)
 *
 * Segments must be non-overlapping and contiguous when assembled for upload.
 * The upload path concatenates segments in offset order to form the complete
 * file.
 */
struct UploadSegment
{
    /** Logical file offset of this segment's first byte. */
    uint64_t offset{0};
    /** Segment payload bytes. The upload path treats size() as logical bytes.
     */
    DirectIoBuffer data;
    /** Length used when this segment represents a zero-filled range. */
    size_t zero_fill_length{0};
    /** True if this segment should be interpreted as zero-filled bytes. */
    bool zero_fill{false};

    size_t LogicalSize() const
    {
        return zero_fill ? zero_fill_length : data.size();
    }
};

class KvTask;
class CloudStoreMgr;
class AsyncHttpManager;
class AsyncIoManager;
class Shard;
class CloudStorageService;

using DirectIoBufferPool = Pool<DirectIoBuffer>;
class ObjectStore
{
public:
    class Task;
    explicit ObjectStore(const KvOptions *options,
                         CloudStorageService *service);
    ~ObjectStore();

    KvError EnsureBucketExists();

    bool ParseListObjectsResponse(
        std::string_view payload,
        const std::string &strip_prefix,
        std::vector<std::string> *objects,
        std::vector<utils::CloudObjectInfo> *infos,
        std::string *next_continuation_token = nullptr) const;

    void SubmitTask(Task *task, Shard *owner_shard);

    void StartHttpRequest(Task *task);
    void RunHttpWork();
    bool HttpWorkIdle() const;

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
        virtual std::string Info() const = 0;

        KvError error_{KvError::NoError};
        DirectIoBuffer response_data_;
        std::string json_data_{};
        curl_slist *headers_{nullptr};

        // ETag from response headers for CAS operations
        std::string etag_{};
        // HTTP response code for CAS conflict detection
        int64_t response_code_{0};

        uint8_t retry_count_ = 0;
        uint8_t max_retries_ = 5;
        bool waiting_retry_{false};

        // KvTask pointer for direct task resumption
        KvTask *kv_task_{nullptr};
        void SetKvTask(KvTask *task)
        {
            kv_task_ = task;
        }

        Shard *owner_shard_{nullptr};
        void SetOwnerShard(Shard *shard)
        {
            owner_shard_ = shard;
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
        std::string Info() const override
        {
            return std::string("Download(") + tbl_id_->ToString() + '/' +
                   std::string(filename_) + ')';
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
        std::string Info() const override
        {
            return std::string("Upload(") + tbl_id_->ToString() + '/' +
                   filename_ + ')';
        }

        const TableIdent *tbl_id_;
        std::string filename_;
        // Total logical object size expected by remote upload.
        size_t file_size_{0};
        // Inline one-buffer upload source. Used by simple uploads (no
        // segments).
        DirectIoBuffer data_buffer_;
        // Internal cursor for inline data path.
        size_t buffer_offset_{0};
        // Segment-based upload source. Offsets are logical file offsets.
        // ReadUploadCallback copies bytes from this vector by read_offset_.
        std::vector<UploadSegment> segments_;
        // Internal cursor used by ReadUploadCallback.
        uint64_t read_offset_{0};
        // For If-Match header
        std::string if_match_{};
        // For If-None-Match header (use "*" for create)
        std::string if_none_match_{};
    };

    class ListTask : public Task
    {
    public:
        explicit ListTask(std::string_view remote_path,
                          bool ensure_trailing_slash = true)
            : ensure_trailing_slash_(ensure_trailing_slash),
              remote_path_(remote_path)
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
        std::string Info() const override
        {
            return std::string("List(") + remote_path_ + ')';
        }

        // Add trailing slash to remote path if ensure_trailing_slash_ is true
        // and remote path does not end with '/'. This is used to ensure the
        // remote path is a directory.
        bool ensure_trailing_slash_{true};
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
        std::string Info() const override
        {
            return std::string("Delete(") + remote_path_ + ')';
        }

        std::string remote_path_;
    };

private:
    std::unique_ptr<AsyncHttpManager> async_http_mgr_;
    CloudStorageService *cloud_service_{nullptr};
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
    AsyncHttpManager(const KvOptions *options, CloudStorageService *service);
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
    static uint32_t ComputeBackoffMs(uint8_t attempt);
    static bool IsCurlRetryable(CURLcode code);
    static bool IsHttpRetryable(int64_t response_code);
    static KvError ClassifyHttpError(int64_t response_code);
    static KvError ClassifyCurlError(CURLcode code);
    void OnTaskFinished(ObjectStore::Task *task);

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                void *userp);

    static size_t HeaderCallback(char *buffer,
                                 size_t size,
                                 size_t nitems,
                                 void *userdata);
    /**
     * @brief CURL read callback for segment-based uploads.
     *
     * This callback is invoked by libcurl during HTTP PUT uploads to provide
     * data bytes. It reads from UploadTask::segments_ in order, maintaining
     * read_offset_ as a cursor across segments. Segments must be contiguous
     * and cover the entire file range [0, file_size_).
     *
     * @param buffer Buffer to fill with upload data
     * @param size Size of each element
     * @param nitems Number of elements
     * @param userdata Pointer to UploadTask instance
     *
     * @return Number of bytes written to buffer, 0 on EOF, CURL_READFUNC_ABORT
     * on error
     */
    static size_t ReadUploadCallback(char *buffer,
                                     size_t size,
                                     size_t nitems,
                                     void *userdata);

    static constexpr uint32_t kInitialRetryDelayMs = 10'000;
    static constexpr uint32_t kMaxRetryDelayMs = 40'000;
    CURLM *multi_handle_{nullptr};
    std::unordered_map<CURL *, ObjectStore::Task *> active_requests_;
    std::multimap<std::chrono::steady_clock::time_point, ObjectStore::Task *>
        pending_retries_;
    int running_handles_{0};

    CloudPathInfo cloud_path_;
    std::unique_ptr<CloudBackend> backend_;
    CloudStorageService *cloud_service_{nullptr};

    std::string ComposeKey(const TableIdent *tbl_id,
                           std::string_view filename) const;
    std::string ComposeKeyFromRemote(std::string_view remote_path,
                                     bool ensure_trailing_slash) const;
};

}  // namespace eloqstore
