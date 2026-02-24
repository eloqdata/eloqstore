#pragma once

#ifdef ELOQSTORE_WITH_TXSERVICE
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#endif

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "types.h"

#ifdef ELOQSTORE_WITH_TXSERVICE
#include "metrics.h"
namespace metrics
{
class Meter;
}  // namespace metrics
#endif

namespace utils
{
struct CloudObjectInfo;
}  // namespace utils

namespace eloqstore
{
class Shard;

enum class RequestType : uint8_t
{
    Read,
    Floor,
    Scan,
    ListObject,
    BatchWrite,
    Reopen,
    Truncate,
    DropTable,
    Archive,
    Compact,
    LocalGc,
    CleanExpired,
    GlobalArchive,
    GlobalReopen
};

inline const char *RequestTypeToString(RequestType type)
{
    switch (type)
    {
    case RequestType::Read:
        return "read";
    case RequestType::Floor:
        return "floor";
    case RequestType::Scan:
        return "scan";
    case RequestType::ListObject:
        return "list_object";
    case RequestType::BatchWrite:
        return "batch_write";
    case RequestType::Reopen:
        return "reopen";
    case RequestType::Truncate:
        return "truncate";
    case RequestType::DropTable:
        return "drop_table";
    case RequestType::Archive:
        return "archive";
    case RequestType::Compact:
        return "compact";
    case RequestType::LocalGc:
        return "local_gc";
    case RequestType::CleanExpired:
        return "clean_expired";
    case RequestType::GlobalArchive:
        return "global_archive";
    case RequestType::GlobalReopen:
        return "global_reopen";
    default:
        return "unknown";
    }
}

class KvRequest
{
public:
    virtual ~KvRequest() = default;
    virtual RequestType Type() const = 0;
    bool ReadOnly() const;
    KvError Error() const;
    bool RetryableErr() const;
    const char *ErrMessage() const;
    void SetTableId(TableIdent tbl_id);
    const TableIdent &TableId() const;
    uint64_t UserData() const;

    /**
     * @brief Test if this request is done.
     */
    bool IsDone() const;
    void Wait() const;

protected:
    void SetDone(KvError err);

    TableIdent tbl_id_;
    uint64_t user_data_{0};
    std::function<void(KvRequest *)> callback_{nullptr};
#ifdef ELOQSTORE_WITH_TXSERVICE
    mutable bthread::ConditionVariable cv_;
    mutable bthread::Mutex mutex_;
    bool done_{true};
#else
    std::atomic<bool> done_{true};
#endif
    KvError err_{KvError::NoError};

    friend class Shard;
    friend class EloqStore;
    friend class PrewarmService;
};

class ReadRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Read;
    }
    void SetArgs(TableIdent tbl_id, const char *key);
    void SetArgs(TableIdent tbl_id, std::string_view key);
    void SetArgs(TableIdent tbl_id, std::string key);
    std::string_view Key() const;

    // output
    std::string value_;
    uint64_t ts_;
    uint64_t expire_ts_;

private:
    // input
    std::variant<std::string_view, std::string> key_;
};

/**
 * @brief Read the biggest key not greater than the search key.
 * @return KvError::NotFound if such a key not exists.
 */
class FloorRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Floor;
    }
    void SetArgs(TableIdent tbl_id, const char *key);
    void SetArgs(TableIdent tid, std::string_view key);
    void SetArgs(TableIdent tid, std::string key);
    std::string_view Key() const;

    // output
    std::string floor_key_;
    std::string value_;
    uint64_t ts_;
    uint64_t expire_ts_;

private:
    // input
    std::variant<std::string_view, std::string> key_;
};

class ScanRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Scan;
    }
    /**
     * @brief Set the scan range.
     * @param tbl_id Table partition identifier.
     * @param begin The begin key of the scan range.
     * @param end The end key of the scan range (not inclusive).
     * @param begin_inclusive Whether the begin key is inclusive.
     */
    void SetArgs(TableIdent tbl_id,
                 std::string_view begin,
                 std::string_view end,
                 bool begin_inclusive = true);
    void SetArgs(TableIdent tbl_id,
                 std::string begin,
                 std::string end,
                 bool begin_inclusive = true);
    void SetArgs(TableIdent tbl_id,
                 const char *begin,
                 const char *end,
                 bool begin_inclusive = true);

    /**
     * @brief Set the pagination of the scan result.
     * @param entries Limit the number of entries in one page.
     * @param size Limit the page size (byte).
     */
    void SetPagination(size_t entries, size_t size);
    /**
     * @brief Set number of pages to prefetch during scan.
     */
    void SetPrefetchPageNum(size_t pages);
    size_t PrefetchPageNum() const;

    std::string_view BeginKey() const;
    std::string_view EndKey() const;

    tcb::span<KvEntry> Entries();

    /**
     * @brief Get the size of the scan result.
     * @return A pair of size_t, where the first element is the number of
     * entries and the second element is the total size in bytes of all entries.
     */
    std::pair<size_t, size_t> ResultSize() const;
    /**
     * @brief Check if there are more entries to scan.
     * @return true if there are more entries to scan, false otherwise.
     * If this returns true, the caller should request again to fetch the
     * next page of entries, and the begin key of the request should be set
     * to the end key of the previously request. If this returns false, the scan
     * is complete and no more entries are available.
     */
    bool HasRemaining() const;

private:
    // input
    bool begin_inclusive_;
    // output
    bool has_remaining_;
    // input
    std::variant<std::string_view, std::string> begin_key_;
    std::variant<std::string_view, std::string> end_key_;
    size_t page_entries_{SIZE_MAX};
    size_t page_size_{SIZE_MAX};
    // output
    std::vector<KvEntry> entries_;
    size_t num_entries_{0};
    size_t prefetch_page_num_{kDefaultScanPrefetchPageCount};
    friend class ScanTask;
};

class ListObjectRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::ListObject;
    }

    explicit ListObjectRequest(std::vector<std::string> *objects)
        : objects_(objects)
    {
    }

    void SetRemotePath(std::string remote_path)
    {
        remote_path_ = std::move(remote_path);
    }
    void SetRecursive(bool recursive)
    {
        recursive_ = recursive;
    }

    const std::string &RemotePath() const
    {
        return remote_path_;
    }

    void SetDetails(std::vector<utils::CloudObjectInfo> *details)
    {
        details_ = details;
    }

    std::vector<std::string> *GetObjects()
    {
        return objects_;
    }

    std::vector<utils::CloudObjectInfo> *GetDetails() const
    {
        return details_;
    }
    bool Recursive() const
    {
        return recursive_;
    }

    void SetContinuationToken(std::string token)
    {
        continuation_token_ = std::move(token);
    }

    const std::string &GetContinuationToken() const
    {
        return continuation_token_;
    }

    std::string *GetNextContinuationToken()
    {
        return &next_continuation_token_;
    }

    bool HasMoreResults() const
    {
        return !next_continuation_token_.empty();
    }

private:
    std::vector<std::string> *objects_;
    std::vector<utils::CloudObjectInfo> *details_{nullptr};
    std::string remote_path_;
    bool recursive_{false};
    std::string continuation_token_;       // input token
    std::string next_continuation_token_;  // output token
};

class WriteRequest : public KvRequest
{
public:
    /**
     * @brief Link to the next pending write request that has been received but
     * not yet processed. And user may use this to manage a chain of free
     * WriteRequests.
     */
    WriteRequest *next_{nullptr};
};

class ReopenRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Reopen;
    }
    void SetArgs(TableIdent tbl_id);
};

/**
 * @brief Batch write atomically.
 */
class BatchWriteRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::BatchWrite;
    }
    void SetArgs(TableIdent tid, std::vector<WriteDataEntry> &&batch);
    void AddWrite(std::string key, std::string value, uint64_t ts, WriteOp op);
    // used by caller.
    void Clear();

    // input
    std::vector<WriteDataEntry> batch_;
};

class TruncateRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Truncate;
    }
    void SetArgs(TableIdent tid, std::string_view position);
    void SetArgs(TableIdent tid, std::string position);

    // input
    std::string_view position_;

private:
    std::string position_storage_;
};

class DropTableRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::DropTable;
    }
    void SetArgs(std::string table_name);
    const std::string &TableName() const;

private:
    std::string table_name_;
    std::vector<std::unique_ptr<TruncateRequest>> truncate_reqs_;
    std::atomic<uint32_t> pending_{0};
    std::atomic<uint8_t> first_error_{static_cast<uint8_t>(KvError::NoError)};

    friend class EloqStore;
};

class ArchiveRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Archive;
    }

    void SetSnapshotTimestamp(uint64_t ts)
    {
        snapshot_ts_ = ts;
    }

    uint64_t GetSnapshotTimestamp() const
    {
        return snapshot_ts_;
    }

private:
    uint64_t snapshot_ts_{0};
};

class GlobalArchiveRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::GlobalArchive;
    }

    void SetSnapshotTimestamp(uint64_t ts)
    {
        snapshot_ts_ = ts;
    }

    uint64_t GetSnapshotTimestamp() const
    {
        return snapshot_ts_;
    }

private:
    uint64_t snapshot_ts_{0};
    std::vector<std::unique_ptr<ArchiveRequest>> archive_reqs_;
    std::atomic<uint32_t> pending_{0};
    std::atomic<uint8_t> first_error_{static_cast<uint8_t>(KvError::NoError)};

    friend class EloqStore;
};

class GlobalReopenRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::GlobalReopen;
    }

private:
    std::vector<std::unique_ptr<ReopenRequest>> reopen_reqs_;
    std::atomic<uint32_t> pending_{0};
    std::atomic<uint8_t> first_error_{static_cast<uint8_t>(KvError::NoError)};

    friend class EloqStore;
};

class CompactRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Compact;
    }
};

class LocalGcRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::LocalGc;
    }
};

class CleanExpiredRequest : public WriteRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::CleanExpired;
    }
};

class ArchiveCrond;
class ObjectStore;
class EloqStoreModule;
class PrewarmService;
class CloudStorageService;

class EloqStore
{
public:
    EloqStore(const KvOptions &opts);
    EloqStore(const EloqStore &) = delete;
    EloqStore(EloqStore &&) = delete;
    ~EloqStore();
    KvError Start(uint64_t term = 0);
    void Stop();
    bool IsStopped() const;
    const KvOptions &Options() const;
    CloudStorageService *CloudService() const
    {
        return cloud_service_.get();
    }

    /**
     * @brief Get the PrewarmService instance
     * @return Pointer to PrewarmService, or nullptr if not available
     */
    PrewarmService *GetPrewarmService() const
    {
        return prewarm_service_.get();
    }

    uint64_t Term() const
    {
        return term_;
    }

    /**
     * @brief Validate KvOptions configuration.
     * @param opts The options to validate
     * This routine may adjust some cloud-mode options to safe defaults instead
     * of failing validation.
     * @return true if options are valid, false otherwise
     */
    static bool ValidateOptions(KvOptions &opts);

    template <typename F>
    bool ExecAsyn(KvRequest *req, uint64_t data, F callback)
    {
        req->user_data_ = data;
        req->callback_ = std::move(callback);
        return SendRequest(req);
    }
    bool ExecAsyn(KvRequest *req);
    void ExecSync(KvRequest *req);

#ifdef ELOQSTORE_WITH_TXSERVICE
    void InitializeMetrics(metrics::MetricsRegistry *metrics_registry,
                           const metrics::CommonLabels &common_labels);

    /**
     * @brief Get the metrics meter for a specific shard.
     * @param shard_id The shard ID.
     * @return Pointer to the meter for the shard, or nullptr if metrics are not
     * enabled or shard_id is invalid.
     */
    metrics::Meter *GetMetricsMeter(size_t shard_id) const;
#endif

    bool EnableMetrics() const
    {
        return enable_eloqstore_metrics_;
    }

private:
    bool SendRequest(KvRequest *req);
    void HandleDropTableRequest(DropTableRequest *req);
    void HandleGlobalArchiveRequest(GlobalArchiveRequest *req);
    void HandleGlobalReopenRequest(GlobalReopenRequest *req);
    KvError CollectTablePartitions(const std::string &table_name,
                                   std::vector<TableIdent> &partitions) const;
    KvError InitStoreSpace();

    KvOptions options_;
    std::vector<int> root_fds_;
    std::vector<std::unique_ptr<Shard>> shards_;
#ifdef ELOQSTORE_WITH_TXSERVICE
    std::vector<std::unique_ptr<metrics::Meter>> metrics_meters_;
#endif
    std::atomic<bool> stopped_{true};
    uint64_t term_{0};

    std::unique_ptr<CloudStorageService> cloud_service_;
    std::unique_ptr<ArchiveCrond> archive_crond_{nullptr};
    std::unique_ptr<PrewarmService> prewarm_service_{nullptr};
#ifdef ELOQ_MODULE_ENABLED
    std::unique_ptr<EloqStoreModule> module_{nullptr};
#endif

    bool enable_eloqstore_metrics_{false};

    friend class Shard;
    friend class AsyncIoManager;
    friend class IouringMgr;
    friend class WriteTask;
    friend class PrewarmService;
};
}  // namespace eloqstore
