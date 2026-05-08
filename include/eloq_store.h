#pragma once

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#endif

#include <atomic>
#include <functional>
#include <limits>
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
class EloqStore;

enum class RequestType : uint8_t
{
    Read,
    Floor,
    Scan,
    ListObject,
    ListStandbyPartition,
    BatchWrite,
    Reopen,
    Truncate,
    DropTable,
    Archive,
    Compact,
    LocalGc,
    CleanExpired,
    GlobalArchive,
    GlobalReopen,
    GlobalListArchiveTags,
    CreateBranch,
    DeleteBranch,
    GlobalCreateBranch
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
    case RequestType::ListStandbyPartition:
        return "list_standby_partition";
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
    case RequestType::GlobalListArchiveTags:
        return "global_list_archive_tags";
    case RequestType::CreateBranch:
        return "create_branch";
    case RequestType::DeleteBranch:
        return "delete_branch";
    case RequestType::GlobalCreateBranch:
        return "global_create_branch";
    default:
        return "unknown";
    }
}

inline StoreMode DeriveStoreMode(const KvOptions &opts)
{
    if (opts.enable_local_standby)
    {
        return opts.standby_master_addr.empty() ? StoreMode::StandbyMaster
                                                : StoreMode::StandbyReplica;
    }
    if (!opts.cloud_store_path.empty())
    {
        return StoreMode::Cloud;
    }
    return StoreMode::Local;
}

class KvRequest
{
public:
    virtual ~KvRequest() = default;
    virtual RequestType Type() const = 0;
    virtual bool AutoReopenRetry() const
    {
        return false;
    }
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
    uint8_t reopen_retry_remaining_{0};
#ifdef ELOQ_MODULE_ENABLED
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
    friend class TaskManager;
};

class ReadRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Read;
    }
    bool AutoReopenRetry() const override
    {
        return true;
    }
    void SetArgs(TableIdent tbl_id, const char *key);
    void SetArgs(TableIdent tbl_id, std::string_view key);
    void SetArgs(TableIdent tbl_id, std::string key);
    std::string_view Key() const;

    // output
    std::string value_;
    IoStringBuffer large_value_;
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
    bool AutoReopenRetry() const override
    {
        return true;
    }
    void SetArgs(TableIdent tbl_id, const char *key);
    void SetArgs(TableIdent tid, std::string_view key);
    void SetArgs(TableIdent tid, std::string key);
    std::string_view Key() const;

    // output
    std::string floor_key_;
    std::string value_;
    IoStringBuffer large_value_;
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
    bool AutoReopenRetry() const override
    {
        return true;
    }
    /**
     * @brief Set the scan range.
     * @param tbl_id Table partition identifier.
     * @param begin The begin key of the scan range.
     * @param end The end key of the scan range.
     * @param begin_inclusive Whether the begin key is inclusive.
     * @param end_inclusive Whether the end key is inclusive.
     */
    void SetArgs(TableIdent tbl_id,
                 std::string_view begin,
                 std::string_view end,
                 bool begin_inclusive = true,
                 bool end_inclusive = false);
    void SetArgs(TableIdent tbl_id,
                 std::string begin,
                 std::string end,
                 bool begin_inclusive = true,
                 bool end_inclusive = false);
    void SetArgs(TableIdent tbl_id,
                 const char *begin,
                 const char *end,
                 bool begin_inclusive = true,
                 bool end_inclusive = false);

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
    bool end_inclusive_{false};
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

class ListStandbyPartitionRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::ListStandbyPartition;
    }

    explicit ListStandbyPartitionRequest(std::vector<std::string> *partitions)
        : partitions_(partitions)
    {
    }

    std::vector<std::string> *GetPartitions() const
    {
        return partitions_;
    }

private:
    std::vector<std::string> *partitions_;
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
    void SetTag(std::string tag)
    {
        tag_ = std::move(tag);
    }
    const std::string &Tag() const
    {
        return tag_;
    }
    // `clean=true` means reopen should accept a missing remote snapshot and
    // replace any existing local partition state with an empty snapshot.
    void SetClean(bool clean)
    {
        clean_ = clean;
    }
    bool Clean() const
    {
        return clean_;
    }

private:
    std::string tag_;
    bool clean_{false};

    friend class EloqStore;
    friend class ReopenTask;
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
    enum class Action : uint8_t
    {
        Create = 0,
        Delete = 1,
    };

    RequestType Type() const override
    {
        return RequestType::Archive;
    }

    void SetTag(std::string tag)
    {
        tag_ = std::move(tag);
    }
    [[deprecated("Use SetTag(std::string) instead.")]]
    void SetSnapshotTimestamp(uint64_t ts);

    const std::string &Tag() const
    {
        return tag_;
    }
    void SetTerm(uint64_t term)
    {
        term_ = term;
    }
    uint64_t Term() const
    {
        return term_;
    }

    void SetAction(Action action)
    {
        action_ = action;
    }

    Action GetAction() const
    {
        return action_;
    }

private:
    static constexpr uint64_t kUseProcessTerm =
        std::numeric_limits<uint64_t>::max();
    std::string tag_;
    uint64_t term_{kUseProcessTerm};
    Action action_{Action::Create};
};

class GlobalArchiveRequest : public KvRequest
{
public:
    enum class Action : uint8_t
    {
        Create = 0,
        Delete = 1,
    };

    RequestType Type() const override
    {
        return RequestType::GlobalArchive;
    }

    void SetTag(std::string tag)
    {
        tag_ = std::move(tag);
    }
    [[deprecated("Use SetTag(std::string) instead.")]]
    void SetSnapshotTimestamp(uint64_t ts);

    const std::string &Tag() const
    {
        return tag_;
    }
    void SetTerm(uint64_t term)
    {
        term_ = term;
    }
    uint64_t Term() const
    {
        return term_;
    }

    void SetAction(Action action)
    {
        action_ = action;
    }

    Action GetAction() const
    {
        return action_;
    }

    // The archive name produced by HandleGlobalArchiveRequest.
    // Callers should read this after a successful ExecSync.
    const std::string &ResultArchive() const
    {
        return result_archive_;
    }

private:
    static constexpr uint64_t kUseProcessTerm =
        std::numeric_limits<uint64_t>::max();
    std::string tag_;
    uint64_t term_{kUseProcessTerm};
    Action action_{Action::Create};
    std::string result_archive_;
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

    void SetTag(std::string tag)
    {
        tag_ = std::move(tag);
    }
    const std::string &Tag() const
    {
        return tag_;
    }

private:
    std::string tag_;
    std::vector<std::unique_ptr<ReopenRequest>> reopen_reqs_;
    std::atomic<uint32_t> pending_{0};
    std::atomic<uint8_t> first_error_{static_cast<uint8_t>(KvError::NoError)};

    friend class EloqStore;
};

class GlobalListArchiveTagsRequest : public KvRequest
{
public:
    struct ArchiveEntry
    {
        uint64_t term{0};
        std::string tag;
    };

    RequestType Type() const override
    {
        return RequestType::GlobalListArchiveTags;
    }

    void SetPrefix(std::string prefix)
    {
        prefix_ = std::move(prefix);
    }
    const std::string &Prefix() const
    {
        return prefix_;
    }
    const std::vector<ArchiveEntry> &Entries() const
    {
        return entries_;
    }

private:
    std::string prefix_;
    std::vector<ArchiveEntry> entries_;

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

class BranchRequest : public WriteRequest
{
public:
    std::string branch_name;
    std::string result_branch;
};

class CreateBranchRequest : public BranchRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::CreateBranch;
    }

    void SetArgs(std::string branch_name_val)
    {
        branch_name = std::move(branch_name_val);
    }
};

class DeleteBranchRequest : public BranchRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::DeleteBranch;
    }

    void SetArgs(std::string branch_name_val)
    {
        branch_name = std::move(branch_name_val);
    }
};

class GlobalCreateBranchRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::GlobalCreateBranch;
    }

    void SetArgs(std::string branch_name)
    {
        branch_name_ = std::move(branch_name);
    }

    const std::string &GetBranchName() const
    {
        return branch_name_;
    }

    // Optional caller-supplied salt timestamp.  When non-zero,
    // HandleGlobalCreateBranchRequest uses the lower 32 bits of this value
    // (formatted as %08x) as the salt instead of the live clock.  This makes
    // the internal filename deterministic and correlated with a known timestamp
    // (e.g. a backup_ts).
    void SetSaltTimestamp(uint64_t ts)
    {
        salt_ts_ = ts;
    }
    uint64_t GetSaltTimestamp() const
    {
        return salt_ts_;
    }

    // The salted internal branch name chosen by
    // HandleGlobalCreateBranchRequest. Callers should use this after a
    // successful ExecSync to refer to the new branch in subsequent operations
    // (delete, read, etc.).
    const std::string &ResultBranch() const
    {
        return result_branch_;
    }

private:
    std::string branch_name_;
    uint64_t salt_ts_{0};
    std::string result_branch_;
    std::vector<std::unique_ptr<CreateBranchRequest>> branch_reqs_;
    std::atomic<uint32_t> pending_{0};
    std::atomic<uint8_t> first_error_{static_cast<uint8_t>(KvError::NoError)};

    friend class EloqStore;
};

class ArchiveCrond;
class ObjectStore;
class EloqStoreModule;
class PrewarmService;
class CloudStorageService;
class StandbyService;

class EloqStore
{
public:
    enum class Status : uint8_t
    {
        Starting = 0,
        Running = 1,
        Stopping = 2,
        Stopped = 3,
    };

    EloqStore(const KvOptions &opts);
    EloqStore(const EloqStore &) = delete;
    EloqStore(EloqStore &&) = delete;
    ~EloqStore();
    KvError Start(std::string_view branch = MainBranchName,
                  uint64_t term = 0,
                  PartitonGroupId partition_group_id = 0);
    void Stop();
    bool IsStopped() const;
    bool Inited() const;
    const KvOptions &Options() const;

    /**
     * @brief Validate KvOptions configuration.
     * @param opts The options to validate
     * This routine may adjust some cloud-mode options to safe defaults instead
     * of failing validation.
     * @return true if options are valid, false otherwise
     */
    static bool ValidateOptions(KvOptions &opts);

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

    StandbyService *GetStandbyService() const
    {
        return standby_service_.get();
    }

    StoreMode Mode() const
    {
        return store_mode_.load(std::memory_order_acquire);
    }

    uint64_t Term() const
    {
        return term_;
    }
    PartitonGroupId PartitionGroupId() const
    {
        return partition_group_id_;
    }

    KvError UpdateStandbyMasterStorePaths(std::vector<std::string> paths,
                                          std::vector<uint64_t> weights);
    KvError UpdateStandbyMasterAddr(std::string standby_master_addr);

    /**
     * @brief Returns the io_uring buffer index base added to a
     * GlobalRegisteredMemory chunk index when the shard registered its
     * global registered memory. Callers integrating their own
     * GlobalRegisteredMemory need this value to populate
     * IoBufferRef::buf_index_ when building user-supplied IoStringBuffers
     * for very-large-value writes, and to pass it to IoStringBuffer::Recycle
     * after a read.
     * @return 0 if the shard does not own a GlobalRegisteredMemory.
     */
    uint16_t GlobalRegMemIndexBase(size_t shard_id) const;

    bool ExecAsyn(KvRequest *req);
    void ExecSync(KvRequest *req);

    template <typename F>
    bool ExecAsyn(KvRequest *req, uint64_t data, F callback)
    {
        req->user_data_ = data;
        req->callback_ = std::move(callback);
        req->reopen_retry_remaining_ = options_.auto_reopen_retry_times;
        return SendRequest(req);
    }

    std::string_view Branch() const
    {
        return branch_;
    }

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
    void HandleGlobalListArchiveTagsRequest(GlobalListArchiveTagsRequest *req);
    void HandleGlobalCreateBranchRequest(GlobalCreateBranchRequest *req);
    KvError CollectTablePartitions(const std::string &table_name,
                                   std::vector<TableIdent> &partitions) const;
    KvError InitStoreSpace();
    KvError BuildStorePathLut();
    void CleanupRuntime(size_t started_shards);

    KvOptions options_;
    std::vector<int> root_fds_;
    // Cloud service must outlive shards because shard teardown triggers
    // async HTTP cleanup that uses CloudStorageService callbacks.
    std::unique_ptr<CloudStorageService> cloud_service_;
    std::vector<std::unique_ptr<Shard>> shards_;
#ifdef ELOQSTORE_WITH_TXSERVICE
    std::vector<std::unique_ptr<metrics::Meter>> metrics_meters_;
#endif
    std::atomic<Status> status_{Status::Stopped};
    uint64_t term_{0};
    PartitonGroupId partition_group_id_{0};
    std::string branch_{MainBranchName};
    std::unique_ptr<ArchiveCrond> archive_crond_{nullptr};
    std::unique_ptr<PrewarmService> prewarm_service_{nullptr};
    std::unique_ptr<StandbyService> standby_service_{nullptr};
#ifdef ELOQ_MODULE_ENABLED
    std::unique_ptr<EloqStoreModule> module_{nullptr};
#endif

    bool enable_eloqstore_metrics_{false};
    std::atomic<StoreMode> store_mode_{StoreMode::Local};

    friend class Shard;
    friend class KvRequest;
    friend class AsyncIoManager;
    friend class IouringMgr;
    friend class WriteTask;
    friend class PrewarmService;
};
}  // namespace eloqstore
