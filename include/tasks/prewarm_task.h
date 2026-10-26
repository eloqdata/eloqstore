#pragma once
#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "tasks/task.h"

namespace utils
{
struct CloudObjectInfo;
}  // namespace utils

namespace eloqstore
{
class EloqStore;
class CloudStoreMgr;

// Prewarm statistics
struct PrewarmStats
{
    size_t total_files_listed{0};
    size_t total_files_pulled{0};
    size_t files_skipped_filtered{0};
    size_t files_skipped_missing{0};
    size_t files_failed{0};
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    enum class CompletionReason
    {
        Success,        // All files processed
        DiskFull,       // Ran out of disk space
        ListingError,   // Failed to list cloud objects
        DownloadError,  // Persistent download errors
        Shutdown        // System shutting down
    } completion_reason{CompletionReason::Success};

    std::string CompletionReasonString() const
    {
        switch (completion_reason)
        {
        case CompletionReason::Success:
            return "completed successfully";
        case CompletionReason::DiskFull:
            return "aborted due to insufficient disk space";
        case CompletionReason::ListingError:
            return "aborted due to cloud listing error";
        case CompletionReason::DownloadError:
            return "aborted due to persistent download errors";
        case CompletionReason::Shutdown:
            return "aborted due to system shutdown";
        default:
            return "unknown";
        }
    }

    double DurationSeconds() const
    {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);
        return duration.count() / 1000.0;
    }
};

struct PrewarmFile
{
    TableIdent tbl_id;
    FileId file_id;
    uint64_t term{0};
    std::string branch_name;
    size_t file_size;
    bool is_manifest;
    bool is_segment{false};
    std::string mod_time;
};

class Prewarmer : public KvTask
{
public:
    explicit Prewarmer(CloudStoreMgr *io_mgr);

    TaskType Type() const override
    {
        return TaskType::Prewarm;
    }

    void Run();
    void Shutdown();

private:
    friend class CloudStoreMgr;
    friend class PrewarmService;

    bool PopNext(PrewarmFile &file);

    CloudStoreMgr *io_mgr_;
    std::atomic<bool> stop_{true};
    bool shutting_down_{false};
};

class PrewarmService
{
public:
    explicit PrewarmService(EloqStore *store);
    ~PrewarmService();

    void Start();
    void Stop();

    /**
     * @brief Queues a round of prewarm for one partition.
     * @param ds_shard_id The data store shard the partition is served for.
     * @param table The partition.
     */
    void Prewarm(DsShardId ds_shard_id, const TableIdent &table);
    /**
     * @brief Queues a round of prewarm for every partition of a data store
     * shard (those the partition filter keeps for it); AttachShard calls it.
     * @param ds_shard_id The data store shard.
     */
    void PrewarmShard(DsShardId ds_shard_id);

    /**
     * @brief Wait for prewarm operation to complete
     * Blocks until the prewarm thread finishes execution
     */
    void Wait()
    {
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

private:
    friend class EloqStore;
    friend class CloudStoreMgr;
    void AbortPrewarmWorkers();
    /**
     * @brief Lists and downloads the files under a cloud path for a data
     * store shard, skipping partitions the filter does not give it.
     * @param remote_path A partition's path, or empty for the whole bucket.
     * @param ds_shard_id The data store shard.
     */
    void PrewarmCloudCache(const std::string &remote_path,
                           DsShardId ds_shard_id);
    bool ListCloudObjects(const std::string &remote_path,
                          std::vector<utils::CloudObjectInfo> &details,
                          const std::string &continuation_token = std::string{},
                          std::string *next_continuation_token = nullptr);
    bool ExtractPartition(const std::string &path,
                          TableIdent &tbl_id,
                          std::string &filename);
    void PrewarmLoop();

private:
    EloqStore *store_;
    std::thread thread_;

    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> stopped_{true};

    /** @brief One queued round: a partition, or a whole data store shard. */
    struct PrewarmJob
    {
        DsShardId ds_shard_id_{kUnsetDsShard};
        /** The partition to prewarm; no value means the whole shard. */
        std::optional<TableIdent> table_;
    };
    // Rounds queued by AttachShard (a shard) and by a partition's reopen.
    moodycamel::BlockingConcurrentQueue<PrewarmJob> prewarm_jobs_;
};
}  // namespace eloqstore
