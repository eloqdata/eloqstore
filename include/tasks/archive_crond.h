#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "eloq_store.h"

namespace eloqstore
{

/**
 * @brief Archives each attached data store shard on its own schedule, every
 * KvOptions::archive_interval_secs.
 *
 * A shard's schedule belongs to its attachment: it starts when the shard is
 * attached (AddShard, from EloqStore::AttachShard) and ends when the shard is
 * detached (RemoveShard, from EloqStore::DetachShard), so a shard attached
 * again at a newer term starts afresh. One thread serves every shard: it
 * waits for the earliest due shard, submits that shard's GlobalArchiveRequest
 * with a completion callback, and sets the shard's next due time one interval
 * after the archive completes. The archives of different shards therefore run
 * independently, and a slow one delays no other shard.
 */
class ArchiveCrond
{
public:
    /** @param store The store whose data store shards are archived. */
    explicit ArchiveCrond(EloqStore *store);

    /**
     * @brief Starts the thread. The schedules a previous run of the store
     * left are forgotten: the shards attached from now on bring their own.
     */
    void Start();
    /**
     * @brief Stops the thread. An archive in flight is not waited for: it
     * completes while the store stops its worker shards, and its completion
     * only updates this object, which the store keeps.
     * @note In module builds it yields while the thread finishes, never
     * blocking a brpc worker: called from a bthread on the worker that drives
     * the shard the thread waits on to list partitions, a blocking join would
     * never return.
     */
    void Stop();
    /**
     * @brief Starts a data store shard's schedule: its first archive is due
     * one interval from now.
     * @param ds_shard_id The shard, just attached.
     */
    void AddShard(DsShardId ds_shard_id);
    /**
     * @brief Ends a data store shard's schedule: no new archive of it starts.
     * An archive of it in flight finishes on its own, and its completion does
     * not schedule another.
     * @param ds_shard_id The shard, being detached.
     */
    void RemoveShard(DsShardId ds_shard_id);

private:
    using Clock = std::chrono::steady_clock;
#ifdef ELOQ_MODULE_ENABLED
    using Lock = std::unique_lock<bthread::Mutex>;
#else
    using Lock = std::unique_lock<std::mutex>;
#endif

    /** @brief One data store shard's archive schedule. */
    struct ShardSchedule
    {
        Clock::time_point next_due_{};  ///< When its next archive starts.
        /**
         * Changed by AddShard and RemoveShard, so that an archive submitted
         * before them does not set the next due time when it completes.
         */
        uint64_t generation_{0};
        bool attached_{false};   ///< Between AddShard and RemoveShard.
        bool in_flight_{false};  ///< An archive of the shard is running.
        /**
         * The running, or last, archive's request. Replaced and freed only
         * while no archive of the shard runs, so a completion never reads a
         * freed request.
         */
        std::unique_ptr<GlobalArchiveRequest> req_;
    };

    /** @brief A due archive the thread took, submitted outside the lock. */
    struct DueArchive
    {
        DsShardId ds_shard_id_;      ///< The shard to archive.
        uint64_t generation_;        ///< Its schedule's generation then.
        GlobalArchiveRequest *req_;  ///< Its request, owned by the entry.
    };

    /** @brief The thread: takes the due shards, submits them, waits. */
    void Crond();
    /**
     * @brief Submits one due archive, or puts it off if the shard is not
     * attached now.
     * @param due The archive.
     */
    void Submit(const DueArchive &due);
    /**
     * @brief Records that a shard's archive ended: the next one is due one
     * interval from now, unless the schedule ended or restarted meanwhile.
     * @param ds_shard_id The shard.
     * @param generation The schedule's generation when the archive started.
     * @param err How the archive ended.
     */
    void OnArchiveDone(DsShardId ds_shard_id, uint64_t generation, KvError err);
    /**
     * @brief Waits until notified, or until @p until if it has a value.
     * @param lk Holds mu_; released while waiting.
     * @param until The earliest due time, if any shard is scheduled.
     */
    void Wait(Lock &lk, std::optional<Clock::time_point> until);
    /** @return KvOptions::archive_interval_secs as a duration. */
    Clock::duration Interval() const;

    EloqStore *store_{nullptr};
    std::thread thd_;
    /** Set by the thread as it returns, so Stop can yield until then. */
    std::atomic<bool> exited_{false};

#ifdef ELOQ_MODULE_ENABLED
    bthread::Mutex mu_;
    bthread::ConditionVariable cond_var_;
#else
    std::mutex mu_;
    std::condition_variable cond_var_;
#endif
    bool stop_requested_{true};  ///< Guarded by mu_.
    /** The attached shards' schedules, by shard id. Guarded by mu_. */
    std::unordered_map<DsShardId, ShardSchedule> schedules_;
};
}  // namespace eloqstore
