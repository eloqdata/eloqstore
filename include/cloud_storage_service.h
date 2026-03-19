#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <vector>

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#endif

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "storage/object_store.h"

namespace eloqstore
{
class EloqStore;

class CloudStorageService
{
public:
    explicit CloudStorageService(EloqStore *store);
    ~CloudStorageService();

    void Start();
    void Stop();

    void RegisterObjectStore(ObjectStore *store, size_t shard_id);
    void UnregisterObjectStore(size_t shard_id);

    void Submit(ObjectStore *store, ObjectStore::Task *task);
    void NotifyTaskFinished(ObjectStore::Task *task);
    struct BootstrapTermSyncState
    {
        KvError err_{KvError::Busy};
#ifdef ELOQ_MODULE_ENABLED
        bthread::Mutex mutex_;
        bthread::ConditionVariable cv_;
        bool done_{false};

        void Wait()
        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            while (!done_)
            {
                cv_.wait(lk);
            }
        }

        void SetDone(KvError err)
        {
            std::lock_guard<bthread::Mutex> lk(mutex_);
            err_ = err;
            done_ = true;
            cv_.notify_one();
        }
#else
        std::atomic<bool> done_{false};

        void Wait()
        {
            done_.wait(false, std::memory_order_acquire);
        }

        void SetDone(KvError err)
        {
            err_ = err;
            done_.store(true, std::memory_order_release);
            done_.notify_one();
        }
#endif
    };

    bool HasPendingJobs() const
    {
        return pending_jobs_.load(std::memory_order_acquire) > 0;
    }
    bool IsStopping() const
    {
        return stopping_.load(std::memory_order_acquire);
    }

private:
    struct PendingJob
    {
        ObjectStore *store;
        ObjectStore::Task *task;
    };

    void RunWorker(size_t worker_index);
    bool ProcessHttpWork(size_t worker_index);

    EloqStore *store_{nullptr};
    // One queue per request thread. Requests from a shard can only be processed
    // by the same request thread to avoid concurrent operations on curl.
    std::vector<moodycamel::BlockingConcurrentQueue<PendingJob>> job_queues_;
    struct alignas(64) ShardLock
    {
        ShardLock() = default;
        ShardLock(const ShardLock &) = delete;
        ShardLock &operator=(const ShardLock &) = delete;
        ShardLock(ShardLock &&) noexcept
        {
        }
        ShardLock &operator=(ShardLock &&) noexcept = delete;
        std::shared_mutex mutex;
    };

    std::vector<ObjectStore *> shard_stores_;
    std::vector<ShardLock> shard_locks_;

    size_t worker_count_{1};
    std::vector<std::thread> workers_;
    std::atomic<bool> stopping_{true};
    std::atomic<bool> accepting_jobs_{false};
    std::atomic<uint64_t> pending_jobs_{0};
    BootstrapTermSyncState bootstrap_state_;

    friend class EloqStore;
};

}  // namespace eloqstore
