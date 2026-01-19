#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <vector>

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
    std::atomic<uint64_t> pending_jobs_{0};
};

}  // namespace eloqstore
