#include "cloud_storage_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include "async_io_manager.h"
#include "eloq_store.h"
#include "storage/shard.h"

namespace eloqstore
{
namespace
{
constexpr std::chrono::milliseconds kIdleWait{10};
}

CloudStorageService::CloudStorageService(EloqStore *store) : store_(store)
{
    CHECK(store_ != nullptr);
    size_t shard_count = store_->Options().num_threads;
    if (shard_count == 0)
    {
        shard_count = 1;
    }
    shard_stores_.assign(shard_count, nullptr);
    shard_locks_.resize(shard_count);

    worker_count_ =
        std::max<size_t>(store_->Options().cloud_request_threads, 1);
    job_queues_.resize(worker_count_);
}

CloudStorageService::~CloudStorageService()
{
    Stop();
}

void CloudStorageService::Start()
{
    bool expected = true;
    if (!stopping_.compare_exchange_strong(expected, false))
    {
        return;
    }
    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i)
    {
        workers_.emplace_back(&CloudStorageService::RunWorker, this, i);
    }
}

void CloudStorageService::Stop()
{
    bool was_running = !stopping_.exchange(true, std::memory_order_acq_rel);
    for (auto &queue : job_queues_)
    {
        queue.enqueue({nullptr, nullptr});
    }
    if (!was_running && workers_.empty())
    {
        return;
    }
    for (auto &worker : workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
    workers_.clear();
}

void CloudStorageService::RegisterObjectStore(ObjectStore *store,
                                              size_t shard_id)
{
    if (shard_id >= shard_stores_.size())
    {
        LOG(ERROR) << "Shard id " << shard_id
                   << " is out of range when registering object store";
        return;
    }
    std::unique_lock lk(shard_locks_[shard_id].mutex);
    shard_stores_[shard_id] = store;
}

void CloudStorageService::UnregisterObjectStore(size_t shard_id)
{
    if (shard_id >= shard_stores_.size())
    {
        return;
    }
    std::unique_lock lk(shard_locks_[shard_id].mutex);
    shard_stores_[shard_id] = nullptr;
}

void CloudStorageService::Submit(ObjectStore *store, ObjectStore::Task *task)
{
    Shard *owner = task->owner_shard_;
    CHECK(owner != nullptr) << "Cloud task missing owner shard";
    pending_jobs_.fetch_add(1, std::memory_order_relaxed);
    size_t worker_idx = owner->shard_id_ % worker_count_;
    job_queues_[worker_idx].enqueue({store, task});
}

void CloudStorageService::NotifyTaskFinished(ObjectStore::Task *task)
{
    CHECK(task != nullptr);
    CHECK(task->owner_shard_ != nullptr && task->kv_task_ != nullptr);
    auto *cloud_mgr =
        reinterpret_cast<CloudStoreMgr *>(task->owner_shard_->IoManager());
    cloud_mgr->EnqueueCloudReadyTask(task->kv_task_);
}

void CloudStorageService::RunWorker(size_t worker_index)
{
    const int64_t wait_timeout_us =
        std::chrono::duration_cast<std::chrono::microseconds>(kIdleWait)
            .count();
    while (true)
    {
        if (stopping_.load(std::memory_order_acquire))
        {
            break;
        }

        bool http_active = ProcessHttpWork(worker_index);

        bool started_jobs = false;
        PendingJob ready_jobs[128];
        size_t nready = job_queues_[worker_index].try_dequeue_bulk(
            ready_jobs, std::size(ready_jobs));
        for (size_t i = 0; i < nready; ++i)
        {
            PendingJob &ready_job = ready_jobs[i];
            if (ready_job.store == nullptr || ready_job.task == nullptr)
            {
                continue;
            }
            pending_jobs_.fetch_sub(1, std::memory_order_relaxed);
            ready_job.store->StartHttpRequest(ready_job.task);
            started_jobs = true;
        }

        if (http_active || started_jobs)
        {
            continue;
        }

        PendingJob job;
        bool has_job =
            job_queues_[worker_index].wait_dequeue_timed(job, wait_timeout_us);
        if (has_job && job.store != nullptr && job.task != nullptr)
        {
            pending_jobs_.fetch_sub(1, std::memory_order_relaxed);
            job.store->StartHttpRequest(job.task);
        }
    }
}

bool CloudStorageService::ProcessHttpWork(size_t worker_index)
{
    bool active = false;
    for (size_t shard_idx = worker_index; shard_idx < shard_stores_.size();
         shard_idx += worker_count_)
    {
        ObjectStore *object_store = nullptr;
        std::shared_lock lk(shard_locks_[shard_idx].mutex);
        object_store = shard_stores_[shard_idx];
        if (object_store == nullptr)
        {
            continue;
        }
        object_store->RunHttpWork();
        active |= !object_store->HttpWorkIdle();
    }
    return active;
}

}  // namespace eloqstore
