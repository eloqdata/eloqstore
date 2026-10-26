#include "storage/shard.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>  // For __rdtsc()
#endif

#include "async_io_manager.h"
#include "error.h"
#include "global_registered_memory.h"
#include "standby_service.h"
#include "tasks/attach_shard_task.h"
#include "tasks/detach_shard_task.h"
#include "tasks/list_object_task.h"
#include "tasks/list_standby_partition_task.h"
#include "tasks/reopen_task.h"
#include "utils.h"

#ifdef ELOQSTORE_WITH_TXSERVICE
#include "eloqstore_metrics.h"
#endif

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/eloq_module.h>
#endif
namespace eloqstore
{
std::once_flag Shard::tsc_frequency_initialized_;
std::atomic<uint64_t> Shard::tsc_cycles_per_microsecond_{0};

#ifdef NDEBUG
DEFINE_uint64(max_processing_time_microseconds,
              400,
              "Max processing time in microseconds for low priority tasks.");
#else
DEFINE_uint64(max_processing_time_microseconds,
              UINT64_MAX,
              "Max processing time in microseconds for low priority tasks.");
#endif

// Per-segment cooperative yield budget. Any long-running task loop polls
// MaybeYield(), which calls YieldToLowPQ() once the current coroutine segment
// (time since its last resume, via CurResumeElapsedUs()) exceeds this many
// microseconds. This bounds how long one uninterrupted segment can stall the
// brpc worker (and, in module mode, the Redis request it co-drives) -- the
// direct cause of foreground SET/GET tail-latency spikes during compaction/GC.
// Distinct from max_processing_time_microseconds, which is a per-round budget
// the scheduler checks only between task resumes and so cannot preempt a single
// non-yielding segment.
DEFINE_uint64(eloqstore_yield_budget_us,
              20,
              "Max microseconds a long-running task coroutine segment may hold "
              "the shard worker thread before cooperatively yielding.");

Shard::Shard(EloqStore *store, size_t shard_id, uint32_t fd_limit)
    : store_(store),
      shard_id_(shard_id),
      page_pool_(&store->options_),
      task_mgr_(&store->options_),
      stack_allocator_(store->options_.coroutine_stack_size),
      io_mgr_(AsyncIoManager::Instance(store, fd_limit)),
      index_mgr_(io_mgr_.get())
{
    const auto &opts = store_->options_;
    if (!opts.global_registered_memories.empty())
    {
        assert(shard_id_ < opts.global_registered_memories.size());
        global_reg_mem_ = opts.global_registered_memories[shard_id_];
    }
}

KvError Shard::Init()
{
    // Inject the active branch into the IO manager before any file operation.
    // Terms are per data store shard: each task carries its shard's term and
    // the manager reads the running task's (AsyncIoManager::ProcessTerm).
    if (io_mgr_ != nullptr)
    {
        std::string_view branch =
            store_ != nullptr ? store_->Branch() : MainBranchName;
        assert(!branch.empty());
        io_mgr_->SetActiveBranch(branch);
    }
    InitializeTscFrequency();
    KvError res = io_mgr_->Init(this);
    return res;
}

void Shard::InitIoMgrAndPagePool()
{
    KvError err = io_mgr_->RestoreStartupState();
    if (err != KvError::NoError)
    {
        LOG(FATAL) << "startup cache restore failed on shard " +
                          std::to_string(shard_id_) + ": " + ErrorString(err);
        exit(1);
    }
    io_mgr_->InitBackgroundJob();
    io_mgr_and_page_pool_inited_.store(true, std::memory_order_release);
}

void Shard::WorkLoop()
{
    shard = this;
    InitIoMgrAndPagePool();

    // Get new requests from the queue, only blocked when there are no requests
    // and no active tasks.
    // This allows the thread to exit gracefully when the store is stopped.
    std::array<KvRequest *, 128> reqs;
    auto dequeue_requests = [this, &reqs]() -> int
    {
        size_t nreqs = requests_.try_dequeue_bulk(reqs.data(), reqs.size());
        // Idle state, wait for new requests or exit.
        while (nreqs == 0 && task_mgr_.NumActive() == 0 && io_mgr_->IsIdle() &&
               delayed_requests_.empty())
        {
            const auto status = store_->status_.load(std::memory_order_relaxed);
            if (io_mgr_->IsStoreStopping() ||
                status == EloqStore::Status::Stopped ||
                status == EloqStore::Status::Stopping)
            {
                return -1;
            }
            if (io_mgr_->NeedPrewarm())
            {
                io_mgr_->RunPrewarm();
                return 0;
            }
            const auto timeout = std::chrono::milliseconds(100);
            nreqs = requests_.wait_dequeue_bulk_timed(
                reqs.data(), reqs.size(), timeout);
        }

        return nreqs;
    };

#ifdef ELOQSTORE_WITH_TXSERVICE
    // Metrics collection setup
    metrics::Meter *meter = nullptr;
    if (store_->EnableMetrics())
    {
        meter = store_->GetMetricsMeter(shard_id_);
        assert(meter != nullptr);
    }
#endif

    while (true)
    {
        ts_ = ReadTimeMicroseconds();
#ifdef ELOQSTORE_WITH_TXSERVICE
        // Metrics collection: start timing the round (one iteration = one
        // round)
        metrics::TimePoint round_start{};
        if (store_->EnableMetrics())
        {
            round_start = metrics::Clock::now();
        }
#endif

        io_mgr_->Submit();

        io_mgr_->PollComplete();
        PromoteReadyDelayedReopenRequests();
        ExecuteReadyTasks();

        int nreqs = dequeue_requests();
        if (nreqs < 0)
        {
            // Exit.
            break;
        }
        for (int i = 0; i < nreqs; i++)
        {
            OnReceivedReq(reqs[i]);
        }

#ifdef ELOQSTORE_WITH_TXSERVICE
        // Metrics collection: end of round
        if (store_->EnableMetrics())
        {
            meter->CollectDuration(
                metrics::NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION, round_start);
            meter->Collect(metrics::NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS,
                           static_cast<double>(task_mgr_.NumActive()));
        }
#endif
    }

    // Complete any requests that raced Stop() into the queue before tearing
    // down, so their callers don't hang forever.
    DrainPendingRequests();
    task_mgr_.Shutdown();
    // Unfinished tasks may still own MemCachedPage::Handle instances.
    // Release task state before tearing down the index-page pool they
    // reference.
    index_mgr_.Shutdown();
    io_mgr_->Stop();
}

void Shard::Start()
{
#ifdef ELOQ_MODULE_ENABLED
    shard = this;
    running_status_.store(ShardStatus::Running, std::memory_order_release);
#else
    thd_ = std::thread([this] { WorkLoop(); });
#endif

#ifdef ELOQSTORE_WITH_TXSERVICE
    // Collect limit metrics once at initialization (these values don't change)
    if (store_->EnableMetrics())
    {
        metrics::Meter *meter = store_->GetMetricsMeter(shard_id_);
        if (meter != nullptr)
        {
            // Collect index buffer pool limit
            size_t index_limit = index_mgr_.GetBufferPoolLimit();
            meter->Collect(metrics::NAME_ELOQSTORE_INDEX_BUFFER_POOL_LIMIT,
                           static_cast<double>(index_limit));

            // Collect open file limit
            size_t open_file_limit = io_mgr_->GetOpenFileLimit();
            meter->Collect(metrics::NAME_ELOQSTORE_OPEN_FILE_LIMIT,
                           static_cast<double>(open_file_limit));

            // Collect local space limit
            size_t local_space_limit = io_mgr_->GetLocalSpaceLimit();
            meter->Collect(metrics::NAME_ELOQSTORE_LOCAL_SPACE_LIMIT,
                           static_cast<double>(local_space_limit));
        }
    }
#endif
}

void Shard::Stop()
{
#ifndef ELOQ_MODULE_ENABLED
    thd_.join();
#endif
}

void Shard::DrainPendingRequests()
{
    KvRequest *req = nullptr;
    [[maybe_unused]] size_t drained = 0;
    while (requests_.try_dequeue(req))
    {
        req->SetDone(KvError::NotRunning);
        ++drained;
    }
    for (auto &[_, state] : pending_reopens_)
    {
        CompleteReopenWaiters(std::move(state.waiters_), KvError::NotRunning);
    }
    pending_reopens_.clear();
    delayed_requests_.clear();
#ifdef ELOQ_MODULE_ENABLED
    if (drained > 0)
    {
        req_queue_size_.fetch_sub(drained, std::memory_order_relaxed);
    }
#endif
}

bool Shard::AddKvRequest(KvRequest *req)
{
    // The store is stopping: refuse new work instead of enqueuing into a queue
    // whose consumer is exiting. Returning false (without completing the
    // request) matches SendRequest's contract -- ExecSync completes it with
    // NotRunning, ExecAsyn returns false to the caller, and the internal
    // OOM-retry re-enqueue site completes it explicitly. Anything already
    // enqueued before this flag was observed is caught by DrainPendingRequests.
    if (io_mgr_->IsStoreStopping())
    {
        return false;
    }
    bool ret = requests_.enqueue(req);
#ifdef ELOQ_MODULE_ENABLED
    if (ret)
    {
        req_queue_size_.fetch_add(1, std::memory_order_relaxed);
        // New request, notify the external processor directly.
        eloq::EloqModule::NotifyWorker(shard_id_);
    }
#endif
    return ret;
}

void Shard::EnqueueReopenWaiter(KvRequest *req)
{
    CHECK(req->Type() != RequestType::Reopen);
    const TableIdent &tbl_id = req->TableId();
    auto [it, inserted] = pending_reopens_.try_emplace(tbl_id);
    auto &state = it->second;
    state.waiters_.push_back(req);
    if (!inserted)
    {
        return;
    }

    ReopenRequest *reopen_req = state.DriverRequest();
    reopen_req->SetArgs(tbl_id);
    // The driver works for the waiter's data store shard, so it is counted
    // and gated like the waiter.
    reopen_req->SetDsShard(req->DsShard());
    reopen_req->SetTag("");
    reopen_req->SetClean(false);
    reopen_req->SetPendingTime(store_->Options().auto_reopen_pending_time_us);
    // The internal request bypasses ExecAsyn, which is where retry budgets are
    // normally seeded; without this initialization an OutOfMem while reopening
    // for waiters would fail every parked waiter instead of retrying.
    reopen_req->oom_retry_remaining_ = store_->Options().auto_oom_retry_times;
    EnqueueDelayedReopenRequest(reopen_req);
}

void Shard::EnqueueDelayedReopenRequest(ReopenRequest *req)
{
    const TableIdent tbl_id = req->TableId();
    uint64_t delay_us = req->PendingTime();
    if (delay_us == 0)
    {
        auto [it_q, _] = pending_queues_.try_emplace(tbl_id);
        it_q->second.PushFront(req);
        TryStartPendingWrite(tbl_id);
        return;
    }

    const uint64_t execute_at = ReadTimeMicroseconds() + delay_us;
    // Keep tbl_id independent of the request pointer: an external
    // ReopenRequest may replace and destroy this internal driver before the
    // delayed entry is promoted.
    delayed_requests_.push_back({tbl_id, req, execute_at});
    std::push_heap(delayed_requests_.begin(),
                   delayed_requests_.end(),
                   std::greater<DelayedEntry>());
}

void Shard::PromoteReadyDelayedReopenRequests()
{
    const uint64_t now_us = ReadTimeMicroseconds();
    while (!delayed_requests_.empty() &&
           delayed_requests_.front().execute_at_us <= now_us)
    {
        std::pop_heap(delayed_requests_.begin(),
                      delayed_requests_.end(),
                      std::greater<DelayedEntry>());
        DelayedEntry entry = std::move(delayed_requests_.back());
        delayed_requests_.pop_back();

        auto it = pending_reopens_.find(entry.tbl_id);
        if (it == pending_reopens_.end() ||
            !it->second.IsCurrentInternalDriver(entry.request))
        {
            continue;
        }

        // Clear pending time so the normal request path won't re-enqueue.
        entry.request->SetPendingTime(0);
        auto [it_q, _] = pending_queues_.try_emplace(entry.tbl_id);
        it_q->second.PushFront(entry.request);
        // The delayed heap no longer keeps the shard active after this pop, so
        // try to start the ready reopen immediately instead of leaving it only
        // in the per-table pending queue.
        TryStartPendingWrite(entry.tbl_id);
    }
}

void Shard::CompleteReopenWaiters(std::vector<KvRequest *> waiters,
                                  KvError reopen_err)
{
    if (reopen_err != KvError::NoError)
    {
        for (KvRequest *pending_req : waiters)
        {
            pending_req->SetDone(reopen_err);
        }
        return;
    }

    for (KvRequest *pending_req : waiters)
    {
        if (!store_->SendRequest(pending_req))
        {
            pending_req->SetDone(KvError::NotRunning);
        }
    }
}

void Shard::AddPendingCompact(const TableIdent &tbl_id)
{
    // Send CompactRequest from internal.
    assert(!HasPendingCompact(tbl_id));
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    CompactRequest &req = pending_q.compact_req_;
    req.SetTableId(tbl_id);
    // A write task of this partition calls this (WriteTask::CompactIfNeeded)
    // to queue a compaction for the data store shard it runs for. Stamped
    // with that shard, the compaction runs in the shard's term and, like the
    // shard's other requests, is admitted only while the shard is attached
    // and is failed when the shard is detached.
    req.SetDsShard(ThdDsShard());
#ifdef ELOQ_MODULE_ENABLED
    {
        std::lock_guard<bthread::Mutex> lk(req.mutex_);
        req.done_ = false;
    }
#else
    req.done_.store(false, std::memory_order_relaxed);
#endif
    pending_q.PushBack(&req);
}

bool Shard::HasPendingCompact(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
#ifdef ELOQ_MODULE_ENABLED
    std::lock_guard<bthread::Mutex> lk(pending_q.compact_req_.mutex_);
    return !pending_q.compact_req_.done_;
#else
    return !pending_q.compact_req_.done_.load(std::memory_order_relaxed);
#endif
}

bool Shard::TryAddFileGc(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    FileGcRequest &req = pending_q.file_gc_req_;
#ifdef ELOQ_MODULE_ENABLED
    {
        std::lock_guard<bthread::Mutex> lk(req.mutex_);
        if (!req.done_)
        {
            return false;
        }
        req.done_ = false;
    }
#else
    bool expected = true;
    if (!req.done_.compare_exchange_strong(
            expected, false, std::memory_order_relaxed))
    {
        return false;
    }
#endif
    req.SetTableId(tbl_id);
    // A write task of this partition calls this (BatchWriteTask::Drop) to
    // queue a file GC for the data store shard it runs for; stamped with that
    // shard as in AddPendingCompact.
    req.SetDsShard(ThdDsShard());
    pending_q.PushBack(&req);
    return true;
}

void Shard::AddPendingTTL(const TableIdent &tbl_id)
{
    // Send CleanExpiredRequest from internal.
    assert(!HasPendingTTL(tbl_id));
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    CleanExpiredRequest &req = pending_q.expire_req_;
    req.SetTableId(tbl_id);
    // A write task of this partition calls this (WriteTask::TriggerTTL) to
    // queue a TTL cleanup for the data store shard it runs for; stamped with
    // that shard as in AddPendingCompact.
    req.SetDsShard(ThdDsShard());
#ifdef ELOQ_MODULE_ENABLED
    {
        std::lock_guard<bthread::Mutex> lk(req.mutex_);
        req.done_ = false;
    }
#else
    req.done_.store(false, std::memory_order_relaxed);
#endif
    pending_q.PushBack(&req);
}

bool Shard::HasPendingTTL(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
#ifdef ELOQ_MODULE_ENABLED
    std::lock_guard<bthread::Mutex> lk(pending_q.expire_req_.mutex_);
    return !pending_q.expire_req_.done_;
#else
    return !pending_q.expire_req_.done_.load(std::memory_order_relaxed);
#endif
}

bool Shard::TryAddLocalGc(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    LocalGcRequest &req = pending_q.local_gc_req_;
#ifdef ELOQ_MODULE_ENABLED
    {
        std::lock_guard<bthread::Mutex> lk(req.mutex_);
        if (!req.done_)
        {
            return false;
        }
        req.done_ = false;
    }
#else
    bool expected = true;
    if (!req.done_.compare_exchange_strong(
            expected, false, std::memory_order_relaxed))
    {
        return false;
    }
#endif
    req.SetTableId(tbl_id);
    // A reopen task of this partition calls this (ReopenTask::Reopen) to
    // queue a local file GC for the data store shard it runs for; stamped
    // with that shard as in AddPendingCompact.
    req.SetDsShard(ThdDsShard());
    pending_q.PushBack(&req);
    return true;
}

#ifdef ELOQ_MODULE_ENABLED
bool Shard::NeedStop() const
{
    return running_status_.load(std::memory_order_relaxed) ==
           ShardStatus::Stopping;
}
#endif

PageManager *Shard::IndexManager()
{
    return &index_mgr_;
}

AsyncIoManager *Shard::IoManager()
{
    return io_mgr_.get();
}

TaskManager *Shard::TaskMgr()
{
    return &task_mgr_;
}

PagesPool *Shard::PagePool()
{
    return &page_pool_;
}

GlobalRegisteredMemory *Shard::GlobalRegMem()
{
    return global_reg_mem_;
}

void Shard::OnReceivedReq(KvRequest *req)
{
    if (req->Type() == RequestType::AttachShard ||
        req->Type() == RequestType::DetachShard)
    {
        // Internal, from EloqStore::AttachShard or DetachShard: it is for no
        // partition, so it bypasses the pending write queues and the
        // admission gate, which refuses a shard that is being attached or
        // detached.
        (void) ProcessReq(req);
        return;
    }
    if (!AdmitRequest(req))
    {
        return;
    }

    if (req->Reopen())
    {
        req->SetReopen(false);
        if (req->Type() == RequestType::Reopen)
        {
            // A reopen request cannot itself be parked as a waiter behind
            // another reopen; reject it instead of CHECK-aborting in
            // EnqueueReopenWaiter (SetReopen is public API).
            req->SetDone(KvError::InvalidArgs);
            return;
        }
        EnqueueReopenWaiter(req);
        return;
    }

    if (!req->ReadOnly())
    {
        auto *wreq = reinterpret_cast<WriteRequest *>(req);
        auto [it, _] = pending_queues_.try_emplace(req->tbl_id_);
        it->second.PushBack(wreq);
        // This only dispatches writes that have already been dequeued from the
        // shard's request queue. Later read requests still sitting in
        // requests_ are not being bypassed by this per-table pending queue.
        TryStartPendingWrite(req->tbl_id_);
        return;
    }

    (void) ProcessReq(req);
}

void Shard::TryStartPendingWrite(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    if (it == pending_queues_.end())
    {
        return;
    }
    PendingWriteQueue &pending_q = it->second;
    while (!pending_q.running_ && !pending_q.Empty())
    {
        WriteRequest *req = pending_q.PopFront();
        assert(req != nullptr);
        // A request queued for a partition whose data store shard has been
        // detached or has expired since is failed here, and the next one is
        // tried. The embedded internal requests just become done again.
        if (!AdmitRequest(req))
        {
            continue;
        }
        pending_q.running_ = true;
        if (!ProcessReq(req))
        {
            pending_q.running_ = false;
            pending_q.PushFront(req);
        }
        return;
    }
    if (!pending_q.running_ && pending_q.Empty())
    {
        // Every queued request was refused, so no task will finish on this
        // queue and erase it in OnTaskFinished; drop it here.
        pending_queues_.erase(it);
    }
}

bool Shard::AdmitRequest(KvRequest *req)
{
    const DsShardId ds_shard = req->DsShard();
    if (ds_shard == kUnsetDsShard)
    {
        return true;
    }
    KvError err;
    switch (store_->ShardState(ds_shard))
    {
    case EloqStore::DsShardState::Attached:
        return true;
    case EloqStore::DsShardState::Expired:
        err = KvError::ExpiredTerm;
        break;
    default:
        err = KvError::NotRunning;
        break;
    }
    if (req->Type() == RequestType::Reopen)
    {
        // The refused request may be the driver of its partition's pending
        // reopen, the request that parked requests wait on. Then the waiters
        // cannot be served either: take them out, drop the pending-reopen
        // state and fail each of them with the same error.
        //
        // If the driver is the internal one, it is an object embedded in the
        // state just dropped and no longer exists, so return without
        // completing it. An external driver is the caller's object and is
        // completed below like any other refused request. OnTaskFinished
        // treats a finished reopen driver the same way.
        auto it = pending_reopens_.find(req->TableId());
        if (it != pending_reopens_.end() && it->second.DriverRequest() == req)
        {
            const bool internal = it->second.IsCurrentInternalDriver(
                static_cast<ReopenRequest *>(req));
            std::vector<KvRequest *> waiters = std::move(it->second.waiters_);
            pending_reopens_.erase(it);
            CompleteReopenWaiters(std::move(waiters), err);
            if (internal)
            {
                return false;
            }
        }
    }
    req->SetDone(err);
    return false;
}

void Shard::FailPendingRequestsOf(DsShardId ds_shard_id)
{
    // Pending writes, including the embedded internal requests. A partition
    // has one owner at a time, so its queue holds one shard's requests: the
    // front one tells. A queue with a running task is left to that task's
    // OnTaskFinished, whose dispatch refuses the rest.
    std::vector<TableIdent> tables;
    for (auto &[tbl_id, pending_q] : pending_queues_)
    {
        if (!pending_q.running_ && !pending_q.Empty() &&
            pending_q.Front()->DsShard() == ds_shard_id)
        {
            tables.push_back(tbl_id);
        }
    }
    for (const TableIdent &tbl_id : tables)
    {
        // TryStartPendingWrite refuses every queued request (AdmitRequest)
        // and erases the emptied queue.
        TryStartPendingWrite(tbl_id);
    }

    // Reopen states whose driver is not running: fail the waiters and drop the
    // state. A running driver keeps its state until OnTaskFinished, which
    // fails the waiters then (the resubmission is refused by the gate).
    std::unordered_set<TableIdent> reopen_tables;
    for (auto it = pending_reopens_.begin(); it != pending_reopens_.end();)
    {
        if (it->second.DriverRequest()->DsShard() != ds_shard_id)
        {
            ++it;
            continue;
        }
        reopen_tables.insert(it->first);
        auto queue_it = pending_queues_.find(it->first);
        const bool driver_running =
            queue_it != pending_queues_.end() && queue_it->second.running_;
        std::vector<KvRequest *> waiters = std::move(it->second.waiters_);
        CompleteReopenWaiters(std::move(waiters), KvError::NotRunning);
        if (driver_running)
        {
            ++it;
        }
        else
        {
            it = pending_reopens_.erase(it);
        }
    }

    // Delayed internal drivers of those partitions: their states are gone.
    // Matched by table, never through the request pointer: a delayed entry
    // can outlive its driver (an external reopen replaces the embedded one,
    // and the erase above destroys it), as PromoteReadyDelayedReopenRequests
    // allows for too.
    const size_t before = delayed_requests_.size();
    std::erase_if(delayed_requests_,
                  [&](const DelayedEntry &entry)
                  { return reopen_tables.contains(entry.tbl_id); });
    if (delayed_requests_.size() != before)
    {
        std::make_heap(delayed_requests_.begin(),
                       delayed_requests_.end(),
                       std::greater<DelayedEntry>());
    }
}

KvError Shard::EvictDsShard(DsShardId ds_shard_id)
{
    // A task that finished after the first pass may have parked a waiter or
    // queued an internal request: clear those too, now that nothing runs.
    FailPendingRequestsOf(ds_shard_id);

    KvError first_err = KvError::NoError;
    auto note = [&first_err](KvError err)
    {
        if (err != KvError::NoError && first_err == KvError::NoError)
        {
            first_err = err;
        }
    };
    // The partitions this worker shard holds state for: those with a root
    // meta or an open file. The root meta may have been evicted while files
    // stayed open, or the other way round, so both are collected into one set.
    std::unordered_set<TableIdent> partitions;
    index_mgr_.RootMetaManager()->CollectPartitions(partitions);
    const bool has_files = !store_->Options().store_path.empty();
    if (has_files)
    {
        static_cast<IouringMgr *>(io_mgr_.get())->CollectPartitions(partitions);
    }
    // Of those, drop the ones the partition filter gives the detaching shard.
    // Without a filter every partition belongs to every shard; AttachShard
    // then hosts only one shard, so all of them are its own.
    const auto &filter = store_->Options().partition_filter;
    for (const TableIdent &tbl_id : partitions)
    {
        if (filter && !filter(ds_shard_id, tbl_id))
        {
            continue;
        }
        auto reopen_it = pending_reopens_.find(tbl_id);
        if (reopen_it != pending_reopens_.end())
        {
            CompleteReopenWaiters(std::move(reopen_it->second.waiters_),
                                  KvError::NotRunning);
            pending_reopens_.erase(reopen_it);
        }
        auto queue_it = pending_queues_.find(tbl_id);
        if (queue_it != pending_queues_.end())
        {
            CHECK(!queue_it->second.running_ && queue_it->second.Empty())
                << "EvictDsShard with a pending write on " << tbl_id;
            pending_queues_.erase(queue_it);
        }
        if (has_files)
        {
            auto *io_mgr = static_cast<IouringMgr *>(io_mgr_.get());
            // A partition served only from the cloud, or one that was never
            // found, has no local directory: nothing to drop there.
            std::error_code ec;
            const bool has_dir = fs::is_directory(
                tbl_id.StorePath(store_->Options().store_path,
                                 store_->Options().store_path_lut),
                ec);
            if (ec)
            {
                // Not "no such directory", which clears ec: the manifest is
                // left where it is, and the detach reports it.
                LOG(WARNING) << "EvictDsShard: cannot stat the directory of "
                             << tbl_id << ": " << ec.message();
                note(ToKvError(ec.value()));
            }
            if (store_->Mode() == StoreMode::Cloud && has_dir)
            {
                // The manifest may be advanced by the next owner; the sealed
                // data files never change and stay cached.
                note(static_cast<CloudStoreMgr *>(io_mgr)->DropLocalManifest(
                    tbl_id));
            }
            note(io_mgr->CloseTableFiles(tbl_id));
        }
        // No task runs on the partition, so no root meta is pinned.
        index_mgr_.RootMetaManager()->Erase(tbl_id);
    }
    return first_err;
}

uint32_t Shard::ActiveTasksOf(DsShardId ds_shard_id) const
{
    auto it = active_tasks_by_shard_.find(ds_shard_id);
    return it == active_tasks_by_shard_.end() ? 0 : it->second;
}

void Shard::TryDispatchPendingWrites()
{
    if (pending_queues_.empty())
    {
        return;
    }

    std::vector<TableIdent> table_ids;
    table_ids.reserve(pending_queues_.size());
    for (const auto &[tbl_id, _] : pending_queues_)
    {
        table_ids.push_back(tbl_id);
    }
    for (const TableIdent &tbl_id : table_ids)
    {
        TryStartPendingWrite(tbl_id);
    }
}

bool Shard::ProcessReq(KvRequest *req)
{
    switch (req->Type())
    {
    case RequestType::Read:
    {
        ReadTask *task = task_mgr_.GetReadTask();
        auto lbd = [task, req]() -> KvError
        {
            auto read_req = static_cast<ReadRequest *>(req);
            // The variant alternative selects the destination; the type
            // system enforces "at most one container" so no runtime check
            // is needed.
            return std::visit(
                [&](auto &&dest) -> KvError
                {
                    using T = std::decay_t<decltype(dest)>;
                    if constexpr (std::is_same_v<T, std::monostate>)
                    {
                        // No destination -- metadata-only mode. No segment
                        // reads; `value_` receives inline value or
                        // metadata. `large_value_only_` is meaningless
                        // here (there is no value bytes destination).
                        return task->Read(req->TableId(),
                                          read_req->Key(),
                                          read_req->value_,
                                          read_req->ts_,
                                          read_req->expire_ts_,
                                          /*iosb=*/nullptr,
                                          /*extract_metadata=*/true);
                    }
                    else if constexpr (std::is_same_v<T, IoStringBuffer>)
                    {
                        // Legacy zero-copy: dispatch fills the
                        // IoStringBuffer that lives inside the variant. The
                        // caller reads fragments back out via
                        // std::get<IoStringBuffer>(...) after the request
                        // completes. `large_value_only_` skips the metadata
                        // trailer extraction.
                        //
                        // Reject this arm in KV Cache pinned mode: the
                        // fragments would be allocated from EloqStore's
                        // internal GC pool, which is reserved for GC /
                        // compaction. No public API exposes that pool, so
                        // the caller cannot Recycle the fragments and the
                        // pool would leak. Pinned-mode callers must use
                        // overload B (metadata + pinned dst) or overload C
                        // (pinned-only) where the destination is caller-
                        // owned.
                        if (!shard->store_->Options()
                                 .pinned_memory_chunks.empty())
                        {
                            return KvError::InvalidArgs;
                        }
                        return task->Read(req->TableId(),
                                          read_req->Key(),
                                          read_req->value_,
                                          read_req->ts_,
                                          read_req->expire_ts_,
                                          &dest,
                                          !read_req->large_value_only_);
                    }
                    else
                    {
                        // KV Cache pinned destination. Overload C skips
                        // metadata extraction; overload B includes it.
                        if (read_req->large_value_only_)
                        {
                            return task->Read(req->TableId(),
                                              read_req->Key(),
                                              read_req->ts_,
                                              read_req->expire_ts_,
                                              dest.first,
                                              dest.second);
                        }
                        return task->Read(req->TableId(),
                                          read_req->Key(),
                                          read_req->value_,
                                          read_req->ts_,
                                          read_req->expire_ts_,
                                          dest.first,
                                          dest.second);
                    }
                },
                read_req->large_value_dest_);
        };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::Floor:
    {
        ReadTask *task = task_mgr_.GetReadTask();
        auto lbd = [task, req]() -> KvError
        {
            auto floor_req = static_cast<FloorRequest *>(req);
            KvError err = task->Floor(req->TableId(),
                                      floor_req->Key(),
                                      floor_req->floor_key_,
                                      floor_req->value_,
                                      floor_req->ts_,
                                      floor_req->expire_ts_,
                                      &floor_req->large_value_);
            return err;
        };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::Scan:
    {
        ScanTask *task = task_mgr_.GetScanTask();
        auto lbd = [task]() -> KvError { return task->Scan(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::ListObject:
    {
        ListObjectTask *task = task_mgr_.GetListObjectTask();
        auto lbd = [req, task]() -> KvError
        {
            // ListObject needs the cloud object store; the CloudStoreMgr cast
            // below is only valid in cloud mode. In Local/Standby/in-memory
            // modes io_mgr_ is not a CloudStoreMgr, so reject with InvalidArgs
            // instead of performing an out-of-bounds downcast (mirrors the
            // ListStandbyPartition guard).
            if (shard->store_->Mode() != StoreMode::Cloud)
            {
                return KvError::InvalidArgs;
            }
            KvTask *current_task = ThdTask();
            auto list_object_req = static_cast<ListObjectRequest *>(req);
            ObjectStore::ListTask list_task(list_object_req->RemotePath());
            list_task.SetRecursive(list_object_req->Recursive());
            list_task.SetContinuationToken(
                list_object_req->GetContinuationToken());

            list_task.SetKvTask(task);
            auto cloud_mgr = static_cast<CloudStoreMgr *>(shard->io_mgr_.get());
            cloud_mgr->AcquireCloudSlot(task);
            cloud_mgr->GetObjectStore().SubmitTask(&list_task, shard);
            current_task->WaitIo();

            if (list_task.error_ != KvError::NoError)
            {
                LOG(ERROR) << "Failed to list objects, error: "
                           << static_cast<int>(list_task.error_);
                return list_task.error_;
            }

            std::string next_token;
            if (!cloud_mgr->GetObjectStore().ParseListObjectsResponse(
                    list_task.response_data_.view(),
                    list_task.json_data_,
                    list_object_req->GetObjects(),
                    list_object_req->GetDetails(),
                    &next_token))
            {
                LOG(ERROR) << "Failed to parse ListObjects response";
                return KvError::IoFail;
            }

            // Store next token in request
            *list_object_req->GetNextContinuationToken() =
                std::move(next_token);
            return KvError::NoError;
        };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::ListStandbyPartition:
    {
        ListStandbyPartitionTask *task =
            task_mgr_.GetListStandbyPartitionTask();
        auto lbd = [req]() -> KvError
        {
            StandbyService *standby = shard->store_->GetStandbyService();
            if (standby == nullptr)
            {
                return KvError::InvalidArgs;
            }
            auto *list_req = static_cast<ListStandbyPartitionRequest *>(req);
            KvTask *current_task = ThdTask();
            CHECK(current_task != nullptr);
            KvError enqueue_err =
                standby->ListRemotePartitions(list_req->GetPartitions());
            CHECK_KV_ERR(enqueue_err);
            current_task->WaitIo();
            return static_cast<KvError>(current_task->io_res_);
        };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::Reopen:
    {
        auto *reopen_req = static_cast<ReopenRequest *>(req);
        CHECK_EQ(reopen_req->PendingTime(), 0);
        ReopenTask *task = task_mgr_.GetReopenTask(req->TableId());
        auto [it, inserted] = pending_reopens_.try_emplace(req->TableId());
        if (inserted || !it->second.IsCurrentInternalDriver(reopen_req))
        {
            // An external ReopenRequest becomes the pending-reopen driver even
            // when there are no waiters yet. Waiters that hit ResourceMissing
            // while this task yields will then attach to this running reopen
            // instead of creating a delayed internal driver. If this request is
            // the embedded internal driver, the state already exists and the
            // pointer comparison above prevents replacing it.
            it->second.SetExternalDriver(reopen_req);
        }
        auto lbd = [task, req]() -> KvError
        { return task->Reopen(req->TableId()); };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::BatchWrite:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto write_req = static_cast<BatchWriteRequest *>(req);
        task->Reset(req->TableId());
        task->SetBatch(write_req->batch_);
        // An empty batch runs as a no-op Apply and completes normally.
        // Skipping StartTask here instead would leave running_ set and the
        // request never completed, wedging the partition's write queue.
        StartTask(task, req, [task]() { return task->Apply(); });
        return true;
    }
    case RequestType::Truncate:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task, req]() -> KvError
        {
            auto trunc_req = static_cast<TruncateRequest *>(req);
            return task->Truncate(trunc_req->position_);
        };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::Drop:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task]() -> KvError { return task->Drop(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::DropTable:
    {
        LOG(ERROR) << "DropTable request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        return true;
    }
    case RequestType::Archive:
    {
        auto *archive_req = static_cast<ArchiveRequest *>(req);
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        const std::string tag = archive_req->Tag();
        const uint64_t term = archive_req->Term();
        const ArchiveRequest::Action action = archive_req->GetAction();
        auto lbd = [task, term, tag, action]() -> KvError
        {
            if (action == ArchiveRequest::Action::Create)
            {
                return task->CreateArchive(tag);
            }
            return task->DeleteArchive(term, tag);
        };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::GlobalArchive:
    {
        LOG(ERROR) << "GlobalArchive request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        return true;
    }
    case RequestType::GlobalReopen:
    {
        LOG(ERROR) << "GlobalReopen request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        return true;
    }
    case RequestType::GlobalListArchiveTags:
    {
        LOG(ERROR)
            << "GlobalListArchiveTags request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        return true;
    }
    case RequestType::Compact:
    {
        // Compaction is a write task; its concurrency is bounded by the shared
        // max_write_concurrency limit inside GetBackgroundWrite (returns null
        // when at the limit, leaving the request pending for retry).
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task]() -> KvError { return task->Compact(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::FileGc:
    {
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task]() -> KvError { return task->RunFileGc(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::LocalGc:
    {
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task]() -> KvError { return task->RunLocalFileGc(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::CleanExpired:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task]() -> KvError { return task->CleanExpiredKeys(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::CreateBranch:
    {
        auto *branch_req = static_cast<CreateBranchRequest *>(req);
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task, branch_req]() -> KvError
        { return task->CreateBranch(branch_req->branch_name); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::DeleteBranch:
    {
        auto *branch_req = static_cast<DeleteBranchRequest *>(req);
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task, branch_req]() -> KvError
        { return task->DeleteBranch(branch_req->branch_name); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::GlobalCreateBranch:
    {
        LOG(ERROR) << "GlobalCreateBranch request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        return true;
    }
    case RequestType::AttachShard:
    {
        AttachShardTask *task = task_mgr_.GetAttachShardTask();
        // StartTask gives the task the request's shard and that shard's term.
        auto lbd = [task]() -> KvError { return task->Attach(); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::DetachShard:
    {
        DetachShardTask *task = task_mgr_.GetDetachShardTask();
        const DsShardId ds_shard = req->DsShard();
        auto lbd = [task, ds_shard]() -> KvError
        { return task->Detach(ds_shard); };
        StartTask(task, req, lbd);
        return true;
    }
    }
    return true;
}

bool Shard::ExecuteReadyTasks()
{
    if (store_->Mode() == StoreMode::Cloud)
    {
        auto *cloud_mgr = static_cast<CloudStoreMgr *>(io_mgr_.get());
        cloud_mgr->ProcessCloudReadyTasks(this);
    }
    if (StandbyService *standby = store_->GetStandbyService();
        standby != nullptr)
    {
        standby->ProcessReadyTasks(shard_id_);
    }
    bool busy = ready_tasks_.Size() > 0;
    while (ready_tasks_.Size() > 0)
    {
        KvTask *task = ready_tasks_.Peek();
        ready_tasks_.Dequeue();
        assert(task->status_ == TaskStatus::Ongoing);
        SwitchToTask(task);
        task->coro_ = task->coro_.resume();
        if (task->status_ == TaskStatus::Finished)
        {
            OnTaskFinished(task);
        }
        if (DurationMicroseconds(ts_) >= FLAGS_max_processing_time_microseconds)
        {
            // run at least one low-priority task to avoid starvation.
            break;
        }
    }

    while (low_priority_ready_tasks_.Size() > 0)
    {
        KvTask *task = low_priority_ready_tasks_.Peek();
        low_priority_ready_tasks_.Dequeue();
        task->status_ = TaskStatus::Ongoing;
        SwitchToTask(task);
        task->coro_ = task->coro_.resume();
        if (task->status_ == TaskStatus::Finished)
        {
            OnTaskFinished(task);
        }
        if (DurationMicroseconds(ts_) >= FLAGS_max_processing_time_microseconds)
        {
            goto finish;
        }
    }

finish:
    LeaveTask();
    return busy;
}

void Shard::RetryOomRequest(KvRequest *req)
{
    // The aborted task released its pins; re-enqueue the request at the tail of
    // this shard's queue so other in-flight tasks get a chance to release their
    // pins before the next attempt.
    req->err_ = KvError::NoError;
#ifdef ELOQ_MODULE_ENABLED
    {
        std::lock_guard<bthread::Mutex> lk(req->mutex_);
        req->done_ = false;
    }
#else
    req->done_.store(false, std::memory_order_relaxed);
#endif
    // AddKvRequest refuses new work once the store is stopping; complete the
    // retried request with NotRunning instead of dropping it.
    if (!AddKvRequest(req))
    {
        req->SetDone(KvError::NotRunning);
    }
}

void Shard::OnTaskFinished(KvTask *task)
{
    KvRequest *req = task->req_;
    const KvError err = task->result_err_;
    const TaskType task_type = task->Type();
#ifdef ELOQSTORE_WITH_TXSERVICE
    const RequestType request_type = req->Type();
    const metrics::TimePoint request_start = finished_request_start_;
#endif
    bool oom_retry = false;
    bool auto_reopen = false;
    bool request_completed = true;

    if (__builtin_expect(err == KvError::OutOfMem, 0))
    {
        if (req->oom_retry_remaining_ > 0)
        {
            --req->oom_retry_remaining_;
            oom_retry = true;
            request_completed = false;
            LOG(WARNING) << "Task hit out of memory; retrying ("
                         << static_cast<int>(req->oom_retry_remaining_)
                         << " attempts left)";
        }
        else
        {
            LOG(ERROR) << "Task is aborted due to out of memory";
        }
    }
    else if (__builtin_expect(err == KvError::ResourceMissing, 0))
    {
        const StoreMode mode = store_->Mode();
        if (req->AutoReopenRetry() && req->reopen_retry_remaining_ > 0 &&
            (mode == StoreMode::Cloud || mode == StoreMode::StandbyReplica))
        {
            CHECK(req->TableId().IsValid());
            --req->reopen_retry_remaining_;
            auto_reopen = true;
            request_completed = false;
        }
    }

    const DsShardId req_shard = task->ds_shard_id_;
    if (req_shard != kUnsetDsShard && task_type != TaskType::DetachShard)
    {
        auto active_it = active_tasks_by_shard_.find(req_shard);
        CHECK(active_it != active_tasks_by_shard_.end() &&
              active_it->second > 0)
            << "task finished for data store shard " << req_shard
            << " that has no running task";
        if (--active_it->second == 0)
        {
            active_tasks_by_shard_.erase(active_it);
            auto waiting_it = waiting_detaches_.find(req_shard);
            if (waiting_it != waiting_detaches_.end())
            {
                KvTask *detach_task = waiting_it->second;
                waiting_detaches_.erase(waiting_it);
                detach_task->Resume();
            }
        }
        if (err == KvError::ExpiredTerm)
        {
            // A newer term owns one of the shard's partitions: the whole
            // shard is stale (the term file is per shard).
            store_->MarkShardExpired(req_shard);
        }
    }

    task->req_ = nullptr;
    task->result_err_ = KvError::NoError;

    KvRequest *done_req = request_completed ? req : nullptr;
    bool dispatch_pending_writes = false;
    bool complete_reopen_waiters = false;
    std::vector<KvRequest *> reopen_waiters;
    if (!task->ReadOnly())
    {
        auto wtask = reinterpret_cast<WriteTask *>(task);
        auto pending_it = pending_queues_.find(wtask->TableId());
        assert(pending_it != pending_queues_.end());
        PendingWriteQueue &pending_q = pending_it->second;
        if (__builtin_expect(task_type == TaskType::Reopen && !oom_retry, 0))
        {
            auto reopen_it = pending_reopens_.find(wtask->TableId());
            if (reopen_it != pending_reopens_.end())
            {
                if (reopen_it->second.IsCurrentInternalDriver(
                        static_cast<ReopenRequest *>(req)))
                {
                    // The embedded internal ReopenRequest is destroyed when
                    // this state is erased; no external caller is waiting on
                    // that request object, so do not SetDone it afterward.
                    done_req = nullptr;
                }
                reopen_waiters = std::move(reopen_it->second.waiters_);
                complete_reopen_waiters = true;
                // Drop the completed reopen state before re-enqueueing
                // waiters; a waiter may immediately re-enter this shard and
                // must not attach to the old state.
                pending_reopens_.erase(reopen_it);
            }
        }
        pending_q.running_ = false;
        if (pending_q.Empty())
        {
            // The internal compact / file-gc / local-gc / clean-expired
            // requests are embedded in this queue; erasing it destroys them.
            // They have no external waiter or callback, so drop done_req when
            // it points at one of them to avoid SetDone()-ing a freed request
            // afterwards (mirrors the embedded internal reopen-driver handling
            // above).
            bool is_embedded = (req == &pending_q.compact_req_ ||
                                req == &pending_q.file_gc_req_ ||
                                req == &pending_q.local_gc_req_ ||
                                req == &pending_q.expire_req_);
            if (is_embedded)
            {
                done_req = nullptr;
            }
            // Do not erase the queue if an embedded request is being retried
            // (e.g. OOM), as erasing it would destroy the request object.
            if (!is_embedded || request_completed)
            {
                // No more write requests, remove the pending queue.
                pending_queues_.erase(pending_it);
            }
        }
        dispatch_pending_writes = true;
    }

    task_mgr_.FreeTask(task);

    if (__builtin_expect(oom_retry, 0))
    {
        RetryOomRequest(req);
        return;
    }

    if (__builtin_expect(auto_reopen, 0))
    {
        EnqueueReopenWaiter(req);
        return;
    }

    if (done_req != nullptr)
    {
        done_req->SetDone(err);
        // SetDone can invoke the user callback or wake a waiter that destroys
        // the request. Everything below uses only values captured above.
        done_req = nullptr;
    }

#ifdef ELOQSTORE_WITH_TXSERVICE
    if (request_completed && store_->EnableMetrics())
    {
        metrics::Meter *meter = store_->GetMetricsMeter(shard_id_);
        assert(meter != nullptr);
        const char *request_type_str =
            eloqstore::RequestTypeToString(request_type);
        meter->CollectDuration(metrics::NAME_ELOQSTORE_REQUEST_LATENCY,
                               request_start,
                               request_type_str);
        meter->Collect(
            metrics::NAME_ELOQSTORE_REQUESTS_COMPLETED, 1.0, request_type_str);
    }
#endif

    if (complete_reopen_waiters)
    {
        CompleteReopenWaiters(std::move(reopen_waiters), err);
    }

    if (dispatch_pending_writes)
    {
        TryDispatchPendingWrites();
    }
}

#ifdef ELOQ_MODULE_ENABLED
void Shard::WorkOneRound()
{
    const bool stop_requested =
        running_status_.load(std::memory_order_relaxed) ==
        ShardStatus::Stopping;

    ts_ = ReadTimeMicroseconds();
#ifdef ELOQSTORE_WITH_TXSERVICE
    // Metrics collection: start timing the round
    metrics::TimePoint round_start{};
#endif

    if (__builtin_expect(
            !io_mgr_and_page_pool_inited_.load(std::memory_order_acquire),
            false))
    {
        InitIoMgrAndPagePool();
    }

    KvRequest *reqs[128];
    size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));

    bool is_idle_round = nreqs == 0 && task_mgr_.NumActive() == 0 &&
                         io_mgr_->IsIdle() && delayed_requests_.empty();
    if (is_idle_round)
    {
        if (stop_requested)
        {
            DrainPendingRequests();
            task_mgr_.Shutdown();
            index_mgr_.Shutdown();
            io_mgr_->Stop();
            running_status_.store(ShardStatus::Stopped,
                                  std::memory_order_release);
            return;
        }
        // No request and no active task and no active io.
        if (io_mgr_->NeedPrewarm())
        {
            io_mgr_->RunPrewarm();
        }
        else
        {
            return;
        }
    }
    else
    {
#ifdef ELOQSTORE_WITH_TXSERVICE
        // Metrics collection: start timing the round
        if (store_->EnableMetrics())
        {
            round_start = metrics::Clock::now();
        }
#endif
    }

    for (size_t i = 0; i < nreqs; ++i)
    {
        OnReceivedReq(reqs[i]);
    }

    req_queue_size_.fetch_sub(nreqs, std::memory_order_relaxed);

    io_mgr_->Submit();

    io_mgr_->PollComplete();
    PromoteReadyDelayedReopenRequests();
    if (DurationMicroseconds(ts_) < FLAGS_max_processing_time_microseconds)
    {
        ExecuteReadyTasks();
    }
#ifdef ELOQSTORE_WITH_TXSERVICE
    // Metrics collection: end of round
    if (store_->EnableMetrics() && !is_idle_round)
    {
        metrics::Meter *meter = store_->GetMetricsMeter(shard_id_);
        meter->CollectDuration(metrics::NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION,
                               round_start);
        meter->Collect(metrics::NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS,
                       static_cast<double>(task_mgr_.NumActive()));

        // Increment round counter for frequency-controlled metric collection
        work_one_round_count_++;
        bool should_collect_gauges =
            (work_one_round_count_ %
             metrics::ELOQSTORE_GAUGE_COLLECTION_INTERVAL) == 0;

        // Collect used/count metrics (frequency-controlled)
        // Note: limit metrics are collected once at initialization in Start()
        if (should_collect_gauges)
        {
            // Collect index buffer pool used
            size_t index_used = index_mgr_.GetBufferPoolUsed();
            meter->Collect(metrics::NAME_ELOQSTORE_INDEX_BUFFER_POOL_USED,
                           static_cast<double>(index_used));

            // Collect open file count
            size_t open_file_count = io_mgr_->GetOpenFileCount();
            meter->Collect(metrics::NAME_ELOQSTORE_OPEN_FILE_COUNT,
                           static_cast<double>(open_file_count));

            // Collect local space used
            size_t local_space_used = io_mgr_->GetLocalSpaceUsed();
            meter->Collect(metrics::NAME_ELOQSTORE_LOCAL_SPACE_USED,
                           static_cast<double>(local_space_used));
        }
    }
#endif
}
#endif

void Shard::PendingWriteQueue::PushBack(WriteRequest *req)
{
    req->next_ = nullptr;
    if (tail_ == nullptr)
    {
        assert(head_ == nullptr);
        head_ = tail_ = req;
    }
    else
    {
        assert(head_ != nullptr);
        req->next_ = nullptr;
        tail_->next_ = req;
        tail_ = req;
    }
}

void Shard::PendingWriteQueue::PushFront(WriteRequest *req)
{
    if (head_ == nullptr)
    {
        req->next_ = nullptr;
        head_ = tail_ = req;
        return;
    }
    req->next_ = head_;
    head_ = req;
}

WriteRequest *Shard::PendingWriteQueue::Front()
{
    return head_;
}

WriteRequest *Shard::PendingWriteQueue::PopFront()
{
    WriteRequest *req = head_;
    if (req != nullptr)
    {
        head_ = req->next_;
        if (head_ == nullptr)
        {
            tail_ = nullptr;
        }
        req->next_ = nullptr;  // Clear next pointer for safety.
    }
    return req;
}

bool Shard::PendingWriteQueue::Empty() const
{
    return head_ == nullptr;
}

bool Shard::HasPendingRequests() const
{
    return requests_.size_approx() > 0;
}

/** @brief
 * Measure TSC frequency by sleeping for 1ms and measuring cycles.
 * Retries until stable (within 1% difference) or up to 16ms total.
 * Should be called once during data substrate initialization.
 * This function is thread-safe and will only execute once.
 */
void Shard::InitializeTscFrequency()
{
#if defined(__x86_64__) || defined(_M_X64)
    std::call_once(
        tsc_frequency_initialized_,
        []()
        {
            constexpr uint64_t SLEEP_MICROSECONDS = 1000;       // 1ms
            constexpr uint64_t MAX_TOTAL_MICROSECONDS = 16000;  // 16ms max
            constexpr double STABILITY_THRESHOLD =
                0.01;  // 1% difference for stability

            uint64_t prev_freq = 0;
            uint64_t total_slept = 0;
            int stable_count = 0;
            constexpr int REQUIRED_STABLE_COUNT =
                2;  // Need 2 consecutive stable measurements

            while (total_slept < MAX_TOTAL_MICROSECONDS)
            {
                uint64_t start_cycles = __rdtsc();
                std::this_thread::sleep_for(
                    std::chrono::microseconds(SLEEP_MICROSECONDS));
                uint64_t end_cycles = __rdtsc();
                uint64_t elapsed_cycles = end_cycles - start_cycles;
                uint64_t freq = elapsed_cycles /
                                SLEEP_MICROSECONDS;  // cycles per microsecond

                total_slept += SLEEP_MICROSECONDS;

                // Check if frequency is stable (within 1% of previous
                // measurement)
                if (prev_freq > 0)
                {
                    double diff_ratio =
                        (freq > prev_freq)
                            ? static_cast<double>(freq - prev_freq) / prev_freq
                            : static_cast<double>(prev_freq - freq) / prev_freq;
                    if (diff_ratio <= STABILITY_THRESHOLD)
                    {
                        stable_count++;
                        if (stable_count >= REQUIRED_STABLE_COUNT)
                        {
                            // Frequency is stable, use the average
                            tsc_cycles_per_microsecond_.store(
                                (prev_freq + freq) / 2,
                                std::memory_order_release);
                            return;
                        }
                    }
                    else
                    {
                        stable_count = 0;  // Reset stability counter
                    }
                }

                prev_freq = freq;
            }

            // If we couldn't get stable measurement, use the last measured
            // value
            if (prev_freq > 0)
            {
                tsc_cycles_per_microsecond_.store(prev_freq,
                                                  std::memory_order_release);
            }
            else
            {
                // Fallback to approximate value if measurement failed
                tsc_cycles_per_microsecond_.store(2000,
                                                  std::memory_order_release);
            }
        });  // End of lambda passed to std::call_once
#elif defined(__aarch64__)
    std::call_once(tsc_frequency_initialized_,
                   []()
                   {
                       uint64_t freq_hz;
                       __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(freq_hz));
                       tsc_cycles_per_microsecond_.store(
                           freq_hz / 1000000, std::memory_order_release);
                   });
#endif
}

uint64_t Shard::ReadTimeMicroseconds()
{
#if defined(__x86_64__) || defined(_M_X64)
    uint64_t cycles_per_us =
        tsc_cycles_per_microsecond_.load(std::memory_order_relaxed);
    assert(cycles_per_us != 0);
    // Read TSC (Time Stamp Counter) - returns CPU cycles
    uint64_t cycles = __rdtsc();
    return cycles / cycles_per_us;
#elif defined(__aarch64__)
    // Ensure ARM timer frequency is initialized (thread-safe, only initializes
    // once)
    uint64_t cycles_per_us =
        tsc_cycles_per_microsecond_.load(std::memory_order_relaxed);
    assert(cycles_per_us != 0);
    // Read ARM virtual counter - returns timer ticks
    uint64_t ticks;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(ticks));
    return ticks / cycles_per_us;
#else
    // Fallback to std::chrono (slower but portable and precise)
    using namespace std::chrono;
    auto now = steady_clock::now();
    return duration_cast<microseconds>(now.time_since_epoch()).count();
#endif
}

uint64_t Shard::DurationMicroseconds(uint64_t start_us)
{
    // Check elapsed time (returns microseconds directly)
    uint64_t end_us = ReadTimeMicroseconds();
    // Handle potential wraparound (unlikely but safe)
    if (end_us >= start_us)
    {
        return end_us - start_us;
    }
    else
    {
        // Wraparound detected, use max value to break loop
        return FLAGS_max_processing_time_microseconds;
    }
}

}  // namespace eloqstore
