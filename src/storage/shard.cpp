#include "storage/shard.h"

#include <glog/logging.h>

#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>  // For __rdtsc()
#endif

#include "async_io_manager.h"
#include "tasks/list_object_task.h"
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

Shard::Shard(const EloqStore *store, size_t shard_id, uint32_t fd_limit)
    : store_(store),
      shard_id_(shard_id),
      page_pool_(&store->options_),
      io_mgr_(AsyncIoManager::Instance(store, fd_limit)),
      index_mgr_(io_mgr_.get()),
      task_mgr_(&store->options_),
      stack_allocator_(store->options_.coroutine_stack_size)
{
    const auto &opts = store_->options_;
    oss_enabled_ = !opts.store_path.empty() && !opts.cloud_store_path.empty();
}

KvError Shard::Init()
{
    // Inject process term into IO manager before any file operations.
    // Only CloudStoreMgr needs term support; IouringMgr always uses term=0.
    if (io_mgr_ != nullptr)
    {
        uint64_t term = store_ != nullptr ? store_->Term() : 0;
        if (auto *cloud_mgr = dynamic_cast<CloudStoreMgr *>(io_mgr_.get());
            cloud_mgr != nullptr)
        {
            cloud_mgr->SetProcessTerm(term);
        }
    }
    InitializeTscFrequency();
    KvError res = io_mgr_->Init(this);
    return res;
}

void Shard::InitIoMgrAndPagePool()
{
    io_mgr_->InitBackgroundJob();
    io_mgr_and_page_pool_inited_ = true;
}

void Shard::WorkLoop()
{
    shard = this;
    io_mgr_->Start();
    InitIoMgrAndPagePool();

    // Get new requests from the queue, only blocked when there are no requests
    // and no active tasks.
    // This allows the thread to exit gracefully when the store is stopped.
    std::array<KvRequest *, 128> reqs;
    auto dequeue_requests = [this, &reqs]() -> int
    {
        size_t nreqs = requests_.try_dequeue_bulk(reqs.data(), reqs.size());
        // Idle state, wait for new requests or exit.
        while (nreqs == 0 && task_mgr_.NumActive() == 0 && io_mgr_->IsIdle())
        {
            if (store_->IsStopped())
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

    io_mgr_->Stop();
}

void Shard::Start()
{
#ifdef ELOQ_MODULE_ENABLED
    shard = this;
    io_mgr_->Start();
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

bool Shard::AddKvRequest(KvRequest *req)
{
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

void Shard::AddPendingCompact(const TableIdent &tbl_id)
{
    // Send CompactRequest from internal.
    assert(!HasPendingCompact(tbl_id));
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    CompactRequest &req = pending_q.compact_req_;
    req.SetTableId(tbl_id);
#ifdef ELOQSTORE_WITH_TXSERVICE
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
#ifdef ELOQSTORE_WITH_TXSERVICE
    std::lock_guard<bthread::Mutex> lk(pending_q.compact_req_.mutex_);
    return !pending_q.compact_req_.done_;
#else
    return !pending_q.compact_req_.done_.load(std::memory_order_relaxed);
#endif
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
#ifdef ELOQSTORE_WITH_TXSERVICE
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
#ifdef ELOQSTORE_WITH_TXSERVICE
    std::lock_guard<bthread::Mutex> lk(pending_q.expire_req_.mutex_);
    return !pending_q.expire_req_.done_;
#else
    return !pending_q.expire_req_.done_.load(std::memory_order_relaxed);
#endif
}

IndexPageManager *Shard::IndexManager()
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

void Shard::OnReceivedReq(KvRequest *req)
{
    if (!req->ReadOnly())
    {
        auto *wreq = reinterpret_cast<WriteRequest *>(req);
        auto [it, inserted] = pending_queues_.try_emplace(req->tbl_id_);
        if (inserted)
        {
            ++running_writing_tasks_;
        }
        it->second.PushBack(wreq);
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
    if (pending_q.running_ || pending_q.Empty())
    {
        return;
    }

    WriteRequest *req = pending_q.PopFront();
    assert(req != nullptr);
    pending_q.running_ = true;
    if (!ProcessReq(req))
    {
        pending_q.running_ = false;
        pending_q.PushFront(req);
    }
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
            KvError err = task->Read(req->TableId(),
                                     read_req->Key(),
                                     read_req->value_,
                                     read_req->ts_,
                                     read_req->expire_ts_);
            return err;
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
                                      floor_req->expire_ts_);
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
    case RequestType::BatchWrite:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task, req]() -> KvError
        {
            auto write_req = static_cast<BatchWriteRequest *>(req);
            if (write_req->batch_.empty())
            {
                return KvError::NoError;
            }
            if (!task->SetBatch(write_req->batch_))
            {
                return KvError::InvalidArgs;
            }
            return task->Apply();
        };
        StartTask(task, req, lbd);
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
        uint64_t snapshot_ts = archive_req->GetSnapshotTimestamp();
        auto lbd = [task, snapshot_ts]() -> KvError
        { return task->CreateArchive(snapshot_ts); };
        StartTask(task, req, lbd);
        return true;
    }
    case RequestType::GlobalArchive:
    {
        LOG(ERROR) << "GlobalArchive request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        return true;
    }
    case RequestType::Compact:
    {
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        if (task == nullptr)
        {
            return false;
        }
        auto lbd = [task]() -> KvError { return task->CompactDataFile(); };
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
    }
    return true;
}

bool Shard::ExecuteReadyTasks()
{
    if (oss_enabled_)
    {
        auto *cloud_mgr = reinterpret_cast<CloudStoreMgr *>(io_mgr_.get());
        cloud_mgr->ProcessCloudReadyTasks(this);
    }
    bool busy = ready_tasks_.Size() > 0;
    while (ready_tasks_.Size() > 0)
    {
        KvTask *task = ready_tasks_.Peek();
        ready_tasks_.Dequeue();
        assert(task->status_ == TaskStatus::Ongoing);
        running_ = task;
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
        running_ = task;
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
    running_ = nullptr;
    return busy;
}

void Shard::OnTaskFinished(KvTask *task)
{
    if (!task->ReadOnly())
    {
        auto wtask = reinterpret_cast<WriteTask *>(task);
        auto it = pending_queues_.find(wtask->TableId());
        assert(it != pending_queues_.end());
        PendingWriteQueue &pending_q = it->second;
        pending_q.running_ = false;
        task_mgr_.FreeTask(task);
        if (pending_q.Empty())
        {
            // No more write requests, remove the pending queue.
            pending_queues_.erase(it);
            --running_writing_tasks_;
        }
        TryDispatchPendingWrites();
    }
    else
    {
        task_mgr_.FreeTask(task);
    }
}

#ifdef ELOQ_MODULE_ENABLED
void Shard::WorkOneRound()
{
    ts_ = ReadTimeMicroseconds();
#ifdef ELOQSTORE_WITH_TXSERVICE
    // Metrics collection: start timing the round
    metrics::TimePoint round_start{};
#endif

    if (__builtin_expect(!io_mgr_and_page_pool_inited_, false))
    {
        InitIoMgrAndPagePool();
    }
    KvRequest *reqs[128];
    size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));

    bool is_idle_round =
        nreqs == 0 && task_mgr_.NumActive() == 0 && io_mgr_->IsIdle();
    if (is_idle_round)
    {
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
