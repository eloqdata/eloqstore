#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <cstdio>
#include <ctime>
#include <memory>
#include <unordered_map>
#include <utility>  // NOLINT(build/include_order)
#include <vector>

#include "circular_queue.h"
#include "eloq_store.h"
#include "storage/page_mapper.h"
#include "tasks/task_manager.h"

#ifdef ELOQSTORE_WITH_TXSERVICE
#include "eloqstore_metrics.h"
#endif

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "concurrentqueue/concurrentqueue.h"

namespace eloqstore
{
#ifdef ELOQ_MODULE_ENABLED
class EloqStoreModule;
#endif

class CloudStoreMgr;
class GlobalRegisteredMemory;

class Shard
{
public:
    Shard(EloqStore *store, size_t shard_id, uint32_t fd_limit);
    KvError Init();
    void Start();
    void Stop();
    bool AddKvRequest(KvRequest *req);

    void AddPendingCompact(const TableIdent &tbl_id);
    bool HasPendingCompact(const TableIdent &tbl_id);
    void AddPendingTTL(const TableIdent &tbl_id);
    bool HasPendingTTL(const TableIdent &tbl_id);
    void AddPendingLocalGc(const TableIdent &tbl_id);
    bool HasPendingLocalGc(const TableIdent &tbl_id);
#ifdef ELOQ_MODULE_ENABLED
    enum class ShardStatus : uint8_t
    {
        Running = 0,
        Stopping = 1,
        Stopped = 2,
    };

    bool NeedStop() const;
#endif

    bool HasPendingRequests() const;

    AsyncIoManager *IoManager();
    IndexPageManager *IndexManager();
    TaskManager *TaskMgr();
    PagesPool *PagePool();
    GlobalRegisteredMemory *GlobalRegMem();

    std::atomic<bool> io_mgr_and_page_pool_inited_{false};

#ifdef ELOQ_MODULE_ENABLED
    std::atomic<ShardStatus> running_status_{ShardStatus::Running};
#endif
    EloqStore *store_;
    const size_t shard_id_{0};
    boost::context::continuation main_;
    uint64_t ts_{};
    KvTask *running_{};
    CircularQueue<KvTask *> ready_tasks_;
    CircularQueue<KvTask *> low_priority_ready_tasks_;

private:
    void WorkLoop();
    void InitIoMgrAndPagePool();
    bool ExecuteReadyTasks();
    void OnTaskFinished(KvTask *task);
    void OnReceivedReq(KvRequest *req);
    bool ProcessReq(KvRequest *req);
    void EnqueueForAutoReopen(KvRequest *req);
    void TryStartPendingWrite(const TableIdent &tbl_id);
    void TryDispatchPendingWrites();

#ifdef ELOQ_MODULE_ENABLED
    void WorkOneRound();
    bool IsIdle() const
    {
        // No request in the queue and no active task(coroutine) and no active
        // io.
        return req_queue_size_.load(std::memory_order_relaxed) == 0 &&
               task_mgr_.NumActive() == 0 && io_mgr_->IsIdle() &&
               !io_mgr_->NeedPrewarm();
    }
    void BindExtThd()
    {
        shard = this;
    }
#endif

    void InitializeTscFrequency();

    uint64_t ReadTimeMicroseconds();

    uint64_t DurationMicroseconds(uint64_t start_us);

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        task->needs_auto_reopen_ = false;
        running_ = task;
        task->coro_ = boost::context::callcc(
            std::allocator_arg,
            stack_allocator_,
            [lbd, this](continuation &&sink)
            {
#ifdef ELOQSTORE_WITH_TXSERVICE
                // Metrics collection: record start time for latency measurement
                metrics::TimePoint request_start;
                metrics::Meter *meter = nullptr;
                if (this->store_->EnableMetrics())
                {
                    request_start = metrics::Clock::now();
                    meter = this->store_->GetMetricsMeter(shard_id_);
                    assert(meter != nullptr);
                }
#endif
                shard->main_ = std::move(sink);
                KvError err = lbd();
                KvTask *task = ThdTask();
                if (err != KvError::NoError)
                {
                    task->Abort();
                    if (err == KvError::OutOfMem)
                    {
                        LOG(ERROR) << "Task is aborted due to out of memory";
                    }
                }

#ifdef ELOQSTORE_WITH_TXSERVICE
                // Save request type before SetDone
                RequestType request_type = task->req_->Type();
#endif
                KvRequest *req = task->req_;
                bool request_completed = true;
                if (__builtin_expect(err != KvError::ResourceMissing, 1))
                {
                    req->SetDone(err);
                }
                else
                {
                    const StoreMode mode = shard->store_->Mode();
                    if (req->AutoReopenRetry() &&
                        req->reopen_retry_remaining_ > 0 &&
                        (mode == StoreMode::Cloud ||
                         mode == StoreMode::StandbyReplica))
                    {
                        CHECK(req->TableId().IsValid());
                        --req->reopen_retry_remaining_;
                        task->needs_auto_reopen_ = true;
                    }
                    request_completed = !task->needs_auto_reopen_;
                    if (request_completed)
                    {
                        req->SetDone(err);
                    }
                }
                task->status_ = TaskStatus::Finished;

#ifdef ELOQSTORE_WITH_TXSERVICE
                // Collect latency metric when request completes
                if (request_completed && this->store_->EnableMetrics())
                {
                    const char *request_type_str =
                        RequestTypeToString(request_type);
                    meter->CollectDuration(
                        metrics::NAME_ELOQSTORE_REQUEST_LATENCY,
                        request_start,
                        request_type_str);
                    // Increment request completion counter
                    meter->Collect(metrics::NAME_ELOQSTORE_REQUESTS_COMPLETED,
                                   1.0,
                                   request_type_str);
                }
#endif
                return std::move(shard->main_);
            });
        running_ = nullptr;
        if (task->status_ == TaskStatus::Finished)
        {
            OnTaskFinished(task);
        }
    }

    static const char *RequestTypeToString(RequestType type)
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
        case RequestType::Reopen:
            return "reopen";
        case RequestType::BatchWrite:
            return "batch_write";
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
        default:
            return "unknown";
        }
    }

    moodycamel::BlockingConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    PagesPool page_pool_;
    // These members depend on thread-local `shard`; ensure their Shutdown/
    // cleanup routines are invoked on the shard thread before destruction.
    TaskManager task_mgr_;
#ifndef NDEBUG
    boost::context::protected_fixedsize_stack stack_allocator_;
#else
    boost::context::pooled_fixedsize_stack stack_allocator_;
#endif
    std::unique_ptr<AsyncIoManager> io_mgr_;
    GlobalRegisteredMemory *global_reg_mem_{nullptr};
    IndexPageManager index_mgr_;

    class PendingWriteQueue
    {
    public:
        void PushBack(WriteRequest *req);
        void PushFront(WriteRequest *req);
        WriteRequest *Front();
        WriteRequest *PopFront();
        bool Empty() const;

        // Requests from internal
        CompactRequest compact_req_;
        LocalGcRequest local_gc_req_;
        CleanExpiredRequest expire_req_;
        bool running_{false};

    private:
        WriteRequest *head_{nullptr};
        WriteRequest *tail_{nullptr};
    };
    std::unordered_map<TableIdent, PendingWriteQueue> pending_queues_;

    struct PendingReopenState
    {
        bool inflight{false};
        std::vector<KvRequest *> waiters;
        std::unique_ptr<ReopenRequest> request;
    };
    std::unordered_map<TableIdent, PendingReopenState> pending_reopens_;

#ifdef ELOQ_MODULE_ENABLED
    std::atomic<size_t> req_queue_size_{0};
#endif

#ifdef ELOQSTORE_WITH_TXSERVICE
    size_t work_one_round_count_{
        0};  // Counter for frequency-controlled metric collection (not atomic
             // since each Shard runs in single-threaded context)
#endif

    // TSC frequency in cycles per microsecond (measured at initialization)
    static std::once_flag tsc_frequency_initialized_;
    static std::atomic<uint64_t> tsc_cycles_per_microsecond_;

    friend class EloqStoreModule;
    friend class EloqStore;
    friend class KvRequest;
};
}  // namespace eloqstore
