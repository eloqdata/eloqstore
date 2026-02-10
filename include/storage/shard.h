#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <cstdio>
#include <ctime>
#include <utility>  // NOLINT(build/include_order)

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

class Shard
{
public:
    Shard(const EloqStore *store, size_t shard_id, uint32_t fd_limit);
    KvError Init();
    void Start();
    void Stop();
    bool AddKvRequest(KvRequest *req);

    void AddPendingCompact(const TableIdent &tbl_id);
    bool HasPendingCompact(const TableIdent &tbl_id);
    void AddPendingTTL(const TableIdent &tbl_id);
    bool HasPendingTTL(const TableIdent &tbl_id);

    bool HasPendingRequests() const;

    AsyncIoManager *IoManager();
    IndexPageManager *IndexManager();
    TaskManager *TaskMgr();
    PagesPool *PagePool();

    const EloqStore *store_;
    const size_t shard_id_{0};
    boost::context::continuation main_;
    uint64_t ts_{};
    KvTask *running_{};
    CircularQueue<KvTask *> ready_tasks_;
    CircularQueue<KvTask *> low_priority_ready_tasks_;
    size_t running_writing_tasks_{};
    bool oss_enabled_{false};
    bool io_mgr_and_page_pool_inited_{false};

private:
    void WorkLoop();
    void InitIoMgrAndPagePool();
    bool ExecuteReadyTasks();
    void OnTaskFinished(KvTask *task);
    void OnReceivedReq(KvRequest *req);
    bool ProcessReq(KvRequest *req);
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
                task->req_->SetDone(err);
                task->req_ = nullptr;
                task->status_ = TaskStatus::Finished;

#ifdef ELOQSTORE_WITH_TXSERVICE
                // Collect latency metric when request completes
                if (this->store_->EnableMetrics())
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
        case RequestType::CleanExpired:
            return "clean_expired";
        default:
            return "unknown";
        }
    }

    moodycamel::BlockingConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    PagesPool page_pool_;
    std::unique_ptr<AsyncIoManager> io_mgr_;
    IndexPageManager index_mgr_;
    TaskManager task_mgr_;
#ifndef NDEBUG
    boost::context::protected_fixedsize_stack stack_allocator_;
#else
    boost::context::pooled_fixedsize_stack stack_allocator_;
#endif

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
        CleanExpiredRequest expire_req_;
        bool running_{false};

    private:
        WriteRequest *head_{nullptr};
        WriteRequest *tail_{nullptr};
    };
    std::unordered_map<TableIdent, PendingWriteQueue> pending_queues_;

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
};
}  // namespace eloqstore
