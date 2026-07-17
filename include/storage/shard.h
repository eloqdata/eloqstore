#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <cstdio>
#include <ctime>
#include <memory>
#include <unordered_map>
#include <utility>  // NOLINT(build/include_order)
#include <variant>
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
    PageManager *IndexManager();
    TaskManager *TaskMgr();
    PagesPool *PagePool();
    GlobalRegisteredMemory *GlobalRegMem();

    // Microseconds the currently-running coroutine has held the worker thread
    // since its last resume (i.e. since its last yield). Long-running task
    // loops poll this via MaybeYield() and YieldToLowPQ() once it exceeds the
    // cooperative yield budget, so no single segment holds the brpc worker --
    // and, in module mode, the Redis request it co-drives -- for much longer
    // than the budget.
    uint64_t CurResumeElapsedUs()
    {
        return DurationMicroseconds(cur_resume_start_us_);
    }

    std::atomic<bool> io_mgr_and_page_pool_inited_{false};

#ifdef ELOQ_MODULE_ENABLED
    std::atomic<ShardStatus> running_status_{ShardStatus::Running};
#endif
    EloqStore *store_;
    const size_t shard_id_{0};
    boost::context::continuation main_;
    uint64_t ts_{};
    // Wall-clock (us) when the currently-running coroutine was last resumed.
    // Drives CurResumeElapsedUs() for cooperative time-budgeted yielding.
    uint64_t cur_resume_start_us_{};
    KvTask *running_{};
    CircularQueue<KvTask *> ready_tasks_;
    CircularQueue<KvTask *> low_priority_ready_tasks_;

private:
    void WorkLoop();
    void InitIoMgrAndPagePool();
    bool ExecuteReadyTasks();
    void OnTaskFinished(KvTask *task);
    void RetryOomRequest(KvRequest *req);
    void OnReceivedReq(KvRequest *req);
    bool ProcessReq(KvRequest *req);
    void EnqueueReopenWaiter(KvRequest *req);
    void EnqueueDelayedReopenRequest(ReopenRequest *req);
    void PromoteReadyDelayedReopenRequests();
    // Called when the reopen driver for parked waiters reaches a final result.
    // Successful reopen resubmits the waiters so they retry their original
    // requests; failed reopen completes every waiter with the reopen error.
    // Transient Reopen OOM retries happen before this function is called.
    void CompleteReopenWaiters(std::vector<KvRequest *> waiters,
                               KvError reopen_err);
    bool HasPendingDelayedRequests() const
    {
        return !delayed_requests_.empty();
    }
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
               !io_mgr_->NeedPrewarm() && delayed_requests_.empty();
    }
    void BindExtThd()
    {
        shard = this;
    }
#endif

    void InitializeTscFrequency();

    // Complete every request still queued on this shard with NotRunning when
    // the shard's loop exits during shutdown. SendRequest's Running gate is
    // advisory (a submitter can enqueue after the shard decides to exit), so
    // without this drain those requests would never complete and their callers
    // would block in Wait() forever. Runs on the shard thread (WorkLoop) or the
    // module worker (WorkOneRound), the sole consumer of requests_.
    void DrainPendingRequests();

    uint64_t ReadTimeMicroseconds();

    uint64_t DurationMicroseconds(uint64_t start_us);

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->result_err_ = KvError::NoError;
        task->status_ = TaskStatus::Ongoing;
        running_ = task;
        // Mark the resume start so a cooperative background loop measures this
        // first segment from now, not from the previously resumed task's
        // timestamp (see CurResumeElapsedUs / MaybeYield).
        cur_resume_start_us_ = ReadTimeMicroseconds();
        task->coro_ = boost::context::callcc(
            std::allocator_arg,
            stack_allocator_,
            [lbd, this](continuation &&sink)
            {
#ifdef ELOQSTORE_WITH_TXSERVICE
                metrics::TimePoint request_start;
                if (this->store_->EnableMetrics())
                {
                    request_start = metrics::Clock::now();
                }
#endif
                shard->main_ = std::move(sink);
                KvError err = lbd();
                KvTask *task = ThdTask();
                task->result_err_ = err;
                if (err != KvError::NoError)
                {
                    task->Abort();
                }
#ifdef ELOQSTORE_WITH_TXSERVICE
                if (this->store_->EnableMetrics())
                {
                    shard->finished_request_start_ = request_start;
                }
#endif
                task->status_ = TaskStatus::Finished;
                return std::move(shard->main_);
            });
        running_ = nullptr;
        if (task->status_ == TaskStatus::Finished)
        {
            OnTaskFinished(task);
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
    PageManager index_mgr_;
#ifdef ELOQSTORE_WITH_TXSERVICE
    metrics::TimePoint finished_request_start_{};
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
        using Driver = std::variant<ReopenRequest, ReopenRequest *>;

        ReopenRequest *DriverRequest()
        {
            if (auto *internal = std::get_if<ReopenRequest>(&driver_))
            {
                return internal;
            }
            return std::get<ReopenRequest *>(driver_);
        }

        bool IsCurrentInternalDriver(ReopenRequest *req) const
        {
            const auto *internal = std::get_if<ReopenRequest>(&driver_);
            return internal != nullptr && internal == req;
        }

        void SetExternalDriver(ReopenRequest *req)
        {
            driver_ = req;
        }

        std::vector<KvRequest *> waiters_;
        Driver driver_;
    };
    std::unordered_map<TableIdent, PendingReopenState> pending_reopens_;

    struct DelayedEntry
    {
        TableIdent tbl_id;
        ReopenRequest *request;
        uint64_t execute_at_us;

        bool operator>(const DelayedEntry &other) const
        {
            return execute_at_us > other.execute_at_us;
        }
    };
    std::vector<DelayedEntry> delayed_requests_;

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
