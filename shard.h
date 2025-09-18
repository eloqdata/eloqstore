#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>

#include "circular_queue.h"
#include "eloq_store.h"
#include "task_manager.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

/**
 * Start execution of a KvTask inside a coroutine.
 *
 * Creates a coroutine that runs the provided callable `lbd` (executed on the task's coroutine stack)
 * and makes `task` the currently running task for the duration. The callable must return a KvError;
 * its return value is passed to the associated request via `req->SetDone(err)`. After the callable
 * returns the request pointer is cleared and the task status is set to Finished. If the task
 * finished immediately on startup, OnTaskFinished(task) is invoked before returning.
 *
 * @param task Pointer to the KvTask to start; this object's fields (req_, status_, coro_) are set.
 * @param req  KvRequest associated with the task; SetDone(err) will be called on it when the task's
 *             coroutine function returns.
 * @param lbd  Callable invoked inside the new coroutine. Must be callable with no arguments and
 *             return a KvError.
 */
namespace eloqstore
{
#ifdef ELOQ_MODULE_ENABLED
class EloqStoreModule;
#endif

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

    const KvOptions *Options() const;
    AsyncIoManager *IoManager();
    IndexPageManager *IndexManager();
    TaskManager *TaskMgr();
    PagesPool *PagePool();

    const EloqStore *store_;
    const size_t shard_id_{0};
    boost::context::continuation main_;
    KvTask *running_;
    CircularQueue<KvTask *> ready_tasks_;

private:
    void WorkLoop();
    bool ExecuteReadyTasks();
    void OnTaskFinished(KvTask *task);
    void OnReceivedReq(KvRequest *req);
    void ProcessReq(KvRequest *req);

#ifdef ELOQ_MODULE_ENABLED
    void WorkOneRound();
    bool IsIdle() const
    {
        // No request in the queue and no active task(coroutine) and no active
        // io.
        return req_queue_size_.load(std::memory_order_relaxed) == 0 &&
               task_mgr_.NumActive() == 0 && io_mgr_->IsIdle();
    }
    void BindExtThd()
    {
        shard = this;
    }
#endif

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        running_ = task;
        task->coro_ = boost::context::callcc(
            std::allocator_arg,
            stack_allocator_,
            [lbd](continuation &&sink)
            {
                shard->main_ = std::move(sink);
                KvError err = lbd();
                KvTask *task = ThdTask();

                task->req_->SetDone(err);
                task->req_ = nullptr;
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
        WriteRequest *PopFront();
        bool Empty() const;

        // Requests from internal
        CompactRequest compact_req_;
        CleanExpiredRequest expire_req_;

    private:
        WriteRequest *head_{nullptr};
        WriteRequest *tail_{nullptr};
    };
    std::unordered_map<TableIdent, PendingWriteQueue> pending_queues_;

#ifdef ELOQ_MODULE_ENABLED
    std::atomic<size_t> req_queue_size_{0};
#endif

    friend class EloqStoreModule;
};
}  // namespace eloqstore