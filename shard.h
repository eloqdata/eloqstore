#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <utility>  // NOLINT(build/include_order)

#include "circular_queue.h"
#include "eloq_store.h"
#include "page_mapper.h"
#include "task_manager.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

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

    bool HasPendingRequests() const;

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
               task_mgr_.NumActive() == 0 && io_mgr_->IsIdle() &&
               !io_mgr_->NeedPrewarm();
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
        task->coro_ = boost::context::callcc(std::allocator_arg,
                                             stack_allocator_,
                                             [lbd](continuation &&sink)
                                             {
                                                 shard->main_ = std::move(sink);
                                                 KvError err = lbd();
                                                 KvTask *task = ThdTask();
                                                 if (err != KvError::NoError)
                                                 {
                                                     task->Abort();
                                                 }

                                                 task->req_->SetDone(err);
                                                 task->req_ = nullptr;
                                                 task->status_ =
                                                     TaskStatus::Finished;
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
    MappingArena mapping_arena_;
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
