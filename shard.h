#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>

#include "circular_queue.h"
#include "eloq_store.h"
#include "task_manager.h"

#ifdef ELOQ_MODULE_ENABLED
#include <condition_variable>

#include "async_io_listener.h"
#endif

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace kvstore
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
    boost::context::continuation main_;
    KvTask *running_;
    CircularQueue<KvTask *> scheduled_;
    CircularQueue<KvTask *> finished_;

private:
    void WorkLoop();
    void ResumeScheduled();
    void PollFinished();

    void OnReceivedReq(KvRequest *req);
    void ProcessReq(KvRequest *req);
    void OnWriteFinished(const TableIdent &tbl_id);

    void WorkOneRound(size_t &req_cnt);
#ifdef ELOQ_MODULE_ENABLED
    bool IsIdle() const
    {
        return req_queue_size_.load(std::memory_order_relaxed) == 0 &&
               !io_mgr_->HasValidIO();
    }
    void BindExtThd()
    {
        shard = this;
    }
    void NotifyListener();
#endif

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        running_ = task;
        task->coro_ = boost::context::callcc(std::allocator_arg,
                                             stack_pool_,
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
                                                     TaskStatus::Idle;
                                                 shard->finished_.Enqueue(task);
                                                 return std::move(shard->main_);
                                             });
        running_ = nullptr;
    }

    size_t shard_id_{0};
    moodycamel::BlockingConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    PagesPool page_pool_;
    std::unique_ptr<AsyncIoManager> io_mgr_;
    IndexPageManager index_mgr_;
    TaskManager task_mgr_;
    boost::context::pooled_fixedsize_stack stack_pool_;

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
    std::atomic<bool> ext_proc_running_{true};
    std::unique_ptr<AsyncIOListener> async_io_listener_{nullptr};
#endif

    friend class EloqStoreModule;
    friend class AsyncIOListener;
};
}  // namespace kvstore