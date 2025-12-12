#include "shard.h"

#include <glog/logging.h>

#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>

#include "async_io_manager.h"
#include "list_object_task.h"
#include "utils.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/eloq_module.h>
#endif
namespace eloqstore
{
Shard::Shard(const EloqStore *store, size_t shard_id, uint32_t fd_limit)
    : store_(store),
      shard_id_(shard_id),
      page_pool_(&store->options_),
      io_mgr_(AsyncIoManager::Instance(store, fd_limit)),
      mapping_arena_(),
      index_mgr_(io_mgr_.get(), &mapping_arena_),
      stack_allocator_(store->options_.coroutine_stack_size)
{
}

KvError Shard::Init()
{
    KvError res = io_mgr_->Init(this);
    return res;
}

void Shard::WorkLoop()
{
    shard = this;
    io_mgr_->Start();
    io_mgr_->InitBackgroundJob();

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

    while (true)
    {
        while (true)
        {
            io_mgr_->Submit();
            io_mgr_->PollComplete();
            bool busy = ExecuteReadyTasks();
            if (!busy)
            {
                // CPU is not busy, we can process more requests.
                break;
            }
        }
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
    req.done_.store(false, std::memory_order_relaxed);
    pending_q.PushBack(&req);
}

bool Shard::HasPendingCompact(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    return !pending_q.compact_req_.done_.load(std::memory_order_relaxed);
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
    req.done_.store(false, std::memory_order_relaxed);
    pending_q.PushBack(&req);
}

bool Shard::HasPendingTTL(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    return !pending_q.expire_req_.done_.load(std::memory_order_relaxed);
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

const KvOptions *Shard::Options() const
{
    return &store_->Options();
}

void Shard::OnReceivedReq(KvRequest *req)
{
    if (auto wreq = dynamic_cast<WriteRequest *>(req); wreq != nullptr)
    {
        // Try acquire lock to ensure write operation is executed
        // sequentially on each table partition.
        auto [it, ok] = pending_queues_.try_emplace(req->tbl_id_);
        if (!ok)
        {
            // Wait on pending write queue because of other write task.
            it->second.PushBack(wreq);
            return;
        }
    }

    ProcessReq(req);
}

void Shard::ProcessReq(KvRequest *req)
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
        break;
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
        break;
    }
    case RequestType::Scan:
    {
        ScanTask *task = task_mgr_.GetScanTask();
        auto lbd = [task]() -> KvError { return task->Scan(); };
        StartTask(task, req, lbd);
        break;
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

            list_task.SetKvTask(task);
            auto cloud_mgr = static_cast<CloudStoreMgr *>(shard->io_mgr_.get());
            cloud_mgr->GetObjectStore().GetHttpManager()->SubmitRequest(
                &list_task);
            current_task->WaitIo();

            if (list_task.error_ != KvError::NoError)
            {
                LOG(ERROR) << "Failed to list objects, error: "
                           << static_cast<int>(list_task.error_);
                return list_task.error_;
            }

            if (!cloud_mgr->GetObjectStore().ParseListObjectsResponse(
                    list_task.response_data_,
                    list_task.json_data_,
                    list_object_req->GetObjects(),
                    list_object_req->GetDetails()))
            {
                LOG(ERROR) << "Failed to parse ListObjects response";
                return KvError::IoFail;
            }
            return KvError::NoError;
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::BatchWrite:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
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
        break;
    }
    case RequestType::Truncate:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        auto lbd = [task, req]() -> KvError
        {
            auto trunc_req = static_cast<TruncateRequest *>(req);
            return task->Truncate(trunc_req->position_);
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::DropTable:
    {
        LOG(ERROR) << "DropTable request routed to shard unexpectedly";
        req->SetDone(KvError::InvalidArgs);
        break;
    }
    case RequestType::Archive:
    {
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        auto lbd = [task]() -> KvError { return task->CreateArchive(); };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Compact:
    {
        BackgroundWrite *task = task_mgr_.GetBackgroundWrite(req->TableId());
        auto lbd = [task]() -> KvError { return task->CompactDataFile(); };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::CleanExpired:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        auto lbd = [task]() -> KvError { return task->CleanExpiredKeys(); };
        StartTask(task, req, lbd);
        break;
    }
    }
}

bool Shard::ExecuteReadyTasks()
{
    bool busy = ready_tasks_.Size() > 0;
    size_t task_num = ready_tasks_.Size();
    while (task_num-- > 0)
    {
        KvTask *task = ready_tasks_.Peek();
        ready_tasks_.Dequeue();
        assert(task->status_ == TaskStatus::Ongoing ||
               task->status_ == TaskStatus::RunNextRound);
        if (task->status_ == TaskStatus::Ongoing)
        {
            running_ = task;
            task->coro_ = task->coro_.resume();
            if (task->status_ == TaskStatus::Finished)
            {
                OnTaskFinished(task);
            }
        }
        else
        {
            task->status_ = TaskStatus::Ongoing;
            ready_tasks_.Enqueue(task);
        }
    }
    running_ = nullptr;
    return busy;
}

void Shard::OnTaskFinished(KvTask *task)
{
    if (auto *wtask = dynamic_cast<WriteTask *>(task); wtask != nullptr)
    {
        auto it = pending_queues_.find(wtask->TableId());
        assert(it != pending_queues_.end());
        task_mgr_.FreeTask(task);
        PendingWriteQueue &pending_q = it->second;
        if (pending_q.Empty())
        {
            // No more write requests, remove the pending queue.
            pending_queues_.erase(it);
        }
        else
        {
            WriteRequest *req = pending_q.PopFront();
            // Continue execute the next pending write request.
            ProcessReq(req);
        }
    }
    else
    {
        task_mgr_.FreeTask(task);
    }
}

#ifdef ELOQ_MODULE_ENABLED
void Shard::WorkOneRound()
{
    if (__builtin_expect(!io_mgr_->BackgroundJobInited(), false))
    {
        io_mgr_->InitBackgroundJob();
    }
    KvRequest *reqs[128];
    size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));
    for (size_t i = 0; i < nreqs; i++)
    {
        OnReceivedReq(reqs[i]);
    }

    if (nreqs == 0 && task_mgr_.NumActive() == 0 && io_mgr_->IsIdle())
    {
        // No request and no active task and no active io.
        if (io_mgr_->NeedPrewarm())
            io_mgr_->RunPrewarm();
        else
            return;
    }

    req_queue_size_.fetch_sub(nreqs, std::memory_order_relaxed);

    io_mgr_->Submit();
    io_mgr_->PollComplete();

    ExecuteReadyTasks();
}
#endif

void Shard::PendingWriteQueue::PushBack(WriteRequest *req)
{
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

}  // namespace eloqstore
