#pragma once

#include <array>
#include <boost/context/pooled_fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <cstddef>
#include <utility>  // NOLINT(build/include_order)

#include "circular_queue.h"
#include "eloq_store.h"
#include "page_mapper.h"
#include "task_manager.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

#ifdef ELOQ_MODULE_ENABLED
#include <glog/logging.h>
#include <mimalloc-2.1/mimalloc.h>
#endif

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
    int64_t last_memory_report_ts_{0};
    struct HeapGuard
    {
        explicit HeapGuard(const Shard *shard)
        {
            prev_thd_ = mi_override_thread(shard->shard_id_);
            prev_heap_ = mi_heap_set_default(shard->heap_);
        }
        ~HeapGuard()
        {
            mi_override_thread(prev_thd_);
            mi_heap_set_default(prev_heap_);
        }
        mi_threadid_t prev_thd_{0};
        mi_heap_t *prev_heap_{nullptr};
    };
    bool heap_inited_{false};
    mi_heap_t *heap_{nullptr};
    mi_threadid_t thread_id_{0};

public:
    enum class ApplyPhase : uint8_t
    {
        MakeCowRoot = 0,
        ApplyBatch,
        ApplyTTLBatch,
        UpdateMeta,
        TriggerTTL,
        Count
    };
    static constexpr size_t kApplyPhaseCount =
        static_cast<size_t>(ApplyPhase::Count);

    static const char *ApplyPhaseLabel(ApplyPhase phase);

    struct HeapRecorder
    {
        HeapRecorder(int64_t &allocated_counter, int64_t &committed_counter)
            : allocated_counter_(allocated_counter),
              committed_counter_(committed_counter)
        {
            mi_thread_stats(&start_allocated_, &start_committed_);
        }
        ~HeapRecorder()
        {
            int64_t current_allocated{0};
            int64_t current_committed{0};
            mi_thread_stats(&current_allocated, &current_committed);
            allocated_counter_ += (current_allocated - start_allocated_);
            committed_counter_ += (current_committed - start_committed_);
        }

    private:
        int64_t &allocated_counter_;
        int64_t &committed_counter_;
        int64_t start_allocated_{0};
        int64_t start_committed_{0};
    };

    struct ApplyHeapRecorder : HeapRecorder
    {
        explicit ApplyHeapRecorder(Shard *shard)
            : HeapRecorder(shard->apply_allocated_, shard->apply_committed_)
        {
        }
    };

    struct ApplyPhaseRecorder : HeapRecorder
    {
        ApplyPhaseRecorder(Shard *shard, ApplyPhase phase)
            : HeapRecorder(
                  shard->apply_phase_allocated_[static_cast<size_t>(phase)],
                  shard->apply_phase_committed_[static_cast<size_t>(phase)])
        {
        }
    };

    struct TriggerGcRecorder : HeapRecorder
    {
        explicit TriggerGcRecorder(Shard *shard)
            : HeapRecorder(shard->trigger_gc_allocated_,
                           shard->trigger_gc_committed_)
        {
        }
    };

    struct CompactDataFileRecorder : HeapRecorder
    {
        explicit CompactDataFileRecorder(Shard *shard)
            : HeapRecorder(shard->compact_data_file_allocated_,
                           shard->compact_data_file_committed_)
        {
        }
    };

private:
    int64_t apply_allocated_{0};
    int64_t apply_committed_{0};
    std::array<int64_t, kApplyPhaseCount> apply_phase_allocated_{};
    std::array<int64_t, kApplyPhaseCount> apply_phase_committed_{};
    int64_t trigger_gc_allocated_{0};
    int64_t trigger_gc_committed_{0};
    int64_t compact_data_file_allocated_{0};
    int64_t compact_data_file_committed_{0};
#endif

    friend class EloqStoreModule;
};
}  // namespace eloqstore
