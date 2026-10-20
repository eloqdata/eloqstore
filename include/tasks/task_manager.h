
#pragma once

#include <cassert>
#include <deque>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "tasks/background_write.h"
#include "tasks/batch_write_task.h"
#include "tasks/list_object_task.h"
#include "tasks/list_standby_partition_task.h"
#include "tasks/read_task.h"
#include "tasks/reopen_task.h"
#include "tasks/scan_task.h"
#include "types.h"

namespace eloqstore
{
struct KvOptions;

class TaskManager
{
public:
    explicit TaskManager(const KvOptions *opts);

    static void SetPoolSizesForTest(uint32_t batch_write_pool_size,
                                    uint32_t background_write_pool_size,
                                    uint32_t read_pool_size,
                                    uint32_t scan_pool_size,
                                    uint32_t list_object_pool_size,
                                    uint32_t reopen_pool_size);

    BatchWriteTask *GetBatchWriteTask(const TableIdent &tbl_id);
    BackgroundWrite *GetBackgroundWrite(const TableIdent &tbl_id);
    ReadTask *GetReadTask();
    ScanTask *GetScanTask();
    ListObjectTask *GetListObjectTask();
    ListStandbyPartitionTask *GetListStandbyPartitionTask();
    ReopenTask *GetReopenTask(const TableIdent &tbl_id);
    void FreeTask(KvTask *task);

    void AddExternalTask();
    void FinishExternalTask();

    size_t NumActive() const;

    void Shutdown();

private:
    template <typename T>
    class TaskPool
    {
    public:
        /**
         * @brief Builds the pool's initial tasks.
         * @param size Number of tasks built now.
         * @param allow_growth Whether GetTask builds another task when none is
         * free.
         * @param opts The store's options, given to the task types built with
         * them (see MakeTask).
         */
        TaskPool(uint32_t size, bool allow_growth, const KvOptions *opts)
            : opts_(opts), allow_growth_(allow_growth)
        {
            for (uint32_t i = 0; i < size; i++)
            {
                if constexpr (std::is_constructible_v<T, const KvOptions *>)
                {
                    init_pool_.emplace_back(opts_);
                }
                else
                {
                    init_pool_.emplace_back();
                }
                FreeTask(&init_pool_.back());
            }
        };

        T *GetTask()
        {
            if (free_head_ != nullptr)
            {
                // Reuse a free task.
                T *task = free_head_;
                free_head_ = static_cast<T *>(task->next_);
                task->next_ = nullptr;
                assert(task->status_ == TaskStatus::Idle);
                return task;
            }
            if (!allow_growth_)
            {
                return nullptr;
            }
            auto &task = ext_pool_.emplace_back(MakeTask());
            return task.get();
        }

        void FreeTask(T *task)
        {
            task->status_ = TaskStatus::Idle;
            task->next_ = free_head_;
            free_head_ = task;
        }

        template <typename F>
        void ForEachTask(F &&visitor)
        {
            for (T &task : init_pool_)
            {
                visitor(&task);
            }
            for (auto &task : ext_pool_)
            {
                visitor(task.get());
            }
        }

        void Clear()
        {
            free_head_ = nullptr;
            ext_pool_.clear();
            init_pool_.clear();
        }

    private:
        /**
         * @brief Builds one task, with the store's options if its type takes
         * them.
         * @return The new task.
         */
        std::unique_ptr<T> MakeTask() const
        {
            if constexpr (std::is_constructible_v<T, const KvOptions *>)
            {
                return std::make_unique<T>(opts_);
            }
            else
            {
                return std::make_unique<T>();
            }
        }

        const KvOptions *opts_;  ///< The store's options, for MakeTask.
        /// The tasks built up front; a deque, so building one never moves
        /// those already handed out.
        std::deque<T> init_pool_;
        std::vector<std::unique_ptr<T>> ext_pool_;
        T *free_head_{nullptr};
        bool allow_growth_{true};
    };

    TaskPool<BatchWriteTask> batch_write_pool_;
    TaskPool<BackgroundWrite> bg_write_pool_;
    TaskPool<ReadTask> read_pool_;
    TaskPool<ScanTask> scan_pool_;
    TaskPool<ListObjectTask> list_object_pool_;
    TaskPool<ListStandbyPartitionTask> list_standby_partition_pool_;
    TaskPool<ReopenTask> reopen_pool_;
    size_t num_active_{0};
    size_t num_active_write_{0};
    size_t max_write_concurrency_{std::numeric_limits<size_t>::max()};
};
}  // namespace eloqstore
