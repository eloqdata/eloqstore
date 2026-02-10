
#pragma once

#include <cassert>
#include <limits>
#include <memory>
#include <vector>

#include "tasks/background_write.h"
#include "tasks/batch_write_task.h"
#include "tasks/list_object_task.h"
#include "tasks/read_task.h"
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
                                    uint32_t list_object_pool_size);

    BatchWriteTask *GetBatchWriteTask(const TableIdent &tbl_id);
    BackgroundWrite *GetBackgroundWrite(const TableIdent &tbl_id);
    ReadTask *GetReadTask();
    ScanTask *GetScanTask();
    ListObjectTask *GetListObjectTask();
    void FreeTask(KvTask *task);

    void AddExternalTask();
    void FinishExternalTask();

    size_t NumActive() const;

private:
    template <typename T>
    class TaskPool
    {
    public:
        TaskPool(uint32_t size, bool allow_growth = true)
            : allow_growth_(allow_growth)
        {
            if (size > 0)
            {
                init_pool_ = std::make_unique<T[]>(size);
                for (uint32_t i = 0; i < size; i++)
                {
                    FreeTask(&init_pool_[i]);
                }
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
            auto &task = ext_pool_.emplace_back(std::make_unique<T>());
            return task.get();
        }

        void FreeTask(T *task)
        {
            task->status_ = TaskStatus::Idle;
            task->next_ = free_head_;
            free_head_ = task;
        }

    private:
        std::unique_ptr<T[]> init_pool_{nullptr};
        std::vector<std::unique_ptr<T>> ext_pool_;
        T *free_head_{nullptr};
        bool allow_growth_{true};
    };

    TaskPool<BatchWriteTask> batch_write_pool_;
    TaskPool<BackgroundWrite> bg_write_pool_;
    TaskPool<ReadTask> read_pool_;
    TaskPool<ScanTask> scan_pool_;
    TaskPool<ListObjectTask> list_object_pool_;
    size_t num_active_{0};
    size_t num_active_write_{0};
    size_t max_write_concurrency_{std::numeric_limits<size_t>::max()};
};
}  // namespace eloqstore
