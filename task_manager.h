
#pragma once

#include <memory>
#include <vector>

#include "background_write.h"
#include "batch_write_task.h"
#include "list_object_task.h"
#include "read_task.h"
#include "scan_task.h"
#include "types.h"

namespace eloqstore
{
class TaskManager
{
public:
    explicit TaskManager();

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
        TaskPool(uint32_t size)
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

        // TODO(zhanghao): TaskPool should have a capacity limit.
        // Push KvRequest that need new KvTask to the back of this list when
        // capacity limit is reached.
        // KvRequest* head_{nullptr}, tail_{nullptr};
    };

    TaskPool<BatchWriteTask> batch_write_pool_;
    TaskPool<BackgroundWrite> bg_write_pool_;
    TaskPool<ReadTask> read_pool_;
    TaskPool<ScanTask> scan_pool_;
    TaskPool<ListObjectTask> list_object_pool_;
    size_t num_active_{0};
};
}  // namespace eloqstore
