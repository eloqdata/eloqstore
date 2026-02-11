#include "tasks/task_manager.h"

#include <boost/context/continuation.hpp>
#include <cassert>
#include <limits>

#include "kv_options.h"
#include "tasks/list_object_task.h"
#include "tasks/read_task.h"
#include "tasks/task.h"

using namespace boost::context;

namespace eloqstore
{
namespace
{
uint32_t batch_write_pool_size_default = 1024;
uint32_t background_write_pool_size_default = 1024;
uint32_t read_pool_size_default = 2048;
uint32_t scan_pool_size_default = 2048;
uint32_t list_object_pool_size_default = 512;
}  // namespace

TaskManager::TaskManager(const KvOptions *opts)
    : batch_write_pool_(opts != nullptr && opts->max_write_concurrency > 0
                            ? opts->max_write_concurrency
                            : batch_write_pool_size_default,
                        !(opts != nullptr && opts->max_write_concurrency > 0)),
      bg_write_pool_(opts != nullptr && opts->max_write_concurrency > 0
                         ? opts->max_write_concurrency
                         : background_write_pool_size_default,
                     !(opts != nullptr && opts->max_write_concurrency > 0)),
      read_pool_(read_pool_size_default),
      scan_pool_(scan_pool_size_default),
      list_object_pool_(list_object_pool_size_default)
{
    if (opts != nullptr && opts->max_write_concurrency > 0)
    {
        max_write_concurrency_ = opts->max_write_concurrency;
    }
    else
    {
        max_write_concurrency_ = std::numeric_limits<size_t>::max();
    }
}

void TaskManager::SetPoolSizesForTest(uint32_t batch_write_pool_size,
                                      uint32_t background_write_pool_size,
                                      uint32_t read_pool_size,
                                      uint32_t scan_pool_size,
                                      uint32_t list_object_pool_size)
{
    batch_write_pool_size_default = batch_write_pool_size;
    background_write_pool_size_default = background_write_pool_size;
    read_pool_size_default = read_pool_size;
    scan_pool_size_default = scan_pool_size;
    list_object_pool_size_default = list_object_pool_size;
}

BatchWriteTask *TaskManager::GetBatchWriteTask(const TableIdent &tbl_id)
{
    if (num_active_write_ >= max_write_concurrency_)
    {
        return nullptr;
    }
    BatchWriteTask *task = batch_write_pool_.GetTask();
    if (task == nullptr)
    {
        return nullptr;
    }
    num_active_++;
    num_active_write_++;
    task->Reset(tbl_id);
    return task;
}

BackgroundWrite *TaskManager::GetBackgroundWrite(const TableIdent &tbl_id)
{
    if (num_active_write_ >= max_write_concurrency_)
    {
        return nullptr;
    }
    BackgroundWrite *task = bg_write_pool_.GetTask();
    if (task == nullptr)
    {
        return nullptr;
    }
    num_active_++;
    num_active_write_++;
    task->Reset(tbl_id);
    return task;
}

ReadTask *TaskManager::GetReadTask()
{
    num_active_++;
    return read_pool_.GetTask();
}

ScanTask *TaskManager::GetScanTask()
{
    num_active_++;
    return scan_pool_.GetTask();
}

ListObjectTask *TaskManager::GetListObjectTask()
{
    num_active_++;
    return list_object_pool_.GetTask();
}

void TaskManager::FreeTask(KvTask *task)
{
    CHECK(task->status_ == TaskStatus::Finished);
    CHECK(task->inflight_io_ == 0);
    num_active_--;
    switch (task->Type())
    {
    case TaskType::Read:
        read_pool_.FreeTask(static_cast<ReadTask *>(task));
        break;
    case TaskType::Scan:
        scan_pool_.FreeTask(static_cast<ScanTask *>(task));
        break;
    case TaskType::BatchWrite:
        assert(num_active_write_ > 0);
        num_active_write_--;
        batch_write_pool_.FreeTask(static_cast<BatchWriteTask *>(task));
        break;
    case TaskType::BackgroundWrite:
        assert(num_active_write_ > 0);
        num_active_write_--;
        bg_write_pool_.FreeTask(static_cast<BackgroundWrite *>(task));
        break;
    case TaskType::ListObject:
        list_object_pool_.FreeTask(static_cast<ListObjectTask *>(task));
        break;
    case TaskType::EvictFile:
        assert(false && "EvictFile task should not be freed here");
        break;
    case TaskType::Prewarm:
        assert(false && "Prewarm task should not be freed here");
    }
}

void TaskManager::AddExternalTask()
{
    num_active_++;
}

void TaskManager::FinishExternalTask()
{
    assert(num_active_ > 0);
    num_active_--;
}

size_t TaskManager::NumActive() const
{
    return num_active_;
}

}  // namespace eloqstore
