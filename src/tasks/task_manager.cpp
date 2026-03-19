#include "tasks/task_manager.h"

#include <boost/context/continuation.hpp>
#include <cassert>
#include <limits>

#include "eloq_store.h"
#include "kv_options.h"
#include "tasks/list_object_task.h"
#include "tasks/read_task.h"
#include "tasks/reopen_task.h"
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
uint32_t list_standby_partition_pool_size_default = 128;
uint32_t reopen_pool_size_default = 256;
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
      list_object_pool_(list_object_pool_size_default),
      list_standby_partition_pool_(list_standby_partition_pool_size_default),
      reopen_pool_(reopen_pool_size_default)
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

void TaskManager::Shutdown()
{
    size_t unfinished_tasks = 0;
    size_t unfinished_reqs = 0;
    auto check_finished = [&](KvTask *task)
    {
        if (task->status_ == TaskStatus::Idle)
        {
            return;
        }
        ++unfinished_tasks;
        if (task->req_ != nullptr)
        {
            ++unfinished_reqs;
            LOG(ERROR) << "TaskManager::Shutdown found unfinished request "
                       << typeid(*task->req_).name();
        }
    };
    batch_write_pool_.ForEachTask(check_finished);
    bg_write_pool_.ForEachTask(check_finished);
    read_pool_.ForEachTask(check_finished);
    scan_pool_.ForEachTask(check_finished);
    list_object_pool_.ForEachTask(check_finished);
    list_standby_partition_pool_.ForEachTask(check_finished);
    reopen_pool_.ForEachTask(check_finished);
    CHECK_EQ(unfinished_tasks, 0)
        << "TaskManager::Shutdown requires a quiesced shard, unfinished_tasks="
        << unfinished_tasks << ", unfinished_reqs=" << unfinished_reqs;
    CHECK_EQ(num_active_, 0)
        << "TaskManager::Shutdown found tasks still registered as active";
    LOG(INFO) << "TaskManager::Shutdown finished";

    num_active_ = 0;
    num_active_write_ = 0;

    batch_write_pool_.Clear();
    bg_write_pool_.Clear();
    read_pool_.Clear();
    scan_pool_.Clear();
    list_object_pool_.Clear();
    list_standby_partition_pool_.Clear();
    reopen_pool_.Clear();
}

void TaskManager::SetPoolSizesForTest(uint32_t batch_write_pool_size,
                                      uint32_t background_write_pool_size,
                                      uint32_t read_pool_size,
                                      uint32_t scan_pool_size,
                                      uint32_t list_object_pool_size,
                                      uint32_t reopen_pool_size)
{
    batch_write_pool_size_default = batch_write_pool_size;
    background_write_pool_size_default = background_write_pool_size;
    read_pool_size_default = read_pool_size;
    scan_pool_size_default = scan_pool_size;
    list_object_pool_size_default = list_object_pool_size;
    reopen_pool_size_default = reopen_pool_size;
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

ListStandbyPartitionTask *TaskManager::GetListStandbyPartitionTask()
{
    num_active_++;
    return list_standby_partition_pool_.GetTask();
}

ReopenTask *TaskManager::GetReopenTask(const TableIdent &tbl_id)
{
    num_active_++;
    ReopenTask *task = reopen_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
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
    case TaskType::ListStandbyPartition:
        list_standby_partition_pool_.FreeTask(
            static_cast<ListStandbyPartitionTask *>(task));
        break;
    case TaskType::Reopen:
        reopen_pool_.FreeTask(static_cast<ReopenTask *>(task));
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
