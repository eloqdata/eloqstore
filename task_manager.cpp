#include "task_manager.h"

#include <boost/context/continuation_fcontext.hpp>
#include <cassert>

#include "list_object_task.h"
#include "read_task.h"
#include "task.h"

using namespace boost::context;

namespace eloqstore
{
BatchWriteTask *TaskManager::GetBatchWriteTask(const TableIdent &tbl_id)
{
    num_active_++;
    BatchWriteTask *task = batch_write_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

BackgroundWrite *TaskManager::GetBackgroundWrite(const TableIdent &tbl_id)
{
    num_active_++;
    BackgroundWrite *task = bg_write_pool_.GetTask();
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
    assert(task->status_ == TaskStatus::Finished);
    assert(task->inflight_io_ == 0);
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
        batch_write_pool_.FreeTask(static_cast<BatchWriteTask *>(task));
        break;
    case TaskType::BackgroundWrite:
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
