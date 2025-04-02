#include "task_manager.h"

#include <boost/context/continuation_fcontext.hpp>
#include <cassert>
#include <utility>

#include "read_task.h"
#include "task.h"
#include "write_task.h"

using namespace boost::context;

namespace kvstore
{
BatchWriteTask *TaskManager::GetBatchWriteTask(const TableIdent &tbl_id)
{
    if (free_batch_write_.empty())
    {
        auto task = std::make_unique<BatchWriteTask>(tbl_id);
        free_batch_write_.push_back(task.get());
        batch_write_tasks_.emplace_back(std::move(task));
    }
    BatchWriteTask *task = free_batch_write_.back();
    free_batch_write_.pop_back();
    task->Reset(tbl_id);
    return task;
}

TruncateTask *TaskManager::GetTruncateTask(const TableIdent &tbl_id)
{
    if (free_truncate_.empty())
    {
        auto task = std::make_unique<TruncateTask>(tbl_id);
        free_truncate_.push_back(task.get());
        truncate_tasks_.emplace_back(std::move(task));
    }
    TruncateTask *task = free_truncate_.back();
    free_truncate_.pop_back();
    task->Reset(tbl_id);
    return task;
}

ReadTask *TaskManager::GetReadTask()
{
    if (free_read_.empty())
    {
        auto task = std::make_unique<ReadTask>();
        free_read_.push_back(task.get());
        read_tasks_.emplace_back(std::move(task));
    }
    ReadTask *task = free_read_.back();
    free_read_.pop_back();
    return task;
}

ScanTask *TaskManager::GetScanTask()
{
    if (free_scan_.empty())
    {
        auto task = std::make_unique<ScanTask>();
        free_scan_.push_back(task.get());
        scan_tasks_.emplace_back(std::move(task));
    }
    ScanTask *task = free_scan_.back();
    free_scan_.pop_back();
    return task;
}

void TaskManager::FreeTask(KvTask *task)
{
    switch (task->Type())
    {
    case TaskType::Read:
        free_read_.push_back(static_cast<ReadTask *>(task));
        break;
    case TaskType::Scan:
        free_scan_.push_back(static_cast<ScanTask *>(task));
        break;
    case TaskType::BatchWrite:
    {
        BatchWriteTask *wtask = static_cast<BatchWriteTask *>(task);
        free_batch_write_.push_back(wtask);
        break;
    }
    case TaskType::Truncate:
    {
        TruncateTask *wtask = static_cast<TruncateTask *>(task);
        free_truncate_.push_back(wtask);
        break;
    }
    }
}

uint32_t TaskManager::NumActive() const
{
    return (read_tasks_.size() - free_read_.size()) +
           (scan_tasks_.size() - free_scan_.size()) +
           (batch_write_tasks_.size() - free_batch_write_.size()) +
           (truncate_tasks_.size() - free_truncate_.size());
}

void TaskManager::ResumeScheduled()
{
    while (scheduled_.Size() > 0)
    {
        thd_task = scheduled_.Peek();
        scheduled_.Dequeue();
        thd_task->coro_ = thd_task->coro_.resume();
    }
}

}  // namespace kvstore