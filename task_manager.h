
#pragma once

#include <memory>
#include <vector>

#include "read_task.h"
#include "scan_task.h"
#include "table_ident.h"
#include "write_task.h"

namespace kvstore
{
class TaskManager
{
public:
    BatchWriteTask *GetBatchWriteTask(const TableIdent &tbl_id);
    TruncateTask *GetTruncateTask(const TableIdent &tbl_id);
    ReadTask *GetReadTask();
    ScanTask *GetScanTask();
    void FreeTask(KvTask *task);
    uint32_t NumActive() const;
    void ResumeScheduled();

    CircularQueue<KvTask *> scheduled_;
    CircularQueue<KvTask *> finished_;

private:
    std::vector<std::unique_ptr<BatchWriteTask>> batch_write_tasks_;
    std::vector<BatchWriteTask *> free_batch_write_;
    std::vector<std::unique_ptr<TruncateTask>> truncate_tasks_;
    std::vector<TruncateTask *> free_truncate_;
    std::vector<std::unique_ptr<ReadTask>> read_tasks_;
    std::vector<ReadTask *> free_read_;
    std::vector<std::unique_ptr<ScanTask>> scan_tasks_;
    std::vector<ScanTask *> free_scan_;
};
}  // namespace kvstore