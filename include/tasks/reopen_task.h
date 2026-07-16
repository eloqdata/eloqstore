#pragma once

#include "eloq_store.h"
#include "tasks/write_task.h"

namespace eloqstore
{
class ReopenTask : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Reopen;
    }
    KvError Reopen(const TableIdent &tbl_id);

    void Reset(const TableIdent &tbl_id) override
    {
        WriteTask::Reset(tbl_id);
    }
};
}  // namespace eloqstore
