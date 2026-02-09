#pragma once

#include "tasks/task.h"

namespace eloqstore
{
class ReopenTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Reopen;
    }
    KvError Reopen(const TableIdent &tbl_id);
};
}  // namespace eloqstore
