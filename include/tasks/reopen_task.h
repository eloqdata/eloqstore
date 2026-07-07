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

    // Latched by Shard::ProcessReq before the task runs. By the time
    // OnTaskFinished inspects a finished reopen, SetDone has already handed
    // a user-issued request back to its owner, so the request must not be
    // dereferenced there — these fields carry what it needs.
    bool auto_reopen_req_{false};
    KvError result_err_{KvError::NoError};
};
}  // namespace eloqstore
