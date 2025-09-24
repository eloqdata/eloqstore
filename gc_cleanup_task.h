#pragma once

#include "write_task.h"

namespace eloqstore
{
class GcCleanupTask : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::GcCleanup;
    }

    KvError CleanupPartition()
    {
        return IoMgr()->RemovePartitionDirIfOnlyManifest(tbl_ident_);
    }
};
}  // namespace eloqstore
