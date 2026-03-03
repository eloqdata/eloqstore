#pragma once

#include "tasks/task.h"

namespace eloqstore
{
class ListStandbyPartitionTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::ListStandbyPartition;
    }
};
}  // namespace eloqstore
