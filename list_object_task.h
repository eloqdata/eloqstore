#pragma once
#include "task.h"
namespace eloqstore
{
class ListObjectTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::ListObject;
    }
};
}  // namespace eloqstore
