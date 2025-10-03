#pragma once
#include <string>
#include <vector>

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

    void SetResults(std::vector<std::string> *objects)
    {
        objects_ = objects;
    }

    std::vector<std::string> *objects_{nullptr};
};
}  // namespace eloqstore