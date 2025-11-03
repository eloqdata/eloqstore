#pragma once

#include "task.h"

namespace eloqstore
{
class PrewarmRequest;

class PrewarmTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Prewarm;
    }

    KvError Prewarm(const PrewarmRequest &request);
};
}  // namespace eloqstore
