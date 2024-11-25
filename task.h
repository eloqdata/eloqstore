#pragma once

#include <cstdint>

#include "error.h"

namespace kvstore
{
enum class TaskStatus : uint8_t
{
    Unstarted = 0,
    Ongoing,
    Finished,
    Rollback,
    Errored,
    BlockedForOne,
    BlockedForAll
};

enum struct TaskType
{
    Read = 0,
    Scan,
    Write
};

class KvTask
{
public:
    virtual ~KvTask() = default;

    virtual TaskType Type() const = 0;

    virtual void Yield() = 0;
    /**
     * @brief Re-schedules the task to run. Note: the resumed task does not run
     * in place.
     *
     */
    virtual void Resume() = 0;
    virtual void Rollback() = 0;

    TaskStatus status_;
    KvError kv_error_;
};
}  // namespace kvstore