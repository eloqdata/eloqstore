#pragma once

#include "tasks/task.h"

namespace eloqstore
{
/**
 * @brief Raises a data store shard's term file for EloqStore::AttachShard,
 * which sends one AttachShardRequest to one worker shard in cloud mode.
 *
 * Runs as a task so that the compare-and-swap goes through the worker shard's
 * object store like every other cloud request: the task waits on the
 * asynchronous downloads and uploads instead of blocking a thread on them.
 */
class AttachShardTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::AttachShard;
    }
    /**
     * @brief Raises the term file of the shard the task carries to the term
     * it carries (CloudStoreMgr::UpsertTermFile). Shard::StartTask took that
     * term from the shard's slot, where AttachShard set it.
     *
     * @return NoError; ExpiredTerm if the file holds a newer term; else the
     * cloud error.
     */
    KvError Attach();
};
}  // namespace eloqstore
