#pragma once

#include "tasks/task.h"

namespace eloqstore
{
/**
 * @brief Lets a worker shard go of a data store shard's partitions, for
 * EloqStore::DetachShard, which sends one DetachShardRequest per worker shard.
 *
 * Runs as a task so that the eviction can wait on the io_uring closes of the
 * partitions' files like any other task does.
 */
class DetachShardTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::DetachShard;
    }
    /**
     * @brief Fails the requests queued for the shard's partitions, waits
     * until no task runs on them, then drops the worker shard's state for
     * them (Shard::EvictDsShard).
     *
     * @param ds_shard_id The data store shard.
     * @return NoError, or the first error the eviction met.
     */
    KvError Detach(DsShardId ds_shard_id);
};
}  // namespace eloqstore
