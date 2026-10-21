#include "tasks/detach_shard_task.h"

#include "storage/shard.h"

namespace eloqstore
{
KvError DetachShardTask::Detach(DsShardId ds_shard_id)
{
    // Nothing new is admitted for the shard: the store set it Detaching
    // before sending this request. Fail what is queued, then wait for what
    // runs.
    shard->FailPendingRequestsOf(ds_shard_id);
    while (shard->ActiveTasksOf(ds_shard_id) > 0)
    {
        // The task that brings the count to 0 resumes this one
        // (Shard::OnTaskFinished).
        shard->waiting_detaches_[ds_shard_id] = this;
        status_ = TaskStatus::Blocked;
        Yield();
    }
    return shard->EvictDsShard(ds_shard_id);
}
}  // namespace eloqstore
