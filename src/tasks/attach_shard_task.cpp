#include "tasks/attach_shard_task.h"

#include "async_io_manager.h"
#include "eloq_store.h"
#include "storage/shard.h"

namespace eloqstore
{
KvError AttachShardTask::Attach()
{
    // Only cloud mode has term files, and AttachShard sends this only then.
    CHECK(shard->store_->Mode() == StoreMode::Cloud);
    auto *cloud_mgr = static_cast<CloudStoreMgr *>(shard->IoManager());
    return cloud_mgr->UpsertTermFile(
        cloud_mgr->GetActiveBranch(), ds_shard_id_, term_);
}
}  // namespace eloqstore
