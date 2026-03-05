#include "tasks/reopen_task.h"

#include "eloq_store.h"
#include "standby_service.h"
#include "storage/index_page_manager.h"
#include "storage/shard.h"
#include "tasks/prewarm_task.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    StoreMode mode = shard->store_->Mode();
    StandbyService *standby_service = nullptr;
    uint64_t snapshot_ts = 0;
    if (mode == StoreMode::StandbyReplica)
    {
        standby_service = shard->store_->GetStandbyService();
        if (standby_service == nullptr || request_ == nullptr)
        {
            request_ = nullptr;
            return KvError::InvalidArgs;
        }
        snapshot_ts = request_->SnapshotTimestamp();
        if (snapshot_ts == 0)
        {
            request_ = nullptr;
            return KvError::InvalidArgs;
        }
        KvTask *current_task = ThdTask();
        CHECK(current_task != nullptr);
        KvError enqueue_err =
            standby_service->RsyncPartition(tbl_id, snapshot_ts);
        if (enqueue_err != KvError::NoError)
        {
            request_ = nullptr;
            return enqueue_err;
        }
        current_task->WaitIo();
        KvError sync_err = static_cast<KvError>(current_task->io_res_);
        if (sync_err != KvError::NoError)
        {
            request_ = nullptr;
            return sync_err;
        }
    }

    KvError err =
        shard->IndexManager()->InstallExternalSnapshot(tbl_id, cow_meta_);
    if (err == KvError::NoError && mode != StoreMode::Local)
    {
        if (mode == StoreMode::Cloud && Options()->prewarm_cloud_cache)
        {
            CHECK(shard->store_ != nullptr);
            PrewarmService *prewarm_service =
                shard->store_->GetPrewarmService();
            CHECK(prewarm_service != nullptr);
            prewarm_service->Prewarm(tbl_id);
        }

        if (!shard->HasPendingLocalGc(tbl_id))
        {
            shard->AddPendingLocalGc(tbl_id);
        }
        if (mode == StoreMode::StandbyReplica && standby_service != nullptr &&
            snapshot_ts != 0)
        {
            KvTask *current_task = ThdTask();
            CHECK(current_task != nullptr);
            KvError cleanup_enqueue_err =
                standby_service->CleanupLocalManifest(tbl_id);
            if (cleanup_enqueue_err == KvError::NoError)
            {
                current_task->WaitIo();
                KvError cleanup_err =
                    static_cast<KvError>(current_task->io_res_);
                if (cleanup_err != KvError::NoError)
                {
                    LOG(WARNING) << "StandbyService cleanup failed for "
                                 << tbl_id << " snapshot " << snapshot_ts
                                 << ": " << ErrorString(cleanup_err);
                }
            }
        }
    }
    request_ = nullptr;
    return err;
}

}  // namespace eloqstore
