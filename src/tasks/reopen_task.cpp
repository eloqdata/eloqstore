#include "tasks/reopen_task.h"

#include <string>

#include "eloq_store.h"
#include "standby_service.h"
#include "storage/page_manager.h"
#include "storage/shard.h"
#include "tasks/prewarm_task.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    auto *request = static_cast<ReopenRequest *>(req_);
    CHECK(request != nullptr);
    StoreMode mode = shard->store_->Mode();
    if (mode == StoreMode::Local)
    {
        return KvError::InvalidArgs;
    }
    StandbyService *standby_service = nullptr;
    std::string tag;
    KvError sync_err = KvError::NoError;
    if (mode == StoreMode::StandbyReplica)
    {
        standby_service = shard->store_->GetStandbyService();
        if (standby_service == nullptr)
        {
            LOG(ERROR) << "Reopen " << tbl_id
                       << " failed: standby_service is null, tag "
                       << request->Tag();
            return KvError::InvalidArgs;
        }
        tag = request->Tag();
        if (!request->Clean())
        {
            KvTask *current_task = ThdTask();
            CHECK(current_task != nullptr);
            KvError enqueue_err = standby_service->RsyncPartition(tbl_id, tag);
            if (enqueue_err != KvError::NoError)
            {
                LOG(ERROR) << "Reopen " << tbl_id
                           << " rsync enqueue failed, tag " << tag << ", error "
                           << static_cast<uint32_t>(enqueue_err);
                return enqueue_err;
            }
            current_task->WaitIo();
            sync_err = static_cast<KvError>(current_task->io_res_);
            if (sync_err != KvError::NoError && sync_err != KvError::NotFound &&
                sync_err != KvError::ResourceMissing)
            {
                LOG(ERROR) << "Reopen " << tbl_id << " rsync failed, tag "
                           << tag << ", error "
                           << static_cast<uint32_t>(sync_err);
                return sync_err;
            }
        }
    }

    KvError err = KvError::NoError;
    bool clear_local_state = request->Clean() ||
                             sync_err == KvError::NotFound ||
                             sync_err == KvError::ResourceMissing;
    if (clear_local_state)
    {
        // Remote partition no longer exists: delete the local manifest and
        // reset in-memory state to an empty root. Reopen is NOT serialized
        // against reads, so the reset goes through the COW UpdateRoot path
        // (InstallEmptyRoot) rather than mutating RootMeta in place — an
        // in-place reset would null mapper_/compression_ that a concurrent
        // read still dereferences after an IO yield. write_manifest is false
        // because DropManifest already removed it (partition absent, not
        // empty-but-present).
        err = shard->IoManager()->DropManifest(tbl_id);
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "Reopen " << tbl_id << " DropManifest failed, tag "
                       << request->Tag() << ", error "
                       << static_cast<uint32_t>(err);
            return err;
        }
        shard->IndexManager()->InstallEmptyRoot(tbl_id, cow_meta_);
    }
    else
    {
        bool install_cleared_local_state = false;
        err = shard->IndexManager()->InstallExternalSnapshot(
            tbl_id, cow_meta_, request->Tag(), install_cleared_local_state);
        if (err == KvError::NoError && install_cleared_local_state)
        {
            clear_local_state = true;
        }
    }
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "Reopen " << tbl_id
                   << " InstallExternalSnapshot failed, tag " << request->Tag()
                   << ", mode " << static_cast<int>(mode) << ", error "
                   << static_cast<uint32_t>(err);
        return err;
    }
    if (mode == StoreMode::Cloud && Options()->prewarm_cloud_cache)
    {
        CHECK(shard->store_ != nullptr);
        PrewarmService *prewarm_service = shard->store_->GetPrewarmService();
        CHECK(prewarm_service != nullptr);
        prewarm_service->Prewarm(tbl_id);
    }

    if (clear_local_state && !shard->HasPendingLocalGc(tbl_id))
    {
        shard->AddPendingLocalGc(tbl_id);
    }
    return err;
}

}  // namespace eloqstore
