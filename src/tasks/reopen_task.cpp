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
    const bool clear_local_state = request->Clean() ||
                                   sync_err == KvError::NotFound ||
                                   sync_err == KvError::ResourceMissing;
    if (clear_local_state)
    {
        // Remote partition no longer exists: delete local manifest and
        // clear in-memory state instead of writing an empty snapshot.
        err = shard->IoManager()->DropManifest(tbl_id);
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "Reopen " << tbl_id << " DropManifest failed, tag "
                       << request->Tag() << ", error "
                       << static_cast<uint32_t>(err);
            return err;
        }
        RootMetaMgr *root_meta_mgr = shard->IndexManager()->RootMetaManager();
        auto *entry = root_meta_mgr->Find(tbl_id);
        if (entry != nullptr)
        {
            RootMeta &meta = entry->meta_;
            meta.root_id_ = MaxPageId;
            meta.ttl_root_id_ = MaxPageId;
            meta.manifest_size_ = 0;
            meta.next_expire_ts_ = 0;
            meta.mapper_.reset();
            meta.mapping_snapshots_.clear();
            meta.compression_.reset();
            root_meta_mgr->UpdateBytes(entry, 0);
        }
        cow_meta_ = CowRootMeta();
        cow_meta_.root_id_ = MaxPageId;
        cow_meta_.ttl_root_id_ = MaxPageId;
    }
    else
    {
        err = shard->IndexManager()->InstallExternalSnapshot(
            tbl_id, cow_meta_, request->Tag());
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

    if (clear_local_state)
    {
        if (!shard->HasPendingLocalGc(tbl_id))
        {
            shard->AddPendingLocalGc(tbl_id);
        }
    }
    else
    {
        const KvOptions *opts = Options();
        if (opts->data_append_mode)
        {
            CompactIfNeeded(cow_meta_.mapper_.get(),
                            opts->file_amplify_factor);
        }
        if (cow_meta_.segment_mapper_ != nullptr)
        {
            CompactIfNeeded(cow_meta_.segment_mapper_.get(),
                            opts->segment_file_amplify_factor);
        }
    }
    return err;
}

}  // namespace eloqstore
