#include "tasks/reopen_task.h"

#include <string>

#include "eloq_store.h"
#include "standby_service.h"
#include "storage/index_page_manager.h"
#include "storage/shard.h"
#include "tasks/prewarm_task.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    CHECK(request_ != nullptr);
    StoreMode mode = shard->store_->Mode();
    StandbyService *standby_service = nullptr;
    std::string tag;
    if (mode == StoreMode::StandbyReplica)
    {
        standby_service = shard->store_->GetStandbyService();
        if (standby_service == nullptr)
        {
            LOG(ERROR) << "Reopen " << tbl_id
                       << " failed: standby_service is null, tag "
                       << request_->Tag();
            request_ = nullptr;
            return KvError::InvalidArgs;
        }
        tag = request_->Tag();
        if (tag.empty())
        {
            LOG(ERROR) << "Reopen " << tbl_id << " failed: empty tag";
            request_ = nullptr;
            return KvError::InvalidArgs;
        }
        KvTask *current_task = ThdTask();
        CHECK(current_task != nullptr);
        KvError enqueue_err = standby_service->RsyncPartition(tbl_id, tag);
        if (enqueue_err != KvError::NoError)
        {
            LOG(ERROR) << "Reopen " << tbl_id << " rsync enqueue failed, tag "
                       << tag << ", error "
                       << static_cast<uint32_t>(enqueue_err);
            request_ = nullptr;
            return enqueue_err;
        }
        current_task->WaitIo();
        KvError sync_err = static_cast<KvError>(current_task->io_res_);
        if (sync_err != KvError::NoError)
        {
            LOG(ERROR) << "Reopen " << tbl_id << " rsync failed, tag " << tag
                       << ", error " << static_cast<uint32_t>(sync_err);
            request_ = nullptr;
            return sync_err;
        }
    }

    KvError err = shard->IndexManager()->InstallExternalSnapshot(
        tbl_id, cow_meta_, request_->Tag());
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "Reopen " << tbl_id
                   << " InstallExternalSnapshot failed, tag " << request_->Tag()
                   << ", mode " << static_cast<int>(mode) << ", error "
                   << static_cast<uint32_t>(err);
    }
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
    }
    request_ = nullptr;
    DLOG(INFO) << "Reopen " << tbl_id << " finish, error "
               << static_cast<uint32_t>(err);
    return err;
}

}  // namespace eloqstore
