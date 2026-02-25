#include "tasks/reopen_task.h"

#include "eloq_store.h"
#include "storage/index_page_manager.h"
#include "storage/shard.h"
#include "tasks/prewarm_task.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    KvError err =
        shard->IndexManager()->InstallExternalSnapshot(tbl_id, cow_meta_);
    if (err == KvError::NoError && !Options()->cloud_store_path.empty())
    {
        if (Options()->prewarm_cloud_cache)
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
    return err;
}

}  // namespace eloqstore
