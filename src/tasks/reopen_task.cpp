#include "tasks/reopen_task.h"

#include "storage/index_page_manager.h"
#include "storage/shard.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    KvError err =
        shard->IndexManager()->InstallExternalSnapshot(tbl_id, cow_meta_);
    if (err == KvError::NoError && !Options()->cloud_store_path.empty())
    {
        if (!shard->HasPendingLocalGc(tbl_id))
        {
            shard->AddPendingLocalGc(tbl_id);
        }
    }
    return err;
}

}  // namespace eloqstore
