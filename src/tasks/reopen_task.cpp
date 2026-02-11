#include "tasks/reopen_task.h"

#include "storage/index_page_manager.h"
#include "storage/shard.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    return shard->IndexManager()->InstallExternalSnapshot(tbl_id, cow_meta_);
}

}  // namespace eloqstore
