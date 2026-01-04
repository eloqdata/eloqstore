#include "eloqstore_module.h"

namespace eloqstore
{
#ifdef ELOQ_MODULE_ENABLED
void EloqStoreModule::ExtThdStart(int thd_id)
{
    assert(static_cast<size_t>(thd_id) < shards_->size());
    Shard *shard = shards_->at(thd_id).get();
    shard->BindExtThd();
}

void EloqStoreModule::ExtThdEnd(int thd_id)
{
    // Do nothing
}

void EloqStoreModule::Process(int thd_id)
{
    // Process tasks on this shard
    assert(static_cast<size_t>(thd_id) < shards_->size());
    Shard *shard = shards_->at(thd_id).get();
    shard->WorkOneRound();
}

bool EloqStoreModule::HasTask(int thd_id) const
{
    assert(static_cast<size_t>(thd_id) < shards_->size());
    return !shards_->at(thd_id)->IsIdle();
}
#endif

}  // namespace eloqstore
