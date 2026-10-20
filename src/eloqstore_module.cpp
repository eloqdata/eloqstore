#include "eloqstore_module.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#include <glog/logging.h>

#include "eloq_store.h"
#endif

namespace eloqstore
{
#ifdef ELOQ_MODULE_ENABLED
EloqStoreModule &EloqStoreModule::Instance()
{
    // Never destroyed: brpc workers may visit the module until the process
    // exits.
    static EloqStoreModule *module = new EloqStoreModule();
    return *module;
}

template <typename Fn>
bool EloqStoreModule::ForEachShard(int thd_id, Fn &&fn) const
{
    for (const Slot &slot : slots_)
    {
        if (slot.store.load(std::memory_order_relaxed) == nullptr)
        {
            continue;
        }
        // Announce, then read the store: RemoveStore clears the slot, then
        // waits for the users to leave, so it either sees this worker or this
        // worker sees the slot cleared.
        slot.users.fetch_add(1, std::memory_order_seq_cst);
        EloqStore *store = slot.store.load(std::memory_order_seq_cst);
        bool stop = false;
        if (store != nullptr &&
            static_cast<size_t>(thd_id) < store->shards_.size() &&
            store->shards_[thd_id] != nullptr)
        {
            stop = fn(store->shards_[thd_id].get());
        }
        slot.users.fetch_sub(1, std::memory_order_release);
        if (stop)
        {
            return true;
        }
    }
    return false;
}

void EloqStoreModule::ExtThdStart(int thd_id)
{
    ForEachShard(thd_id,
                 [](Shard *shard)
                 {
                     shard->BindExtThd();
                     return false;
                 });
}

void EloqStoreModule::ExtThdEnd(int thd_id)
{
    // Do nothing
}

void EloqStoreModule::Process(int thd_id)
{
    ForEachShard(thd_id,
                 [](Shard *shard)
                 {
                     // The worker runs every store's shard in turn: bind this
                     // one's shard and store before running it.
                     shard->BindExtThd();
                     shard->WorkOneRound();
                     return false;
                 });
}

bool EloqStoreModule::HasTask(int thd_id) const
{
    return ForEachShard(thd_id,
                        [](Shard *shard)
                        { return !shard->IsIdle() || shard->NeedStop(); });
}

void EloqStoreModule::AddStore(EloqStore *store)
{
    std::lock_guard<std::mutex> lk(mux_);
    bool added = false;
    for (Slot &slot : slots_)
    {
        if (slot.store.load(std::memory_order_relaxed) == nullptr)
        {
            slot.store.store(store, std::memory_order_seq_cst);
            added = true;
            break;
        }
    }
    CHECK(added) << "More than " << kMaxStores
                 << " EloqStores running in one process";
    if (!registered_)
    {
        eloq::register_module(this);
        registered_ = true;
    }
}

void EloqStoreModule::RemoveStore(EloqStore *store)
{
    std::lock_guard<std::mutex> lk(mux_);
    for (Slot &slot : slots_)
    {
        if (slot.store.load(std::memory_order_relaxed) != store)
        {
            continue;
        }
        slot.store.store(nullptr, std::memory_order_seq_cst);
        while (slot.users.load(std::memory_order_seq_cst) != 0)
        {
            bthread_usleep(100);
        }
        return;
    }
}
#endif

}  // namespace eloqstore
