#pragma once
#ifdef ELOQ_MODULE_ENABLED
#include <bthread/eloq_module.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <mutex>

#include "storage/shard.h"

namespace eloqstore
{
class EloqStore;

/**
 * @brief The brpc module that runs the shards of every EloqStore in the
 * process on brpc workers.
 *
 * One module serves all running stores: a process may run several (a data
 * store service runs one per data shard it leads), and brpc registers few
 * modules, each registration or removal waiting for every worker. Worker
 * thd_id runs shard thd_id of each store in turn. A store joins at Start
 * (AddStore) and leaves once its shards have stopped (RemoveStore), which
 * waits until no worker is still running one of them.
 */
class EloqStoreModule : public eloq::EloqModule
{
public:
    /**
     * @brief The process's module, registered with brpc when the first store
     * joins.
     * @return The module.
     */
    static EloqStoreModule &Instance();

    eloq::ModuleType Type() const override
    {
        return eloq::ModuleType::kEloqStore;
    }

    void ExtThdStart(int thd_id) override;
    void ExtThdEnd(int thd_id) override;
    void Process(int thd_id) override;
    bool HasTask(int thd_id) const override;

    /**
     * @brief Adds a store whose shards the workers start running.
     * @param store The store, its shards started.
     */
    void AddStore(EloqStore *store);

    /**
     * @brief Removes a store, and waits until no worker is running one of its
     * shards. Called once the shards have stopped: the workers run them until
     * then.
     * @param store The store.
     */
    void RemoveStore(EloqStore *store);

    /** @brief How many stores may run in one process at once. */
    static constexpr size_t kMaxStores = 8;

private:
    EloqStoreModule() = default;

    /** @brief A store the workers run, and how many are using it now. */
    struct Slot
    {
        std::atomic<EloqStore *> store{nullptr};  ///< nullptr: free.
        mutable std::atomic<int> users{0};  ///< Workers inside the store now.
    };

    /**
     * @brief Calls fn on shard thd_id of each store, until fn returns true.
     * @param thd_id The worker, and the shard index.
     * @param fn Called as fn(Shard *); true stops the walk.
     * @return true if fn returned true.
     */
    template <typename Fn>
    bool ForEachShard(int thd_id, Fn &&fn) const;

    std::array<Slot, kMaxStores> slots_;
    std::mutex mux_;          ///< Serializes AddStore and RemoveStore.
    bool registered_{false};  ///< Registered with brpc; guarded by mux_.
};

}  // namespace eloqstore
#endif
