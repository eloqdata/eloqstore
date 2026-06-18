#include <glog/logging.h>

#include "internal.h"

namespace eloqstore::sdk
{

using namespace runtime_internal;

namespace
{

uint64_t SteadyNowNs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace

bool KVCacheRuntimeManagerOps::BeginSave(KVCacheManager *manager,
                                         size_t shard_index,
                                         const std::string &key,
                                         uint32_t partition_id,
                                         uint32_t payload_bytes,
                                         KVCacheBufferHandle *out_buffer,
                                         std::string *error_message)
{
    auto &shard = manager->impl_->shards[shard_index];
    uint32_t entry_id = 0;
    if (!KVCacheRuntimeHelpers::AllocateEntryForRequest(
            manager,
            shard_index,
            &shard,
            key,
            partition_id,
            KVCacheManager::Impl::EntryStateKind::ReservedForSave,
            &entry_id,
            error_message))
    {
        return false;
    }

    auto &entry = manager->impl_->entries[entry_id];
    entry.partition_id = partition_id;
    entry.payload_bytes = payload_bytes;
    entry.key = key;
    const uint64_t request_id =
        MakeRequestId(manager->impl_->next_request_sequence,
                      manager->impl_->shards.size(),
                      shard.layout.shard_id);

    {
        std::lock_guard<std::mutex> state_lock(
            manager->impl_->request_state_mutex);
        manager->impl_->request_states[request_id] = KVCacheRequestState{
            .request_id = request_id,
            .status = KVCacheRequestStatus::Pending,
            .offset_bytes = KVCacheRuntimeHelpers::EntryOffsetBytes(
                manager->options_, entry_id),
            .payload_bytes = payload_bytes,
        };
    }
    if (out_buffer != nullptr)
    {
        out_buffer->request_id = request_id;
        out_buffer->offset_bytes = KVCacheRuntimeHelpers::EntryOffsetBytes(
            manager->options_, entry_id);
        out_buffer->payload_bytes = payload_bytes;
    }
    return true;
}

bool KVCacheRuntimeManagerOps::FinishSave(KVCacheManager *manager,
                                          uint64_t request_id,
                                          std::string *error_message)
{
    const size_t shard_index =
        RequestShardIndex(request_id, manager->impl_->shards.size());
    auto &shard = manager->impl_->shards[shard_index];
    uint32_t entry_id = 0;
    uint32_t payload_bytes = 0;

    std::lock_guard<std::mutex> lock(shard.mutex);
    {
        std::lock_guard<std::mutex> state_lock(
            manager->impl_->request_state_mutex);
        const auto state_it = manager->impl_->request_states.find(request_id);
        if (state_it == manager->impl_->request_states.end())
        {
            if (error_message != nullptr)
            {
                *error_message = "unknown save request id";
            }
            return false;
        }
        entry_id = static_cast<uint32_t>(
            state_it->second.offset_bytes /
            std::max<uint64_t>(1, manager->options_.entry_size));
        payload_bytes = state_it->second.payload_bytes;
    }
    if (entry_id >= manager->impl_->entries.size())
    {
        if (error_message != nullptr)
        {
            *error_message = "request entry is out of range";
        }
        return false;
    }
    auto &entry = manager->impl_->entries[entry_id];
    entry.state = KVCacheManager::Impl::EntryStateKind::MemoryOnlyDirty;
    entry.flush_in_progress = false;
    entry.payload_bytes = payload_bytes;
    shard
        .resident_entries[MakeResidentIndexKey(entry.key, entry.partition_id)] =
        entry_id;
    KVCacheRuntimeHelpers::TouchResidentLru(&shard, entry_id);
    shard.last_flush_error.clear();
    KVCacheRuntimeHelpers::EnqueueFlushWork(manager->impl_,
                                            &shard,
                                            static_cast<uint32_t>(shard_index),
                                            &entry,
                                            false);

    {
        std::lock_guard<std::mutex> state_lock(
            manager->impl_->request_state_mutex);
        auto &state = manager->impl_->request_states[request_id];
        state.status = KVCacheRequestStatus::Ready;
        state.offset_bytes = KVCacheRuntimeHelpers::EntryOffsetBytes(
            manager->options_, entry_id);
        state.payload_bytes = payload_bytes;
        state.error_message.clear();
    }
    return true;
}

bool KVCacheRuntimeManagerOps::BeginLoad(KVCacheManager *manager,
                                         size_t shard_index,
                                         const std::string &key,
                                         uint32_t partition_id,
                                         uint32_t payload_bytes,
                                         uint64_t *out_request_id,
                                         std::string *error_message)
{
    auto &shard = manager->impl_->shards[shard_index];
    const uint64_t request_id =
        MakeRequestId(manager->impl_->next_request_sequence,
                      manager->impl_->shards.size(),
                      shard.layout.shard_id);
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        const auto it = shard.resident_entries.find(
            MakeResidentIndexKey(key, partition_id));
        if (it != shard.resident_entries.end())
        {
            const auto &entry = manager->impl_->entries[it->second];
            if (KVCacheRuntimeHelpers::IsResidentState(entry.state))
            {
                KVCacheRuntimeHelpers::TouchResidentLru(&shard, entry.entry_id);
                std::lock_guard<std::mutex> state_lock(
                    manager->impl_->request_state_mutex);
                manager->impl_->request_states[request_id] =
                    KVCacheRequestState{
                        .request_id = request_id,
                        .status = KVCacheRequestStatus::Ready,
                        .offset_bytes = KVCacheRuntimeHelpers::EntryOffsetBytes(
                            manager->options_, entry.entry_id),
                        .payload_bytes = entry.payload_bytes,
                    };
                if (out_request_id != nullptr)
                {
                    *out_request_id = request_id;
                }
                return true;
            }
        }
    }

    uint32_t entry_id = 0;
    if (!KVCacheRuntimeHelpers::AllocateEntryForRequest(
            manager,
            shard_index,
            &shard,
            key,
            partition_id,
            KVCacheManager::Impl::EntryStateKind::ReservedForLoad,
            &entry_id,
            error_message))
    {
        return false;
    }

    void *entry_ptr =
        static_cast<char *>(manager->shm_addr_) +
        KVCacheRuntimeHelpers::EntryOffsetBytes(manager->options_, entry_id);

    EloqStore *store = manager->impl_->store.get();
    if (store == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "eloqstore runtime is not started";
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &entry = manager->impl_->entries[entry_id];
        if (entry.state == KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
        {
            entry.generation += 1;
            KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
            shard.free_entries.push_back(entry_id);
            shard.cv.notify_all();
        }
        return false;
    }

    const uint64_t load_start_ns = SteadyNowNs();
    TableIdent table(manager->options_.table_name, partition_id);
    ReadRequest read_req;
    read_req.SetArgs(table, key);
    const size_t read_bytes = PinnedReadBytes(payload_bytes);
    read_req.large_value_dest_ =
        std::make_pair(reinterpret_cast<char *>(entry_ptr), read_bytes);
    store->ExecSync(&read_req);
    manager->impl_->load_store_latency_ns_total.fetch_add(
        SteadyNowNs() - load_start_ns, std::memory_order_relaxed);

    if (read_req.Error() != KvError::NoError)
    {
        if (error_message != nullptr)
        {
            *error_message = read_req.Error() == KvError::NotFound
                                  ? "kv cache key not found"
                                  : "eloqstore load failed";
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &entry = manager->impl_->entries[entry_id];
        if (entry.state == KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
        {
            entry.generation += 1;
            KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
            shard.free_entries.push_back(entry_id);
            shard.cv.notify_all();
        }
        manager->impl_->load_store_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    uint32_t loaded_payload_bytes = payload_bytes;
    if (!DecodePayloadMetadata(read_req.value_, payload_bytes,
                                &loaded_payload_bytes))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid kv cache load metadata";
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &entry = manager->impl_->entries[entry_id];
        if (entry.state == KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
        {
            entry.generation += 1;
            KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
            shard.free_entries.push_back(entry_id);
            shard.cv.notify_all();
        }
        manager->impl_->load_store_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (loaded_payload_bytes > manager->options_.entry_size)
    {
        if (error_message != nullptr)
        {
            *error_message = "loaded payload exceeds entry_size";
        }
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &entry = manager->impl_->entries[entry_id];
        if (entry.state == KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
        {
            entry.generation += 1;
            KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
            shard.free_entries.push_back(entry_id);
            shard.cv.notify_all();
        }
        manager->impl_->load_store_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &entry = manager->impl_->entries[entry_id];
        entry.state =
            KVCacheManager::Impl::EntryStateKind::MemoryAndEloqStoreClean;
        entry.flush_enqueued = false;
        entry.flush_in_progress = false;
        entry.flush_after_write = false;
        entry.partition_id = partition_id;
        entry.payload_bytes = loaded_payload_bytes;
        entry.key = key;
        shard.resident_entries[MakeResidentIndexKey(key, partition_id)] =
            entry_id;
        KVCacheRuntimeHelpers::TouchResidentLru(&shard, entry_id);
    }

    {
        std::lock_guard<std::mutex> state_lock(
            manager->impl_->request_state_mutex);
        manager->impl_->request_states[request_id] = KVCacheRequestState{
            .request_id = request_id,
            .status = KVCacheRequestStatus::Ready,
            .offset_bytes = KVCacheRuntimeHelpers::EntryOffsetBytes(
                manager->options_, entry_id),
            .payload_bytes = loaded_payload_bytes,
        };
    }
    manager->impl_->load_store_hits.fetch_add(1, std::memory_order_relaxed);
    manager->impl_->load_store_bytes.fetch_add(loaded_payload_bytes,
                                                std::memory_order_relaxed);
    if (out_request_id != nullptr)
    {
        *out_request_id = request_id;
    }
    return true;
}

bool KVCacheRuntimeManagerOps::ContainsKey(KVCacheManager *manager,
                                           size_t shard_index,
                                           const std::string &key,
                                           uint32_t partition_id,
                                           bool *out_exists,
                                           std::string *error_message)
{
    auto &shard = manager->impl_->shards[shard_index];
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (shard.resident_entries.contains(
                MakeResidentIndexKey(key, partition_id)))
        {
            manager->impl_->contains_memory_hits.fetch_add(
                1, std::memory_order_relaxed);
            *out_exists = true;
            return true;
        }
    }

    EloqStore *store = manager->impl_->store.get();
    if (store == nullptr)
    {
        manager->impl_->contains_store_errors.fetch_add(
            1, std::memory_order_relaxed);
        if (error_message != nullptr)
        {
            *error_message = "eloqstore runtime is not started";
        }
        return false;
    }

    const uint64_t lookup_start_ns = SteadyNowNs();
    TableIdent table(manager->options_.table_name, partition_id);
    ReadRequest read_req;
    read_req.SetArgs(table, key);
    store->ExecSync(&read_req);
    manager->impl_->contains_store_lookup_ns_total.fetch_add(
        SteadyNowNs() - lookup_start_ns, std::memory_order_relaxed);

    if (read_req.Error() == KvError::NoError)
    {
        manager->impl_->contains_store_hits.fetch_add(
            1, std::memory_order_relaxed);
        *out_exists = true;
        return true;
    }
    if (read_req.Error() == KvError::NotFound)
    {
        manager->impl_->contains_store_misses.fetch_add(
            1, std::memory_order_relaxed);
        *out_exists = false;
        return true;
    }

    manager->impl_->contains_store_errors.fetch_add(1,
                                                    std::memory_order_relaxed);
    if (error_message != nullptr)
    {
        *error_message = "eloqstore contains-key lookup failed";
    }
    return false;
}

bool KVCacheRuntimeManagerOps::ContainsKeys(
    KVCacheManager *manager,
    const std::vector<size_t> &shard_indices,
    const std::vector<std::string> &keys,
    const std::vector<uint32_t> &partition_ids,
    std::vector<bool> *out_exists,
    std::string *error_message)
{
    const size_t n = keys.size();
    out_exists->assign(n, false);
    std::vector<bool> needs_store_probe(n, false);
    size_t memory_hits = 0;

    for (size_t i = 0; i < n; ++i)
    {
        auto &shard = manager->impl_->shards[shard_indices[i]];
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (shard.resident_entries.contains(
                MakeResidentIndexKey(keys[i], partition_ids[i])))
        {
            (*out_exists)[i] = true;
            ++memory_hits;
        }
        else
        {
            needs_store_probe[i] = true;
        }
    }
    manager->impl_->contains_memory_hits.fetch_add(
        memory_hits, std::memory_order_relaxed);

    EloqStore *store = manager->impl_->store.get();
    if (store == nullptr)
    {
        size_t store_errors = 0;
        for (size_t i = 0; i < n; ++i)
        {
            if (needs_store_probe[i])
            {
                ++store_errors;
            }
        }
        manager->impl_->contains_store_errors.fetch_add(
            store_errors, std::memory_order_relaxed);
        if (error_message != nullptr)
        {
            *error_message = "eloqstore runtime is not started";
        }
        return false;
    }

    size_t store_hits = 0;
    size_t store_misses = 0;
    size_t store_errors = 0;
    const uint64_t probe_start_ns = SteadyNowNs();

    for (size_t i = 0; i < n; ++i)
    {
        if (!needs_store_probe[i])
        {
            continue;
        }
        TableIdent table(manager->options_.table_name, partition_ids[i]);
        ReadRequest read_req;
        read_req.SetArgs(table, keys[i]);
        store->ExecSync(&read_req);

        if (read_req.Error() == KvError::NoError)
        {
            (*out_exists)[i] = true;
            ++store_hits;
        }
        else if (read_req.Error() == KvError::NotFound)
        {
            (*out_exists)[i] = false;
            ++store_misses;
        }
        else
        {
            (*out_exists)[i] = false;
            ++store_errors;
        }
    }

    manager->impl_->contains_store_lookup_ns_total.fetch_add(
        SteadyNowNs() - probe_start_ns, std::memory_order_relaxed);
    manager->impl_->contains_store_hits.fetch_add(
        store_hits, std::memory_order_relaxed);
    manager->impl_->contains_store_misses.fetch_add(
        store_misses, std::memory_order_relaxed);
    if (store_errors > 0)
    {
        manager->impl_->contains_store_errors.fetch_add(
            store_errors, std::memory_order_relaxed);
        if (error_message != nullptr)
        {
            *error_message = "eloqstore contains-keys lookup failed";
        }
        return false;
    }
    return true;
}

bool KVCacheRuntimeManagerOps::BeginLoads(
    KVCacheManager *manager,
    const std::vector<size_t> &shard_indices,
    const std::vector<std::string> &keys,
    const std::vector<uint32_t> &partition_ids,
    const std::vector<uint32_t> &payload_bytes_list,
    std::vector<uint64_t> *out_request_ids,
    std::string *error_message)
{
    const size_t n = keys.size();
    out_request_ids->resize(n, 0);

    struct PendingLoad
    {
        size_t index;
        uint32_t entry_id;
    };
    std::vector<PendingLoad> pending_loads;

    for (size_t i = 0; i < n; ++i)
    {
        auto &shard = manager->impl_->shards[shard_indices[i]];
        const uint64_t request_id =
            MakeRequestId(manager->impl_->next_request_sequence,
                          manager->impl_->shards.size(),
                          shard.layout.shard_id);
        (*out_request_ids)[i] = request_id;

        bool memory_hit = false;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            const auto it = shard.resident_entries.find(
                MakeResidentIndexKey(keys[i], partition_ids[i]));
            if (it != shard.resident_entries.end())
            {
                const auto &entry = manager->impl_->entries[it->second];
                if (KVCacheRuntimeHelpers::IsResidentState(entry.state))
                {
                    KVCacheRuntimeHelpers::TouchResidentLru(&shard,
                                                           entry.entry_id);
                    std::lock_guard<std::mutex> state_lock(
                        manager->impl_->request_state_mutex);
                    manager->impl_->request_states[request_id] =
                        KVCacheRequestState{
                            .request_id = request_id,
                            .status = KVCacheRequestStatus::Ready,
                            .offset_bytes =
                                KVCacheRuntimeHelpers::EntryOffsetBytes(
                                    manager->options_, entry.entry_id),
                            .payload_bytes = entry.payload_bytes,
                        };
                    memory_hit = true;
                }
            }
        }

        if (memory_hit)
        {
            manager->impl_->load_memory_hits.fetch_add(
                1, std::memory_order_relaxed);
            continue;
        }

        uint32_t entry_id = 0;
        if (!KVCacheRuntimeHelpers::AllocateEntryForRequest(
                manager,
                shard_indices[i],
                &shard,
                keys[i],
                partition_ids[i],
                KVCacheManager::Impl::EntryStateKind::ReservedForLoad,
                &entry_id,
                error_message))
        {
            manager->impl_->load_store_errors.fetch_add(
                1, std::memory_order_relaxed);
            continue;
        }

        pending_loads.push_back({i, entry_id});
    }

    EloqStore *store = manager->impl_->store.get();
    if (store == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "eloqstore runtime is not started";
        }
        for (const auto &pending : pending_loads)
        {
            auto &shard = manager->impl_->shards[shard_indices[pending.index]];
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto &entry = manager->impl_->entries[pending.entry_id];
            if (entry.state ==
                KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
            {
                entry.generation += 1;
                KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
                shard.free_entries.push_back(pending.entry_id);
                shard.cv.notify_all();
            }
        }
        return false;
    }

    const uint64_t load_start_ns = SteadyNowNs();
    size_t store_hits = 0;
    size_t store_errors = 0;

    for (const auto &pending : pending_loads)
    {
        const size_t i = pending.index;
        const uint32_t entry_id = pending.entry_id;
        void *entry_ptr =
            static_cast<char *>(manager->shm_addr_) +
            KVCacheRuntimeHelpers::EntryOffsetBytes(manager->options_,
                                                     entry_id);

        TableIdent table(manager->options_.table_name, partition_ids[i]);
        ReadRequest read_req;
        read_req.SetArgs(table, keys[i]);
        const size_t read_bytes =
            PinnedReadBytes(payload_bytes_list[i]);
        read_req.large_value_dest_ =
            std::make_pair(reinterpret_cast<char *>(entry_ptr), read_bytes);
        store->ExecSync(&read_req);

        if (read_req.Error() != KvError::NoError)
        {
            if (error_message != nullptr)
            {
                *error_message = read_req.Error() == KvError::NotFound
                                      ? "kv cache key not found"
                                      : "eloqstore load failed";
            }
            auto &shard = manager->impl_->shards[shard_indices[i]];
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto &entry = manager->impl_->entries[entry_id];
            if (entry.state ==
                KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
            {
                entry.generation += 1;
                KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
                shard.free_entries.push_back(entry_id);
                shard.cv.notify_all();
            }
            ++store_errors;
            continue;
        }

        uint32_t loaded_payload_bytes = payload_bytes_list[i];
        if (!DecodePayloadMetadata(read_req.value_, payload_bytes_list[i],
                                   &loaded_payload_bytes))
        {
            if (error_message != nullptr)
            {
                *error_message = "invalid kv cache load metadata";
            }
            auto &shard = manager->impl_->shards[shard_indices[i]];
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto &entry = manager->impl_->entries[entry_id];
            if (entry.state ==
                KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
            {
                entry.generation += 1;
                KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
                shard.free_entries.push_back(entry_id);
                shard.cv.notify_all();
            }
            ++store_errors;
            continue;
        }
        if (loaded_payload_bytes > manager->options_.entry_size)
        {
            if (error_message != nullptr)
            {
                *error_message = "loaded payload exceeds entry_size";
            }
            auto &shard = manager->impl_->shards[shard_indices[i]];
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto &entry = manager->impl_->entries[entry_id];
            if (entry.state ==
                KVCacheManager::Impl::EntryStateKind::ReservedForLoad)
            {
                entry.generation += 1;
                KVCacheRuntimeHelpers::ResetEntryToFree(&entry);
                shard.free_entries.push_back(entry_id);
                shard.cv.notify_all();
            }
            ++store_errors;
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(
                manager->impl_->shards[shard_indices[i]].mutex);
            auto &entry = manager->impl_->entries[entry_id];
            entry.state =
                KVCacheManager::Impl::EntryStateKind::MemoryAndEloqStoreClean;
            entry.flush_enqueued = false;
            entry.flush_in_progress = false;
            entry.flush_after_write = false;
            entry.partition_id = partition_ids[i];
            entry.payload_bytes = loaded_payload_bytes;
            entry.key = keys[i];
            manager->impl_->shards[shard_indices[i]].resident_entries
                [MakeResidentIndexKey(keys[i], partition_ids[i])] = entry_id;
            KVCacheRuntimeHelpers::TouchResidentLru(
                &manager->impl_->shards[shard_indices[i]], entry_id);
        }

        {
            std::lock_guard<std::mutex> state_lock(
                manager->impl_->request_state_mutex);
            manager->impl_->request_states[(*out_request_ids)[i]] =
                KVCacheRequestState{
                    .request_id = (*out_request_ids)[i],
                    .status = KVCacheRequestStatus::Ready,
                    .offset_bytes =
                        KVCacheRuntimeHelpers::EntryOffsetBytes(
                            manager->options_, entry_id),
                    .payload_bytes = loaded_payload_bytes,
                };
        }
        ++store_hits;
        manager->impl_->load_store_bytes.fetch_add(
            loaded_payload_bytes, std::memory_order_relaxed);
    }

    manager->impl_->load_store_latency_ns_total.fetch_add(
        SteadyNowNs() - load_start_ns, std::memory_order_relaxed);
    manager->impl_->load_store_hits.fetch_add(
        store_hits, std::memory_order_relaxed);
    manager->impl_->load_store_errors.fetch_add(
        store_errors, std::memory_order_relaxed);

    if (store_errors > 0)
    {
        return false;
    }
    return true;
}

// Manager-side runtime:
// - owns the shared pinned-memory pool
// - runs the background flush path
// - serves scheduler-local contains() and worker save/load requests
KVCacheManager::KVCacheManager(KVCacheOptions options)
    : options_(std::move(options))
{
    // Keep the public class small and hide runtime state behind Impl so the C
    // API can pass opaque manager handles across the FFI boundary.
    impl_ = new Impl();
}

KVCacheManager::~KVCacheManager()
{
    // Destruction is equivalent to an explicit Stop plus impl cleanup.
    Stop();
    delete impl_;
    impl_ = nullptr;
}

bool KVCacheManager::Start(std::string *error_message)
{
    // Start establishes the manager-owned control plane and shared-memory pool
    // but does not yet register the segment with EloqStore pinned I/O.
    if (started_)
    {
        return true;
    }
    if (options_.shared_memory_bytes == 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "shared_memory_bytes must be greater than zero";
        }
        return false;
    }
    if (options_.entry_size == 0 || options_.entry_count == 0)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "entry_size and entry_count must both be greater than zero";
        }
        return false;
    }
    if (options_.entry_alignment == 0 ||
        (options_.entry_alignment & (options_.entry_alignment - 1)) != 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "entry_alignment must be a non-zero power of two";
        }
        return false;
    }
    if (options_.num_threads == 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "num_threads must be greater than zero";
        }
        return false;
    }
    if (options_.num_threads > options_.entry_count)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "num_threads must be less than or equal to entry_count";
        }
        return false;
    }
    if (options_.entry_size % options_.entry_alignment != 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "entry_size must be aligned to entry_alignment";
        }
        return false;
    }
    if (options_.submission_queue_depth == 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "submission_queue_depth must be greater than zero";
        }
        return false;
    }
    if (options_.partition_count == 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "partition_count must be greater than zero";
        }
        return false;
    }

    const std::string shm_name = BuildDefaultSharedMemoryName(options_);
    // Reuse a stable per-deployment shm name. If a previous manager crashed and
    // leaked the segment, unlink it before recreating the shared buffer.
    CleanupSharedMemory(shm_name);
    shm_fd_ = CreateSharedMemory(shm_name);
    if (shm_fd_ < 0)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "shm_open(create) failed: " + std::string(std::strerror(errno));
        }
        return false;
    }
    if (::ftruncate(shm_fd_,
                    static_cast<off_t>(options_.shared_memory_bytes)) != 0)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "ftruncate failed: " + std::string(std::strerror(errno));
        }
        ::close(shm_fd_);
        shm_fd_ = -1;
        return false;
    }
    shm_addr_ = ::mmap(nullptr,
                       options_.shared_memory_bytes,
                       PROT_READ | PROT_WRITE,
                       MAP_SHARED,
                       shm_fd_,
                       0);
    if (shm_addr_ == MAP_FAILED)
    {
        shm_addr_ = nullptr;
        if (error_message != nullptr)
        {
            *error_message =
                "mmap failed: " + std::string(std::strerror(errno));
        }
        ::close(shm_fd_);
        shm_fd_ = -1;
        return false;
    }

    options_.shared_memory_name = shm_name;
    shm_path_ = BuildSharedMemoryFsPath(shm_name);
    // The shared segment is partitioned once at startup. Workers later attach
    // to the same physical pages, but each process keeps its own registration
    // state for CUDA or io_uring/EloqStore.
    shards_ = BuildShards(options_);
    impl_->entries.clear();
    impl_->entries.reserve(options_.entry_count);
    for (uint32_t entry_id = 0; entry_id < options_.entry_count; ++entry_id)
    {
        impl_->entries.push_back(Impl::BufferEntryState{
            .entry_id = entry_id,
            .generation = 0,
        });
    }
    impl_->shards.clear();
    for (const auto &shard_layout : shards_)
    {
        impl_->shards.emplace_back();
        auto &shard_state = impl_->shards.back();
        shard_state.layout = shard_layout;
        for (uint32_t entry_id = shard_layout.start_entry;
             entry_id < shard_layout.start_entry + shard_layout.entry_count;
             ++entry_id)
        {
            shard_state.free_entries.push_back(entry_id);
        }
    }
    impl_->stopping = false;
    impl_->ipc_endpoint = options_.ipc_path;
    if (!impl_->ipc_endpoint.empty())
    {
        try
        {
            impl_->zmq_context = std::make_unique<zmq::context_t>(1);
            impl_->ipc_socket = std::make_unique<zmq::socket_t>(
                *impl_->zmq_context, zmq::socket_type::rep);
            impl_->ipc_socket->set(zmq::sockopt::linger, 0);
            impl_->ipc_socket->set(zmq::sockopt::rcvtimeo, 100);
            impl_->ipc_socket->set(zmq::sockopt::sndtimeo, 100);
            impl_->ipc_socket->bind(impl_->ipc_endpoint);
        }
        catch (const zmq::error_t &e)
        {
            if (error_message != nullptr)
            {
                *error_message = std::string("ZeroMQ bind failed: ") + e.what();
            }
            Stop();
            return false;
        }
        impl_->ipc_thread = std::thread(
            [this]()
            {
                while (!impl_->stopping.load())
                {
                    std::vector<std::string> request_frames;
                    std::string transport_error;
                    if (!ReceiveFrames(*impl_->ipc_socket,
                                       &request_frames,
                                       &transport_error))
                    {
                        if (impl_->stopping.load())
                        {
                            return;
                        }
                        if (transport_error.find(
                                "Resource temporarily unavailable") !=
                            std::string::npos)
                        {
                            continue;
                        }
                        continue;
                    }
                    auto response_frames =
                        HandleManagerIpcMessage(this, request_frames);
                    transport_error.clear();
                    if (!SendFrames(*impl_->ipc_socket,
                                    response_frames,
                                    &transport_error) &&
                        impl_->stopping.load())
                    {
                        return;
                    }
                }
            });
    }
    started_ = true;
    return true;
}

void KVCacheManager::Stop()
{
    // Stop is best-effort and idempotent so both explicit shutdown and wrapper
    // destructors can safely call it.
    if (impl_ != nullptr)
    {
        KVCacheRuntimeMetrics metrics;
        std::string metrics_error;
        if (GetMetrics(&metrics, &metrics_error))
        {
            LOG(INFO)
                << "KVCacheRuntimeMetrics "
                << "flush_batches_submitted=" << metrics.flush_batches_submitted
                << " flush_batches_completed="
                << metrics.flush_batches_completed
                << " flush_batches_failed=" << metrics.flush_batches_failed
                << " flush_entries_submitted="
                << metrics.flush_entries_submitted
                << " flush_entries_completed="
                << metrics.flush_entries_completed
                << " flush_entries_failed=" << metrics.flush_entries_failed
                << " flush_batch_latency_ns_total="
                << metrics.flush_batch_latency_ns_total
                << " contains_memory_hits=" << metrics.contains_memory_hits
                << " contains_store_hits=" << metrics.contains_store_hits
                << " contains_store_misses=" << metrics.contains_store_misses
                << " contains_store_errors=" << metrics.contains_store_errors
                << " contains_store_lookup_ns_total="
                << metrics.contains_store_lookup_ns_total
                << " load_memory_hits=" << metrics.load_memory_hits
                << " load_store_hits=" << metrics.load_store_hits
                << " load_store_errors=" << metrics.load_store_errors
                << " load_store_bytes=" << metrics.load_store_bytes
                << " load_store_latency_ns_total="
                << metrics.load_store_latency_ns_total
                << " dirty_entries_current=" << metrics.dirty_entries_current
                << " flush_queue_entries_current="
                << metrics.flush_queue_entries_current
                << " flush_inflight_entries_current="
                << metrics.flush_inflight_entries_current;
        }
        else
        {
            LOG(WARNING) << "KVCacheRuntimeMetrics unavailable: "
                         << metrics_error;
        }
    }
    if (impl_ != nullptr)
    {
        // Drain remaining dirty entries before stopping the flush thread so
        // in-flight data is persisted to EloqStore instead of silently lost.
        if (impl_->flush_thread.joinable())
        {
            const auto drain_deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(30);
            for (size_t shard_idx = 0;
                 shard_idx < impl_->shards.size();
                 ++shard_idx)
            {
                auto &shard = impl_->shards[shard_idx];
                std::lock_guard<std::mutex> lock(shard.mutex);
                const uint32_t start = shard.layout.start_entry;
                const uint32_t end = start + shard.layout.entry_count;
                for (uint32_t entry_idx = start;
                     entry_idx < end;
                     ++entry_idx)
                {
                    auto &entry = impl_->entries[entry_idx];
                    if (KVCacheRuntimeHelpers::IsDirtyState(entry.state))
                    {
                        KVCacheRuntimeHelpers::EnqueueFlushWork(
                            impl_,
                            &shard,
                            static_cast<uint32_t>(shard_idx),
                            &entry,
                            false);
                    }
                }
            }

            bool all_drained = false;
            while (!all_drained)
            {
                all_drained = true;
                for (auto &shard : impl_->shards)
                {
                    std::lock_guard<std::mutex> lock(shard.mutex);
                    if (!shard.flush_queue.empty())
                    {
                        all_drained = false;
                        break;
                    }
                }
                if (all_drained)
                {
                    bool any_inflight = false;
                    for (auto &shard : impl_->shards)
                    {
                        std::lock_guard<std::mutex> lock(shard.mutex);
                        for (uint32_t entry_idx = shard.layout.start_entry;
                             entry_idx < shard.layout.start_entry +
                                 shard.layout.entry_count;
                             ++entry_idx)
                        {
                            if (impl_->entries[entry_idx].flush_in_progress)
                            {
                                any_inflight = true;
                                break;
                            }
                        }
                        if (any_inflight)
                        {
                            break;
                        }
                    }
                    if (!any_inflight)
                    {
                        break;
                    }
                }
                if (std::chrono::steady_clock::now() >= drain_deadline)
                {
                    LOG(WARNING) << "KVCacheManager drain timed out, "
                                    "some dirty entries may not be persisted";
                    break;
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(10));
            }
        }

        impl_->stopping = true;
        impl_->flush_scheduler_cv.notify_all();
        for (auto &shard : impl_->shards)
        {
            shard.cv.notify_all();
        }
        if (impl_->flush_thread.joinable())
        {
            impl_->flush_thread.join();
        }
        impl_->shards.clear();
        impl_->entries.clear();
        {
            std::lock_guard<std::mutex> lock(impl_->request_state_mutex);
            impl_->request_states.clear();
        }
        if (impl_->store != nullptr)
        {
            impl_->store->Stop();
            impl_->store.reset();
        }
        if (impl_->zmq_context != nullptr)
        {
            impl_->zmq_context->shutdown();
        }
        if (impl_->ipc_thread.joinable())
        {
            impl_->ipc_thread.join();
        }
        if (impl_->ipc_socket != nullptr)
        {
            impl_->ipc_socket->close();
            impl_->ipc_socket.reset();
        }
        if (impl_->zmq_context != nullptr)
        {
            impl_->zmq_context->close();
            impl_->zmq_context.reset();
        }
        impl_->ipc_endpoint.clear();
    }
    io_uring_registered_ = false;
    if (shm_addr_ != nullptr)
    {
        ::munmap(shm_addr_, options_.shared_memory_bytes);
        shm_addr_ = nullptr;
    }
    if (shm_fd_ >= 0)
    {
        ::close(shm_fd_);
        shm_fd_ = -1;
    }
    if (!options_.shared_memory_name.empty())
    {
        CleanupSharedMemory(options_.shared_memory_name);
        options_.shared_memory_name.clear();
    }
    shm_path_.clear();
    shards_.clear();
    started_ = false;
}

bool KVCacheManager::RegisterIoUringBuffers(std::string *error_message)
{
    constexpr size_t kMaxRegisteredChunkBytes = 1ULL << 30;

    // This is the point where the manager-side shared-memory mapping becomes an
    // EloqStore-accessible pinned buffer region for real save/load I/O.
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before io_uring registration";
        }
        return false;
    }
    if (shm_addr_ == nullptr || shm_fd_ < 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "shared memory region is not initialized";
        }
        return false;
    }
    // EloqStore registers the long-lived shared segment when the store starts.
    // That registration is process-local and should happen once, then be
    // reused for every subsequent save/load request.
    KvOptions kv_options;
    kv_options.num_threads = options_.num_threads;
    kv_options.store_path = options_.store_paths;
    auto *base = reinterpret_cast<char *>(shm_addr_);
    size_t remaining = options_.shared_memory_bytes;
    size_t offset = 0;
    while (remaining > 0)
    {
        const size_t chunk_bytes =
            std::min(remaining, kMaxRegisteredChunkBytes);
        kv_options.pinned_memory_chunks.emplace_back(base + offset,
                                                     chunk_bytes);
        remaining -= chunk_bytes;
        offset += chunk_bytes;
    }
    if (!EloqStore::ValidateOptions(kv_options))
    {
        if (error_message != nullptr)
        {
            *error_message =
                "eloqstore kv options validation failed for kv cache manager";
        }
        return false;
    }
    if (impl_->store == nullptr)
    {
        impl_->store = std::make_unique<EloqStore>(kv_options);
        const KvError err = impl_->store->Start(
            options_.branch, options_.term, options_.partition_group_id);
        if (err != KvError::NoError)
        {
            impl_->store.reset();
            if (error_message != nullptr)
            {
                *error_message =
                    "eloqstore start failed for kv cache manager";
            }
            return false;
        }
    }
    io_uring_registered_ = true;
    if (impl_ != nullptr && !impl_->flush_thread.joinable())
    {
        impl_->flush_thread = std::thread(
            [this]()
            {
                struct PendingFlushEntry
                {
                    uint32_t entry_id{0};
                    uint32_t entry_generation{0};
                    uint32_t partition_id{0};
                    uint32_t payload_bytes{0};
                    bool release_after_write{false};
                    std::string key;
                };

                constexpr size_t kMaxFlushBatchEntries = 32;

                while (!impl_->stopping.load())
                {
                    uint32_t shard_index = 0;
                    {
                        std::unique_lock<std::mutex> scheduler_lock(
                            impl_->flush_scheduler_mutex);
                        impl_->flush_scheduler_cv.wait(
                            scheduler_lock,
                            [&]() {
                                return impl_->stopping.load() ||
                                       !impl_->runnable_flush_shards.empty();
                            });
                        if (impl_->stopping.load())
                        {
                            return;
                        }
                        shard_index = impl_->runnable_flush_shards.front();
                        impl_->runnable_flush_shards.pop_front();
                    }

                    auto &shard = impl_->shards[shard_index];
                    std::vector<PendingFlushEntry> batch_entries;
                    batch_entries.reserve(kMaxFlushBatchEntries);
                    {
                        std::lock_guard<std::mutex> lock(shard.mutex);
                        if (shard.flush_queue.empty())
                        {
                            shard.queued_for_flusher = false;
                            continue;
                        }

                        uint32_t batch_partition_id = 0;
                        while (!shard.flush_queue.empty() &&
                               batch_entries.size() < kMaxFlushBatchEntries)
                        {
                            const auto flush_item =
                                shard.flush_queue.front();
                            shard.flush_queue.pop_front();
                            if (flush_item.entry_id >=
                                impl_->entries.size())
                            {
                                continue;
                            }

                            auto &entry =
                                impl_->entries[flush_item.entry_id];
                            if (entry.generation !=
                                    flush_item.entry_generation ||
                                !KVCacheRuntimeHelpers::IsDirtyState(
                                    entry.state))
                            {
                                entry.flush_enqueued = false;
                                entry.flush_after_write = false;
                                continue;
                            }

                            if (batch_entries.empty())
                            {
                                batch_partition_id = entry.partition_id;
                            }
                            else if (entry.partition_id !=
                                     batch_partition_id)
                            {
                                shard.flush_queue.push_front(flush_item);
                                break;
                            }

                            entry.flush_enqueued = false;
                            entry.flush_in_progress = true;
                            batch_entries.push_back(PendingFlushEntry{
                                .entry_id = flush_item.entry_id,
                                .entry_generation =
                                    flush_item.entry_generation,
                                .partition_id = entry.partition_id,
                                .payload_bytes = entry.payload_bytes,
                                .release_after_write =
                                    entry.flush_after_write,
                                .key = entry.key,
                            });
                        }
                    }

                    if (batch_entries.empty())
                    {
                        std::lock_guard<std::mutex> lock(shard.mutex);
                        if (shard.flush_queue.empty())
                        {
                            shard.queued_for_flusher = false;
                        }
                        else
                        {
                            std::lock_guard<std::mutex> scheduler_lock(
                                impl_->flush_scheduler_mutex);
                            impl_->runnable_flush_shards.push_back(
                                shard_index);
                            impl_->flush_scheduler_cv.notify_one();
                        }
                        continue;
                    }

                        std::sort(batch_entries.begin(),
                                  batch_entries.end(),
                                  [](const PendingFlushEntry &lhs,
                                     const PendingFlushEntry &rhs)
                                  { return lhs.key < rhs.key; });
                        batch_entries.erase(
                            std::unique(batch_entries.begin(),
                                        batch_entries.end(),
                                        [](const PendingFlushEntry &lhs,
                                           const PendingFlushEntry &rhs)
                                        { return lhs.key == rhs.key; }),
                            batch_entries.end());

std::string flush_error;
                    EloqStore *store = impl_->store.get();
                    if (store == nullptr)
                    {
                        flush_error = "eloqstore runtime is not started";
                    }
                    else
                    {
                        TableIdent table(
                            options_.table_name,
                            batch_entries.front().partition_id);
                        auto *write_req = new BatchWriteRequest();
                        std::vector<WriteDataEntry> batch;
                        batch.reserve(batch_entries.size());
                        for (const auto &flush_entry : batch_entries)
                        {
                            void *entry_ptr =
                                static_cast<char *>(shm_addr_) +
                                KVCacheRuntimeHelpers::EntryOffsetBytes(
                                    options_, flush_entry.entry_id);
                            batch.emplace_back(
                                flush_entry.key,
                                EncodePayloadMetadata(
                                    flush_entry.payload_bytes),
                                std::make_pair(
                                    reinterpret_cast<const char *>(
                                        entry_ptr),
                                    static_cast<size_t>(
                                        flush_entry.payload_bytes)),
                                0,
                                WriteOp::Upsert);
                        }
                        write_req->SetArgs(table, std::move(batch));
                        const uint32_t callback_shard_index =
                            shard_index;
                        Impl *callback_impl = impl_;
                        const uint64_t batch_start_ns = SteadyNowNs();
                        impl_->flush_batches_submitted.fetch_add(
                            1, std::memory_order_relaxed);
                        impl_->flush_entries_submitted.fetch_add(
                            batch_entries.size(),
                            std::memory_order_relaxed);
                        const bool submitted = store->ExecAsyn(
                            write_req,
                            0,
                            [callback_impl,
                             callback_shard_index,
                             callback_entries =
                                 std::move(batch_entries),
                             batch_start_ns](KvRequest *done_req)
                            {
                                auto *batch_req =
                                    static_cast<BatchWriteRequest *>(
                                        done_req);
                                auto &callback_shard =
                                    callback_impl
                                        ->shards[callback_shard_index];
                                {
                                    std::lock_guard<std::mutex> lock(
                                        callback_shard.mutex);
                                    const bool success =
                                        batch_req->Error() ==
                                        KvError::NoError;
                                    callback_impl
                                        ->flush_batch_latency_ns_total
                                        .fetch_add(
                                            SteadyNowNs() -
                                                batch_start_ns,
                                            std::memory_order_relaxed);
                                    if (success)
                                    {
                                        callback_impl
                                            ->flush_batches_completed
                                            .fetch_add(
                                                1,
                                                std::
                                                    memory_order_relaxed);
                                        callback_impl
                                            ->flush_entries_completed
                                            .fetch_add(
                                                callback_entries.size(),
                                                std::
                                                    memory_order_relaxed);
                                    }
                                    else
                                    {
                                        callback_impl
                                            ->flush_batches_failed
                                            .fetch_add(
                                                1,
                                                std::
                                                    memory_order_relaxed);
                                        callback_impl
                                            ->flush_entries_failed
                                            .fetch_add(
                                                callback_entries.size(),
                                                std::
                                                    memory_order_relaxed);
                                    }
                                    if (!success)
                                    {
                                        callback_shard
                                            .last_flush_error =
                                            "eloqstore flush failed";
                                    }
                                    else
                                    {
                                        callback_shard.last_flush_error
                                            .clear();
                                    }

                                    for (const auto &flush_entry :
                                         callback_entries)
                                    {
                                        if (flush_entry.entry_id >=
                                            callback_impl->entries
                                                .size())
                                        {
                                            continue;
                                        }

                                        auto &entry =
                                            callback_impl->entries
                                                [flush_entry.entry_id];
                                        if (entry.generation !=
                                            flush_entry
                                                .entry_generation)
                                        {
                                            continue;
                                        }

                                        entry.flush_in_progress = false;
                                        if (!success)
                                        {
                                            continue;
                                        }

                                        if (flush_entry
                                                .release_after_write)
                                        {
                                            KVCacheRuntimeHelpers::
                                                RemoveResidentEntryMapping(
                                                    &callback_shard,
                                                    &entry);
                                            entry.generation += 1;
                                            KVCacheRuntimeHelpers::
                                                ResetEntryToFree(
                                                    &entry);
                                            callback_shard.free_entries
                                                .push_back(
                                                    flush_entry
                                                        .entry_id);
                                        }
                                        else
                                        {
                                            entry.state =
                                                Impl::EntryStateKind::
                                                    MemoryAndEloqStoreClean;
                                            entry.flush_after_write =
                                                false;
                                            KVCacheRuntimeHelpers::
                                                TouchResidentLru(
                                                    &callback_shard,
                                                    flush_entry
                                                        .entry_id);
                                        }
                                    }
                                    callback_shard.cv.notify_all();
                                }
                                delete batch_req;
                            });
                        if (!submitted)
                        {
                            delete write_req;
                            impl_->flush_batches_failed.fetch_add(
                                1, std::memory_order_relaxed);
                            impl_->flush_entries_failed.fetch_add(
                                batch_entries.size(),
                                std::memory_order_relaxed);
                            flush_error =
                                "eloqstore async flush submit failed";
                        }
                    }

                        if (!flush_error.empty())
                        {
                            std::lock_guard<std::mutex> lock(shard.mutex);
                            for (const auto &flush_entry : batch_entries)
                            {
                                if (flush_entry.entry_id >=
                                    impl_->entries.size())
                                {
                                    continue;
                                }
                                auto &entry =
                                    impl_->entries[flush_entry.entry_id];
                                if (entry.generation ==
                                    flush_entry.entry_generation)
                                {
                                    entry.flush_in_progress = false;
                                    shard.last_flush_error = flush_error;
                                    KVCacheRuntimeHelpers::EnqueueFlushWork(
                                        impl_,
                                        &shard,
                                        static_cast<uint32_t>(shard_index),
                                        &entry,
                                        entry.flush_after_write);
                                }
                            }
                            shard.cv.notify_all();
                        }

                        // Re-enqueue the shard for fair round-robin
                        // scheduling if it still has pending flush work,
                        // giving other shards a chance to be processed.
                        {
                            std::lock_guard<std::mutex> lock(shard.mutex);
                            if (shard.flush_queue.empty())
                            {
                                shard.queued_for_flusher = false;
                            }
                            else
                            {
                                std::lock_guard<std::mutex> scheduler_lock(
                                    impl_->flush_scheduler_mutex);
                                impl_->runnable_flush_shards.push_back(
                                    shard_index);
                                impl_->flush_scheduler_cv.notify_one();
                            }
                        }
                    }
            });
    }
    return true;
}

std::string KVCacheManager::ExportBufferPoolDescriptor() const
{
    // The descriptor is intentionally compact: enough for worker attach and
    // lane routing, but no process-local pointer values leak across the
    // boundary.
    std::ostringstream oss;
    oss << options_.shared_memory_name << "|" << shm_path_ << "|"
        << options_.shared_memory_bytes << "|" << options_.entry_size << "|"
        << options_.entry_count << "|" << options_.entry_alignment << "|"
        << options_.num_threads << "|" << options_.submission_queue_depth << "|"
        << options_.partition_count;
    return oss.str();
}

bool KVCacheManager::BeginSave(const std::string &key,
                               uint32_t payload_bytes,
                               KVCacheBufferHandle *out_buffer,
                               std::string *error_message)
{
    if (out_buffer == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "begin-save output buffer is null";
        }
        return false;
    }
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before begin-save";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "io_uring buffers must be registered before begin-save";
        }
        return false;
    }
    if (payload_bytes > options_.entry_size)
    {
        if (error_message != nullptr)
        {
            *error_message = "payload_bytes exceeds entry_size";
        }
        return false;
    }
    if (impl_ == nullptr || shards_.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager is not initialized";
        }
        return false;
    }
    const uint32_t partition_id =
        runtime_internal::PartitionIdForKey(options_, key);
    const size_t shard_index =
        static_cast<size_t>(partition_id % shards_.size());
    if (!KVCacheRuntimeManagerOps::BeginSave(this,
                                             shard_index,
                                             key,
                                             partition_id,
                                             payload_bytes,
                                             out_buffer,
                                             error_message))
    {
        return false;
    }
    return true;
}

bool KVCacheManager::FinishSave(uint64_t request_id, std::string *error_message)
{
    if (!started_ || impl_ == nullptr || impl_->shards.empty())
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before finish-save";
        }
        return false;
    }
    return KVCacheRuntimeManagerOps::FinishSave(
        this, request_id, error_message);
}

bool KVCacheManager::BeginLoad(const std::string &key,
                               uint32_t payload_bytes,
                               uint64_t *out_request_id,
                               std::string *error_message)
{
    if (out_request_id == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "begin-load output request id is null";
        }
        return false;
    }
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before begin-load";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "io_uring buffers must be registered before begin-load";
        }
        return false;
    }
    if (payload_bytes > options_.entry_size)
    {
        if (error_message != nullptr)
        {
            *error_message = "payload_bytes exceeds entry_size";
        }
        return false;
    }
    if (impl_ == nullptr || shards_.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager is not initialized";
        }
        return false;
    }
    const uint32_t partition_id =
        runtime_internal::PartitionIdForKey(options_, key);
    const size_t shard_index =
        static_cast<size_t>(partition_id % shards_.size());
    if (!KVCacheRuntimeManagerOps::BeginLoad(this,
                                             shard_index,
                                             key,
                                             partition_id,
                                             payload_bytes,
                                             out_request_id,
                                             error_message))
    {
        return false;
    }
    return true;
}

bool KVCacheManager::CheckRequest(uint64_t request_id,
                                  KVCacheRequestState *out_state,
                                  std::string *error_message)
{
    if (out_state == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "check-request output state is null";
        }
        return false;
    }
    std::lock_guard<std::mutex> state_lock(impl_->request_state_mutex);
    const auto it = impl_->request_states.find(request_id);
    if (it == impl_->request_states.end())
    {
        if (error_message != nullptr)
        {
            *error_message = "unknown request id";
        }
        return false;
    }
    *out_state = it->second;
    return true;
}

bool KVCacheManager::GetReadyBuffer(uint64_t request_id,
                                    KVCacheBufferHandle *out_buffer,
                                    std::string *error_message)
{
    if (out_buffer == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "get-ready-buffer output is null";
        }
        return false;
    }
    KVCacheRequestState state;
    if (!CheckRequest(request_id, &state, error_message))
    {
        return false;
    }
    if (state.status != KVCacheRequestStatus::Ready)
    {
        if (error_message != nullptr)
        {
            *error_message = state.error_message.empty()
                                 ? "request is not ready"
                                 : state.error_message;
        }
        return false;
    }
    out_buffer->request_id = state.request_id;
    out_buffer->offset_bytes = state.offset_bytes;
    out_buffer->payload_bytes = state.payload_bytes;
    return true;
}

bool KVCacheManager::ContainsKey(const std::string &key,
                                 bool *out_exists,
                                 std::string *error_message)
{
    return ContainsKey(key,
                       runtime_internal::PartitionIdForKey(options_, key),
                       out_exists,
                       error_message);
}

bool KVCacheManager::ContainsKey(const std::string &key,
                                 uint32_t partition_id,
                                 bool *out_exists,
                                 std::string *error_message)
{
    if (out_exists == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "contains-key output pointer is null";
        }
        return false;
    }
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before contains-key";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "io_uring buffers must be registered before contains-key";
        }
        return false;
    }
    if (impl_ == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager impl is null";
        }
        return false;
    }
    *out_exists = false;
    if (shards_.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "shards are not initialized";
        }
        return false;
    }
    const size_t shard_index =
        static_cast<size_t>(partition_id % shards_.size());
    return KVCacheRuntimeManagerOps::ContainsKey(
        this, shard_index, key, partition_id, out_exists, error_message);
}

bool KVCacheManager::ContainsKeys(
    const std::vector<std::string> &keys,
    std::vector<bool> *out_exists,
    std::string *error_message)
{
    if (out_exists == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "contains-keys output pointer is null";
        }
        return false;
    }
    const size_t n = keys.size();
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before contains-keys";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "io_uring buffers must be registered before contains-keys";
        }
        return false;
    }
    if (impl_ == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager impl is null";
        }
        return false;
    }
    if (shards_.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "shards are not initialized";
        }
        return false;
    }
    std::vector<uint32_t> partition_ids(n);
    std::vector<size_t> shard_indices(n);
    for (size_t i = 0; i < n; ++i)
    {
        partition_ids[i] =
            runtime_internal::PartitionIdForKey(options_, keys[i]);
        shard_indices[i] = static_cast<size_t>(
            partition_ids[i] % shards_.size());
    }
    return KVCacheRuntimeManagerOps::ContainsKeys(
        this, shard_indices, keys, partition_ids, out_exists, error_message);
}

bool KVCacheManager::BeginLoads(
    const std::vector<std::string> &keys,
    const std::vector<uint32_t> &payload_bytes_list,
    std::vector<uint64_t> *out_request_ids,
    std::string *error_message)
{
    if (out_request_ids == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "begin-loads output request ids is null";
        }
        return false;
    }
    const size_t n = keys.size();
    if (n != payload_bytes_list.size())
    {
        if (error_message != nullptr)
        {
            *error_message = "begin-loads keys and payload_bytes_list size mismatch";
        }
        return false;
    }
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "kv cache manager must be started before begin-loads";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message =
                "io_uring buffers must be registered before begin-loads";
        }
        return false;
    }
    if (impl_ == nullptr || shards_.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager is not initialized";
        }
        return false;
    }
    for (size_t i = 0; i < n; ++i)
    {
        if (payload_bytes_list[i] > options_.entry_size)
        {
            if (error_message != nullptr)
            {
                *error_message = "payload_bytes exceeds entry_size";
            }
            return false;
        }
    }

    std::vector<uint32_t> partition_ids(n);
    std::vector<size_t> shard_indices(n);
    for (size_t i = 0; i < n; ++i)
    {
        partition_ids[i] =
            runtime_internal::PartitionIdForKey(options_, keys[i]);
        shard_indices[i] = static_cast<size_t>(
            partition_ids[i] % shards_.size());
    }
    return KVCacheRuntimeManagerOps::BeginLoads(
        this,
        shard_indices,
        keys,
        partition_ids,
        payload_bytes_list,
        out_request_ids,
        error_message);
}

bool KVCacheManager::GetMetrics(KVCacheRuntimeMetrics *out_metrics,
                                std::string *error_message)
{
    if (out_metrics == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "get-metrics output pointer is null";
        }
        return false;
    }
    if (impl_ == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager impl is null";
        }
        return false;
    }

    uint64_t dirty_entries_current = 0;
    uint64_t flush_queue_entries_current = 0;
    uint64_t flush_inflight_entries_current = 0;
    for (auto &shard : impl_->shards)
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        flush_queue_entries_current += shard.flush_queue.size();
    }
    for (const auto &entry : impl_->entries)
    {
        if (entry.state == Impl::EntryStateKind::MemoryOnlyDirty)
        {
            dirty_entries_current += 1;
        }
        if (entry.flush_in_progress)
        {
            flush_inflight_entries_current += 1;
        }
    }

    *out_metrics = KVCacheRuntimeMetrics{
        .flush_batches_submitted =
            impl_->flush_batches_submitted.load(std::memory_order_relaxed),
        .flush_batches_completed =
            impl_->flush_batches_completed.load(std::memory_order_relaxed),
        .flush_batches_failed =
            impl_->flush_batches_failed.load(std::memory_order_relaxed),
        .flush_entries_submitted =
            impl_->flush_entries_submitted.load(std::memory_order_relaxed),
        .flush_entries_completed =
            impl_->flush_entries_completed.load(std::memory_order_relaxed),
        .flush_entries_failed =
            impl_->flush_entries_failed.load(std::memory_order_relaxed),
        .flush_batch_latency_ns_total =
            impl_->flush_batch_latency_ns_total.load(std::memory_order_relaxed),
        .contains_memory_hits =
            impl_->contains_memory_hits.load(std::memory_order_relaxed),
        .contains_store_hits =
            impl_->contains_store_hits.load(std::memory_order_relaxed),
        .contains_store_misses =
            impl_->contains_store_misses.load(std::memory_order_relaxed),
        .contains_store_errors =
            impl_->contains_store_errors.load(std::memory_order_relaxed),
        .contains_store_lookup_ns_total =
            impl_->contains_store_lookup_ns_total.load(
                std::memory_order_relaxed),
        .load_memory_hits =
            impl_->load_memory_hits.load(std::memory_order_relaxed),
        .load_store_hits =
            impl_->load_store_hits.load(std::memory_order_relaxed),
        .load_store_errors =
            impl_->load_store_errors.load(std::memory_order_relaxed),
        .load_store_bytes =
            impl_->load_store_bytes.load(std::memory_order_relaxed),
        .load_store_latency_ns_total =
            impl_->load_store_latency_ns_total.load(std::memory_order_relaxed),
        .dirty_entries_current = dirty_entries_current,
        .flush_queue_entries_current = flush_queue_entries_current,
        .flush_inflight_entries_current = flush_inflight_entries_current,
    };
    return true;
}

}  // namespace eloqstore::sdk
