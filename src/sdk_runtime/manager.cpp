#include "internal.h"

namespace eloqstore::sdk
{

using namespace runtime_internal;

bool KVCacheRuntimeManagerOps::ReserveSaveSlot(KVCacheManager *manager,
                                               size_t shard_index,
                                               const std::string &key,
                                               uint32_t partition_id,
                                               uint32_t payload_bytes,
                                               KVCacheRequest *out_request,
                                               std::string *error_message)
{
    auto &shard = manager->impl_->shards[shard_index];
    uint32_t slot_id = 0;
    if (!KVCacheRuntimeHelpers::AllocateSlotForRequest(
            manager,
            shard_index,
            &shard,
            key,
            partition_id,
            KVCacheManager::Impl::SlotStateKind::ReservedForSave,
            &slot_id,
            error_message))
    {
        return false;
    }

    auto &slot = manager->impl_->slots[slot_id];
    slot.partition_id = partition_id;
    slot.payload_bytes = 0;
    slot.key.clear();

    KVCacheRequest request;
    request.request_id = MakeRequestId(
        manager->impl_->next_request_sequence, manager->impl_->shards.size(), shard.layout.shard_id);
    request.kind = KVCacheRequestKind::Save;
    request.partition_id = partition_id;
    request.shard_id = shard.layout.shard_id;
    request.slot_id = slot_id;
    request.slot_generation = slot.generation;
    request.payload_bytes = payload_bytes;
    request.key = key;

    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pending_save_requests.emplace(request.request_id, request);
    }
    if (out_request != nullptr)
    {
        *out_request = request;
    }
    return true;
}

bool KVCacheRuntimeManagerOps::FinalizeSaveReady(KVCacheManager *manager,
                                                 uint64_t request_id,
                                                 KVCacheCompletion *out_completion,
                                                 std::string *error_message)
{
    const size_t shard_index = RequestShardIndex(request_id, manager->impl_->shards.size());
    auto &shard = manager->impl_->shards[shard_index];

    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto it = shard.pending_save_requests.find(request_id);
    if (it == shard.pending_save_requests.end())
    {
        if (error_message != nullptr)
        {
            *error_message = "unknown save request id";
        }
        return false;
    }

    const KVCacheRequest request = it->second;
    auto &slot = manager->impl_->slots[request.slot_id];
    slot.state = KVCacheManager::Impl::SlotStateKind::MemoryOnlyDirty;
    slot.flush_in_progress = false;
    slot.partition_id = request.partition_id;
    slot.payload_bytes = request.payload_bytes;
    slot.key = request.key;
    shard.resident_slots[MakeResidentIndexKey(request.key, request.partition_id)] = request.slot_id;
    KVCacheRuntimeHelpers::TouchResidentLru(&shard, request.slot_id);
    shard.pending_save_requests.erase(it);
    shard.last_flush_error.clear();
    KVCacheRuntimeHelpers::EnqueueFlushWork(
        manager->impl_, &shard, static_cast<uint32_t>(shard_index), &slot, false);

    if (out_completion != nullptr)
    {
        out_completion->request_id = request.request_id;
        out_completion->kind = request.kind;
        out_completion->status = KVCacheRequestStatus::Completed;
        out_completion->partition_id = request.partition_id;
        out_completion->shard_id = request.shard_id;
        out_completion->slot_id = request.slot_id;
        out_completion->slot_generation = request.slot_generation;
        out_completion->payload_bytes = request.payload_bytes;
        out_completion->key = request.key;
    }
    return true;
}

bool KVCacheRuntimeManagerOps::SubmitLoad(KVCacheManager *manager,
                                          size_t shard_index,
                                          const std::string &key,
                                          uint32_t partition_id,
                                          uint32_t payload_bytes,
                                          KVCacheRequest *out_request,
                                          std::string *error_message)
{
    auto &shard = manager->impl_->shards[shard_index];
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        const auto it = shard.resident_slots.find(MakeResidentIndexKey(key, partition_id));
        if (it != shard.resident_slots.end())
        {
            const auto &slot = manager->impl_->slots[it->second];
            if (KVCacheRuntimeHelpers::IsResidentState(slot.state))
            {
                KVCacheRequest request;
                request.request_id = MakeRequestId(
                    manager->impl_->next_request_sequence,
                    manager->impl_->shards.size(),
                    shard.layout.shard_id);
                request.kind = KVCacheRequestKind::Load;
                request.partition_id = partition_id;
                request.shard_id = shard.layout.shard_id;
                request.slot_id = slot.slot_id;
                request.slot_generation = slot.generation;
                request.payload_bytes = slot.payload_bytes;
                request.key = key;
                KVCacheRuntimeHelpers::TouchResidentLru(&shard, request.slot_id);
                if (out_request != nullptr)
                {
                    *out_request = request;
                }
                return true;
            }
        }
    }

    uint32_t slot_id = 0;
    if (!KVCacheRuntimeHelpers::AllocateSlotForRequest(
            manager,
            shard_index,
            &shard,
            key,
            partition_id,
            KVCacheManager::Impl::SlotStateKind::ReservedForLoad,
            &slot_id,
            error_message))
    {
        return false;
    }

    KVCacheRequest request;
    request.request_id = MakeRequestId(
        manager->impl_->next_request_sequence, manager->impl_->shards.size(), shard.layout.shard_id);
    request.kind = KVCacheRequestKind::Load;
    request.partition_id = partition_id;
    request.shard_id = shard.layout.shard_id;
    request.slot_id = slot_id;
    request.slot_generation = manager->impl_->slots[slot_id].generation;
    request.payload_bytes = payload_bytes;
    request.key = key;

    void *slot_ptr =
        static_cast<char *>(manager->shm_addr_) + (static_cast<size_t>(request.slot_id) * manager->options_.slot_size);
    {
        std::lock_guard<std::mutex> store_lock(manager->impl_->store_mutex);
        if (manager->impl_->store == nullptr)
        {
            if (error_message != nullptr)
            {
                *error_message = "eloqstore runtime is not started";
            }
            goto load_fail;
        }
        TableIdent table(manager->options_.table_name, request.partition_id);
        ReadRequest read_req;
        read_req.SetArgs(table, request.key);
        const size_t read_bytes = PinnedReadBytes(request.payload_bytes);
        read_req.large_value_dest_ =
            std::make_pair(reinterpret_cast<char *>(slot_ptr), read_bytes);
        manager->impl_->store->ExecSync(&read_req);
        if (read_req.Error() != KvError::NoError)
        {
            if (error_message != nullptr)
            {
                *error_message = read_req.Error() == KvError::NotFound ? "kv cache key not found"
                                                                       : "eloqstore load failed";
            }
            goto load_fail;
        }
        uint32_t loaded_payload_bytes = request.payload_bytes;
        if (!DecodePayloadMetadata(read_req.value_, request.payload_bytes, &loaded_payload_bytes))
        {
            if (error_message != nullptr)
            {
                *error_message = "invalid kv cache load metadata";
            }
            goto load_fail;
        }
        if (loaded_payload_bytes > manager->options_.slot_size)
        {
            if (error_message != nullptr)
            {
                *error_message = "loaded payload exceeds slot_size";
            }
            goto load_fail;
        }
        request.payload_bytes = loaded_payload_bytes;
    }

    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &slot = manager->impl_->slots[request.slot_id];
        slot.state = KVCacheManager::Impl::SlotStateKind::MemoryAndEloqStoreClean;
        slot.flush_enqueued = false;
        slot.flush_in_progress = false;
        slot.flush_after_write = false;
        slot.partition_id = request.partition_id;
        slot.payload_bytes = request.payload_bytes;
        slot.key = request.key;
        shard.resident_slots[MakeResidentIndexKey(request.key, request.partition_id)] = request.slot_id;
        KVCacheRuntimeHelpers::TouchResidentLru(&shard, request.slot_id);
    }

    if (out_request != nullptr)
    {
        *out_request = request;
    }
    return true;

load_fail:
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto &slot = manager->impl_->slots[request.slot_id];
        if (slot.state == KVCacheManager::Impl::SlotStateKind::ReservedForLoad)
        {
            slot.generation += 1;
            KVCacheRuntimeHelpers::ResetSlotToFree(&slot);
            shard.free_slots.push_back(request.slot_id);
            shard.cv.notify_all();
        }
    }
    return false;
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
        if (shard.resident_slots.contains(MakeResidentIndexKey(key, partition_id)))
        {
            *out_exists = true;
            return true;
        }
    }
    std::lock_guard<std::mutex> store_lock(manager->impl_->store_mutex);
    if (manager->impl_->store == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "eloqstore runtime is not started";
        }
        return false;
    }
    TableIdent table(manager->options_.table_name, partition_id);
    ReadRequest read_req;
    read_req.SetArgs(table, key);
    manager->impl_->store->ExecSync(&read_req);
    if (read_req.Error() == KvError::NoError)
    {
        *out_exists = true;
        return true;
    }
    if (read_req.Error() == KvError::NotFound)
    {
        *out_exists = false;
        return true;
    }
    if (error_message != nullptr)
    {
        *error_message = "eloqstore contains-key failed";
    }
    return false;
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
    if (options_.slot_size == 0 || options_.slot_count == 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "slot_size and slot_count must both be greater than zero";
        }
        return false;
    }
    if (options_.slot_alignment == 0 || (options_.slot_alignment & (options_.slot_alignment - 1)) != 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "slot_alignment must be a non-zero power of two";
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
    if (options_.num_threads > options_.slot_count)
    {
        if (error_message != nullptr)
        {
            *error_message = "num_threads must be less than or equal to slot_count";
        }
        return false;
    }
    if (options_.slot_size % options_.slot_alignment != 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "slot_size must be aligned to slot_alignment";
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
    shm_fd_ = CreateSharedMemory(shm_name);
    if (shm_fd_ < 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "shm_open(create) failed: " + std::string(std::strerror(errno));
        }
        return false;
    }
    if (::ftruncate(shm_fd_, static_cast<off_t>(options_.shared_memory_bytes)) != 0)
    {
        if (error_message != nullptr)
        {
            *error_message = "ftruncate failed: " + std::string(std::strerror(errno));
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
            *error_message = "mmap failed: " + std::string(std::strerror(errno));
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
    impl_->slots.clear();
    impl_->slots.reserve(options_.slot_count);
    for (uint32_t slot_id = 0; slot_id < options_.slot_count; ++slot_id)
    {
        impl_->slots.push_back(Impl::SlotState{
            .slot_id = slot_id,
            .generation = 0,
        });
    }
    impl_->shards.clear();
    for (const auto &shard_layout : shards_)
    {
        impl_->shards.emplace_back();
        auto &shard_state = impl_->shards.back();
        shard_state.layout = shard_layout;
        for (uint32_t slot_id = shard_layout.start_slot;
             slot_id < shard_layout.start_slot + shard_layout.slot_count;
             ++slot_id)
        {
            shard_state.free_slots.push_back(slot_id);
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
        impl_->ipc_thread = std::thread([this]() {
            while (!impl_->stopping.load())
            {
                std::vector<std::string> request_frames;
                std::string transport_error;
                if (!ReceiveFrames(*impl_->ipc_socket, &request_frames, &transport_error))
                {
                    if (impl_->stopping.load())
                    {
                        return;
                    }
                    if (transport_error.find("Resource temporarily unavailable") != std::string::npos)
                    {
                        continue;
                    }
                    continue;
                }
                auto response_frames = HandleManagerIpcMessage(this, request_frames);
                transport_error.clear();
                if (!SendFrames(*impl_->ipc_socket, response_frames, &transport_error) &&
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
        impl_->slots.clear();
        {
            std::lock_guard<std::mutex> lock(impl_->completion_mutex);
            impl_->completions.clear();
        }
        {
            std::lock_guard<std::mutex> lock(impl_->store_mutex);
            if (impl_->store != nullptr)
            {
                impl_->store->Stop();
                impl_->store.reset();
            }
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
        ::shm_unlink(options_.shared_memory_name.c_str());
    }
    shm_path_.clear();
    shards_.clear();
    started_ = false;
}

bool KVCacheManager::RegisterIoUringBuffers(std::string *error_message)
{
    // This is the point where the manager-side shared-memory mapping becomes an
    // EloqStore-accessible pinned buffer region for real save/load I/O.
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager must be started before io_uring registration";
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
    kv_options.pinned_memory_chunks.emplace_back(
        reinterpret_cast<char *>(shm_addr_),
        options_.shared_memory_bytes);
    if (!EloqStore::ValidateOptions(kv_options))
    {
        if (error_message != nullptr)
        {
            *error_message = "eloqstore kv options validation failed for kv cache manager";
        }
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(impl_->store_mutex);
        if (impl_->store == nullptr)
        {
            impl_->store = std::make_unique<EloqStore>(kv_options);
            const KvError err = impl_->store->Start(
                options_.branch,
                options_.term,
                options_.partition_group_id);
            if (err != KvError::NoError)
            {
                impl_->store.reset();
                if (error_message != nullptr)
                {
                    *error_message = "eloqstore start failed for kv cache manager";
                }
                return false;
            }
        }
    }
    io_uring_registered_ = true;
    if (impl_ != nullptr && !impl_->flush_thread.joinable())
    {
        impl_->flush_thread = std::thread([this]() {
            while (!impl_->stopping.load())
            {
                uint32_t shard_index = 0;
                {
                    std::unique_lock<std::mutex> scheduler_lock(impl_->flush_scheduler_mutex);
                    impl_->flush_scheduler_cv.wait(scheduler_lock, [&]() {
                        return impl_->stopping.load() || !impl_->runnable_flush_shards.empty();
                    });
                    if (impl_->stopping.load())
                    {
                        return;
                    }
                    shard_index = impl_->runnable_flush_shards.front();
                    impl_->runnable_flush_shards.pop_front();
                }

                auto &shard = impl_->shards[shard_index];
                while (!impl_->stopping.load())
                {
                    Impl::ShardState::FlushItem flush_item;
                    uint32_t partition_id = 0;
                    uint32_t payload_bytes = 0;
                    std::string key;
                    bool release_after_write = false;
                    {
                        std::lock_guard<std::mutex> lock(shard.mutex);
                        if (shard.flush_queue.empty())
                        {
                            shard.queued_for_flusher = false;
                            break;
                        }
                        flush_item = shard.flush_queue.front();
                        shard.flush_queue.pop_front();
                        if (flush_item.slot_id >= impl_->slots.size())
                        {
                            continue;
                        }
                        auto &slot = impl_->slots[flush_item.slot_id];
                        if (slot.generation != flush_item.slot_generation ||
                            !KVCacheRuntimeHelpers::IsDirtyState(slot.state))
                        {
                            slot.flush_enqueued = false;
                            slot.flush_after_write = false;
                            continue;
                        }
                        slot.flush_enqueued = false;
                        slot.flush_in_progress = true;
                        partition_id = slot.partition_id;
                        payload_bytes = slot.payload_bytes;
                        key = slot.key;
                        release_after_write = slot.flush_after_write;
                    }

                    std::string flush_error;
                    {
                        std::lock_guard<std::mutex> store_lock(impl_->store_mutex);
                        if (impl_->store == nullptr)
                        {
                            flush_error = "eloqstore runtime is not started";
                        }
                        else
                        {
                            void *slot_ptr = static_cast<char *>(shm_addr_) +
                                             (static_cast<size_t>(flush_item.slot_id) *
                                              options_.slot_size);
                            TableIdent table(options_.table_name, partition_id);
                            std::string metadata = EncodePayloadMetadata(payload_bytes);
                            WriteDataEntry entry(
                                key,
                                std::move(metadata),
                                std::make_pair(
                                    reinterpret_cast<const char *>(slot_ptr),
                                    static_cast<size_t>(payload_bytes)),
                                0,
                                WriteOp::Upsert);
                            BatchWriteRequest write_req;
                            std::vector<WriteDataEntry> batch;
                            batch.push_back(std::move(entry));
                            write_req.SetArgs(table, std::move(batch));
                            impl_->store->ExecSync(&write_req);
                            if (write_req.Error() != KvError::NoError)
                            {
                                flush_error = "eloqstore flush failed";
                            }
                        }
                    }

                    {
                        std::lock_guard<std::mutex> lock(shard.mutex);
                        auto &slot = impl_->slots[flush_item.slot_id];
                        if (slot.generation != flush_item.slot_generation)
                        {
                            shard.cv.notify_all();
                            continue;
                        }
                        slot.flush_in_progress = false;
                        if (!flush_error.empty())
                        {
                            shard.last_flush_error = flush_error;
                            shard.cv.notify_all();
                            continue;
                        }
                        shard.last_flush_error.clear();
                        if (release_after_write)
                        {
                            KVCacheRuntimeHelpers::RemoveResidentSlotMapping(&shard, &slot);
                            slot.generation += 1;
                            KVCacheRuntimeHelpers::ResetSlotToFree(&slot);
                            shard.free_slots.push_back(flush_item.slot_id);
                        }
                        else
                        {
                            slot.state = Impl::SlotStateKind::MemoryAndEloqStoreClean;
                            slot.flush_after_write = false;
                            KVCacheRuntimeHelpers::TouchResidentLru(&shard, flush_item.slot_id);
                        }
                        shard.cv.notify_all();
                    }
                }
            }
        });
    }
    return true;
}

std::string KVCacheManager::ExportBufferPoolDescriptor() const
{
    // The descriptor is intentionally compact: enough for worker attach and lane
    // routing, but no process-local pointer values leak across the boundary.
    std::ostringstream oss;
    oss << options_.shared_memory_name
        << "|" << shm_path_
        << "|" << options_.shared_memory_bytes
        << "|" << options_.slot_size
        << "|" << options_.slot_count
        << "|" << options_.slot_alignment
        << "|" << options_.num_threads
        << "|" << options_.submission_queue_depth
        << "|" << options_.partition_count;
    return oss.str();
}

bool KVCacheManager::SubmitSaveRequest(const std::string &key,
                                       uint32_t partition_id,
                                       uint32_t payload_bytes,
                                       KVCacheRequest *out_request,
                                       std::string *error_message)
{
    // Reserve one cache entry. The worker fills shared memory first; the entry
    // becomes visible to future loads only after MarkSaveRequestReady.
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager must be started before submit-save";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message = "io_uring buffers must be registered before submit-save";
        }
        return false;
    }
    if (payload_bytes > options_.slot_size)
    {
        if (error_message != nullptr)
        {
            *error_message = "payload_bytes exceeds slot_size";
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
    const size_t shard_index = static_cast<size_t>(partition_id % shards_.size());
    return KVCacheRuntimeManagerOps::ReserveSaveSlot(
        this, shard_index, key, partition_id, payload_bytes, out_request, error_message);
}

bool KVCacheManager::MarkSaveRequestReady(uint64_t request_id,
                                          std::string *error_message)
{
    // Save completion means the shared-memory entry is now resident and visible
    // to future loads. The entry remains dirty until the manager later flushes
    // it into EloqStore.
    if (!started_ || impl_ == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager must be started before mark-save-ready";
        }
        return false;
    }
    if (impl_->shards.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "unknown save request shard";
        }
        return false;
    }
    KVCacheCompletion completion;
    if (!KVCacheRuntimeManagerOps::FinalizeSaveReady(
            this, request_id, &completion, error_message))
    {
        return false;
    }
    std::lock_guard<std::mutex> completion_lock(impl_->completion_mutex);
    impl_->completions.push_back(std::move(completion));
    return true;
}

bool KVCacheManager::SubmitLoadRequest(const std::string &key,
                                       uint32_t partition_id,
                                       uint32_t payload_bytes,
                                       KVCacheRequest *out_request,
                                       std::string *error_message)
{
    // Load first checks the resident shared-memory cache. Only cache misses
    // fall back to EloqStore and fill a free entry.
    if (!started_)
    {
        if (error_message != nullptr)
        {
            *error_message = "kv cache manager must be started before submit-load";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message = "io_uring buffers must be registered before submit-load";
        }
        return false;
    }
    if (payload_bytes > options_.slot_size)
    {
        if (error_message != nullptr)
        {
            *error_message = "payload_bytes exceeds slot_size";
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
    const size_t shard_index = static_cast<size_t>(partition_id % shards_.size());
    KVCacheRequest request;
    if (!KVCacheRuntimeManagerOps::SubmitLoad(
            this, shard_index, key, partition_id, payload_bytes, &request, error_message))
    {
        return false;
    }
    if (out_request != nullptr)
    {
        *out_request = request;
    }
    KVCacheCompletion completion;
    completion.request_id = request.request_id;
    completion.kind = request.kind;
    completion.status = KVCacheRequestStatus::Completed;
    completion.partition_id = request.partition_id;
    completion.shard_id = request.shard_id;
    completion.slot_id = request.slot_id;
    completion.slot_generation = request.slot_generation;
    completion.payload_bytes = request.payload_bytes;
    completion.key = request.key;
    std::lock_guard<std::mutex> completion_lock(impl_->completion_mutex);
    impl_->completions.push_back(std::move(completion));
    return true;
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
            *error_message = "kv cache manager must be started before contains-key";
        }
        return false;
    }
    if (!io_uring_registered_)
    {
        if (error_message != nullptr)
        {
            *error_message = "io_uring buffers must be registered before contains-key";
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
    const size_t shard_index = static_cast<size_t>(partition_id % shards_.size());
    return KVCacheRuntimeManagerOps::ContainsKey(
        this, shard_index, key, partition_id, out_exists, error_message);
}

bool KVCacheManager::PollCompletion(KVCacheCompletion *out_completion,
                                    std::string *error_message)
{
    // Empty completion queues are not an error; callers treat "false + no error"
    // as "nothing finished yet".
    if (out_completion == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "completion output pointer is null";
        }
        return false;
    }
    std::lock_guard<std::mutex> lock(impl_->completion_mutex);
    if (impl_->completions.empty())
    {
        return false;
    }
    *out_completion = impl_->completions.front();
    impl_->completions.pop_front();
    return true;
}

}  // namespace eloqstore::sdk
