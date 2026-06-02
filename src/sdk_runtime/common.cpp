#include "internal.h"

namespace eloqstore::sdk
{

using namespace runtime_internal;

namespace runtime_internal
{

std::string BuildDefaultSharedMemoryName(const KVCacheOptions &options)
{
    if (!options.shared_memory_name.empty())
    {
        return options.shared_memory_name;
    }
    std::ostringstream oss;
    oss << "/eloqstore-kvcache-" << ::getpid();
    return oss.str();
}

std::string BuildSharedMemoryFsPath(const std::string &name)
{
    if (name.empty())
    {
        return "";
    }
    if (name.front() == '/')
    {
        return "/dev/shm" + name;
    }
    return "/dev/shm/" + name;
}

int CreateSharedMemory(const std::string &name)
{
    int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd >= 0 || errno != EEXIST)
    {
        return fd;
    }
    ::shm_unlink(name.c_str());
    return ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
}

std::string EncodePayloadMetadata(uint32_t payload_bytes)
{
    std::string metadata(sizeof(uint32_t), '\0');
    std::memcpy(metadata.data(), &payload_bytes, sizeof(uint32_t));
    return metadata;
}

bool DecodePayloadMetadata(const std::string &metadata,
                           uint32_t fallback_bytes,
                           uint32_t *out_payload_bytes)
{
    if (out_payload_bytes == nullptr)
    {
        return false;
    }
    if (metadata.empty())
    {
        *out_payload_bytes = fallback_bytes;
        return true;
    }
    if (metadata.size() != sizeof(uint32_t))
    {
        return false;
    }
    uint32_t payload_bytes = 0;
    std::memcpy(&payload_bytes, metadata.data(), sizeof(uint32_t));
    *out_payload_bytes = payload_bytes;
    return true;
}

size_t AlignUp(size_t value, size_t alignment)
{
    return alignment == 0 ? value : ((value + alignment - 1) / alignment) * alignment;
}

size_t PinnedReadBytes(uint32_t payload_bytes)
{
    constexpr size_t kPageAlignment = 4096;
    constexpr size_t kSegmentSize = 256 * 1024;
    if (payload_bytes == 0)
    {
        return 0;
    }
    const size_t prefix_bytes = payload_bytes <= kSegmentSize
                                    ? 0
                                    : ((payload_bytes - 1) / kSegmentSize) * kSegmentSize;
    const size_t tail_bytes = payload_bytes - prefix_bytes;
    return prefix_bytes + AlignUp(tail_bytes, kPageAlignment);
}

std::vector<ShardLayout> BuildShards(const KVCacheOptions &options)
{
    std::vector<ShardLayout> shards;
    const uint32_t shard_count = std::max<uint32_t>(1, options.num_threads);
    const uint32_t slots_per_shard = options.slot_count / shard_count;
    const uint32_t remainder = options.slot_count % shard_count;
    uint32_t next_start_slot = 0;
    for (uint32_t shard_id = 0; shard_id < shard_count; ++shard_id)
    {
        const uint32_t extra_slot = shard_id < remainder ? 1 : 0;
        const uint32_t shard_slots = slots_per_shard + extra_slot;
        shards.push_back(ShardLayout{
            .shard_id = shard_id,
            .start_slot = next_start_slot,
            .slot_count = shard_slots,
        });
        next_start_slot += shard_slots;
    }
    return shards;
}

std::vector<std::string> Split(const std::string &value, char delim)
{
    std::vector<std::string> parts;
    std::stringstream ss(value);
    std::string part;
    while (std::getline(ss, part, delim))
    {
        parts.push_back(part);
    }
    return parts;
}

std::optional<uint32_t> ParseUint32(std::string_view value)
{
    try
    {
        return static_cast<uint32_t>(std::stoul(std::string(value)));
    }
    catch (const std::exception &)
    {
        return std::nullopt;
    }
}

std::optional<uint64_t> ParseUint64(std::string_view value)
{
    try
    {
        return static_cast<uint64_t>(std::stoull(std::string(value)));
    }
    catch (const std::exception &)
    {
        return std::nullopt;
    }
}

bool SendFrames(zmq::socket_t &socket,
                const std::vector<std::string> &frames,
                std::string *error_message)
{
    try
    {
        for (size_t i = 0; i < frames.size(); ++i)
        {
            const zmq::send_flags flags =
                i + 1 < frames.size() ? zmq::send_flags::sndmore : zmq::send_flags::none;
            socket.send(zmq::buffer(frames[i]), flags);
        }
        return true;
    }
    catch (const zmq::error_t &e)
    {
        if (error_message != nullptr)
        {
            *error_message = std::string("ZeroMQ send failed: ") + e.what();
        }
        return false;
    }
}

bool ReceiveFrames(zmq::socket_t &socket,
                   std::vector<std::string> *frames,
                   std::string *error_message)
{
    if (frames == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "output frames pointer is null";
        }
        return false;
    }
    frames->clear();
    try
    {
        while (true)
        {
            zmq::message_t frame;
            const auto result = socket.recv(frame, zmq::recv_flags::none);
            if (!result)
            {
                return false;
            }
            frames->emplace_back(static_cast<const char *>(frame.data()), frame.size());
            if (!socket.get(zmq::sockopt::rcvmore))
            {
                return true;
            }
        }
    }
    catch (const zmq::error_t &e)
    {
        if (error_message != nullptr)
        {
            *error_message = std::string("ZeroMQ recv failed: ") + e.what();
        }
        return false;
    }
}

uint64_t MakeRequestId(std::atomic<uint64_t> &next_request_sequence,
                       size_t shard_count,
                       uint32_t shard_id)
{
    const uint64_t sequence = next_request_sequence.fetch_add(1);
    const uint64_t shard_count_u64 = std::max<size_t>(1, shard_count);
    return sequence * shard_count_u64 + static_cast<uint64_t>(shard_id) + 1;
}

size_t RequestShardIndex(uint64_t request_id, size_t shard_count)
{
    return shard_count == 0 ? 0 : static_cast<size_t>((request_id - 1) % shard_count);
}

std::string MakeResidentIndexKey(const std::string &key, uint32_t partition_id)
{
    return std::to_string(partition_id) + "|" + key;
}

std::vector<std::string> EncodeRequestRecord(const KVCacheRequest &request)
{
    return {
        std::to_string(request.request_id),
        std::to_string(static_cast<uint32_t>(request.kind)),
        std::to_string(request.partition_id),
        std::to_string(request.shard_id),
        std::to_string(request.slot_id),
        std::to_string(request.slot_generation),
        std::to_string(request.payload_bytes),
    };
}

std::vector<std::string> EncodeCompletionRecord(const KVCacheCompletion &completion)
{
    return {
        std::to_string(completion.request_id),
        std::to_string(static_cast<uint32_t>(completion.kind)),
        std::to_string(static_cast<uint32_t>(completion.status)),
        std::to_string(completion.partition_id),
        std::to_string(completion.shard_id),
        std::to_string(completion.slot_id),
        std::to_string(completion.slot_generation),
        std::to_string(completion.payload_bytes),
        completion.key,
        completion.error_message,
    };
}

bool DecodeRequestRecord(const std::vector<std::string> &frames,
                         size_t offset,
                         const std::string &key,
                         KVCacheRequest *out_request)
{
    if (out_request == nullptr || frames.size() != offset + 7)
    {
        return false;
    }
    const auto request_id = ParseUint64(frames[offset + 0]);
    const auto kind = ParseUint32(frames[offset + 1]);
    const auto partition_id = ParseUint32(frames[offset + 2]);
    const auto shard_id = ParseUint32(frames[offset + 3]);
    const auto slot_id = ParseUint32(frames[offset + 4]);
    const auto slot_generation = ParseUint32(frames[offset + 5]);
    const auto payload_bytes = ParseUint32(frames[offset + 6]);
    if (!request_id.has_value() || !kind.has_value() || !partition_id.has_value() ||
        !shard_id.has_value() || !slot_id.has_value() || !slot_generation.has_value() ||
        !payload_bytes.has_value())
    {
        return false;
    }
    out_request->request_id = *request_id;
    out_request->kind = static_cast<KVCacheRequestKind>(*kind);
    out_request->partition_id = *partition_id;
    out_request->shard_id = *shard_id;
    out_request->slot_id = *slot_id;
    out_request->slot_generation = *slot_generation;
    out_request->payload_bytes = *payload_bytes;
    out_request->key = key;
    return true;
}

bool DecodeCompletionRecord(const std::vector<std::string> &frames,
                            size_t offset,
                            KVCacheCompletion *out_completion)
{
    if (out_completion == nullptr || frames.size() != offset + 10)
    {
        return false;
    }
    const auto request_id = ParseUint64(frames[offset + 0]);
    const auto kind = ParseUint32(frames[offset + 1]);
    const auto status = ParseUint32(frames[offset + 2]);
    const auto partition_id = ParseUint32(frames[offset + 3]);
    const auto shard_id = ParseUint32(frames[offset + 4]);
    const auto slot_id = ParseUint32(frames[offset + 5]);
    const auto slot_generation = ParseUint32(frames[offset + 6]);
    const auto payload_bytes = ParseUint32(frames[offset + 7]);
    if (!request_id.has_value() || !kind.has_value() || !status.has_value() ||
        !partition_id.has_value() || !shard_id.has_value() || !slot_id.has_value() ||
        !slot_generation.has_value() || !payload_bytes.has_value())
    {
        return false;
    }
    out_completion->request_id = *request_id;
    out_completion->kind = static_cast<KVCacheRequestKind>(*kind);
    out_completion->status = static_cast<KVCacheRequestStatus>(*status);
    out_completion->partition_id = *partition_id;
    out_completion->shard_id = *shard_id;
    out_completion->slot_id = *slot_id;
    out_completion->slot_generation = *slot_generation;
    out_completion->payload_bytes = *payload_bytes;
    out_completion->key = frames[offset + 8];
    out_completion->error_message = frames[offset + 9];
    return true;
}

std::vector<std::string> HandleManagerIpcMessage(
    KVCacheManager *manager,
    const std::vector<std::string> &request_frames)
{
    if (request_frames.empty())
    {
        return {"error", "missing kv cache ipc command"};
    }
    const std::string &command = request_frames[0];
    std::string error_message;
    if (command == "submit_save" || command == "submit_load")
    {
        if (request_frames.size() != 4)
        {
            return {"error", "invalid kv cache submit request"};
        }
        const auto partition_id = ParseUint32(request_frames[2]);
        const auto payload_bytes = ParseUint32(request_frames[3]);
        if (!partition_id.has_value() || !payload_bytes.has_value())
        {
            return {"error", "failed to parse kv cache submit request integers"};
        }
        KVCacheRequest request;
        const bool ok = command == "submit_save"
                             ? manager->SubmitSaveRequest(
                                   request_frames[1],
                                   *partition_id,
                                   *payload_bytes,
                                   &request,
                                   &error_message)
                             : manager->SubmitLoadRequest(
                                   request_frames[1],
                                   *partition_id,
                                   *payload_bytes,
                                   &request,
                                   &error_message);
        if (!ok)
        {
            return {"error", error_message};
        }
        auto response = EncodeRequestRecord(request);
        response.insert(response.begin(), "ok");
        return response;
    }
    if (command == "mark_save_ready")
    {
        if (request_frames.size() != 2)
        {
            return {"error", "invalid mark-save-ready request"};
        }
        const auto request_id = ParseUint64(request_frames[1]);
        if (!request_id.has_value())
        {
            return {"error", "failed to parse mark-save-ready request id"};
        }
        if (!manager->MarkSaveRequestReady(*request_id, &error_message))
        {
            return {"error", error_message};
        }
        return {"ok"};
    }
    if (command == "poll_completion")
    {
        KVCacheCompletion completion;
        if (!manager->PollCompletion(&completion, &error_message))
        {
            if (!error_message.empty())
            {
                return {"error", error_message};
            }
            return {"empty"};
        }
        auto response = EncodeCompletionRecord(completion);
        response.insert(response.begin(), "ok");
        return response;
    }
    return {"error", "unknown kv cache ipc command"};
}

bool EnsureWorkerSocketConnected(const KVCacheOptions &options,
                                 std::unique_ptr<zmq::context_t> *context,
                                 std::unique_ptr<zmq::socket_t> *socket,
                                 std::string *error_message)
{
    if (context == nullptr || socket == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "worker ipc context/socket pointer is null";
        }
        return false;
    }
    if (options.ipc_path.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "ipc_path must not be empty for kv cache worker";
        }
        return false;
    }
    try
    {
        if (*context == nullptr)
        {
            *context = std::make_unique<zmq::context_t>(1);
        }
        if (*socket == nullptr)
        {
            auto new_socket = std::make_unique<zmq::socket_t>(**context, zmq::socket_type::req);
            new_socket->set(zmq::sockopt::linger, 0);
            new_socket->set(zmq::sockopt::rcvtimeo, 5000);
            new_socket->set(zmq::sockopt::sndtimeo, 5000);
            new_socket->connect(options.ipc_path);
            *socket = std::move(new_socket);
        }
        return true;
    }
    catch (const zmq::error_t &e)
    {
        socket->reset();
        context->reset();
        if (error_message != nullptr)
        {
            *error_message = std::string("ZeroMQ worker connect failed: ") + e.what();
        }
        return false;
    }
}

bool ExchangeWorkerIpc(zmq::socket_t *socket,
                       const std::vector<std::string> &request_frames,
                       std::vector<std::string> *response_frames,
                       std::string *error_message)
{
    if (socket == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "worker ipc socket is null";
        }
        return false;
    }
    if (response_frames == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "output ipc response pointer is null";
        }
        return false;
    }
    try
    {
        if (!SendFrames(*socket, request_frames, error_message))
        {
            return false;
        }
        return ReceiveFrames(*socket, response_frames, error_message);
    }
    catch (const zmq::error_t &e)
    {
        if (error_message != nullptr)
        {
            *error_message = std::string("ZeroMQ worker exchange failed: ") + e.what();
        }
        return false;
    }
}

bool IsBenignPollCompletionTransportError(const std::string &error_message)
{
    return error_message.find("Interrupted system call") != std::string::npos ||
           error_message.find("Context was terminated") != std::string::npos ||
           error_message.find("Socket operation on non-socket") != std::string::npos;
}

}  // namespace runtime_internal

bool KVCacheRuntimeHelpers::IsResidentState(KVCacheManager::Impl::SlotStateKind state)
{
    return state == KVCacheManager::Impl::SlotStateKind::MemoryOnlyDirty ||
           state == KVCacheManager::Impl::SlotStateKind::MemoryAndEloqStoreClean;
}

bool KVCacheRuntimeHelpers::IsDirtyState(KVCacheManager::Impl::SlotStateKind state)
{
    return state == KVCacheManager::Impl::SlotStateKind::MemoryOnlyDirty;
}

void KVCacheRuntimeHelpers::ResetSlotToFree(KVCacheManager::Impl::SlotState *slot)
{
    if (slot == nullptr)
    {
        return;
    }
    slot->state = KVCacheManager::Impl::SlotStateKind::Free;
    slot->flush_enqueued = false;
    slot->flush_in_progress = false;
    slot->flush_after_write = false;
    slot->partition_id = 0;
    slot->payload_bytes = 0;
    slot->key.clear();
}

void KVCacheRuntimeHelpers::TouchResidentLru(KVCacheManager::Impl::ShardState *shard,
                                             uint32_t slot_id)
{
    if (shard == nullptr)
    {
        return;
    }
    shard->resident_lru.erase(
        std::remove(shard->resident_lru.begin(), shard->resident_lru.end(), slot_id),
        shard->resident_lru.end());
    shard->resident_lru.push_back(slot_id);
}

void KVCacheRuntimeHelpers::RemoveResidentSlotMapping(
    KVCacheManager::Impl::ShardState *shard,
    KVCacheManager::Impl::SlotState *slot)
{
    if (shard == nullptr || slot == nullptr || slot->key.empty())
    {
        return;
    }
    shard->resident_slots.erase(runtime_internal::MakeResidentIndexKey(slot->key, slot->partition_id));
    shard->resident_lru.erase(
        std::remove(shard->resident_lru.begin(), shard->resident_lru.end(), slot->slot_id),
        shard->resident_lru.end());
}

bool KVCacheRuntimeHelpers::EnqueueFlushWork(KVCacheManager::Impl *impl,
                                             KVCacheManager::Impl::ShardState *shard,
                                             uint32_t shard_index,
                                             KVCacheManager::Impl::SlotState *slot,
                                             bool release_after_write)
{
    if (impl == nullptr || shard == nullptr || slot == nullptr)
    {
        return false;
    }
    if (!IsDirtyState(slot->state))
    {
        return false;
    }
    slot->flush_after_write = slot->flush_after_write || release_after_write;
    if (slot->flush_enqueued || slot->flush_in_progress)
    {
        return true;
    }
    slot->flush_enqueued = true;
    shard->flush_queue.push_back(KVCacheManager::Impl::ShardState::FlushItem{
        .slot_id = slot->slot_id,
        .slot_generation = slot->generation,
    });
    if (!shard->queued_for_flusher)
    {
        shard->queued_for_flusher = true;
        std::lock_guard<std::mutex> scheduler_lock(impl->flush_scheduler_mutex);
        impl->runnable_flush_shards.push_back(shard_index);
        impl->flush_scheduler_cv.notify_one();
    }
    return true;
}

bool KVCacheRuntimeHelpers::AllocateSlotForRequest(
    KVCacheManager *manager,
    size_t shard_index,
    KVCacheManager::Impl::ShardState *shard,
    const std::string &key,
    uint32_t partition_id,
    KVCacheManager::Impl::SlotStateKind reserved_state,
    uint32_t *out_slot_id,
    std::string *error_message)
{
    if (manager == nullptr || manager->impl_ == nullptr || shard == nullptr ||
        out_slot_id == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid slot allocation arguments";
        }
        return false;
    }

    auto &impl = *manager->impl_;
    auto try_reuse_existing_slot = [&]() -> std::optional<uint32_t> {
        const auto existing_it =
            shard->resident_slots.find(runtime_internal::MakeResidentIndexKey(key, partition_id));
        if (existing_it == shard->resident_slots.end())
        {
            return std::nullopt;
        }
        auto &slot = impl.slots[existing_it->second];
        if (!IsResidentState(slot.state) || slot.flush_in_progress)
        {
            return std::nullopt;
        }
        RemoveResidentSlotMapping(shard, &slot);
        slot.generation += 1;
        ResetSlotToFree(&slot);
        return slot.slot_id;
    };

    auto try_take_clean_victim = [&]() -> std::optional<uint32_t> {
        while (!shard->resident_lru.empty())
        {
            const uint32_t candidate_slot_id = shard->resident_lru.front();
            shard->resident_lru.pop_front();
            auto &candidate_slot = impl.slots[candidate_slot_id];
            if (!IsResidentState(candidate_slot.state) || candidate_slot.flush_in_progress)
            {
                continue;
            }
            if (candidate_slot.state !=
                KVCacheManager::Impl::SlotStateKind::MemoryAndEloqStoreClean)
            {
                shard->resident_lru.push_back(candidate_slot_id);
                continue;
            }
            RemoveResidentSlotMapping(shard, &candidate_slot);
            candidate_slot.generation += 1;
            ResetSlotToFree(&candidate_slot);
            return candidate_slot_id;
        }
        return std::nullopt;
    };

    auto queue_pressure_flush = [&]() -> bool {
        const size_t resident_count = shard->resident_lru.size();
        for (size_t idx = 0; idx < resident_count; ++idx)
        {
            const uint32_t candidate_slot_id = shard->resident_lru[idx];
            auto &candidate_slot = impl.slots[candidate_slot_id];
            if (!IsDirtyState(candidate_slot.state))
            {
                continue;
            }
            if (EnqueueFlushWork(
                    &impl, shard, static_cast<uint32_t>(shard_index), &candidate_slot, true))
            {
                return true;
            }
        }
        return false;
    };

    std::unique_lock<std::mutex> lock(shard->mutex);
    while (true)
    {
        if (const auto slot_id = try_reuse_existing_slot(); slot_id.has_value())
        {
            auto &slot = impl.slots[*slot_id];
            slot.state = reserved_state;
            slot.partition_id = partition_id;
            *out_slot_id = *slot_id;
            return true;
        }
        if (!shard->free_slots.empty())
        {
            const uint32_t slot_id = shard->free_slots.front();
            shard->free_slots.pop_front();
            auto &slot = impl.slots[slot_id];
            slot.state = reserved_state;
            slot.partition_id = partition_id;
            *out_slot_id = slot_id;
            return true;
        }
        if (const auto slot_id = try_take_clean_victim(); slot_id.has_value())
        {
            auto &slot = impl.slots[*slot_id];
            slot.state = reserved_state;
            slot.partition_id = partition_id;
            *out_slot_id = *slot_id;
            return true;
        }
        if (!shard->last_flush_error.empty())
        {
            if (error_message != nullptr)
            {
                *error_message = shard->last_flush_error;
            }
            return false;
        }
        if (!queue_pressure_flush())
        {
            if (error_message != nullptr)
            {
                *error_message = "no slot available and no dirty resident entry can be flushed";
            }
            return false;
        }
        shard->cv.wait(lock, [&]() {
            return impl.stopping.load() || !shard->free_slots.empty() ||
                   !shard->last_flush_error.empty();
        });
        if (impl.stopping.load())
        {
            if (error_message != nullptr)
            {
                *error_message = "kv cache manager is stopping";
            }
            return false;
        }
    }
}

}  // namespace eloqstore::sdk
