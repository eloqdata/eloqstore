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

void CleanupSharedMemory(const std::string &name)
{
    if (name.empty())
    {
        return;
    }
    ::shm_unlink(name.c_str());
}

int CreateSharedMemory(const std::string &name)
{
    int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd >= 0 || errno != EEXIST)
    {
        return fd;
    }
    CleanupSharedMemory(name);
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
    return alignment == 0 ? value
                          : ((value + alignment - 1) / alignment) * alignment;
}

size_t PinnedReadBytes(uint32_t payload_bytes)
{
    constexpr size_t kPageAlignment = 4096;
    constexpr size_t kSegmentSize = 256 * 1024;
    if (payload_bytes == 0)
    {
        return 0;
    }
    const size_t prefix_bytes =
        payload_bytes <= kSegmentSize
            ? 0
            : ((payload_bytes - 1) / kSegmentSize) * kSegmentSize;
    const size_t tail_bytes = payload_bytes - prefix_bytes;
    return prefix_bytes + AlignUp(tail_bytes, kPageAlignment);
}

std::vector<ShardLayout> BuildShards(const KVCacheOptions &options)
{
    std::vector<ShardLayout> shards;
    const uint32_t shard_count = std::max<uint32_t>(1, options.num_threads);
    const uint32_t entries_per_shard = options.entry_count / shard_count;
    const uint32_t remainder = options.entry_count % shard_count;
    uint32_t next_start_entry = 0;
    for (uint32_t shard_id = 0; shard_id < shard_count; ++shard_id)
    {
        const uint32_t extra_entry = shard_id < remainder ? 1 : 0;
        const uint32_t shard_entries = entries_per_shard + extra_entry;
        shards.push_back(ShardLayout{
            .shard_id = shard_id,
            .start_entry = next_start_entry,
            .entry_count = shard_entries,
        });
        next_start_entry += shard_entries;
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

uint32_t PartitionIdForKey(const KVCacheOptions &options,
                           const std::string &key)
{
    const uint32_t partition_count =
        std::max<uint32_t>(1, options.partition_count);
    return static_cast<uint32_t>(std::hash<std::string>{}(key) %
                                 partition_count);
}

bool SendFrames(zmq::socket_t &socket,
                const std::vector<std::string> &frames,
                std::string *error_message)
{
    try
    {
        for (size_t i = 0; i < frames.size(); ++i)
        {
            const zmq::send_flags flags = i + 1 < frames.size()
                                              ? zmq::send_flags::sndmore
                                              : zmq::send_flags::none;
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
            frames->emplace_back(static_cast<const char *>(frame.data()),
                                 frame.size());
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
    return shard_count == 0
               ? 0
               : static_cast<size_t>((request_id - 1) % shard_count);
}

std::string MakeResidentIndexKey(const std::string &key, uint32_t partition_id)
{
    return std::to_string(partition_id) + "|" + key;
}

std::vector<std::string> EncodeBufferHandleRecord(
    const KVCacheBufferHandle &buffer)
{
    return {
        std::to_string(buffer.request_id),
        std::to_string(buffer.offset_bytes),
        std::to_string(buffer.payload_bytes),
    };
}

std::vector<std::string> EncodeRequestStateRecord(
    const KVCacheRequestState &state)
{
    return {
        std::to_string(state.request_id),
        std::to_string(static_cast<uint32_t>(state.status)),
        std::to_string(state.offset_bytes),
        std::to_string(state.payload_bytes),
        state.error_message,
    };
}

bool DecodeBufferHandleRecord(const std::vector<std::string> &frames,
                              size_t offset,
                              KVCacheBufferHandle *out_buffer)
{
    if (out_buffer == nullptr || frames.size() != offset + 3)
    {
        return false;
    }
    const auto request_id = ParseUint64(frames[offset + 0]);
    const auto offset_bytes = ParseUint64(frames[offset + 1]);
    const auto payload_bytes = ParseUint32(frames[offset + 2]);
    if (!request_id.has_value() || !offset_bytes.has_value() ||
        !payload_bytes.has_value())
    {
        return false;
    }
    out_buffer->request_id = *request_id;
    out_buffer->offset_bytes = *offset_bytes;
    out_buffer->payload_bytes = *payload_bytes;
    return true;
}

bool DecodeRequestStateRecord(const std::vector<std::string> &frames,
                              size_t offset,
                              KVCacheRequestState *out_state)
{
    if (out_state == nullptr || frames.size() != offset + 5)
    {
        return false;
    }
    const auto request_id = ParseUint64(frames[offset + 0]);
    const auto status = ParseUint32(frames[offset + 1]);
    const auto offset_bytes = ParseUint64(frames[offset + 2]);
    const auto payload_bytes = ParseUint32(frames[offset + 3]);
    if (!request_id.has_value() || !status.has_value() ||
        !offset_bytes.has_value() || !payload_bytes.has_value())
    {
        return false;
    }
    out_state->request_id = *request_id;
    out_state->status = static_cast<KVCacheRequestStatus>(*status);
    out_state->offset_bytes = *offset_bytes;
    out_state->payload_bytes = *payload_bytes;
    out_state->error_message = frames[offset + 4];
    return true;
}

std::vector<std::string> HandleManagerIpcMessage(
    KVCacheManager *manager, const std::vector<std::string> &request_frames)
{
    if (request_frames.empty())
    {
        return {"error", "missing kv cache ipc command"};
    }
    const std::string &command = request_frames[0];
    std::string error_message;
    if (command == "begin_save")
    {
        if (request_frames.size() != 3)
        {
            return {"error", "invalid begin-save request"};
        }
        const auto payload_bytes = ParseUint32(request_frames[2]);
        if (!payload_bytes.has_value())
        {
            return {"error", "failed to parse begin-save payload bytes"};
        }
        KVCacheBufferHandle buffer;
        if (!manager->BeginSave(
                request_frames[1], *payload_bytes, &buffer, &error_message))
        {
            return {"error", error_message};
        }
        auto response = EncodeBufferHandleRecord(buffer);
        response.insert(response.begin(), "ok");
        return response;
    }
    if (command == "finish_save")
    {
        if (request_frames.size() != 2)
        {
            return {"error", "invalid finish-save request"};
        }
        const auto request_id = ParseUint64(request_frames[1]);
        if (!request_id.has_value())
        {
            return {"error", "failed to parse finish-save request id"};
        }
        if (!manager->FinishSave(*request_id, &error_message))
        {
            return {"error", error_message};
        }
        return {"ok"};
    }
    if (command == "begin_load")
    {
        if (request_frames.size() != 3)
        {
            return {"error", "invalid begin-load request"};
        }
        const auto payload_bytes = ParseUint32(request_frames[2]);
        if (!payload_bytes.has_value())
        {
            return {"error", "failed to parse begin-load payload bytes"};
        }
        uint64_t request_id = 0;
        if (!manager->BeginLoad(
                request_frames[1], *payload_bytes, &request_id, &error_message))
        {
            return {"error", error_message};
        }
        return {"ok", std::to_string(request_id)};
    }
    if (command == "check_request")
    {
        if (request_frames.size() != 2)
        {
            return {"error", "invalid check-request request"};
        }
        const auto request_id = ParseUint64(request_frames[1]);
        if (!request_id.has_value())
        {
            return {"error", "failed to parse check-request id"};
        }
        KVCacheRequestState state;
        if (!manager->CheckRequest(*request_id, &state, &error_message))
        {
            return {"error", error_message};
        }
        auto response = EncodeRequestStateRecord(state);
        response.insert(response.begin(), "ok");
        return response;
    }
    if (command == "get_ready_buffer")
    {
        if (request_frames.size() != 2)
        {
            return {"error", "invalid get-ready-buffer request"};
        }
        const auto request_id = ParseUint64(request_frames[1]);
        if (!request_id.has_value())
        {
            return {"error", "failed to parse get-ready-buffer id"};
        }
        KVCacheBufferHandle buffer;
        if (!manager->GetReadyBuffer(*request_id, &buffer, &error_message))
        {
            return {"error", error_message};
        }
        auto response = EncodeBufferHandleRecord(buffer);
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
            auto new_socket = std::make_unique<zmq::socket_t>(
                **context, zmq::socket_type::req);
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
            *error_message =
                std::string("ZeroMQ worker connect failed: ") + e.what();
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
            *error_message =
                std::string("ZeroMQ worker exchange failed: ") + e.what();
        }
        return false;
    }
}

}  // namespace runtime_internal

bool KVCacheRuntimeHelpers::IsResidentState(
    KVCacheManager::Impl::EntryStateKind state)
{
    return state == KVCacheManager::Impl::EntryStateKind::MemoryOnlyDirty ||
           state ==
               KVCacheManager::Impl::EntryStateKind::MemoryAndEloqStoreClean;
}

bool KVCacheRuntimeHelpers::IsDirtyState(
    KVCacheManager::Impl::EntryStateKind state)
{
    return state == KVCacheManager::Impl::EntryStateKind::MemoryOnlyDirty;
}

void KVCacheRuntimeHelpers::ResetEntryToFree(
    KVCacheManager::Impl::BufferEntryState *entry)
{
    if (entry == nullptr)
    {
        return;
    }
    entry->state = KVCacheManager::Impl::EntryStateKind::Free;
    entry->flush_enqueued = false;
    entry->flush_in_progress = false;
    entry->flush_after_write = false;
    entry->partition_id = 0;
    entry->payload_bytes = 0;
    entry->key.clear();
}

void KVCacheRuntimeHelpers::TouchResidentLru(
    KVCacheManager::Impl::ShardState *shard, uint32_t entry_id)
{
    if (shard == nullptr)
    {
        return;
    }
    shard->resident_lru.erase(
        std::remove(
            shard->resident_lru.begin(), shard->resident_lru.end(), entry_id),
        shard->resident_lru.end());
    shard->resident_lru.push_back(entry_id);
}

void KVCacheRuntimeHelpers::RemoveResidentEntryMapping(
    KVCacheManager::Impl::ShardState *shard,
    KVCacheManager::Impl::BufferEntryState *entry)
{
    if (shard == nullptr || entry == nullptr || entry->key.empty())
    {
        return;
    }
    shard->resident_entries.erase(runtime_internal::MakeResidentIndexKey(
        entry->key, entry->partition_id));
    shard->resident_lru.erase(std::remove(shard->resident_lru.begin(),
                                          shard->resident_lru.end(),
                                          entry->entry_id),
                              shard->resident_lru.end());
}

bool KVCacheRuntimeHelpers::EnqueueFlushWork(
    KVCacheManager::Impl *impl,
    KVCacheManager::Impl::ShardState *shard,
    uint32_t shard_index,
    KVCacheManager::Impl::BufferEntryState *entry,
    bool release_after_write)
{
    if (impl == nullptr || shard == nullptr || entry == nullptr)
    {
        return false;
    }
    if (!IsDirtyState(entry->state))
    {
        return false;
    }
    entry->flush_after_write = entry->flush_after_write || release_after_write;
    if (entry->flush_enqueued || entry->flush_in_progress)
    {
        return true;
    }
    entry->flush_enqueued = true;
    shard->flush_queue.push_back(KVCacheManager::Impl::ShardState::FlushItem{
        .entry_id = entry->entry_id,
        .entry_generation = entry->generation,
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

uint64_t KVCacheRuntimeHelpers::EntryOffsetBytes(const KVCacheOptions &options,
                                                 uint32_t entry_id)
{
    return static_cast<uint64_t>(entry_id) *
           static_cast<uint64_t>(options.entry_size);
}

bool KVCacheRuntimeHelpers::AllocateEntryForRequest(
    KVCacheManager *manager,
    size_t shard_index,
    KVCacheManager::Impl::ShardState *shard,
    const std::string &key,
    uint32_t partition_id,
    KVCacheManager::Impl::EntryStateKind reserved_state,
    uint32_t *out_entry_id,
    std::string *error_message)
{
    if (manager == nullptr || manager->impl_ == nullptr || shard == nullptr ||
        out_entry_id == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid block-slot allocation arguments";
        }
        return false;
    }

    auto &impl = *manager->impl_;
    auto try_reuse_existing_entry = [&]() -> std::optional<uint32_t>
    {
        const auto existing_it = shard->resident_entries.find(
            runtime_internal::MakeResidentIndexKey(key, partition_id));
        if (existing_it == shard->resident_entries.end())
        {
            return std::nullopt;
        }
        auto &entry = impl.entries[existing_it->second];
        if (!IsResidentState(entry.state) || entry.flush_in_progress)
        {
            return std::nullopt;
        }
        RemoveResidentEntryMapping(shard, &entry);
        entry.generation += 1;
        ResetEntryToFree(&entry);
        return entry.entry_id;
    };

    auto try_take_clean_victim = [&]() -> std::optional<uint32_t>
    {
        while (!shard->resident_lru.empty())
        {
            const uint32_t candidate_entry_id = shard->resident_lru.front();
            shard->resident_lru.pop_front();
            auto &candidate_entry = impl.entries[candidate_entry_id];
            if (!IsResidentState(candidate_entry.state) ||
                candidate_entry.flush_in_progress)
            {
                continue;
            }
            if (candidate_entry.state !=
                KVCacheManager::Impl::EntryStateKind::MemoryAndEloqStoreClean)
            {
                shard->resident_lru.push_back(candidate_entry_id);
                continue;
            }
            RemoveResidentEntryMapping(shard, &candidate_entry);
            candidate_entry.generation += 1;
            ResetEntryToFree(&candidate_entry);
            return candidate_entry_id;
        }
        return std::nullopt;
    };

    auto queue_pressure_flush = [&]() -> bool
    {
        const size_t resident_count = shard->resident_lru.size();
        for (size_t idx = 0; idx < resident_count; ++idx)
        {
            const uint32_t candidate_entry_id = shard->resident_lru[idx];
            auto &candidate_entry = impl.entries[candidate_entry_id];
            if (!IsDirtyState(candidate_entry.state))
            {
                continue;
            }
            if (EnqueueFlushWork(&impl,
                                 shard,
                                 static_cast<uint32_t>(shard_index),
                                 &candidate_entry,
                                 true))
            {
                return true;
            }
        }
        return false;
    };

    std::unique_lock<std::mutex> lock(shard->mutex);
    while (true)
    {
        if (const auto entry_id = try_reuse_existing_entry();
            entry_id.has_value())
        {
            auto &entry = impl.entries[*entry_id];
            entry.state = reserved_state;
            entry.partition_id = partition_id;
            *out_entry_id = *entry_id;
            return true;
        }
        if (!shard->free_entries.empty())
        {
            const uint32_t entry_id = shard->free_entries.front();
            shard->free_entries.pop_front();
            auto &entry = impl.entries[entry_id];
            entry.state = reserved_state;
            entry.partition_id = partition_id;
            *out_entry_id = entry_id;
            return true;
        }
        if (const auto entry_id = try_take_clean_victim(); entry_id.has_value())
        {
            auto &entry = impl.entries[*entry_id];
            entry.state = reserved_state;
            entry.partition_id = partition_id;
            *out_entry_id = *entry_id;
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
                *error_message =
                    "no block slot available and no dirty resident slot can be "
                    "flushed";
            }
            return false;
        }
        shard->cv.wait(lock,
                       [&]()
                       {
                           return impl.stopping.load() ||
                                  !shard->free_entries.empty() ||
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
