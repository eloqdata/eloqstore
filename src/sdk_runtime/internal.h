#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <zmq.hpp>

#include "eloq_store.h"
#include "sdk_runtime.h"

namespace eloqstore::sdk
{

struct KVCacheManager::Impl
{
    enum class EntryStateKind : uint8_t
    {
        Free = 0,
        ReservedForSave = 1,
        ReservedForLoad = 2,
        MemoryOnlyDirty = 3,
        MemoryAndEloqStoreClean = 4,
    };

    struct BufferEntryState
    {
        uint32_t entry_id{0};
        uint32_t generation{0};
        EntryStateKind state{EntryStateKind::Free};
        bool flush_enqueued{false};
        bool flush_in_progress{false};
        bool flush_after_write{false};
        uint32_t partition_id{0};
        uint32_t payload_bytes{0};
        std::string key;
    };

    struct ShardState
    {
        struct FlushItem
        {
            uint32_t entry_id{0};
            uint32_t entry_generation{0};
        };

        ShardLayout layout;
        std::mutex mutex;
        std::condition_variable cv;
        std::deque<uint32_t> free_entries;
        std::unordered_map<std::string, uint32_t> resident_entries;
        std::deque<uint32_t> resident_lru;
        std::deque<FlushItem> flush_queue;
        std::string last_flush_error;
        bool queued_for_flusher{false};
    };

    std::atomic<uint64_t> next_request_sequence{0};
    std::mutex flush_scheduler_mutex;
    std::condition_variable flush_scheduler_cv;
    std::deque<uint32_t> runnable_flush_shards;
    std::mutex request_state_mutex;
    std::unordered_map<uint64_t, KVCacheRequestState> request_states;
    std::vector<BufferEntryState> entries;
    std::deque<ShardState> shards;
    std::thread flush_thread;
    std::atomic<bool> stopping{false};
    std::mutex store_mutex;
    std::unique_ptr<EloqStore> store;
    std::atomic<uint64_t> flush_batches_submitted{0};
    std::atomic<uint64_t> flush_batches_completed{0};
    std::atomic<uint64_t> flush_batches_failed{0};
    std::atomic<uint64_t> flush_entries_submitted{0};
    std::atomic<uint64_t> flush_entries_completed{0};
    std::atomic<uint64_t> flush_entries_failed{0};
    std::atomic<uint64_t> flush_batch_latency_ns_total{0};
    std::atomic<uint64_t> contains_memory_hits{0};
    std::atomic<uint64_t> contains_store_hits{0};
    std::atomic<uint64_t> contains_store_misses{0};
    std::atomic<uint64_t> contains_store_errors{0};
    std::atomic<uint64_t> contains_store_lookup_ns_total{0};
    std::atomic<uint64_t> load_memory_hits{0};
    std::atomic<uint64_t> load_store_hits{0};
    std::atomic<uint64_t> load_store_errors{0};
    std::atomic<uint64_t> load_store_bytes{0};
    std::atomic<uint64_t> load_store_latency_ns_total{0};
    std::string ipc_endpoint;
    std::unique_ptr<zmq::context_t> zmq_context;
    std::unique_ptr<zmq::socket_t> ipc_socket;
    std::thread ipc_thread;
};

struct KVCacheWorker::Impl
{
    std::unique_ptr<zmq::context_t> ipc_context;
    std::unique_ptr<zmq::socket_t> ipc_socket;
};

struct KVCacheRuntimeHelpers
{
    static bool IsResidentState(KVCacheManager::Impl::EntryStateKind state);
    static bool IsDirtyState(KVCacheManager::Impl::EntryStateKind state);
    static void ResetEntryToFree(KVCacheManager::Impl::BufferEntryState *entry);
    static void TouchResidentLru(KVCacheManager::Impl::ShardState *shard,
                                 uint32_t entry_id);
    static void RemoveResidentEntryMapping(
        KVCacheManager::Impl::ShardState *shard,
        KVCacheManager::Impl::BufferEntryState *entry);
    static bool EnqueueFlushWork(KVCacheManager::Impl *impl,
                                 KVCacheManager::Impl::ShardState *shard,
                                 uint32_t shard_index,
                                 KVCacheManager::Impl::BufferEntryState *entry,
                                 bool release_after_write);
    static uint64_t EntryOffsetBytes(const KVCacheOptions &options,
                                     uint32_t entry_id);
    static bool AllocateEntryForRequest(
        KVCacheManager *manager,
        size_t shard_index,
        KVCacheManager::Impl::ShardState *shard,
        const std::string &key,
        uint32_t partition_id,
        KVCacheManager::Impl::EntryStateKind reserved_state,
        uint32_t *out_entry_id,
        std::string *error_message);
};

struct KVCacheRuntimeManagerOps
{
    static bool BeginSave(KVCacheManager *manager,
                          size_t shard_index,
                          const std::string &key,
                          uint32_t partition_id,
                          uint32_t payload_bytes,
                          KVCacheBufferHandle *out_buffer,
                          std::string *error_message);
    static bool FinishSave(KVCacheManager *manager,
                           uint64_t request_id,
                           std::string *error_message);
    static bool BeginLoad(KVCacheManager *manager,
                          size_t shard_index,
                          const std::string &key,
                          uint32_t partition_id,
                          uint32_t payload_bytes,
                          uint64_t *out_request_id,
                          std::string *error_message);
    static bool ContainsKey(KVCacheManager *manager,
                            size_t shard_index,
                            const std::string &key,
                            uint32_t partition_id,
                            bool *out_exists,
                            std::string *error_message);
};

namespace runtime_internal
{

std::string BuildDefaultSharedMemoryName(const KVCacheOptions &options);
std::string BuildSharedMemoryFsPath(const std::string &name);
void CleanupSharedMemory(const std::string &name);
int CreateSharedMemory(const std::string &name);
std::string EncodePayloadMetadata(uint32_t payload_bytes);
bool DecodePayloadMetadata(const std::string &metadata,
                           uint32_t fallback_bytes,
                           uint32_t *out_payload_bytes);
size_t AlignUp(size_t value, size_t alignment);
size_t PinnedReadBytes(uint32_t payload_bytes);
std::vector<ShardLayout> BuildShards(const KVCacheOptions &options);
std::vector<std::string> Split(const std::string &value, char delim);
std::optional<uint32_t> ParseUint32(std::string_view value);
std::optional<uint64_t> ParseUint64(std::string_view value);
bool SendFrames(zmq::socket_t &socket,
                const std::vector<std::string> &frames,
                std::string *error_message);
bool ReceiveFrames(zmq::socket_t &socket,
                   std::vector<std::string> *frames,
                   std::string *error_message);
uint64_t MakeRequestId(std::atomic<uint64_t> &next_request_sequence,
                       size_t shard_count,
                       uint32_t shard_id);
size_t RequestShardIndex(uint64_t request_id, size_t shard_count);
std::string MakeResidentIndexKey(const std::string &key, uint32_t partition_id);
std::vector<std::string> EncodeBufferHandleRecord(
    const KVCacheBufferHandle &buffer);
std::vector<std::string> EncodeRequestStateRecord(
    const KVCacheRequestState &state);
bool DecodeBufferHandleRecord(const std::vector<std::string> &frames,
                              size_t offset,
                              KVCacheBufferHandle *out_buffer);
bool DecodeRequestStateRecord(const std::vector<std::string> &frames,
                              size_t offset,
                              KVCacheRequestState *out_state);
std::vector<std::string> HandleManagerIpcMessage(
    KVCacheManager *manager, const std::vector<std::string> &request_frames);
bool EnsureWorkerSocketConnected(const KVCacheOptions &options,
                                 std::unique_ptr<zmq::context_t> *context,
                                 std::unique_ptr<zmq::socket_t> *socket,
                                 std::string *error_message);
bool ExchangeWorkerIpc(zmq::socket_t *socket,
                       const std::vector<std::string> &request_frames,
                       std::vector<std::string> *response_frames,
                       std::string *error_message);
uint32_t PartitionIdForKey(const KVCacheOptions &options,
                           const std::string &key);

}  // namespace runtime_internal

}  // namespace eloqstore::sdk
