#pragma once

#include "sdk_runtime.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <zmq.hpp>

#include "eloq_store.h"

namespace eloqstore::sdk
{

struct KVCacheManager::Impl
{
    enum class SlotStateKind : uint8_t
    {
        Free = 0,
        ReservedForSave = 1,
        ReservedForLoad = 2,
        MemoryOnlyDirty = 3,
        MemoryAndEloqStoreClean = 4,
    };

    struct SlotState
    {
        uint32_t slot_id{0};
        uint32_t generation{0};
        SlotStateKind state{SlotStateKind::Free};
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
            uint32_t slot_id{0};
            uint32_t slot_generation{0};
        };

        ShardLayout layout;
        std::mutex mutex;
        std::condition_variable cv;
        std::deque<uint32_t> free_slots;
        std::unordered_map<uint64_t, KVCacheRequest> pending_save_requests;
        std::unordered_map<std::string, uint32_t> resident_slots;
        std::deque<uint32_t> resident_lru;
        std::deque<FlushItem> flush_queue;
        std::string last_flush_error;
        bool queued_for_flusher{false};
    };

    std::atomic<uint64_t> next_request_sequence{0};
    std::mutex flush_scheduler_mutex;
    std::condition_variable flush_scheduler_cv;
    std::deque<uint32_t> runnable_flush_shards;
    std::mutex completion_mutex;
    std::deque<KVCacheCompletion> completions;
    std::vector<SlotState> slots;
    std::deque<ShardState> shards;
    std::thread flush_thread;
    std::atomic<bool> stopping{false};
    std::mutex store_mutex;
    std::unique_ptr<EloqStore> store;
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
    static bool IsResidentState(KVCacheManager::Impl::SlotStateKind state);
    static bool IsDirtyState(KVCacheManager::Impl::SlotStateKind state);
    static void ResetSlotToFree(KVCacheManager::Impl::SlotState *slot);
    static void TouchResidentLru(KVCacheManager::Impl::ShardState *shard, uint32_t slot_id);
    static void RemoveResidentSlotMapping(KVCacheManager::Impl::ShardState *shard,
                                          KVCacheManager::Impl::SlotState *slot);
    static bool EnqueueFlushWork(KVCacheManager::Impl *impl,
                                 KVCacheManager::Impl::ShardState *shard,
                                 uint32_t shard_index,
                                 KVCacheManager::Impl::SlotState *slot,
                                 bool release_after_write);
    static bool AllocateSlotForRequest(KVCacheManager *manager,
                                       size_t shard_index,
                                       KVCacheManager::Impl::ShardState *shard,
                                       const std::string &key,
                                       uint32_t partition_id,
                                       KVCacheManager::Impl::SlotStateKind reserved_state,
                                       uint32_t *out_slot_id,
                                       std::string *error_message);
};

struct KVCacheRuntimeManagerOps
{
    static bool ReserveSaveSlot(KVCacheManager *manager,
                                size_t shard_index,
                                const std::string &key,
                                uint32_t partition_id,
                                uint32_t payload_bytes,
                                KVCacheRequest *out_request,
                                std::string *error_message);
    static bool FinalizeSaveReady(KVCacheManager *manager,
                                  uint64_t request_id,
                                  KVCacheCompletion *out_completion,
                                  std::string *error_message);
    static bool SubmitLoad(KVCacheManager *manager,
                           size_t shard_index,
                           const std::string &key,
                           uint32_t partition_id,
                           uint32_t payload_bytes,
                           KVCacheRequest *out_request,
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
std::vector<std::string> EncodeRequestRecord(const KVCacheRequest &request);
std::vector<std::string> EncodeCompletionRecord(const KVCacheCompletion &completion);
bool DecodeRequestRecord(const std::vector<std::string> &frames,
                         size_t offset,
                         const std::string &key,
                         KVCacheRequest *out_request);
bool DecodeCompletionRecord(const std::vector<std::string> &frames,
                            size_t offset,
                            KVCacheCompletion *out_completion);
std::vector<std::string> HandleManagerIpcMessage(
    KVCacheManager *manager,
    const std::vector<std::string> &request_frames);
bool EnsureWorkerSocketConnected(const KVCacheOptions &options,
                                 std::unique_ptr<zmq::context_t> *context,
                                 std::unique_ptr<zmq::socket_t> *socket,
                                 std::string *error_message);
bool ExchangeWorkerIpc(zmq::socket_t *socket,
                       const std::vector<std::string> &request_frames,
                       std::vector<std::string> *response_frames,
                       std::string *error_message);
bool IsBenignPollCompletionTransportError(const std::string &error_message);

}  // namespace runtime_internal

}  // namespace eloqstore::sdk
