#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace eloqstore::sdk
{

struct KVCacheRuntimeHelpers;
struct KVCacheRuntimeManagerOps;

struct KVCacheOptions
{
    // These options describe the machine-local runtime shared by the engine
    // core manager and all worker stubs on the same host.
    //
    // The fields intentionally mix three concerns in one transport object:
    // 1) EloqStore session startup, 2) shared-memory/buffer-pool shape,
    // 3) worker<->manager IPC/runtime coordination. The manager consumes most
    // of them to build the runtime; workers reuse the same type to attach to an
    // already created runtime and inherit its descriptor-derived settings.
    std::vector<std::string> store_paths;
    std::string table_name{"default"};
    std::string branch{"main"};
    std::string ipc_path;
    std::string shared_memory_name;
    uint64_t term{0};
    uint32_t partition_group_id{0};
    uint16_t num_threads{1};
    uint32_t partition_count{1};
    size_t shared_memory_bytes{0};
    uint32_t entry_size{0};
    uint32_t entry_count{0};
    uint32_t entry_alignment{4096};
    uint32_t submission_queue_depth{128};
    bool eager_io_uring_register{true};
};

struct ShardLayout
{
    uint32_t shard_id{0};
    uint32_t start_entry{0};
    uint32_t entry_count{0};
};

enum class KVCacheRequestStatus : uint8_t
{
    Pending = 1,
    Ready = 2,
    Failed = 3,
};

struct KVCacheBufferHandle
{
    uint64_t request_id{0};
    uint64_t offset_bytes{0};
    uint32_t payload_bytes{0};
};

struct KVCacheRequestState
{
    uint64_t request_id{0};
    KVCacheRequestStatus status{KVCacheRequestStatus::Pending};
    uint64_t offset_bytes{0};
    uint32_t payload_bytes{0};
    std::string error_message;
};

struct KVCacheRuntimeMetrics
{
    uint64_t flush_batches_submitted{0};
    uint64_t flush_batches_completed{0};
    uint64_t flush_batches_failed{0};
    uint64_t flush_entries_submitted{0};
    uint64_t flush_entries_completed{0};
    uint64_t flush_entries_failed{0};
    uint64_t flush_batch_latency_ns_total{0};
    uint64_t contains_memory_hits{0};
    uint64_t contains_store_hits{0};
    uint64_t contains_store_misses{0};
    uint64_t contains_store_errors{0};
    uint64_t contains_store_lookup_ns_total{0};
    uint64_t load_memory_hits{0};
    uint64_t load_store_hits{0};
    uint64_t load_store_errors{0};
    uint64_t load_store_bytes{0};
    uint64_t load_store_latency_ns_total{0};
    uint64_t dirty_entries_current{0};
    uint64_t flush_queue_entries_current{0};
    uint64_t flush_inflight_entries_current{0};
};

class KVCacheManager
{
public:
    // The manager is the scheduler-side owner. It creates shared memory,
    // registers the process-local EloqStore pinned buffer view, accepts worker
    // IPC requests, and drives the real save/load I/O.
    explicit KVCacheManager(KVCacheOptions options);
    ~KVCacheManager();

    KVCacheManager(const KVCacheManager &) = delete;
    KVCacheManager &operator=(const KVCacheManager &) = delete;

    // Allocate the shared-memory pool, initialize shard-local entry ranges, and
    // start the optional IPC listener used only by worker-side stubs.
    bool Start(std::string *error_message);
    // Tear down worker-facing IPC, stop the background flush path, and release
    // the shared segment owned by the manager.
    void Stop();

    // Register the manager-local shared-memory mapping with EloqStore so save
    // and load requests can reuse the same pinned/fixed-buffer region.
    bool RegisterIoUringBuffers(std::string *error_message);
    // Serialize the manager-owned buffer pool into a descriptor that workers
    // can parse and attach to from another process.
    std::string ExportBufferPoolDescriptor() const;
    bool BeginSave(const std::string &key,
                   uint32_t payload_bytes,
                   KVCacheBufferHandle *out_buffer,
                   std::string *error_message);
    bool FinishSave(uint64_t request_id, std::string *error_message);
    bool BeginLoad(const std::string &key,
                   uint32_t payload_bytes,
                   uint64_t *out_request_id,
                   std::string *error_message);
    bool CheckRequest(uint64_t request_id,
                      KVCacheRequestState *out_state,
                      std::string *error_message);
    bool CheckRequests(const std::vector<uint64_t> &request_ids,
                       std::vector<KVCacheRequestState> *out_states,
                       std::string *error_message);
    bool GetReadyBuffer(uint64_t request_id,
                        KVCacheBufferHandle *out_buffer,
                        std::string *error_message);
    bool ContainsKey(const std::string &key,
                     bool *out_exists,
                     std::string *error_message);
    // Check whether one key already exists, first in memory then in EloqStore.
    bool ContainsKey(const std::string &key,
                     uint32_t partition_id,
                     bool *out_exists,
                     std::string *error_message);
    bool GetMetrics(KVCacheRuntimeMetrics *out_metrics,
                    std::string *error_message);
    const std::vector<ShardLayout> &shards() const
    {
        return shards_;
    }

    bool started() const
    {
        return started_;
    }
    bool io_uring_registered() const
    {
        return io_uring_registered_;
    }
    const KVCacheOptions &options() const
    {
        return options_;
    }

private:
    friend struct KVCacheRuntimeHelpers;
    friend struct KVCacheRuntimeManagerOps;
    KVCacheOptions options_;
    bool started_{false};
    bool io_uring_registered_{false};
    int shm_fd_{-1};
    std::string shm_path_;
    void *shm_addr_{nullptr};
    std::vector<ShardLayout> shards_;
    struct Impl;
    Impl *impl_{nullptr};
};

class KVCacheWorker
{
public:
    // The worker is only a control-plane stub. It attaches the exported buffer
    // pool descriptor and forwards save/load state transitions to the manager
    // over IPC.
    explicit KVCacheWorker(KVCacheOptions options);
    ~KVCacheWorker();

    KVCacheWorker(const KVCacheWorker &) = delete;
    KVCacheWorker &operator=(const KVCacheWorker &) = delete;

    // Parse and store the manager-exported descriptor. This does not mmap the
    // segment; higher layers decide how to map and register shared pages.
    bool AttachBufferPool(const std::string &descriptor,
                          std::string *error_message);
    // Drop the attached descriptor from the worker stub.
    void DetachBufferPool();
    bool BeginSave(const std::string &key,
                   uint32_t payload_bytes,
                   KVCacheBufferHandle *out_buffer,
                   std::string *error_message);
    bool FinishSave(uint64_t request_id, std::string *error_message);
    bool BeginLoad(const std::string &key,
                   uint32_t payload_bytes,
                   uint64_t *out_request_id,
                   std::string *error_message);
    bool CheckRequest(uint64_t request_id,
                      KVCacheRequestState *out_state,
                      std::string *error_message);
    bool CheckRequests(const std::vector<uint64_t> &request_ids,
                       std::vector<KVCacheRequestState> *out_states,
                       std::string *error_message);
    bool GetReadyBuffer(uint64_t request_id,
                        KVCacheBufferHandle *out_buffer,
                        std::string *error_message);
    bool attached() const
    {
        return attached_;
    }
    const std::string &buffer_pool_descriptor() const
    {
        return buffer_pool_descriptor_;
    }

private:
    struct Impl;
    KVCacheOptions options_;
    bool attached_{false};
    std::string buffer_pool_descriptor_;
    mutable std::mutex ipc_mutex_;
    Impl *impl_{nullptr};
};

}  // namespace eloqstore::sdk
