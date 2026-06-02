#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <mutex>
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
    uint32_t slot_size{0};
    uint32_t slot_count{0};
    uint32_t slot_alignment{4096};
    uint32_t submission_queue_depth{128};
    bool eager_io_uring_register{true};
};

struct ShardLayout
{
    uint32_t shard_id{0};
    uint32_t start_slot{0};
    uint32_t slot_count{0};
};

enum class KVCacheRequestKind : uint8_t
{
    Save = 1,
    Load = 2,
};

enum class KVCacheRequestStatus : uint8_t
{
    Submitted = 1,
    Completed = 2,
    Rejected = 3,
    Failed = 4,
};

struct KVCacheRequest
{
    // slot_generation lets workers detect stale completions after a slot has
    // been recycled for a newer request.
    uint64_t request_id{0};
    KVCacheRequestKind kind{KVCacheRequestKind::Save};
    uint32_t partition_id{0};
    uint32_t shard_id{0};
    uint32_t slot_id{0};
    uint32_t slot_generation{0};
    uint32_t payload_bytes{0};
    std::string key;
};

struct KVCacheCompletion
{
    uint64_t request_id{0};
    KVCacheRequestKind kind{KVCacheRequestKind::Save};
    KVCacheRequestStatus status{KVCacheRequestStatus::Submitted};
    uint32_t partition_id{0};
    uint32_t shard_id{0};
    uint32_t slot_id{0};
    uint32_t slot_generation{0};
    uint32_t payload_bytes{0};
    std::string key;
    std::string error_message;
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

    // Allocate the shared-memory pool, initialize shard-local slot ranges, and
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
    // Reserve one slot for a save request in the shard selected by partition_id.
    // The request remains blocked until MarkSaveRequestReady is called.
    bool SubmitSaveRequest(const std::string &key,
                           uint32_t partition_id,
                           uint32_t payload_bytes,
                           KVCacheRequest *out_request,
                           std::string *error_message);
    // Transition a previously reserved save request into the runnable state
    // after the worker has finished writing payload bytes into the slot.
    bool MarkSaveRequestReady(uint64_t request_id, std::string *error_message);
    // Reserve one slot for a load request in the shard selected by partition_id.
    bool SubmitLoadRequest(const std::string &key,
                           uint32_t partition_id,
                           uint32_t payload_bytes,
                           KVCacheRequest *out_request,
                           std::string *error_message);
    // Check whether one key already exists, first in memory then in EloqStore.
    bool ContainsKey(const std::string &key,
                     uint32_t partition_id,
                     bool *out_exists,
                     std::string *error_message);
    // Return the next finished request, if any. False with an empty error means
    // the completion queue is currently empty.
    bool PollCompletion(KVCacheCompletion *out_completion,
                        std::string *error_message);
    const std::vector<ShardLayout> &shards() const { return shards_; }

    bool started() const { return started_; }
    bool io_uring_registered() const { return io_uring_registered_; }
    const KVCacheOptions &options() const { return options_; }

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
    // pool descriptor, recommends a partition for its lane, and forwards
    // save/load state transitions to the manager over IPC.
    explicit KVCacheWorker(KVCacheOptions options);
    ~KVCacheWorker();

    KVCacheWorker(const KVCacheWorker &) = delete;
    KVCacheWorker &operator=(const KVCacheWorker &) = delete;

    // Parse and store the manager-exported descriptor. This does not mmap the
    // segment; higher layers decide how to map and register shared pages.
    bool AttachBufferPool(const std::string &descriptor, std::string *error_message);
    // Drop the attached descriptor from the worker stub.
    void DetachBufferPool();
    // Recommend one partition for a worker lane so most requests keep a stable
    // partition->shard routing pattern.
    uint32_t RecommendPartition(uint32_t worker_lane) const;
    // Forward a save-slot reservation request to the manager over IPC.
    bool SubmitSaveRequest(const std::string &key,
                           uint32_t partition_id,
                           uint32_t payload_bytes,
                           KVCacheRequest *out_request,
                           std::string *error_message);
    // Forward a load-slot reservation request to the manager over IPC.
    bool SubmitLoadRequest(const std::string &key,
                           uint32_t partition_id,
                           uint32_t payload_bytes,
                           KVCacheRequest *out_request,
                           std::string *error_message);
    // Notify the manager that a previously reserved save slot has been filled
    // by the worker and can enter real I/O execution.
    bool MarkSaveRequestReady(uint64_t request_id, std::string *error_message);
    // Poll the manager for one completion record via IPC.
    bool PollCompletion(KVCacheCompletion *out_completion,
                        std::string *error_message);

    bool attached() const { return attached_; }
    const std::string &buffer_pool_descriptor() const { return buffer_pool_descriptor_; }

  private:
    struct Impl;
    KVCacheOptions options_;
    bool attached_{false};
    std::string buffer_pool_descriptor_;
    mutable std::mutex ipc_mutex_;
    Impl *impl_{nullptr};
};

}  // namespace eloqstore::sdk
