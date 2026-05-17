#pragma once

#include <zstd.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

#include "comparator.h"
#include "types.h"
#include "utils.h"
namespace eloqstore
{
class GlobalRegisteredMemory;

constexpr int KB = 1 << 10;
constexpr int MB = 1 << 20;
constexpr int GB = 1 << 30;
constexpr int64_t TB = 1LL << 40;

constexpr uint8_t max_overflow_pointers = 128;
constexpr uint16_t max_read_pages_batch = max_overflow_pointers;
constexpr uint16_t max_segments_batch = 8;

struct KvOptions
{
    int LoadFromIni(const char *path);
    bool operator==(const KvOptions &other) const;

    /**
     * @brief Number of shards (threads).
     */
    uint16_t num_threads = 1;

    const Comparator *comparator_ = Comparator::DefaultComparator();
    uint16_t data_page_restart_interval = 16;
    uint16_t index_page_restart_interval = 16;
    uint32_t init_page_count = 1 << 15;

    /**
     * @brief Skip checksum verification when reading pages.
     * This is useful for performance testing, but should not be used in
     * production.
     */
    bool skip_verify_checksum = false;
    /**
     * @brief Max size of cached index pages per shard (in bytes).
     */
    uint64_t buffer_pool_size = 32 * MB;
    /**
     * @brief Max size of cached RootMeta mappings (global, in bytes).
     */
    uint64_t root_meta_cache_size = 1 * GB;
    /**
     * @brief Limit manifest file size.
     */
    uint32_t manifest_limit = 8 * MB;
    /**
     * @brief Max number of open files.
     */
    uint32_t fd_limit = 10000;
    /**
     * @brief Size of io-uring submission queue per shard.
     */
    uint32_t io_queue_size = 4096;
    /**
     * @brief Max amount of inflight write IO per shard.
     * Only take effect in non-append write mode.
     */
    uint32_t max_inflight_write = 32 << 10;
    /**
     * @brief The maximum number of pages per batch for the write task.
     */
    uint32_t max_write_batch_pages = 32;
    /**
     * @brief Size of coroutine stack.
     * According to the latest test results, at least 16KB is required.
     */
    uint32_t coroutine_stack_size = 32 * KB;
    /**
     * @brief Limit number of retained archives.
     * Only take effect when data_append_mode is enabled.
     */
    uint32_t num_retained_archives = 100;
    /**
     * @brief Set the (minimum) archive time interval in seconds.
     * 0 means do not generate archives automatically.
     * Only take effect when data_append_mode is enabled and
     * num_retained_archives is not 0.
     */
    uint32_t archive_interval_secs = 0;
    /**
     * @brief The maximum number of running archive tasks at the same time.
     */
    uint16_t max_archive_tasks = 256;
    /**
     * @brief Maximum number of per-partition requests submitted at a time by
     * global operations such as global archive/reopen.
     */
    uint32_t max_global_request_batch = 1000;
    /**
     * @brief Move pages in data file that space amplification factor
     * bigger than this value.
     * Only take effect when data_append_mode is enabled.
     */
    uint8_t file_amplify_factor = 2;
    /**
     * @brief Move segments in a segment file when its space amplification
     * factor exceeds this value. A value of 0 disables segment file
     * compaction independently of data file compaction. Defaults to
     * file_amplify_factor; segment rewrites are ~two orders of magnitude
     * more expensive per unit than page rewrites, so this knob can be
     * raised to prefer space over churn.
     * Only takes effect when data_append_mode is enabled.
     */
    uint8_t segment_file_amplify_factor = 2;
    /**
     * @brief Yield to low-priority queue after processing this many
     * segments during background segment compaction. Each segment is
     * 128KB-512KB, so the page-compaction yield cadence would hold the
     * coroutine far longer between yields; a smaller value protects
     * foreground read/write tail latency. Set to 0 to disable intra-file
     * yielding (outer loops still yield between files).
     */
    uint32_t segment_compact_yield_every = max_segments_batch;
    /**
     * @brief Limit total size of local files.
     * Only take effect when cloud store is enabled.
     */
    size_t local_space_limit = size_t(1) * TB;
    /**
     * @brief Reserved space ratio for new created/download files.
     * At most (local_space_limit / reserve_space_ratio) bytes is reserved.
     * Only take effect when cloud store is enabled.
     */
    uint16_t reserve_space_ratio = 100;
    /**
     * @brief Max number of files uploaded concurrently in a batch.
     */
    uint32_t max_cloud_concurrency = 20;
    /**
     * @brief Maximum number of concurrent write tasks per shard.
     * 0 means unlimited (legacy behavior). In cloud mode, 0 is rewritten to
     * max_cloud_concurrency during option validation.
     */
    uint32_t max_write_concurrency = 0;
    /**
     * @brief Number of dedicated threads that process cloud HTTP requests.
     * Each thread runs curl's multi loop and handles a subset of shards.
     */
    uint16_t cloud_request_threads = 1;
    /**
     * @brief Maximum number of concurrent standby rsync/ssh child processes
     * managed by the single standby supervisor thread.
     */
    uint16_t standby_max_concurrency = 1;
    /**
     * @brief Max cached DirectIO buffers per shard.
     */
    uint32_t direct_io_buffer_pool_size = 16;
    /**
     * @brief Size of each write buffer used for append-mode aggregation.
     * Only take effect when data_append_mode is enabled.
     */
    uint64_t write_buffer_size = 1 * MB;
    /**
     * @brief Batch size for non-page direct IO (e.g. snapshot or upload IO).
     * Must be page-aligned and non-zero.
     */
    uint64_t non_page_io_batch_size = 1 * MB;
    /**
     * @brief Ratio of buffer_pool_size reserved for append-mode write buffers.
     * Only take effect when data_append_mode is enabled.
     */
    double write_buffer_ratio = 0.05;
    /**
     * @brief Reuse files already present in the local cache directory when the
     * store starts.
     */
    bool allow_reuse_local_caches = false;
    /**
     * @brief The chunk size of a big string in local storage.
     */
    uint32_t chunk_size = 256;
    /**
     * @brief The segment size in GlobalRegisteredMemory for storing very large
     * strings. Default 256 KB. Must be 4 KB aligned.
     */
    uint32_t segment_size = 256 * KB;
    /**
     * @brief Per-shard pointers to GlobalRegisteredMemory instances owned
     * externally (by the surrounding system that also hosts the networking
     * module and in-memory cache). When non-empty, must have exactly
     * num_threads entries; each instance is registered with that shard's
     * io_uring and used for zero-copy very-large-value I/O. Leave empty to
     * disable the feature on this EloqStore instance.
     *
     * Mutually exclusive with `pinned_memory_chunks` (KV Cache mode).
     */
    std::vector<GlobalRegisteredMemory *> global_registered_memories;
    /**
     * @brief Raw pinned memory chunks owned by KV Cache (PyTorch-registered).
     * When non-empty, EloqStore registers these chunks in every shard's
     * io_uring as fixed buffers for zero-copy reads/writes of very large
     * values. Unlike `global_registered_memories`, the chunks are shared
     * across all shards (KV Cache manages them centrally) and each shard's
     * ring registers them independently. Each chunk's base address and size
     * must be 4 KiB aligned. Leave empty to disable KV Cache mode.
     *
     * Mutually exclusive with `global_registered_memories`.
     */
    std::vector<std::pair<char *, size_t>> pinned_memory_chunks;
    /**
     * @brief Per-shard capacity for the internal GlobalRegisteredMemory
     * EloqStore allocates in KV Cache mode (pinned_memory_chunks non-empty)
     * to back GC and compaction reads/writes. Online reads/writes use the
     * caller's pinned chunks; background tasks (GC, compaction) cannot,
     * because EloqStore does not manage their lifetime, so an internal pool
     * is required. Default 32 MiB; must satisfy
     * `max_segments_batch * segment_size <= gc_global_mem_size_per_shard`.
     */
    size_t gc_global_mem_size_per_shard = 32ULL * MB;
    /**
     * @brief Number of segment-sized scratch slots EloqStore allocates per
     * shard to back the pinned-write tail fallback. Only meaningful when
     * `pinned_memory_chunks` is non-empty.
     *
     * Background: a pinned write of `size` bytes flushes K = ceil(size /
     * segment_size) full segments to disk. When `size` does not divide
     * `segment_size`, the kernel's fixed write of the final segment reads
     * `K*segment_size - size` bytes past the caller's logical value end.
     * If those trailing bytes still lie inside the caller's registered
     * chunk (the common case), no scratch is needed and the write is
     * zero-copy. If they fall outside the registered chunk (the caller's
     * pinned sub-range ends at a chunk boundary or has no slack), EloqStore
     * acquires one of these scratch slots, copies the meaningful tail into
     * it, zero-pads the rest, and writes from there instead.
     *
     * Each slot is `segment_size` bytes. Defaults to `max_segments_batch`
     * (sized to bound concurrent tail-fallback writes per shard). Set to 0
     * to disable the fallback -- the caller must then guarantee that
     * `[ptr, ptr + K*segment_size)` is fully inside one registered chunk
     * for every pinned write.
     */
    uint16_t pinned_tail_scratch_slots = max_segments_batch;
    /**
     * @brief Number of segments per segment file (1 <<
     * segments_per_file_shift). Default 7 means 128 segments per file (32MB
     * file at 256KB segment).
     */
    uint8_t segments_per_file_shift = 7;

    /* NOTE:
     * The following options will be persisted in storage, so after the first
     * setting, them cannot be changed anymore in the future.
     */

    /**
     * @brief EloqStore storage path list.
     * This can be multiple storage paths corresponding to multiple disks, and
     * partitions will be evenly distributed on each disk. In-memory storage
     * will be used if this is empty.
     */
    std::vector<std::string> store_path;
    /**
     * @brief Optional per-store-path weights used to compute the LUT.
     * Parsed from `store_path` suffix `:<w1,w2,...>`.
     * When specified the weights vector must match store_path in size and the
     * automatic disk-capacity based detection will be skipped.
     */
    std::vector<uint64_t> store_path_weights;
    /**
     * @brief Lookup table that maps partition ids to store_path indexes.
     * Built during initialization to honor disk-capacity based weights.
     * (automatically calculated)
     */
    std::vector<uint32_t> store_path_lut;
    /**
     * @brief Storage path on cloud service.
     * Store all data locally if this is empty.
     * Example: mybucket/eloqstore
     */
    std::string cloud_store_path;
    /**
     * @brief Selects the cloud backend implementation (e.g. aws, gcs).
     */
    std::string cloud_provider = "aws";
    /**
     * @brief Optional override for the cloud endpoint URL.
     */
    std::string cloud_endpoint;
    /**
     * @brief Cloud region/zone identifier.
     */
    std::string cloud_region = "us-east-1";
    /**
     * @brief Access key for cloud storage (e.g. AWS access key ID).
     */
    std::string cloud_access_key = "minioadmin";
    /**
     * @brief Secret key for cloud storage.
     */
    std::string cloud_secret_key = "minioadmin";
    /**
     * @brief Automatically retrieve credentials from the environment or
     * instance metadata rather than using cloud_access_key/cloud_secret_key.
     */
    bool cloud_auto_credentials = false;
    /**
     * @brief Enable standby replication mode in local storage. Cloud mode
     * support standby feature automatically.
     */
    bool enable_local_standby = false;
    /**
     * @brief Standby source for local standby replication.
     * When populated the store runs in standby mode and rsyncs files from the
     * specified source instead of using cloud storage.
     * Examples:
     * - local
     * - username@host_addr
     */
    std::string standby_master_addr;
    /**
     * @brief Remote store path list for the master when running in standby
     * replica mode. These must be absolute paths and correspond to the master
     * store paths selected by standby_master_store_path_lut.
     */
    std::vector<std::string> standby_master_store_paths;
    /**
     * @brief store_path_weights of master in standby mode.
     * Parsed from `standby_master_store_paths` suffix `:<w1,w2,...>`.
     */
    std::vector<uint64_t> standby_master_store_path_weights;
    /**
     * @brief store_path_lut of master in standby mode. (automatically
     * calculated)
     */
    std::vector<uint32_t> standby_master_store_path_lut;
    /**
     * @brief Whether to verify TLS certificates when talking to the cloud
     * endpoint.
     */
    bool cloud_verify_ssl = false;

    /**
     * @brief Size of B+Tree index/data node (page).
     * Ensure that it is aligned to the system's page size.
     */
    uint16_t data_page_size = 4 * KB;

    size_t FilePageOffsetMask() const
    {
        return (1 << pages_per_file_shift) - 1;
    }

    size_t DataFileSize() const
    {
        return static_cast<size_t>(data_page_size) << pages_per_file_shift;
    }

    size_t SegmentFileSize() const
    {
        return static_cast<size_t>(segment_size) << segments_per_file_shift;
    }

    /**
     * @brief Amount of pages per data file (1 << pages_per_file_shift).
     * It is recommended to set a smaller file size like 4MB in append write
     * mode, and ideally, each batch write operation should exactly fill a new
     * file, as this will make uploading to S3 the most efficient.
     * For non-append mode, it is advisable to set a larger file size like
     * 1GB to avoid the need for fdatasync to be executed for each small file
     * with minor modifications, ideally every partition consists of exactly one
     * data file.
     */
    uint8_t pages_per_file_shift = 18;

    /**
     * @brief Amount of pointers stored in overflow page.
     * The maximum can be set to 128 (max_overflow_pointers).
     */
    uint8_t overflow_pointers = 16;
    /**
     * @brief Write data file pages in append only mode.
     */
    bool data_append_mode = false;
    /**
     * @brief Compression is enabled.
     */
    bool enable_compression = false;
    /**
     * @brief The compression level to use when compression is enabled.
     * Use ZSTD_CLEVEL_DEFAULT by default.
     */
    int zstd_compression_level = ZSTD_CLEVEL_DEFAULT;
    /**
     * @brief Download recent files from cloud into local cache during startup.
     * The partition filter returning true means that this table partition
     * needs to be prewarmed.
     */
    bool prewarm_cloud_cache = false;
    /**
     * @brief Number of prewarm tasks per shard when cloud cache prewarm is
     * enabled.
     */
    uint16_t prewarm_task_count = 3;
    /**
     * @brief Maximum automatic reopen-retry attempts when a request fails with
     * missing underlying resources in cloud/standby mode.
     */
    uint8_t auto_reopen_retry_times = 10;

    /**
     * @brief Filter function to determine which partitions belong to this
     * instance.
     * The filter returning true means that this table partition belongs to the
     * current instance and should be included in operations like prewarming,
     * snapshotting, etc.
     * If not set (empty), all partitions are considered to belong to this
     * instance.
     */
    std::function<bool(const TableIdent &)> partition_filter;
};

}  // namespace eloqstore
