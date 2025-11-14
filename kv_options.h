#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

#include "comparator.h"
#include "types.h"
namespace eloqstore
{
constexpr int KB = 1 << 10;
constexpr int MB = 1 << 20;
constexpr int GB = 1 << 30;
constexpr int64_t TB = 1LL << 40;

constexpr uint8_t max_overflow_pointers = 128;
constexpr uint16_t max_read_pages_batch = max_overflow_pointers;

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
    uint64_t index_buffer_pool_size = 32 * MB;
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
    uint32_t max_write_batch_pages = 256;
    /**
     * @brief Size of io-uring selected buffer ring.
     * It must be a power-of 2, and can be up to 32768.
     */
    uint16_t buf_ring_size = 1 << 12;
    /**
     * @brief Size of coroutine stack.
     * According to the latest test results, at least 16KB is required.
     */
    uint32_t coroutine_stack_size = 32 * KB;
    /**
     * @brief Limit number of retained archives.
     * Only take effect when data_append_mode is enabled.
     */
    uint32_t num_retained_archives = 0;
    /**
     * @brief Set the (minimum) archive time interval in seconds.
     * 0 means do not generate archives automatically.
     * Only take effect when data_append_mode is enabled and
     * num_retained_archives is not 0.
     */
    uint32_t archive_interval_secs = 86400;  // 1 day
    /**
     * @brief The maximum number of running archive tasks at the same time.
     */
    uint16_t max_archive_tasks = 256;
    /**
     * @brief Move pages in data file that space amplification factor
     * bigger than this value.
     * Only take effect when data_append_mode is enabled.
     */
    uint8_t file_amplify_factor = 2;
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
     * @brief Storage path on cloud service.
     * Store all data locally if this is empty.
     * Example: eloq-s3:mybucket/eloqstore
     */
    std::string cloud_store_path;

    /**
     * @brief URL of cloud store daemon.
     * you should start rclone server before using cloud mode
     */
    std::string cloud_store_daemon_url = "http://127.0.0.1:5572";

    /**
     * @brief Size of B+Tree index/data node (page).
     * Ensure that it is aligned to the system's page size.
     */
    uint16_t data_page_size = 4 * KB;

    size_t FilePageOffsetMask() const;
    size_t DataFileSize() const;
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
     * @brief Download recent files from cloud into local cache during startup.
     * The filter returning true means that this table partition needs to be
     * prewarmed.
     */
    bool prewarm_cloud_cache = false;

    std::function<bool(const TableIdent &)> prewarm_filter;
};
}  // namespace eloqstore
