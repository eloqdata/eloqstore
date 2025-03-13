#pragma once

#include <unistd.h>

#include <cstdint>
#include <string>

#include "comparator.h"

namespace kvstore
{
struct KvOptions
{
    /**
     * @brief Key-Value database storage path.
     * In-memory storage will be used if path is empty.
     */
    std::string db_path;
    /**
     * @brief Amount of threads.
     */
    uint16_t num_threads = 1;

    const Comparator *comparator_ = Comparator::DefaultComparator();
    uint16_t data_page_restart_interval = 16;
    uint16_t index_page_restart_interval = 16;
    uint16_t index_page_read_queue = 1024;
    uint32_t init_page_count = 1 << 15;

    /**
     * @brief Max amount of cached index pages per thread.
     */
    uint32_t index_buffer_pool_size = UINT32_MAX;
    /**
     * @brief Limit manifest file size.
     */
    uint64_t manifest_limit = 8 << 20;  // 8MB
    /**
     * @brief Max amount of opened files per thread.
     */
    uint32_t fd_limit = 1024;
    /**
     * @brief Size of io-uring submission queue.
     * Limit max amount of inflight IO per thread.
     */
    uint32_t io_queue_size = 4096;
    /**
     * @brief Size of io-uring selected buffer ring.
     */
    uint16_t buf_ring_size = 1 << 10;
    /**
     * @brief Size of coroutine stack.
     */
    uint32_t coroutine_stack_size = 8 * 1024;

    /*
     * The following options will be persisted. User cannot change them after
     * setting for the first time.
     */

    /**
     * @brief Size of B+Tree index/data node (page).
     */
    uint16_t data_page_size = 1 << 12;  // 4KB
    /**
     * @brief Amount of pages per data file (1 << num_file_pages_shift).
     */
    uint8_t num_file_pages_shift = 11;  // 2048
};

inline static size_t page_align = sysconf(_SC_PAGESIZE);

}  // namespace kvstore