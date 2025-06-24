#pragma once
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>

#include "db_stress_shared_state.h"
#include "db_stress_test_base.h"
#include "random.h"

DECLARE_uint64(ops_per_partition);
DECLARE_int64(max_key);
DECLARE_int64(active_width);
DECLARE_double(hot_key_alpha);
DECLARE_uint64(seed);
DECLARE_uint32(write_percent);
DECLARE_bool(test_batched_ops_stress);
DECLARE_uint32(num_readers_per_partition);
DECLARE_uint32(max_verify_ops_per_write);
DECLARE_uint32(keys_per_batch);
DECLARE_bool(syn_scan);

DECLARE_string(db_path);
DECLARE_string(options);
DECLARE_uint32(n_tables);
DECLARE_uint32(n_partitions);
DECLARE_uint32(num_client_threads);
DECLARE_uint32(num_readers);
DECLARE_uint32(kill_odds);

DECLARE_uint32(num_threads);
DECLARE_uint32(data_page_restart_interval);
DECLARE_uint32(index_page_restart_interval);
DECLARE_uint32(index_page_read_queue);
DECLARE_uint32(init_page_count);
DECLARE_uint32(index_buffer_pool_size);
DECLARE_uint64(manifest_limit);
DECLARE_uint32(fd_limit);
DECLARE_uint32(io_queue_size);
DECLARE_uint32(buf_ring_size);
DECLARE_uint32(coroutine_stack_size);
DECLARE_uint32(data_page_size);
DECLARE_uint32(pages_per_file_shift);
DECLARE_uint32(max_inflight_write);
DECLARE_uint32(max_write_batch_pages);
DECLARE_uint32(num_retained_archives);
DECLARE_uint32(archive_interval_secs);
DECLARE_uint32(max_archive_tasks);
DECLARE_uint32(file_amplify_factor);
DECLARE_uint32(num_gc_threads);
DECLARE_uint64(local_space_limit);
DECLARE_uint32(reserve_space_ratio);
DECLARE_uint32(rclone_threads);
DECLARE_uint32(overflow_pointers);
DECLARE_bool(data_append_mode);

constexpr int KB = 1024;
constexpr int MB = KB * KB;
constexpr int key_window = 128;

namespace StressTest
{

void InitializeHotKeyGenerator(double alpha);
uint64_t GetOneHotKeyID(double rand_seed, int64_t max_key);
int64_t GenerateOneKey(StressTest::Partition *partition, uint64_t iteration);
std::vector<int64_t> GenerateNKeys(StressTest::Partition *partition,
                                   uint64_t iteration);
uint8_t JudgeKeyLevel(int64_t key);
int64_t KeyStringToInt(std::string k);

StressTest *CreateBatchedOpsStressTest();
StressTest *CreateNonBatchedOpsStressTest();

}  // namespace StressTest