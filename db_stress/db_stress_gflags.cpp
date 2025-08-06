#include <gflags/gflags.h>

#include <cstddef>

#include "db_stress_common.h"

// The capacity of Mem for the test should be more than:
// n_partitions * (keys_per_batch + num_readers_per_partition) *
// value_sz*n_tables If choose asyn_scan,Mem should be more than: max_key *
// n_partitions * value_sz*n_tables If choose syn_scan,Mem should be more than:
// max_key * value_sz*n_tables
// but this will cost more time

// The data size (DB contained when testing) is about:
// n_partitions * max_key * (12B + value_sz)*n_tables

// the expect value of a key modified:
// keys_per_batch * ops_per_partition / (max_key - active_width)
// the expect value of a key read:
// max_verify_ops_per_write * ops_per_partition / (max_key - active_width)
// So it is recommended that:
// max_verify_ops_per_write >= keys_per_batch

// To reduce the time cost of generating random keys,
// it is suggested that,
// keys_per_batch <= 1/5 * active_width

// It is suggested that,
// num_readers_per_partition <= max_verify_ops_per_write

// params for db_stress
DEFINE_string(options, "", "path to ini config file");
DEFINE_string(db_path, "data/stress_test", "Path to database");
DEFINE_string(shared_state_path,
              "data/db_stress_helper",
              "Path to shared state directory");
DEFINE_uint32(n_tables, 10, "nums of threads/tables");
DEFINE_uint32(n_partitions, 300, "nums of partitions");
// I think it is better to set a large value for ops_per_partition
DEFINE_uint64(ops_per_partition,
              300000000000000,
              "ops of batch write on each partition");
DEFINE_uint32(kill_odds, 0, "odds (1/this) of each killpoint to crash");
DEFINE_uint32(num_client_threads, 1, "Amount of client threads");
DEFINE_int64(max_key, 1000, "the value of the max key");
DEFINE_int64(active_width, 500, "width of active range");
DEFINE_double(hot_key_alpha, 0, "alpha of hot key,recommend:[0.8,1.5]");
DEFINE_uint64(seed, 2341234, "seed for random");
DEFINE_uint32(write_percent, 75, "percentage of upsert in upsert/delete");
DEFINE_uint32(point_read_percent,
              25,
              "percentage of point read in point_read/scan_read");
DEFINE_bool(test_batched_ops_stress, false, "whether test batched ops");
DEFINE_uint32(num_readers_per_partition,
              10,
              "nums of readers per partition,reader should be at least one per "
              "partition");
DEFINE_uint32(max_verify_ops_per_write, 15, "max verify ops after per write");
DEFINE_uint32(keys_per_batch,
              200,
              "keys for writing per batch,0 represents random in "
              "[100,500],keys_per_batch must be less than active_width");
DEFINE_bool(
    open_wfile,
    false,
    "using file to store and verify,it should be open when testing crash");
DEFINE_bool(syn_scan, true, "choose scan syn or asyn");
// DEFINE_uint32(
//     value_sz_mode,
//     0,
//     "0: 32B - 160B, 1: 1KB - 4KB, 2: 100KB - 1001KB, 3: 50MB - 301MB");
// I think it is better to define two flags shortest_value and longest_value
DEFINE_uint32(shortest_value, 1024, "minimum value size in bytes");
DEFINE_uint32(longest_value, 4096, "maximum value size in bytes");
// random params for crash_test
DEFINE_uint32(num_threads, 10, "Amount of threads");
DEFINE_uint32(data_page_restart_interval, 16, "interval of datapage restart");
DEFINE_uint32(index_page_restart_interval, 16, "interval of indexpage restart");
DEFINE_uint32(init_page_count, 1 << 15, "nums of init page");
DEFINE_uint32(index_buffer_pool_size, 1 << 15, "size of index buffer pool");
DEFINE_uint64(manifest_limit, 16 << 20, "limit of manifest");
DEFINE_uint32(fd_limit, 10000, "limit of fd");
DEFINE_uint32(io_queue_size, 4096, "size of io_queue");
DEFINE_uint32(buf_ring_size, 1 << 10, "size of buf_ring");
DEFINE_uint32(coroutine_stack_size, 32 * 1024, "size of coroutine stack");
DEFINE_uint32(data_page_size, 1 << 12, "size of datapage");
DEFINE_uint32(pages_per_file_shift, 11, "nums of filepage shift");
DEFINE_uint32(max_inflight_write,
              4096,
              "Max amount of inflight write IO per thread");
DEFINE_uint32(max_write_batch_pages, 64, "max pages per write batch");
DEFINE_uint32(num_retained_archives, 0, "limit number of retained archives");
DEFINE_uint32(archive_interval_secs, 86400, "archive time interval in secs");
DEFINE_uint32(max_archive_tasks, 256, "max running archive tasks");
DEFINE_uint32(file_amplify_factor,
              4,
              "move pages in data file that space amplification factor");
DEFINE_uint32(num_gc_threads, 1, "num of bg file GC threads");
DEFINE_uint64(local_space_limit,
              size_t(1) << 40,
              "limit total size of local files per shard");
DEFINE_uint32(reserve_space_ratio,
              100,
              "reserved space ratio for new created/download files");
DEFINE_uint32(rclone_threads,
              1,
              "number of threads used by rclone to upload/download files");
DEFINE_uint32(overflow_pointers,
              16,
              "amount of pointers stored in overflow page");
DEFINE_bool(data_append_mode, false, "write data in append only mode");
DEFINE_string(cloud_store_path, "", "path to cloud store");
DEFINE_bool(enable_latency_monitoring,
            false,
            "enable request latency monitoring and logging");
DEFINE_bool(enable_throughput_monitoring,
            true,
            "enable IO throughput monitoring and logging");
DEFINE_uint32(throughput_report_interval_secs,
              5,
              "interval in seconds to report IO throughput statistics");
DEFINE_string(throughput_log_file,
              "throughput.log",
              "file path to write throughput statistics");