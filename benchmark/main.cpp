#include <gflags/gflags.h>
#include <glog/logging.h>

#include "eloq_store_bm.h"
#ifdef WITH_ROCKSDB
#include "rocksdb_bm.h"
#endif

DEFINE_string(command, "GET", "Test command[LOAD|GET|SET]");
DEFINE_uint32(worker_num, 1, "RocksDB threads count.");
DEFINE_string(kvoptions, "", "Path to config file of EloqStore options");
DEFINE_uint32(key_size, 4, "Key size in bytes (default: 4)");
DEFINE_uint32(val_size, 32, "Object data size in bytes (default: 32)");
DEFINE_uint64(data_size,
              10000000,
              "The total amount of the data to load (default: 1000000)");
DEFINE_string(key_prefix, "", "Prefix for keys (default: \"eloqstore_\")");
DEFINE_uint32(key_minimum, 0, "Key ID minimum value (default: 0)");
DEFINE_uint64(key_maximum,
              10000000,
              "Key ID maximum value (default: 10000000)");
DEFINE_int32(open_files, 1024, "The maximum opened files for all worker.");
// DEFINE_bool(random_data, true, "Indicate that data should be
// randomized");
DEFINE_string(key_pattern,
              "S:R",
              "Set:Get pattern (default: S:R)\
                                   G for Gaussian distribution.\
                                   R for uniform Random.\
                                   S for Sequential.\
                                   P for Parallel (Sequential were each client has a subset of the key-range).");
DEFINE_uint32(test_time, 10, "Number of seconds to run the test (default: 10)");
DEFINE_uint32(check_interval, 10, "The interval to check the test duration");
DEFINE_uint32(partition_count, 4, "The total partition count.");
DEFINE_uint32(batch_size,
              64,
              "The batch size of one write request (MB) (default: 64).");
DEFINE_string(storage, "eloqstore", "The storage used to store the data.");
DEFINE_uint32(client_threads, 4, "GET2: number of client threads.");
DEFINE_uint32(inflight_per_client,
              125,
              "GET2: async requests each client keeps in flight.");
DEFINE_uint32(per_shard_cap,
              0,
              "GET2: max outstanding per shard per client (0 = unlimited); "
              "bounds the blast radius of a stalled shard.");
DEFINE_uint32(request_cnt,
              32,
              "The number of concurrent read requests (default: 32)");
#ifdef WITH_ROCKSDB
DEFINE_uint32(num_column_families,
              1,
              "Number of Column Families to use (default: 1).");
DEFINE_uint64(
    block_cache_size,
    32 << 20,  // 32MB
    "Number of bytes to use as a cache of uncompressed data(default: 32MB)");
DEFINE_string(cache_type, "lru_cache", "Type of block cache.");
DEFINE_uint32(user_timestamp_size,
              8,
              "Number of bytes in a user-defined timestamp(default: 8)");
DEFINE_string(value_size_distribution_type,
              "fixed",
              "Value size distribution type: fixed, uniform, normal");
DEFINE_int64(write_buffer_size,
             64 << 20,  // 64MB
             "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             2,
             "The number of in-memory memtables. Each memtable is of size"
             " write_buffer_size bytes.");
DEFINE_bool(disable_wal,
            true,
            "If true, do not write WAL for write.(default: true)");
DEFINE_bool(cache_index_and_filter_blocks,
            false,
            "Whether to put index/filter blocks in the block cache.");
DEFINE_uint64(subcompactions,
              1,
              "For CompactRange, set max_subcompactions for each compaction "
              "job in this CompactRange, for auto compactions, this is "
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");
#endif

int main(int argc, char *argv[])
{
    FLAGS_logtostderr = true;
    google::InitGoogleLogging("EloqStore_benchmark");

    LOG(INFO) << "Hello, from eloqstore_benchmark!\n";

    google::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Start test for command: " << FLAGS_command
              << ", total partition count: " << FLAGS_partition_count
              << ", total load data count: " << FLAGS_data_size
              << ", key size: " << FLAGS_key_size
              << ", value size: " << FLAGS_val_size
              << ". Batch write size: " << FLAGS_batch_size << "(MB)"
              << ". Concurrent request count: " << FLAGS_request_cnt
              << ". Storage: " << FLAGS_storage;

    if (FLAGS_storage == "eloqstore")
    {
        // EloqStoreBM::test_random_key_value(FLAGS_key_size,
        //                                    FLAGS_val_size,
        //                                    FLAGS_key_prefix,
        //                                    FLAGS_key_minimum,
        //                                    FLAGS_key_maximum,
        //                                    FLAGS_key_pattern,
        //                                    10);
        // exit(0);

        eloqstore::KvOptions kvoptions;
        if (int res = kvoptions.LoadFromIni(FLAGS_kvoptions.c_str()); res != 0)
        {
            LOG(FATAL) << "Failed to parse " << FLAGS_kvoptions << " at "
                       << res;
        }

        EloqStoreBM::Benchmark bench_mark(FLAGS_command,
                                          FLAGS_batch_size,
                                          FLAGS_data_size,
                                          FLAGS_partition_count,
                                          FLAGS_key_size,
                                          FLAGS_val_size,
                                          FLAGS_key_prefix,
                                          FLAGS_key_pattern,
                                          FLAGS_key_minimum,
                                          FLAGS_key_maximum,
                                          FLAGS_request_cnt,
                                          FLAGS_test_time,
                                          kvoptions.num_threads);

        if (!bench_mark.OpenEloqStore(kvoptions))
        {
            exit(-1);
        }

        // Run the test
        bench_mark.RunBenchmark();

        // Shutdown the eloq store
        bench_mark.CloseEloqStore();
    }
#ifdef WITH_ROCKSDB
    else if (FLAGS_storage == "rocksdb")
    {
        // bool w = FLAGS_command == "LOAD";
        // RocksDBBM::test_rocksdb(w);
        // exit(0);

        DLOG(INFO) << "RocksDB configs >>"
                   << " Column family count: " << FLAGS_num_column_families
                   << ", Block cache size: " << (FLAGS_block_cache_size >> 10)
                   << "(KB)[" << (FLAGS_block_cache_size >> 20) << "(MB)]"
                   << ", Block cache type: " << FLAGS_cache_type
                   << ", User timestamp size: " << FLAGS_user_timestamp_size
                   << ", Value size distribution type: "
                   << FLAGS_value_size_distribution_type
                   << ", Disable WAL: " << FLAGS_disable_wal
                   << ", Put index/filter blocks in block cache: "
                   << FLAGS_cache_index_and_filter_blocks
                   << ", Write buffer size: " << (FLAGS_write_buffer_size >> 10)
                   << "(KB)[" << (FLAGS_write_buffer_size >> 20) << "(MB)]";

        std::string path("/tmp/rocksdb-benchmark");
        RocksDBBM::Benchmark benchmark(path,
                                       FLAGS_num_column_families,
                                       FLAGS_command,
                                       FLAGS_worker_num,
                                       FLAGS_key_size,
                                       FLAGS_val_size,
                                       FLAGS_data_size,
                                       FLAGS_batch_size,
                                       FLAGS_key_maximum,
                                       FLAGS_user_timestamp_size,
                                       FLAGS_key_prefix,
                                       FLAGS_write_buffer_size,
                                       FLAGS_max_write_buffer_number,
                                       FLAGS_block_cache_size,
                                       FLAGS_value_size_distribution_type,
                                       FLAGS_disable_wal,
                                       FLAGS_cache_index_and_filter_blocks,
                                       FLAGS_cache_type,
                                       FLAGS_subcompactions,
                                       FLAGS_test_time);
        benchmark.Run();
    }
#endif
    else
    {
        LOG(INFO) << "No support storage: " << FLAGS_storage;
    }

    google::ShutdownGoogleLogging();
    return 0;
}