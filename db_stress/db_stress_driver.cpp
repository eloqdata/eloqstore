#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "batched_ops_stress.h"
#include "db_stress_common.h"
#include "db_stress_shared_state.h"
#include "db_stress_test_base.h"
#include "eloq_store.h"
#include "error.h"
#include "kill_point.h"
#include "kv_options.h"
#include "non_batched_ops_stress.h"

namespace StressTest
{

void print_test_params()
{
    // std::vector<std::string> v_sz_mode = {
    //     "32B-160B", "1KB-4KB", "100KB-1001KB", "50MB-301MB"};
    LOG(INFO) << "test_params-------"
              << "\nn_tables:" << FLAGS_n_tables
              << "\nn_partitions:" << FLAGS_n_partitions
              << "\nmax_key:" << FLAGS_max_key
              << "\nops_per_partition:" << FLAGS_ops_per_partition
              << "\nkeys_per_batch:" << FLAGS_keys_per_batch
              << "\nactive_width:" << FLAGS_active_width
              << "\nnum_readers_per_partition:"
              << FLAGS_num_readers_per_partition
              << "\nmax_verify_ops_per_write:"
              << FLAGS_max_verify_ops_per_write
              //   << "\nvalue_sz_mode:" << v_sz_mode[FLAGS_value_sz_mode];
              << "\nshortest_value:" << FLAGS_shortest_value
              << "\nlongest_value:" << FLAGS_longest_value;

    if (FLAGS_active_width < FLAGS_keys_per_batch)
    {
        LOG(FATAL) << "active_width < keys_per_batch, test cannot continue.";
    }

    if (FLAGS_num_readers_per_partition <= 0)
    {
        LOG(FATAL) << "num_readers_per_partition <= 0,test cannot continue.";
    }

    // uint64_t value_sz = 0;
    // if (FLAGS_value_sz_mode == 0)
    // {
    //     value_sz = 100;
    // }
    // else if (FLAGS_value_sz_mode == 1)
    // {
    //     value_sz = 3 * KB;
    // }
    // else if (FLAGS_value_sz_mode == 2)
    // {
    //     value_sz = 600 * KB;
    // }
    // else
    // {
    //     value_sz = 200 * MB;
    // }
    // use (shortest_value + longest_value) /2
    uint64_t value_sz = (FLAGS_shortest_value + FLAGS_longest_value) / 2;
    double mem = 0.0;
    int max_key = FLAGS_max_key;
    int readers = FLAGS_num_readers_per_partition;
    int keys = FLAGS_keys_per_batch;
    int partitions = FLAGS_n_partitions;
    int tables = FLAGS_n_tables;
    if (!FLAGS_syn_scan)
    {
        mem = (double) std::max(max_key, readers + keys) * partitions *
              value_sz * tables;
        mem = (double) mem / KB / KB / KB;
        LOG(INFO) << "The test may occupy Mem: " << mem
                  << " GB, please pay attention!Or you can choose syn_scan.";
    }
    else
    {
        mem = (double) std::max(max_key, (readers + keys) * partitions) *
              value_sz * tables;
        mem = (double) mem / KB / KB / KB;
        LOG(INFO) << "The test may occupy Mem: " << mem
                  << " GB, please pay attention!";
    }
    double storage = 0.0;
    storage = (double) max_key * partitions * (12 + value_sz);
    storage = (double) storage / KB / KB / KB;
    LOG(INFO) << "The data DB contains would be about " << storage << " GB.";
}

void RunStressTest(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostdout = true;
    FLAGS_colorlogtostdout = true;
    FLAGS_alsologtostderr = true;
    FLAGS_stderrthreshold = google::GLOG_WARNING;
    FLAGS_logbuflevel = google::GLOG_INFO;

    eloqstore::KvOptions opts;
    if (!FLAGS_options.empty())
    {
        if (int res = opts.LoadFromIni(FLAGS_options.c_str()); res != 0)
        {
            LOG(FATAL) << "Failed to parse " << FLAGS_options << " at " << res;
        }
    }
    else
    {
        opts.store_path = {FLAGS_db_path};
        opts.buf_ring_size = FLAGS_buf_ring_size;
        opts.coroutine_stack_size = FLAGS_coroutine_stack_size;
        opts.data_page_size = FLAGS_data_page_size;
        opts.data_page_restart_interval = FLAGS_data_page_restart_interval;
        opts.index_page_restart_interval = FLAGS_index_page_restart_interval;
        opts.index_buffer_pool_size = FLAGS_index_buffer_pool_size;
        opts.io_queue_size = FLAGS_io_queue_size;
        opts.fd_limit = FLAGS_fd_limit;
        opts.manifest_limit = FLAGS_manifest_limit;
        opts.init_page_count = FLAGS_init_page_count;
        opts.num_threads = FLAGS_num_threads;
        opts.pages_per_file_shift = FLAGS_pages_per_file_shift;
        opts.max_inflight_write = FLAGS_max_inflight_write;
        opts.max_write_batch_pages = FLAGS_max_write_batch_pages;
        opts.num_retained_archives = FLAGS_num_retained_archives;
        opts.archive_interval_secs = FLAGS_archive_interval_secs;
        opts.max_archive_tasks = FLAGS_max_archive_tasks;
        opts.file_amplify_factor = FLAGS_file_amplify_factor;
        opts.num_gc_threads = FLAGS_num_gc_threads;
        opts.local_space_limit = FLAGS_local_space_limit;
        opts.reserve_space_ratio = FLAGS_reserve_space_ratio;
        opts.rclone_threads = FLAGS_rclone_threads;
        opts.overflow_pointers = FLAGS_overflow_pointers;
        opts.data_append_mode = FLAGS_data_append_mode;
    }

    eloqstore::KillPoint::GetInstance().kill_odds_ = FLAGS_kill_odds;

    eloqstore::EloqStore store(opts);
    eloqstore::KvError err = store.Start();
    CHECK(err == eloqstore::KvError::NoError);

    std::vector<std::unique_ptr<StressTest>> stress(FLAGS_n_tables);

    // 根据实际线程数设置,下面两条其实不用,因为stress_test只会被初始化一次
    StressTest::total_threads_.store(FLAGS_n_tables);
    StressTest::init_completed_count_.store(0);
    StressTest::all_init_done_.store(false);


    if (FLAGS_test_batched_ops_stress)
    {
        for (size_t i = 0; i < stress.size(); ++i)
        {
            std::string table_name = "stress_test" + std::to_string(i);
            stress[i] = std::make_unique<BatchedOpsStressTest>(table_name);
        }
        LOG(INFO) << "Current TestType:BatchedOpsStressTest";
    }
    else
    {
        for (size_t i = 0; i < stress.size(); ++i)
        {
            std::string table_name = "stress_test" + std::to_string(i);
            stress[i] = std::make_unique<NonBatchedOpsStressTest>(table_name);
        }

        LOG(INFO) << "Current TestType:NonBatchedOpsStressTest";
    }
    for (size_t i = 0; i < stress.size(); ++i)
    {
        stress[i]->LinkToStore(&store);
    }

    LOG(INFO) << "StressTest is started";
    print_test_params();

    InitializeHotKeyGenerator(FLAGS_hot_key_alpha);

    for (size_t i = 0; i < stress.size(); ++i)
    {
        stress[i]->Start();
    }

    for (size_t i = 0; i < stress.size(); ++i)
    {
        stress[i]->Join();
    }
    store.Stop();

    LOG(INFO) << "StressTest ends.";
    google::ShutdownGoogleLogging();
}

}  // namespace StressTest

int main(int argc, char **argv)
{
    StressTest::RunStressTest(argc, argv);
    return 0;
}