#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cassert>
#include <cstdlib>

#include "kill_point.h"
#include "kv_options.h"
#include "test_utils.h"

DEFINE_string(options, "", "path to ini config file");
DEFINE_uint32(num_client_threads, 1, "Amount of threads");
DEFINE_int32(n_partitions, 500, "nums of partitions");
DEFINE_int32(seg_count, 100, "count of segments");
DEFINE_int32(num_readers, 500, "Amount of readers");
DEFINE_int32(read_ops, 10000, "read operations per reader");
DEFINE_int32(write_pause, 10, "pause interval between each round of write");
DEFINE_uint32(kill_odds, 0, "odds (1/this) of each killpoint to crash");
DEFINE_int32(seg_size, 16, "size of each segment");
DEFINE_int32(val_size, 200, "length of each value");

void RunClientThreads(eloqstore::EloqStore &store, uint16_t n_thds)
{
    std::vector<std::thread> threads;
    threads.reserve(n_thds);
    for (uint32_t i = 0; i < n_thds; i++)
    {
        threads.emplace_back(
            [&store, i]
            {
                std::string tbl_name = "t" + std::to_string(i);
                test_util::ConcurrencyTester tester(&store,
                                                    std::move(tbl_name),
                                                    FLAGS_n_partitions,
                                                    FLAGS_seg_count,
                                                    FLAGS_seg_size,
                                                    FLAGS_val_size);
                tester.Init();
                tester.Run(
                    FLAGS_num_readers, FLAGS_read_ops, FLAGS_write_pause);
                tester.Clear();
            });
    }
    for (auto &thd : threads)
    {
        thd.join();
    }
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    eloqstore::KillPoint::GetInstance().kill_odds_ = FLAGS_kill_odds;

    eloqstore::KvOptions options;
    if (!FLAGS_options.empty())
    {
        if (!std::filesystem::exists(FLAGS_options))
        {
            LOG(ERROR) << "ini config not exists: " << FLAGS_options;
            return -1;
        }
        if (int res = options.LoadFromIni(FLAGS_options.c_str()); res != 0)
        {
            LOG(ERROR) << "Failed to parse " << FLAGS_options << " at " << res;
            return -1;
        }
    }
    eloqstore::EloqStore store(options);
    store.Start();
    if (FLAGS_num_client_threads == 1)
    {
        test_util::ConcurrencyTester tester(&store,
                                            "t0",
                                            FLAGS_n_partitions,
                                            FLAGS_seg_count,
                                            FLAGS_seg_size,
                                            FLAGS_val_size);
        tester.Init();
        tester.Run(FLAGS_num_readers, FLAGS_read_ops, FLAGS_write_pause);
        tester.Clear();
    }
    else
    {
        RunClientThreads(store, FLAGS_num_client_threads);
    }
    store.Stop();

    google::ShutdownGoogleLogging();
}
