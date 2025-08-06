#include "db_stress_test_base.h"

#include <glog/logging.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "db_stress_common.h"
#include "db_stress_shared_state.h"
#include "eloq_store.h"
#include "error.h"
#include "expected_value.h"
#include "external/random.h"

namespace StressTest
{

void StressTest::InitDb()
{
    LOG(INFO) << "Start Initing verify table.";
    if (FLAGS_test_batched_ops_stress)
    {
    }
    else if (!FLAGS_open_wfile)
    {
        // reset directly
        // VerifyAndSyncValues();
        thread_state_->ResetValues();

        VerifyDb();
    }
    else
    {  // ps: seqno is fetched from seqno table (in constructor), after fetching
       // it gets incremented to become current table

        thread_state_->Init();        // This will first call filetomem to load
                                      // latest.state table, then call replay to
                                      // restore verify table from trace table
        VerifyAndSyncValues();        // Scan database once for verification
        thread_state_->FinishInit();  // Call memtofile to write back latest,
                                      // then savaAtAfter copies latest.state to
                                      // seqno.state, delete history tables, and
                                      // finally start seqno.trace
    }
    // LOG(INFO) << "Db has been Inited."; // This log is incorrect, only verify
    // table is ready

    // fetch_add is used to increment init_completed_count_ by 1 and return the
    // value before increment, so +1 is the actual completed count
    int completed = init_completed_count_.fetch_add(1) + 1;
    LOG(INFO) << "Thread initialization completed. Progress: " << completed
              << "/" << total_threads_.load();
}

void StressTest::WaitForAllInitComplete()
{
    // mutex is used to work with condition variable
    std::unique_lock<std::mutex> lock(init_barrier_mutex_);

    // If all initialization is already complete, return directly
    if (all_init_done_.load())
    {
        return;
    }

    // Wait for all threads to complete initialization
    init_barrier_cv_.wait(lock,
                          []
                          {
                              return init_completed_count_.load() >=
                                         total_threads_.load() ||
                                     all_init_done_.load();
                          });

    // The last thread to arrive is responsible for waking up all waiting
    // threads
    if (init_completed_count_.load() >= total_threads_.load() &&
        !all_init_done_.exchange(true))
    {
        // Not every thread will execute notify_all, because exchange sets it to
        // true and returns the old value, so only threads that were previously
        // false can enter this if
        LOG(INFO) << "All threads initialization completed. Starting "
                     "operations DB...";
        init_barrier_cv_.notify_all();
    }
}

void StressTest::OperateDb()
{
    LOG(INFO) << "Start operating Db.";
    uint64_t ts1 = UnixTimestamp();
    uint64_t ts_rand = 0;
    uint64_t ts_verify = 0;
    uint64_t ts_gen_v = 0;
    start_time = ts1;

    do
    {
        uint64_t user_data;
        while (finished_reqs_.try_dequeue(user_data))
        {
            // First bit of userdata indicates if it's a write, remaining bits
            // are id 1<<63 then & can get the highest bit 1<<63-1 then & can
            // get the lower 63 bits
            bool is_write = (user_data & (uint64_t(1) << 63));
            uint32_t id = (user_data & ((static_cast<uint64_t>(1) << 63) - 1));

            if (is_write)
            {
                auto partition = partitions_[id];
                partition->FinishWrite();
            }

            else
            {
                auto reader = readers_[id];
                uint64_t ts22 = UnixTimestamp();
                reader->VerifyGet(thread_state_);
                ts_verify += UnixTimestamp() - ts22;
            }
        }
        // the rand_keys is vector so the TestPut is a batch operator
        // GenerateNKeys will generate a vector of randow keys,
        // The number is determined by FLAGS_keys_per_batch,
        // which defaults to a random value between 100 and 500.
        // support slice window(Flags_hot_key_alpha == 0)
        // support hot key(FLAGS_hot_key_alpha > 0)
        // due to the asynchronous nature of the test, we need to traverse the
        // unfinished_id_ to find the partitions that have not finished the test
        for (auto partition_id : unfinished_id_)
        {
            auto partition = partitions_[partition_id];

            if (!partition->IsWriting() && !partition->should_stop)
            {
                uint64_t ts11 = UnixTimestamp();
                std::vector<int64_t> rand_keys =
                    GenerateNKeys(partition, partition->FinishedRounds());
                // ts_rand use to count the time cost of GenerateNKeys ,it will
                // be cout in the log
                // LOG(INFO) << "Rand keys for " << ts_rand << ","<< (double)
                // ts_rand * 100 / (ts2 - ts1) << "%";  //
                ts_rand += UnixTimestamp() - ts11;

                TestMixedOps(partition_id, rand_keys);
            }

            for (uint32_t i = partition_id * FLAGS_num_readers_per_partition;
                 i < (partition_id + 1) * FLAGS_num_readers_per_partition;
                 ++i)
            {
                auto reader = readers_[i];
                // reader->IsReading blocks here, so it won't read repeatedly
                if (!reader->IsReading &&
                    partition->verify_cnt < FLAGS_max_verify_ops_per_write &&
                    !reader->should_stop)
                {
                    int64_t rand_key =
                        GenerateOneKey(partition, partition->FinishedRounds());

                    if (partition->rand_.PercentTrue(FLAGS_point_read_percent))
                    {
                        TestGet(i, rand_key);
                    }
                    else
                    {
                        TestScan(i, rand_key);
                    }
                }
            }
        }

    } while (!AllPartitionsFinished());

    for (size_t i = 0; i < FLAGS_n_partitions; ++i)
    {
        ts_gen_v += partitions_[i]->gen_v_time;
    }
    uint64_t ts2 = UnixTimestamp();
    LOG(INFO) << "Operate Db ends.";
    LOG(INFO) << "Operate Db for " << ts2 - ts1;
    LOG(INFO) << "Rand keys for " << ts_rand << ","
              << (double) ts_rand * 100 / (ts2 - ts1) << "%";
    LOG(INFO) << "Verify for " << ts_verify << ","
              << (double) ts_verify * 100 / (ts2 - ts1) << "%";
    LOG(INFO) << "Gen v for " << ts_gen_v << ","
              << (double) ts_gen_v * 100 / (ts2 - ts1) << "%";
}

void StressTest::ClearDb()
{
    LOG(INFO) << "Start Clearing Db.";
    VerifyDb();
    uint64_t ts1 = UnixTimestamp();
    uint64_t user_data;
    while (finished_reqs_.try_dequeue(user_data))
        ;
    for (auto partition : partitions_)
    {
        partition->trun_req_.SetArgs(
            {thread_state_->table_name_, partition->id_}, {});
        user_data = partition->id_;
        bool ok =
            store_->ExecAsyn(&partition->trun_req_,
                             user_data,
                             [this](eloqstore::KvRequest *req) { Wake(req); });
        CHECK(ok);
    }

    uint32_t cnt = FLAGS_n_partitions;
    while (cnt > 0)
    {
        while (finished_reqs_.try_dequeue(user_data))
        {
            uint32_t id = user_data;
            auto partition = partitions_[id];
            auto reader = readers_[id * FLAGS_num_readers_per_partition];
            reader->scan_req_.SetArgs({thread_state_->table_name_, id}, {}, {});
            CHECK(reader->scan_req_.Error() == eloqstore::KvError::NotFound ||
                  reader->scan_req_.Error() == eloqstore::KvError::NoError);
            --cnt;
        }
    }

    thread_state_->Clear();
    LOG(INFO) << "Truncate cost " << UnixTimestamp() - ts1;
    LOG(INFO) << "Db has been cleared.";
}

bool StressTest::Partition::IsWriting() const
{
    return ticks_ & 1;
}

uint64_t StressTest::Partition::FinishedRounds()
{
    return ticks_ >> 1;
}

void StressTest::Partition::CheckIfAllFinished()
{
    if (FinishedRounds() >= FLAGS_ops_per_partition)
    {
        should_stop = true;
    }
}

std::string StressTest::Partition::GenerateValue(uint32_t base)
{
    // assert(value_sz >= 32);
    // make sure parameter is valid
    assert(FLAGS_longest_value >= FLAGS_shortest_value);
    assert(FLAGS_shortest_value >=
           32);  // I don't now this is valid,shortest_value must >= 32?
    // ergeng: it ensure it do not overflow when memset
    size_t value_sz;
    if (FLAGS_longest_value == FLAGS_shortest_value)
    {
        value_sz = FLAGS_shortest_value;
    }
    else
    {
        value_sz =
            FLAGS_shortest_value +
            rand_.Uniform(FLAGS_longest_value - FLAGS_shortest_value + 1);
    }
    /*
    if shortest_value == 64,longest_value == 128,value_sz = 100 ,base = 12345
    00000000000000000000000012345777777777777777777777777777777777777777777777777
    |<------- 32-char prefix --->|<---------- remaining 48 chars--------------->
    */
    char post_fix_ch = '0' + rand_.Uniform(10);  // [0,9]
    std::string ret(value_sz, post_fix_ch);
    std::string value_base = std::to_string(base);
    size_t zero_pad = 32 - value_base.size();
    std::memset(&ret[0], '0', zero_pad);
    std::memcpy(&ret[zero_pad], value_base.data(), value_base.size());

    return ret;
}

void StressTest::Partition::FinishWrite()
{
    CHECK(req_.Error() == eloqstore::KvError::NoError);
    ticks_++;
    verify_cnt = 0;
    RecordWriteBytes(write_bytes_, 1);
    write_bytes_ = 0;
    if (table->type == TestType::NonBatchedOpsStressTest)
    {
        for (size_t i = 0; i < pending_expected_values.size(); ++i)
        {
            pending_expected_values[i].Commit();
        }
    }
    req_.batch_.clear();

    ++table->ops_fin;

    CheckIfAllFinished();
}

void StressTest::Reader::VerifyGet(ThreadState *thread_state_)
{
    // the BatchedOpsStressTest is designed to test atomic batch operations
    // ensuring that either all operations in a batch succeed or all fail
    // together

    // take a single base key (eg.helllo),and create 10 related entries with
    // suffixes 0~9(eg.hello0,hello1,hello2...) and the value is also suffixes
    // 0~9

    /*  in the BatchedOpsStressTest::testGet
    void BatchedOpsStressTest::TestGet(uint32_t reader_id, int64_t rand_key)
    {
        Reader *reader = readers_[reader_id];
        reader->begin_key_ = Key(rand_key) + "0";     // Start from suffix "0"
        reader->end_key_ = Key(rand_key + 1) + "0";   // End before next key
    group
        // This scan will capture all 10 entries: key+"0" to key+"9"
    }
    */

    if (partition_->table->type == TestType::BatchedOpsStressTest)
    {
        CHECK(scan_req_.Error() == eloqstore::KvError::NoError ||
              scan_req_.Error() == eloqstore::KvError::NotFound);
        partition_->verify_cnt++;
        if (scan_req_.Entries().empty())
            return;

        std::vector<std::string> v_res;
        for (auto [k, v, ts, _] : scan_req_.Entries())
        {
            assert(!k.empty());
            assert(!v.empty());
            // Last char of key == last char of value
            CHECK(k.back() == v.back());
            // make value0~0 to value (rm the suffix)
            v.pop_back();
            // and then push to v_res(vector<string>)
            v_res.emplace_back(v);
        }

        CHECK_EQ(v_res.size(), 10);
        for (int i = 0; i < 10; ++i)
        {
            // check all value are same
            CHECK(v_res[i] == v_res[0]);
        }

        IsReading = false;  // set IsReading = false when finishing verify
    }
    else  // NonBatchedOpsStressTest
    {
        partition_->verify_cnt++;

        if (is_scan_mode_)
        {
            CHECK(scan_req_.Error() == eloqstore::KvError::NoError ||
                  scan_req_.Error() == eloqstore::KvError::NotFound);

            auto entries = scan_req_.Entries();

            uint64_t read_bytes = 0;

            for (const auto &entry : entries)
            {
                read_bytes += entry.key_.size() + entry.value_.size();
            }
            StressTest::RecordReadBytes(read_bytes, 1);

            size_t entry_idx = 0;

            // Traverse all the expected keys and validate in order
            for (size_t i = 0; i < key_readings_.size(); ++i)
            {
                // i think it is not necessary to get string
                const std::string &expected_key = key_readings_[i];
                int64_t key_int = KeyStringToInt(expected_key);

                ExpectedValue post_expected =
                    thread_state_->Load(partition_->id_, key_int);
                ExpectedValue pre_expected =
                    (i < pre_read_expected_values.size())
                        ? pre_read_expected_values[i]
                        : ExpectedValue();

                // check if the current entry matches the expected key
                if (entry_idx < entries.size() &&
                    entries[entry_idx].key_ == expected_key)
                {
                    // check the value is valid if find the match entry
                    const std::string &actual_value = entries[entry_idx].value_;
                    assert(!actual_value.empty());
                    assert(!ExpectedValueHelper::MustHaveNotExisted(
                        pre_expected, post_expected));

                    uint32_t value_base = std::stoi(actual_value.substr(0, 32));
                    assert(ExpectedValueHelper::InExpectedValueBaseRange(
                        value_base, pre_expected, post_expected));

                    entry_idx++;  // move to the next entry
                }
                else
                {
                    // not found the match entry, verify if it should not exist
                    assert(!ExpectedValueHelper::MustHaveExisted(
                        pre_expected, post_expected));
                }
            }
        }
        else
        {
            // read only one: proccess result directly
            assert(key_readings_.size() ==
                   1);  // read only one mode only have one result

            // assert the result is either found or not found
            CHECK(read_req_.Error() == eloqstore::KvError::NoError ||
                  read_req_.Error() == eloqstore::KvError::NotFound);

            const std::string &expected_key = key_readings_[0];
            int64_t key_int = KeyStringToInt(expected_key);

            ExpectedValue post_expected =
                thread_state_->Load(partition_->id_, key_int);
            ExpectedValue pre_expected = (!pre_read_expected_values.empty())
                                             ? pre_read_expected_values[0]
                                             : ExpectedValue();

            if (read_req_.Error() == eloqstore::KvError::NoError)
            {
                uint64_t read_bytes =
                    expected_key.size() + read_req_.value_.size();
                StressTest::RecordReadBytes(read_bytes, 1);

                // check the value is valid if find the match entry
                const std::string &actual_value = read_req_.value_;
                assert(!actual_value.empty());
                assert(!ExpectedValueHelper::MustHaveNotExisted(pre_expected,
                                                                post_expected));

                uint32_t value_base = std::stoi(actual_value.substr(0, 32));
                assert(ExpectedValueHelper::InExpectedValueBaseRange(
                    value_base, pre_expected, post_expected));
            }
            else
            {
                StressTest::RecordReadBytes(expected_key.size(), 1);
                // check the value is not exist if not found
                assert(!ExpectedValueHelper::MustHaveExisted(pre_expected,
                                                             post_expected));
            }
        }

        IsReading = false;
    }
    if (partition_->should_stop == true)
    {
        should_stop = true;
        --partition_->active_readers;
        if (partition_->active_readers <= 0)
        {
            partition_->table->unfinished_id_.erase(partition_->id_);
        }
    }
}

bool StressTest::AllPartitionsFinished()
{
    return unfinished_id_.empty();
}

void StressTest::VerifyAndSyncValues()
{
    if (!thread_state_->HasHistory())
    {
        thread_state_->ResetValues();
        return;
    }
    // change to use assert to make sure call it only have history
    // assert(thread_state_->HasHistory());

    if (FLAGS_syn_scan)
    {
        for (auto partition : partitions_)
        {
            auto reader = readers_[0];
            // table_name+partition_id = B+ tree id, {}{} means scan all
            reader->scan_req_.SetArgs(
                {thread_state_->table_name_, partition->id_}, {}, {});
            store_->ExecSync(&reader->scan_req_);
            size_t idx = 0;
            auto ents = reader->scan_req_.Entries();
            for (size_t j = 0; j < FLAGS_max_key; ++j)
            {
                ExpectedValue expected_value(
                    thread_state_->Load(partition->id_, j));
                if (!expected_value.IsDeleted())
                {
                    if (idx < ents.size())
                    {
                        const auto &[k, v, ts, _] = ents[idx];
                        if (KeyStringToInt(k) == j)
                        {
                            // crash test bug in this check
                            CHECK(stoi(v.substr(0, 32)) ==
                                      expected_value.GetValueBase() ||
                                  stoi(v.substr(0, 32)) ==
                                      expected_value.GetValueBase() - 1);
                            expected_value.SetValueBase(stoi(v.substr(0, 32)));
                            thread_state_->Value(partition->id_, j) =
                                expected_value.Read();
                            ++idx;
                        }
                        else
                        {
                            expected_value.SetDeleted();
                            thread_state_->Value(partition->id_, j) =
                                expected_value.Read();
                        }
                    }
                    else
                    {
                        expected_value.SetDeleted();
                        thread_state_->Value(partition->id_, j) =
                            expected_value.Read();
                    }
                }
                else
                {
                    if (idx >= ents.size())
                        continue;
                    auto [k, v, ts, _] = ents[idx];
                    if (KeyStringToInt(k) == j)
                    {
                        CHECK(stoi(v.substr(17, 15)) ==
                              expected_value.GetValueBase());
                        expected_value.ClearDeleted();
                        thread_state_->Value(partition->id_, j) =
                            expected_value.Read();
                        ++idx;
                    }
                }
            }
        }
    }

    else
    {
        for (uint32_t reader_id = 0; reader_id < readers_.size();
             reader_id += FLAGS_num_readers_per_partition)
        {
            auto reader = readers_[reader_id];
            reader->scan_req_.SetArgs(
                {thread_state_->table_name_, reader->partition_->id_}, {}, {});
            uint64_t user_data = reader->id_;
            bool ok = store_->ExecAsyn(&reader->scan_req_,
                                       user_data,
                                       [this](eloqstore::KvRequest *req) -> void
                                       { Wake(req); });
            CHECK(ok);
        }
        uint32_t cnt = FLAGS_n_partitions;
        while (cnt > 0)
        {
            uint64_t user_data;
            while (finished_reqs_.try_dequeue(user_data))
            {
                uint32_t id =
                    (user_data & ((static_cast<uint64_t>(1) << 63) - 1));
                auto reader = readers_[id];
                auto partition = reader->partition_;
                size_t idx = 0;
                auto ents = reader->scan_req_.Entries();
                for (size_t j = 0; j < FLAGS_max_key; ++j)
                {
                    ExpectedValue expected_value(
                        thread_state_->Load(partition->id_, j));
                    if (!expected_value.IsDeleted())
                    {
                        if (idx < ents.size())
                        {
                            const auto &[k, v, ts, _] = ents[idx];
                            if (KeyStringToInt(k) == j)
                            {
                                CHECK(stoi(v.substr(0, 32)) ==
                                          expected_value.GetValueBase() ||
                                      stoi(v.substr(0, 32)) ==
                                          expected_value.GetValueBase() - 1);
                                expected_value.SetValueBase(
                                    stoi(v.substr(0, 32)));
                                thread_state_->Value(partition->id_, j) =
                                    expected_value.Read();
                                ++idx;
                            }
                            else
                            {
                                expected_value.SetDeleted();
                                thread_state_->Value(partition->id_, j) =
                                    expected_value.Read();
                            }
                        }
                        else
                        {
                            expected_value.SetDeleted();
                            thread_state_->Value(partition->id_, j) =
                                expected_value.Read();
                        }
                    }
                    else
                    {
                        if (idx >= ents.size())
                            continue;
                        auto [k, v, ts, _] = ents[idx];
                        if (KeyStringToInt(k) == j)
                        {
                            CHECK(stoi(v.substr(17, 15)) ==
                                  expected_value.GetValueBase());
                            expected_value.ClearDeleted();
                            thread_state_->Value(partition->id_, j) =
                                expected_value.Read();
                            ++idx;
                        }
                    }
                }
                --cnt;
            }
        }
    }
}
std::atomic<int> StressTest::init_completed_count_{0};
std::atomic<int> StressTest::total_threads_{0};
std::condition_variable StressTest::init_barrier_cv_;
std::mutex StressTest::init_barrier_mutex_;
std::atomic<bool> StressTest::all_init_done_{false};

StressTest::ThroughputStats StressTest::throughput_stats_;
std::mutex StressTest::throughput_mutex_;
std::thread StressTest::throughput_reporter_thread_;
std::atomic<bool> StressTest::stop_throughput_monitoring_{false};
std::ofstream StressTest::throughput_log_;
uint64_t StressTest::theoretical_disk_usage_{0};

struct SystemIOStats
{
    uint64_t total_read_bytes = 0;
    uint64_t total_write_bytes = 0;
    uint64_t total_read_ops = 0;
    uint64_t total_write_ops = 0;
};
SystemIOStats GetSystemIOStats()
{
    SystemIOStats total_stats;

    try
    {
        std::ifstream diskstats_file("/proc/diskstats");
        if (!diskstats_file.is_open())
        {
            LOG(WARNING) << "Cannot open /proc/diskstats";
            return total_stats;
        }

        std::string line;
        while (std::getline(diskstats_file, line))
        {
            std::istringstream iss(line);
            std::string device_name;
            uint64_t major, minor;
            uint64_t read_ios, read_merges, read_sectors, read_ticks;
            uint64_t write_ios, write_merges, write_sectors, write_ticks;
            uint64_t dummy;

            // /proc/diskstats 格式：major minor device_name read_ios
            // read_merges read_sectors read_ticks write_ios write_merges
            // write_sectors write_ticks ...
            if (iss >> major >> minor >> device_name >> read_ios >>
                read_merges >> read_sectors >> read_ticks >> write_ios >>
                write_merges >> write_sectors >> write_ticks)
            {
                if (device_name == "nvme0n1" || device_name == "sda")
                {
                    total_stats.total_read_ops += read_ios;
                    total_stats.total_write_ops += write_ios;
                    total_stats.total_read_bytes += read_sectors * 512;
                    total_stats.total_write_bytes += write_sectors * 512;
                }
            }
        }
    }
    catch (const std::exception &e)
    {
        LOG(WARNING) << "Error reading system IO stats: " << e.what();
    }

    return total_stats;
}
uint64_t StressTest::GetDiskUsage(const std::string &path)
{
    uint64_t total_size = 0;
    try
    {
        if (std::filesystem::exists(path))
        {
            for (const auto &entry :
                 std::filesystem::recursive_directory_iterator(path))
            {
                if (entry.is_regular_file())
                {
                    total_size += entry.file_size();
                }
            }
        }
    }
    catch (const std::filesystem::filesystem_error &e)
    {
        LOG(WARNING) << "Error calculating disk usage: " << e.what();
    }
    return total_size;
}
void StressTest::StartThroughputMonitoring()
{
    if (!FLAGS_enable_throughput_monitoring)
    {
        return;
    }

    stop_throughput_monitoring_.store(false);
    throughput_stats_.last_report_time.store(UnixTimestamp());

    double avg_value_size = (FLAGS_shortest_value + FLAGS_longest_value) / 2.0;
    theoretical_disk_usage_ =
        static_cast<uint64_t>(FLAGS_n_tables * FLAGS_n_partitions *
                              FLAGS_max_key * (12 + avg_value_size) * 0.75);

    std::lock_guard<std::mutex> lock(throughput_mutex_);
    throughput_log_.open(FLAGS_throughput_log_file, std::ios::app);
    if (throughput_log_.is_open())
    {
        throughput_log_ << "# Timestamp, Write_MB/s, Read_MB/s, Write_Qps, "
                           "Read_Qps, Total_Write_MB, Total_Read_MB, "
                           "Current_Disk_Usage_MB, Theoretical_Disk_Usage_MB, "
                           "Total_Write_Ops, Total_Read_Ops, "
                           "System_Write_MB/s, System_Read_MB/s, "
                           "System_Write_Qps, System_Read_Qps\n";
        throughput_log_.flush();
    }

    throughput_reporter_thread_ = std::thread(&StressTest::ThroughputReporter);
}

void StressTest::StopThroughputMonitoring()
{
    if (!FLAGS_enable_throughput_monitoring)
    {
        return;
    }

    stop_throughput_monitoring_.store(true);
    if (throughput_reporter_thread_.joinable())
    {
        throughput_reporter_thread_.join();
    }

    std::lock_guard<std::mutex> lock(throughput_mutex_);
    if (throughput_log_.is_open())
    {
        throughput_log_.close();
    }
}

void StressTest::ThroughputReporter()
{
    static SystemIOStats last_system_stats;
    static bool first_run = true;
    while (!stop_throughput_monitoring_.load())
    {
        std::this_thread::sleep_for(
            std::chrono::seconds(FLAGS_throughput_report_interval_secs));

        if (stop_throughput_monitoring_.load())
        {
            break;
        }

        uint64_t current_time = UnixTimestamp();
        uint64_t last_time =
            throughput_stats_.last_report_time.exchange(current_time);
        uint64_t time_diff_ns = current_time - last_time;

        if (time_diff_ns == 0)
            continue;

        uint64_t current_write_bytes =
            throughput_stats_.total_write_bytes.load();
        uint64_t current_read_bytes = throughput_stats_.total_read_bytes.load();
        uint64_t current_write_ops = throughput_stats_.total_write_ops.load();
        uint64_t current_read_ops = throughput_stats_.total_read_ops.load();

        uint64_t last_write_bytes =
            throughput_stats_.last_write_bytes.exchange(current_write_bytes);
        uint64_t last_read_bytes =
            throughput_stats_.last_read_bytes.exchange(current_read_bytes);
        uint64_t last_write_ops =
            throughput_stats_.last_write_ops.exchange(current_write_ops);
        uint64_t last_read_ops =
            throughput_stats_.last_read_ops.exchange(current_read_ops);

        uint64_t write_bytes_diff = current_write_bytes - last_write_bytes;
        uint64_t read_bytes_diff = current_read_bytes - last_read_bytes;
        uint64_t write_ops_diff = current_write_ops - last_write_ops;
        uint64_t read_ops_diff = current_read_ops - last_read_ops;

        double time_diff_secs = static_cast<double>(time_diff_ns) / 1e9;
        double write_mbps =
            (static_cast<double>(write_bytes_diff) / (1024.0 * 1024.0)) /
            time_diff_secs;
        double read_mbps =
            (static_cast<double>(read_bytes_diff) / (1024.0 * 1024.0)) /
            time_diff_secs;
        double write_ops_per_sec =
            static_cast<double>(write_ops_diff) / time_diff_secs;
        double read_ops_per_sec =
            static_cast<double>(read_ops_diff) / time_diff_secs;

        double total_write_mb =
            static_cast<double>(current_write_bytes) / (1024.0 * 1024.0);
        double total_read_mb =
            static_cast<double>(current_read_bytes) / (1024.0 * 1024.0);

        uint64_t current_disk_usage = GetDiskUsage(FLAGS_db_path);
        double current_disk_usage_mb =
            static_cast<double>(current_disk_usage) / (1024.0 * 1024.0);
        double theoretical_disk_usage_mb =
            static_cast<double>(theoretical_disk_usage_) / (1024.0 * 1024.0);

        SystemIOStats current_system_stats = GetSystemIOStats();

        double system_read_mbps = 0.0;
        double system_write_mbps = 0.0;
        double system_read_ops_per_sec = 0.0;
        double system_write_ops_per_sec = 0.0;

        if (!first_run)
        {
            uint64_t system_read_bytes_diff =
                current_system_stats.total_read_bytes -
                last_system_stats.total_read_bytes;
            uint64_t system_write_bytes_diff =
                current_system_stats.total_write_bytes -
                last_system_stats.total_write_bytes;
            uint64_t system_read_ops_diff =
                current_system_stats.total_read_ops -
                last_system_stats.total_read_ops;
            uint64_t system_write_ops_diff =
                current_system_stats.total_write_ops -
                last_system_stats.total_write_ops;

            system_read_mbps = (static_cast<double>(system_read_bytes_diff) /
                                (1024.0 * 1024.0)) /
                               time_diff_secs;
            system_write_mbps = (static_cast<double>(system_write_bytes_diff) /
                                 (1024.0 * 1024.0)) /
                                time_diff_secs;
            system_read_ops_per_sec =
                static_cast<double>(system_read_ops_diff) / time_diff_secs;
            system_write_ops_per_sec =
                static_cast<double>(system_write_ops_diff) / time_diff_secs;
        }
        last_system_stats = current_system_stats;
        first_run = false;
        std::lock_guard<std::mutex> lock(throughput_mutex_);
        if (throughput_log_.is_open())
        {
            throughput_log_
                << current_time << ", " << std::fixed << std::setprecision(2)
                << write_mbps << ", " << read_mbps << ", " << std::fixed
                << std::setprecision(0) << write_ops_per_sec << ", "
                << read_ops_per_sec << ", " << std::fixed
                << std::setprecision(2) << total_write_mb << ", "
                << total_read_mb << ", " << current_disk_usage_mb << ", "
                << theoretical_disk_usage_mb << ", " << std::fixed
                << std::setprecision(0) << current_write_ops << ", "
                << current_read_ops << ", " << std::fixed
                << std::setprecision(2) << system_write_mbps << ", "
                << system_read_mbps << ", " << std::fixed
                << std::setprecision(0) << system_write_ops_per_sec << ", "
                << system_read_ops_per_sec << "\n";
            throughput_log_.flush();
        }
    }
}

void StressTest::RecordWriteBytes(uint64_t bytes, uint32_t ops)
{
    if (!FLAGS_enable_throughput_monitoring)
    {
        return;
    }
    throughput_stats_.total_write_bytes.fetch_add(bytes);
    throughput_stats_.total_write_ops.fetch_add(ops);
}

void StressTest::RecordReadBytes(uint64_t bytes, uint32_t ops)
{
    if (!FLAGS_enable_throughput_monitoring)
    {
        return;
    }
    throughput_stats_.total_read_bytes.fetch_add(bytes);
    throughput_stats_.total_read_ops.fetch_add(ops);
}

}  // namespace StressTest