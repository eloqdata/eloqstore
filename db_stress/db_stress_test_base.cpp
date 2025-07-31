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
std::atomic<int> StressTest::init_completed_count_{0};
std::atomic<int> StressTest::total_threads_{0};
std::condition_variable StressTest::init_barrier_cv_;
std::mutex StressTest::init_barrier_mutex_;
std::atomic<bool> StressTest::all_init_done_{false};

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

    char post_fix_ch = '0' + rand_.Uniform(10);
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
            size_t entry_idx = 0;

            // Traverse all the expected keys and validate in order
            for (size_t i = 0; i < key_readings_.size(); ++i)
            {
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

}  // namespace StressTest