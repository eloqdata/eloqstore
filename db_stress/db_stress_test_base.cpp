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

#include "db_stress_common.h"
#include "db_stress_shared_state.h"
#include "eloq_store.h"
#include "error.h"
#include "expected_value.h"
#include "random.h"

namespace StressTest
{

void StressTest::InitDb(ThreadState *thread)
{
    LOG(INFO) << "Start Initing Db.";
    if (FLAGS_test_batched_ops_stress)
    {
    }
    else if (!FLAGS_open_wfile)
    {
        VerifyAndSyncValues(thread);
        VerifyDb(thread);
    }
    else
    {
        thread->Init();
        VerifyAndSyncValues(thread);
        thread->FinishInit();
    }
    LOG(INFO) << "Db has been Inited.";
}

void StressTest::OperateDb(ThreadState *thread)
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
            bool is_write = (user_data & (uint64_t(1) << 63));
            uint32_t id = (user_data & ((uint64_t(1) << 63) - 1));

            if (is_write)
            {
                auto partition = partitions_[id];
                partition->FinishWrite();
            }

            else
            {
                auto reader = readers_[id];
                uint64_t ts22 = UnixTimestamp();
                reader->VerifyGet(thread);
                ts_verify += UnixTimestamp() - ts22;
            }
        }

        for (auto partition_id : unfinished_id_)
        {
            auto partition = partitions_[partition_id];

            if (!partition->IsWriting() && !partition->should_stop)
            {
                if (partition->rand_.PercentTrue(FLAGS_write_percent))
                {
                    uint64_t ts11 = UnixTimestamp();
                    std::vector<int64_t> rand_keys =
                        GenerateNKeys(partition, partition->FinishedRounds());
                    ts_rand += UnixTimestamp() - ts11;

                    TestPut(thread, partition_id, rand_keys);
                }

                else
                {
                    uint64_t ts11 = UnixTimestamp();
                    std::vector<int64_t> rand_keys =
                        GenerateNKeys(partition, partition->FinishedRounds());
                    ts_rand += UnixTimestamp() - ts11;

                    TestDelete(thread, partition_id, rand_keys);
                }
            }

            for (uint32_t i = partition_id * FLAGS_num_readers_per_partition;
                 i < (partition_id + 1) * FLAGS_num_readers_per_partition;
                 ++i)
            {
                auto reader = readers_[i];
                if (!reader->IsReading &&
                    partition->verify_cnt < FLAGS_max_verify_ops_per_write &&
                    !reader->should_stop)
                {
                    int64_t rand_key =
                        GenerateOneKey(partition, partition->FinishedRounds());
                    TestGet(thread, i, rand_key);
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

void StressTest::ClearDb(ThreadState *thread)
{
    LOG(INFO) << "Start Clearing Db.";
    VerifyDb(thread);
    uint64_t ts1 = UnixTimestamp();
    uint64_t user_data;
    while (finished_reqs_.try_dequeue(user_data))
        ;
    for (auto partition : partitions_)
    {
        partition->trun_req_.SetArgs({thread->table_name_, partition->id_}, {});
        user_data = partition->id_;
        bool ok =
            store_->ExecAsyn(&partition->trun_req_,
                             user_data,
                             [this](kvstore::KvRequest *req) { Wake(req); });
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
            reader->scan_req_.SetArgs({thread->table_name_, id}, {}, {});
            CHECK(reader->scan_req_.Error() == kvstore::KvError::NotFound ||
                  reader->scan_req_.Error() == kvstore::KvError::NoError);
            --cnt;
        }
    }

    thread->Clear();
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
    size_t value_sz;
    if (FLAGS_value_sz_mode == 0)
    {
        value_sz = (rand_.Uniform(5) + 1) * 32;
    }
    else if (FLAGS_value_sz_mode == 1)
    {
        value_sz = (rand_.Uniform(3) + 1) * KB + rand_.Uniform(1024);
    }
    else if (FLAGS_value_sz_mode == 2)
    {
        value_sz = (rand_.Uniform(900) + 100) * KB + rand_.Uniform(1024);
    }
    else
    {
        value_sz = (rand_.Uniform(250) + 50) * MB + rand_.Uniform(1024) * KB +
                   rand_.Uniform(1024);
    }
    assert(value_sz >= 32);

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
    CHECK(req_.Error() == kvstore::KvError::NoError);
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
    // if (parent->ops_fin == parent->process_rate * parent->ops_all / 100)
    // {
    //     LOG(INFO) << " process rate:" << parent->process_rate
    //               << "%, it costs:" << UnixTimestamp() - parent->start_time;
    //     parent->process_rate += 10;
    // }
    CheckIfAllFinished();
}

void StressTest::Reader::VerifyGet(ThreadState *thread)
{
    if (partition_->table->type == TestType::BatchedOpsStressTest)
    {
        CHECK(scan_req_.Error() == kvstore::KvError::NoError ||
              scan_req_.Error() == kvstore::KvError::NotFound);
        partition_->verify_cnt++;
        if (scan_req_.Entries().empty())
            return;

        std::vector<std::string> v_res;
        for (auto [k, v, ts, _] : scan_req_.Entries())
        {
            assert(!k.empty());
            assert(!v.empty());

            CHECK(k.back() == v.back());
            v.pop_back();
            v_res.emplace_back(v);
        }

        CHECK(v_res.size() == 10);
        for (int i = 0; i < 10; ++i)
        {
            CHECK(v_res[i] == v_res[0]);
        }

        IsReading = false;
    }

    else
    {
        partition_->verify_cnt++;
        post_read_expected_value =
            thread->Load(partition_->id_, KeyStringToInt(key_reading_));
        if (read_req_.Error() == kvstore::KvError::NotFound)
        {
            assert(!ExpectedValueHelper::MustHaveExisted(
                pre_read_expected_value, post_read_expected_value));
        }
        else
        {
            CHECK(read_req_.Error() == kvstore::KvError::NoError);
            assert(!ExpectedValueHelper::MustHaveNotExisted(
                pre_read_expected_value, post_read_expected_value));
            uint32_t value_base = std::stoi(read_req_.value_.substr(0, 32));
            assert(ExpectedValueHelper::InExpectedValueBaseRange(
                value_base, pre_read_expected_value, post_read_expected_value));
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

void StressTest::VerifyAndSyncValues(ThreadState *thread)
{
    if (!thread->HasHistory())
    {
        thread->ResetValues();
        return;
    }
    if (FLAGS_syn_scan)
    {
        for (auto partition : partitions_)
        {
            auto reader = readers_[0];
            reader->scan_req_.SetArgs(
                {thread->table_name_, partition->id_}, {}, {});
            store_->ExecSync(&reader->scan_req_);
            size_t idx = 0;
            auto ents = reader->scan_req_.Entries();
            for (size_t j = 0; j < FLAGS_max_key; ++j)
            {
                ExpectedValue expected_value(thread->Load(partition->id_, j));
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
                            expected_value.SetValueBase(stoi(v.substr(0, 32)));
                            thread->Value(partition->id_, j) =
                                expected_value.Read();
                            ++idx;
                        }
                        else
                        {
                            expected_value.SetDeleted();
                            thread->Value(partition->id_, j) =
                                expected_value.Read();
                        }
                    }
                    else
                    {
                        expected_value.SetDeleted();
                        thread->Value(partition->id_, j) =
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
                        thread->Value(partition->id_, j) =
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
                {thread->table_name_, reader->partition_->id_}, {}, {});
            uint64_t user_data = reader->id_;
            bool ok = store_->ExecAsyn(&reader->scan_req_,
                                       user_data,
                                       [this](kvstore::KvRequest *req) -> void
                                       { Wake(req); });
            CHECK(ok);
        }
        uint32_t cnt = FLAGS_n_partitions;
        while (cnt > 0)
        {
            uint64_t user_data;
            while (finished_reqs_.try_dequeue(user_data))
            {
                uint32_t id = (user_data & ((uint64_t(1) << 63) - 1));
                auto reader = readers_[id];
                auto partition = reader->partition_;
                size_t idx = 0;
                auto ents = reader->scan_req_.Entries();
                for (size_t j = 0; j < FLAGS_max_key; ++j)
                {
                    ExpectedValue expected_value(
                        thread->Load(partition->id_, j));
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
                                thread->Value(partition->id_, j) =
                                    expected_value.Read();
                                ++idx;
                            }
                            else
                            {
                                expected_value.SetDeleted();
                                thread->Value(partition->id_, j) =
                                    expected_value.Read();
                            }
                        }
                        else
                        {
                            expected_value.SetDeleted();
                            thread->Value(partition->id_, j) =
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
                            thread->Value(partition->id_, j) =
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