#include "non_batched_ops_stress.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include "db_stress_common.h"
#include "db_stress_shared_state.h"
#include "db_stress_test_base.h"
#include "eloq_store.h"
#include "error.h"
#include "expected_value.h"
#include "write_task.h"

namespace StressTest
{

NonBatchedOpsStressTest::NonBatchedOpsStressTest(const std::string &table_name)
    : StressTest(table_name)
{
    type = TestType::NonBatchedOpsStressTest;
}

void NonBatchedOpsStressTest::TestPut(uint32_t partition_id,
                                      std::vector<int64_t> &rand_keys)
{
    Partition *partition = partitions_[partition_id];
    assert(!partition->IsWriting());
    partition->ticks_++;
    partition->pending_expected_values.resize(rand_keys.size());

    uint64_t ts = UnixTimestamp();
    // writeDataEntry class is an unit of kv work ,entries is a batch of kv
    // work.
    std::vector<eloqstore::WriteDataEntry> entries;
    // must be sorted,because the eloqstore should make sure the order of the
    // key
    sort(rand_keys.begin(), rand_keys.end());
    for (size_t i = 0; i < rand_keys.size(); ++i)
    {
        const std::string k = Key(rand_keys[i]);

        partition->pending_expected_values[i] =
            thread_state_->PreparePut(partition_id, rand_keys[i]);

        const uint32_t value_base =
            partition->pending_expected_values[i].GetFinalValueBase();

        eloqstore::WriteDataEntry &ent = entries.emplace_back();
        ent.key_ = k;
        uint64_t ts1 = UnixTimestamp();
        ent.val_ = partition->GenerateValue(value_base);
        partition->gen_v_time += UnixTimestamp() - ts1;
        ent.timestamp_ = ts;
        ent.op_ = eloqstore::WriteOp::Upsert;
    }
    thread_state_->TraceOneBatch(partition_id, rand_keys, true);
    partition->req_.SetArgs({thread_state_->table_name_, partition->id_},
                            std::move(entries));
    uint64_t user_data = (partition->id_ | (uint64_t(1) << 63));
    bool ok =
        store_->ExecAsyn(&partition->req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}

void NonBatchedOpsStressTest::TestDelete(uint32_t partition_id,
                                         std::vector<int64_t> &rand_keys)
{
    Partition *partition = partitions_[partition_id];
    assert(!partition->IsWriting());
    partition->ticks_++;
    partition->pending_expected_values.resize(rand_keys.size());

    uint64_t ts = UnixTimestamp();
    std::vector<eloqstore::WriteDataEntry> entries;

    sort(rand_keys.begin(), rand_keys.end());
    for (size_t i = 0; i < rand_keys.size(); ++i)
    {
        const std::string k = Key(rand_keys[i]);

        partition->pending_expected_values[i] =
            thread_state_->PrepareDelete(partition_id, rand_keys[i]);

        eloqstore::WriteDataEntry &ent = entries.emplace_back();
        ent.key_ = k;
        ent.timestamp_ = ts;
        ent.op_ = eloqstore::WriteOp::Delete;
    }
    thread_state_->TraceOneBatch(partition_id, rand_keys, false);
    partition->req_.SetArgs({thread_state_->table_name_, partition->id_},
                            std::move(entries));
    uint64_t user_data = (partition->id_ | (uint64_t(1) << 63));
    bool ok =
        store_->ExecAsyn(&partition->req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}
void NonBatchedOpsStressTest::TestMixedOps(uint32_t partition_id,
                                           std::vector<int64_t> &rand_keys)
{
    Partition *partition = partitions_[partition_id];
    assert(!partition->IsWriting());
    partition->ticks_++;
    partition->pending_expected_values.resize(rand_keys.size());

    uint64_t ts = UnixTimestamp();
    // writeDataEntry class is an unit of kv work ,entries is a batch of kv
    // work.
    std::vector<eloqstore::WriteDataEntry> entries;

    // use it to record the keys that need to be upserted and deleted.
    std::vector<int64_t> upsert_keys;
    std::vector<int64_t> delete_keys;
    // must be sorted,because the eloqstore should make sure the order of the
    // key
    sort(rand_keys.begin(), rand_keys.end());
    for (size_t i = 0; i < rand_keys.size(); ++i)
    {
        const std::string k = Key(rand_keys[i]);
        eloqstore::WriteDataEntry &ent = entries.emplace_back();
        // The common operations for deletion and insertion are preassigned in
        // advance.
        ent.key_ = k;
        ent.timestamp_ = ts;

        // choose upsert or delete randomly
        if (partition->rand_.PercentTrue(FLAGS_write_percent))
        {
            // Upsert操作
            upsert_keys.push_back(rand_keys[i]);
            partition->pending_expected_values[i] =
                thread_state_->PreparePut(partition_id, rand_keys[i]);
            const uint32_t value_base =
                partition->pending_expected_values[i].GetFinalValueBase();
            uint64_t ts1 = UnixTimestamp();
            ent.val_ = partition->GenerateValue(value_base);
            partition->gen_v_time += UnixTimestamp() - ts1;
            ent.op_ = eloqstore::WriteOp::Upsert;
        }
        else
        {
            // Delete
            delete_keys.push_back(rand_keys[i]);
            partition->pending_expected_values[i] =
                thread_state_->PrepareDelete(partition_id, rand_keys[i]);
            // delete operator didn't need to set value
            ent.op_ = eloqstore::WriteOp::Delete;
        }
    }

    // thread_state_->TraceOneBatch(partition_id, rand_keys, true); //
    // those may be modify
    if (!upsert_keys.empty())
    {
        thread_state_->TraceOneBatch(
            partition_id, upsert_keys, true);  // upsert use true
    }
    if (!delete_keys.empty())
    {
        thread_state_->TraceOneBatch(
            partition_id, delete_keys, false);  // delete use false
    }
    partition->write_start_time_ = UnixTimestamp();
    partition->req_.SetArgs({thread_state_->table_name_, partition->id_},
                            std::move(entries));
    // user_data is actually partition_id when write operation,and the highest
    // bit is 1(& 0x8000000000000000)
    uint64_t user_data = (partition->id_ | (uint64_t(1) << 63));
    bool ok =
        store_->ExecAsyn(&partition->req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}
void NonBatchedOpsStressTest::TestGet(uint32_t reader_id, int64_t rand_key)
{
    Reader *reader = readers_[reader_id];
    // set read mode
    reader->is_scan_mode_ = false;
    reader->key_readings_.clear();
    // actually ,key_readings can be vector<uint64_t>,not string
    reader->key_readings_.push_back(Key(rand_key));
    reader->pre_read_expected_values.clear();
    reader->pre_read_expected_values.push_back(
        thread_state_->Load(reader->partition_->id_, rand_key));

    std::string_view read_key(reader->key_readings_[0]);
    reader->read_start_time_ = UnixTimestamp();
    reader->read_req_.SetArgs(
        {thread_state_->table_name_, reader->partition_->id_}, read_key);
    uint64_t user_data = reader->id_;  // this user_data is actually reader_id
    bool ok =
        store_->ExecAsyn(&reader->read_req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
    reader->IsReading = true;
}
void NonBatchedOpsStressTest::TestScan(uint32_t reader_id, int64_t rand_key)
{
    Reader *reader = readers_[reader_id];
    // set scan mode
    reader->is_scan_mode_ = true;

    int64_t scan_start = rand_key;
    // Implement probability-based scan range selection
    // Small ranges have higher probability, large ranges have lower probability
    int64_t max_possible_range = FLAGS_max_key - rand_key;
    int64_t scan_range;

    if (max_possible_range <= 0)
    {
        scan_range = 1;
    }
    else
    {
        // Use exponential decay distribution: small ranges have high
        // probability, large ranges have low probability Parameters can be
        // adjusted based on max_key; larger max_key leads to faster decay
        // decay_factor 越 小 ⇒ 分布越“平滑”（尾部更厚，概率下降更慢）
        double decay_factor = std::max(
            0.1, 1000.0 / FLAGS_max_key);  // Dynamically adjust decay factor
        double random_val =
            reader->partition_->rand_.Uniform(1000) / 1000.0;  // [0,1)

        // Exponential distribution: range = -log(1-random_val) / decay_factor
        // Constrain within reasonable bounds
        double raw_range = -std::log(1.0 - random_val * 0.999) / decay_factor;
        scan_range = std::min(max_possible_range,
                              std::max(1L, static_cast<int64_t>(raw_range)));
    }

    int64_t scan_end = scan_start + scan_range;
    reader->begin_key_ = Key(scan_start);
    reader->end_key_ = Key(scan_end);

    // fix bug: clear key_readings_ to let verifyGet know this is Scan
    reader->key_readings_.clear();
    reader->pre_read_expected_values.clear();

    // add actual scaned keys to key_readings_
    for (int64_t i = scan_start; i < scan_end; ++i)
    {
        reader->key_readings_.push_back(Key(i));
        reader->pre_read_expected_values.push_back(
            thread_state_->Load(reader->partition_->id_, i));
    }
    std::string_view begin(reader->begin_key_);
    std::string_view end(reader->end_key_);
    reader->read_start_time_ = UnixTimestamp();
    reader->scan_req_.SetArgs(
        {thread_state_->table_name_, reader->partition_->id_}, begin, end);
    uint64_t user_data = reader->id_;
    bool ok =
        store_->ExecAsyn(&reader->scan_req_,
                         user_data,
                         [this](eloqstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
    reader->IsReading = true;
}
void NonBatchedOpsStressTest::VerifyDb()
{
    uint64_t ts1 = UnixTimestamp();
    if (!FLAGS_syn_scan)
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
                size_t idx = 0;
                for (size_t key = 0; key < FLAGS_max_key; ++key)
                {
                    ExpectedValue expected_value(
                        thread_state_->Load(reader->partition_->id_, key));
                    if (!expected_value.IsDeleted() &&
                        !expected_value.PendingWrite() &&
                        !expected_value.PendingDelete())
                    {
                        assert(idx < reader->scan_req_.Entries().size());
                        const auto &[k, v, ts, _] =
                            reader->scan_req_.Entries()[idx];
                        assert(KeyStringToInt(k) == key &&
                               std::stoi(v.substr(0, 32)) ==
                                   expected_value.GetFinalValueBase());
                        ++idx;
                    }
                }
                assert(idx == reader->scan_req_.Entries().size());
                --cnt;
            }
        }
    }
    else
    {
        for (auto partition : partitions_)
        {
            auto reader = readers_[0];
            reader->scan_req_.SetArgs(
                {thread_state_->table_name_, partition->id_}, {}, {});
            store_->ExecSync(&reader->scan_req_);
            CHECK(reader->scan_req_.Error() == eloqstore::KvError::NoError ||
                  reader->scan_req_.Error() == eloqstore::KvError::NotFound);
            size_t idx = 0;
            for (size_t key = 0; key < FLAGS_max_key; ++key)
            {
                ExpectedValue expected_value(
                    thread_state_->Load(partition->id_, key));
                if (!expected_value.IsDeleted() &&
                    !expected_value.PendingWrite() &&
                    !expected_value.PendingDelete())
                {
                    assert(idx < reader->scan_req_.Entries().size());
                    const auto &[k, v, ts, _] =
                        reader->scan_req_.Entries()[idx];
                    assert(KeyStringToInt(k) == key &&
                           std::stoi(v.substr(0, 32)) ==
                               expected_value.GetFinalValueBase());
                    ++idx;
                }
            }
            assert(idx == reader->scan_req_.Entries().size());
        }
    }
    uint64_t ts2 = UnixTimestamp();
}

}  // namespace StressTest