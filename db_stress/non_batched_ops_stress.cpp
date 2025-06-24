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
class NonBatchedOpsStressTest : public StressTest
{
public:
    NonBatchedOpsStressTest()
    {
        type = TestType::NonBatchedOpsStressTest;
    }
    virtual ~NonBatchedOpsStressTest() = default;

    void TestPut(ThreadState *thread,
                 uint32_t partition_id,
                 std::vector<int64_t> &rand_keys) override
    {
        Partition *partition = partitions_[partition_id];
        assert(!partition->IsWriting());
        partition->ticks_++;
        partition->pending_expected_values.resize(rand_keys.size());

        uint64_t ts = UnixTimestamp();
        std::vector<kvstore::WriteDataEntry> entries;

        sort(rand_keys.begin(), rand_keys.end());
        for (size_t i = 0; i < rand_keys.size(); ++i)
        {
            const std::string k = Key(rand_keys[i]);

            partition->pending_expected_values[i] =
                thread->PreparePut(partition_id, rand_keys[i]);

            const uint32_t value_base =
                partition->pending_expected_values[i].GetFinalValueBase();

            kvstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = k;
            uint64_t ts1 = UnixTimestamp();
            ent.val_ = partition->GenerateValue(value_base);
            partition->gen_v_time += UnixTimestamp() - ts1;
            ent.timestamp_ = ts;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
        thread->TraceOneBatch(partition_id, rand_keys, true);
        partition->req_.SetArgs({thread->table_name_, partition->id_},
                                std::move(entries));
        uint64_t user_data = (partition->id_ | (uint64_t(1) << 63));
        bool ok =
            store_->ExecAsyn(&partition->req_,
                             user_data,
                             [this](kvstore::KvRequest *req) { Wake(req); });
        CHECK(ok);
    }

    void TestDelete(ThreadState *thread,
                    uint32_t partition_id,
                    std::vector<int64_t> &rand_keys) override
    {
        Partition *partition = partitions_[partition_id];
        assert(!partition->IsWriting());
        partition->ticks_++;
        partition->pending_expected_values.resize(rand_keys.size());

        uint64_t ts = UnixTimestamp();
        std::vector<kvstore::WriteDataEntry> entries;

        sort(rand_keys.begin(), rand_keys.end());
        for (size_t i = 0; i < rand_keys.size(); ++i)
        {
            const std::string k = Key(rand_keys[i]);

            partition->pending_expected_values[i] =
                thread->PrepareDelete(partition_id, rand_keys[i]);

            kvstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = k;
            ent.timestamp_ = ts;
            ent.op_ = kvstore::WriteOp::Delete;
        }
        thread->TraceOneBatch(partition_id, rand_keys, false);
        partition->req_.SetArgs({thread->table_name_, partition->id_},
                                std::move(entries));
        uint64_t user_data = (partition->id_ | (uint64_t(1) << 63));
        bool ok =
            store_->ExecAsyn(&partition->req_,
                             user_data,
                             [this](kvstore::KvRequest *req) { Wake(req); });
        CHECK(ok);
    }

    void TestGet(ThreadState *thread,
                 uint32_t reader_id,
                 int64_t rand_key) override
    {
        Reader *reader = readers_[reader_id];
        reader->key_reading_ = Key(rand_key);
        reader->pre_read_expected_value =
            thread->Load(reader->partition_->id_, rand_key);

        std::string_view read_key(reader->key_reading_);
        reader->read_req_.SetArgs(
            {thread->table_name_, reader->partition_->id_}, read_key);
        uint64_t user_data = reader->id_;
        bool ok =
            store_->ExecAsyn(&reader->read_req_,
                             user_data,
                             [this](kvstore::KvRequest *req) { Wake(req); });
        CHECK(ok);
        reader->IsReading = true;
    }

    void VerifyDb(ThreadState *thread) override
    {
        uint64_t ts1 = UnixTimestamp();
        if (!FLAGS_syn_scan)
        {
            for (uint32_t reader_id = 0; reader_id < readers_.size();
                 reader_id += FLAGS_num_readers_per_partition)
            {
                auto reader = readers_[reader_id];
                reader->scan_req_.SetArgs(
                    {thread->table_name_, reader->partition_->id_}, {}, {});
                uint64_t user_data = reader->id_;
                bool ok = store_->ExecAsyn(
                    &reader->scan_req_,
                    user_data,
                    [this](kvstore::KvRequest *req) -> void { Wake(req); });
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
                    size_t idx = 0;
                    for (size_t key = 0; key < FLAGS_max_key; ++key)
                    {
                        ExpectedValue expected_value(
                            thread->Load(reader->partition_->id_, key));
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
                    {thread->table_name_, partition->id_}, {}, {});
                store_->ExecSync(&reader->scan_req_);
                CHECK(reader->scan_req_.Error() == kvstore::KvError::NoError ||
                      reader->scan_req_.Error() == kvstore::KvError::NotFound);
                size_t idx = 0;
                for (size_t key = 0; key < FLAGS_max_key; ++key)
                {
                    ExpectedValue expected_value(
                        thread->Load(partition->id_, key));
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

        LOG(INFO) << " verify successfully.Scan costs " << ts2 - ts1;
    }
};

StressTest *CreateNonBatchedOpsStressTest()
{
    return new NonBatchedOpsStressTest();
}

}  // namespace StressTest