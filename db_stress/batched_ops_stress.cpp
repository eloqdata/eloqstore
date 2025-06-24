#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "db_stress_common.h"
#include "db_stress_shared_state.h"
#include "db_stress_test_base.h"
#include "eloq_store.h"
#include "error.h"
// #include "write_op.h"
#include "write_task.h"

namespace StressTest
{
class BatchedOpsStressTest : public StressTest
{
public:
    BatchedOpsStressTest()
    {
        type = TestType::BatchedOpsStressTest;
    }
    virtual ~BatchedOpsStressTest() = default;

    void TestPut(ThreadState *thread,
                 uint32_t partition_id,
                 std::vector<int64_t> &rand_keys) override
    {
        Partition *partition = partitions_[partition_id];
        assert(!partition->IsWriting());
        partition->ticks_++;
        std::string key_body = Key(rand_keys[0]);
        uint32_t value_base = partition->rand_.Next();
        std::string value_body = partition->GenerateValue(value_base);

        uint64_t ts = UnixTimestamp();
        std::vector<kvstore::WriteDataEntry> entries;
        for (int i = 0; i <= 9; ++i)
        {
            std::string num = std::to_string(i);
            const std::string k = key_body + num;
            const std::string v = value_body + num;

            kvstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = k;
            ent.val_ = v;
            ent.timestamp_ = ts;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
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

        std::string key_body = Key(rand_keys[0]);

        uint64_t ts = UnixTimestamp();
        std::vector<kvstore::WriteDataEntry> entries;

        for (int i = 0; i <= 9; ++i)
        {
            const std::string k = key_body + std::to_string(i);

            kvstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = k;
            ent.timestamp_ = ts;
            ent.op_ = kvstore::WriteOp::Delete;
        }

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
        reader->begin_key_ = Key(rand_key) + "0";
        reader->end_key_ = Key(rand_key + 1) + "0";

        std::string_view begin(reader->begin_key_);
        std::string_view end(reader->end_key_);
        reader->scan_req_.SetArgs(
            {thread->table_name_, reader->partition_->id_}, begin, end);
        uint64_t user_data = reader->id_;
        bool ok =
            store_->ExecAsyn(&reader->scan_req_,
                             user_data,
                             [this](kvstore::KvRequest *req) { Wake(req); });
        CHECK(ok);
        reader->IsReading = true;
    }

    void VerifyDb(ThreadState *thread) override
    {
        for (auto partition : partitions_)
        {
            kvstore::ScanRequest scan_req;
            scan_req.SetArgs({thread->table_name_, partition->id_}, {}, {});
            store_->ExecSync(&scan_req);
            CHECK(scan_req.Error() == kvstore::KvError::NoError ||
                  scan_req.Error() == kvstore::KvError::NotFound);

            if (!scan_req.Entries().empty())
            {
                std::vector<std::string> v_res(10);
                int idx = 0;
                for (auto [k, v, ts, _] : scan_req.Entries())
                {
                    assert(!k.empty());
                    assert(!v.empty());
                    CHECK(k.back() == v.back());
                    v.pop_back();
                    v_res[idx++] = v;
                    if (idx == 10)
                    {
                        for (int i = 0; i < 10; ++i)
                        {
                            CHECK(v_res[i] == v_res[0]);
                        }
                        idx = 0;
                    }
                }
            }
        }

        LOG(INFO) << " pass the VerifyDb successfully!";
    }
};

StressTest *CreateBatchedOpsStressTest()
{
    return new BatchedOpsStressTest();
}

}  // namespace StressTest