#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <queue>
#include <unordered_set>

#include "db_stress_shared_state.h"
#include "eloq_store.h"
#include "expected_value.h"

#undef BLOCK_SIZE
#include "concurrentqueue/concurrentqueue.h"

namespace StressTest
{
class StressTest
{
public:
    StressTest()
    {
        for (uint32_t i = 0; i < FLAGS_n_partitions; ++i)
        {
            partitions_.emplace_back(new Partition(i));
            partitions_[i]->table = this;
            unfinished_id_.insert(i);
            spare_id.push(i);
        }

        for (uint32_t i = 0;
             i < FLAGS_n_partitions * FLAGS_num_readers_per_partition;
             ++i)
        {
            readers_.emplace_back(new Reader(i));
            readers_[i]->partition_ =
                partitions_[i / FLAGS_num_readers_per_partition];
        }

        ops_all = FLAGS_n_partitions * FLAGS_ops_per_partition;
    }
    virtual ~StressTest()
    {
    }

    bool LinkToStore(kvstore::EloqStore *store)
    {
        if (!store)
            return false;
        store_ = store;
        return true;
    }

    void Wake(kvstore::KvRequest *req)
    {
        bool ok = finished_reqs_.enqueue(req->UserData());
        CHECK(ok);
    }

    void VerifyAndSyncValues(ThreadState *thread);

    void InitDb(ThreadState *thread);
    void OperateDb(ThreadState *thread);
    virtual void VerifyDb(ThreadState *thread)
    {
    }
    void ClearDb(ThreadState *thread);

    virtual void TestPut(ThreadState *thread,
                         uint32_t partition_id,
                         std::vector<int64_t> &rand_keys)
    {
    }
    virtual void TestDelete(ThreadState *thread,
                            uint32_t partition_id,
                            std::vector<int64_t> &rand_keys)
    {
    }
    virtual void TestGet(ThreadState *thread,
                         uint32_t reader_id,
                         int64_t rand_key)
    {
    }

    bool AllPartitionsFinished();

    struct Partition
    {
        bool IsWriting() const;
        void FinishWrite();

        uint64_t FinishedRounds();
        void CheckIfAllFinished();

        std::string GenerateValue(uint32_t base);

        Partition(uint32_t id)
            : id_(id),
              rand_(1000 + id + FLAGS_seed),
              active_readers(FLAGS_num_readers_per_partition)
        {
            ops.resize(FLAGS_max_key);
            for (size_t i = 0; i < ops.size(); ++i)
            {
                ops[i].first = 0;
                ops[i].second = 0;
            }
        }

        uint32_t id_;
        uint64_t ticks_{0};
        kvstore::BatchWriteRequest req_;
        kvstore::TruncateRequest trun_req_;
        std::vector<PendingExpectedValue> pending_expected_values;
        Random rand_;
        StressTest *table;
        uint32_t verify_cnt{0};
        bool should_stop = false;
        uint32_t active_readers;

        std::vector<std::pair<int, int>> ops;

        uint64_t gen_v_time = 0;
    };

    struct Reader
    {
        Reader(uint32_t id) : id_(id), IsReading(false), should_stop(false)
        {
        }

        void VerifyGet(ThreadState *thread);

        uint32_t id_;
        bool IsReading;
        bool should_stop;
        kvstore::ScanRequest scan_req_;
        kvstore::ReadRequest read_req_;

        ExpectedValue pre_read_expected_value;
        ExpectedValue post_read_expected_value;
        Partition *partition_;

        std::string begin_key_;
        std::string end_key_;
        std::string key_reading_;
    };

    std::vector<Partition *> partitions_;
    std::vector<Reader *> readers_;
    std::queue<uint32_t> spare_id;
    std::unordered_set<uint32_t> unfinished_id_;
    uint16_t process_rate = 10;
    uint64_t start_time = 0;
    uint64_t ops_fin = 0;
    uint64_t ops_all = 0;

    kvstore::EloqStore *store_;
    moodycamel::ConcurrentQueue<uint64_t> finished_reqs_;

    enum TestType
    {
        BatchedOpsStressTest,
        NonBatchedOpsStressTest
    };

    TestType type;
};

}  // namespace StressTest