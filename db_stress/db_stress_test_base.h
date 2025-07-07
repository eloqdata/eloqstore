#pragma once

#include <sys/types.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <queue>
#include <unordered_set>
#include <vector>

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
    StressTest(const std::string &table_name)
    {
        // according FLAGS_open_wfile to creator ThreadState or FileThreadState
        if (!FLAGS_open_wfile)
        {
            thread_state_ = new ThreadState(table_name);
        }
        else
        {
            thread_state_ = new FileThreadState(table_name);
        }
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
        delete thread_state_;
    }

    bool LinkToStore(eloqstore::EloqStore *store)
    {
        if (!store)
            return false;
        store_ = store;
        return true;
    }
    // add
    void Start()
    {
        worker_thread_ = std::thread(&StressTest::OperateTest, this);
    }

    // add
    void Join()
    {
        if (worker_thread_.joinable())
        {
            worker_thread_.join();
        }
    }
    void Wake(eloqstore::KvRequest *req)
    {
        bool ok = finished_reqs_.enqueue(req->UserData());
        CHECK(ok);
    }

    // remove threadstate parameter
    void VerifyAndSyncValues();
    void InitDb();
    void OperateDb();
    // 仅仅这个是虚函数
    virtual void VerifyDb()
    {
    }
    void ClearDb();

    virtual void TestPut(uint32_t partition_id, std::vector<int64_t> &rand_keys)
    {
    }
    virtual void TestDelete(uint32_t partition_id,
                            std::vector<int64_t> &rand_keys)
    {
    }
    // add a new virtual function for mixed upsert and delete
    virtual void TestMixedOps(uint32_t partition_id, std::vector<int64_t> &rand_keys)
    {

    }
    virtual void TestGet(uint32_t reader_id, int64_t rand_key)
    {
    }
    virtual void TestScan(uint32_t reader_id, int64_t rand_key)
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
        eloqstore::BatchWriteRequest req_;
        eloqstore::TruncateRequest trun_req_;
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
        Reader(uint32_t id) : id_(id), IsReading(false), should_stop(false), is_scan_mode_(false)
        {
        }

        // currently, this function will verify batch by scan ,and verify nonbatch by two metheds
        void VerifyGet(ThreadState *thread);

        uint32_t id_;
        bool IsReading;
        bool should_stop;
        
        // scan or read one
        bool is_scan_mode_;
        // 有两种模式,scan目前只在batch模式和verify的时候被用到
        // read目前在nonbatch的testget中用到
        eloqstore::ScanRequest scan_req_;
        eloqstore::ReadRequest read_req_;

        // // reader会存储读之前和读之后的,用来verify
        // ExpectedValue pre_read_expected_value;
        // ExpectedValue post_read_expected_value;

        // add for scan , old can't be support scan to store pre and post number
        // now the key_reading is the key of the vec[0]
        std::vector<ExpectedValue> pre_read_expected_values;
        // post actually not be a member
        // std::vector<ExpectedValue> post_read_expected_values;
        std::vector<std::string> key_readings_;  // new key_reading be supported for scan
        Partition *partition_;

        std::string begin_key_;
        std::string end_key_;
        // std::string key_reading_;
    };

    std::vector<Partition *> partitions_;
    std::vector<Reader *> readers_;
    std::queue<uint32_t> spare_id;
    std::unordered_set<uint32_t> unfinished_id_;
    uint16_t process_rate = 10;
    uint64_t start_time = 0;
    uint64_t ops_fin = 0;
    uint64_t ops_all = 0;

    eloqstore::EloqStore *store_;
    moodycamel::ConcurrentQueue<uint64_t> finished_reqs_;
    // should be a member
    ThreadState *thread_state_;
    enum class TestType
    {
        BatchedOpsStressTest,
        NonBatchedOpsStressTest
    };
    TestType type;


public:  //用于线程同步
    static std::atomic<int> init_completed_count_;
    static std::atomic<int> total_threads_;
    static std::condition_variable init_barrier_cv_;
    static std::mutex init_barrier_mutex_;
    static std::atomic<bool> all_init_done_;
private:
    void OperateTest()
    {
        InitDb();
        WaitForAllInitComplete(); //新增同步屏障
        OperateDb();
        ClearDb();
    }
    void WaitForAllInitComplete(); //线程同步屏障,用于等待所有线程初始化完成
    std::thread worker_thread_;
};

}  // namespace StressTest