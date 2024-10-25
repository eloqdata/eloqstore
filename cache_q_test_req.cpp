#include <glog/logging.h>

#include <array>
#include <atomic>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "shard_queue.h"

size_t core_cnt = 4;
size_t seconds = 5;

struct CcReq
{
    using uptr = std::unique_ptr<CcReq>;

    uint64_t data0_;
    uint16_t from_core_id_;
    uint16_t status_;
};

thread_local std::vector<std::unique_ptr<CcReq>> cc_req_pool;
thread_local size_t req_alloc_cnt = 0;

CcReq::uptr GetCcReq()
{
    if (cc_req_pool.empty())
    {
        ++req_alloc_cnt;
        return std::make_unique<CcReq>();
    }
    else
    {
        CcReq::uptr req = std::move(cc_req_pool.back());
        cc_req_pool.pop_back();
        return req;
    }
}

void Run(uint16_t core_id,
         uint16_t core_cnt,
         std::atomic<bool> *finish_flag,
         std::vector<eloq::ShardQueue<CcReq::uptr>> *global_queue,
         std::atomic<size_t> *finish_cnt)
{
    std::random_device dev{"/dev/urandom"};
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> num_gen(0,
                                                                     1000000);

    std::vector<std::vector<CcReq::uptr>> dispatch_cache;
    dispatch_cache.resize(core_cnt);

    std::vector<uint16_t> tail_rec;
    tail_rec.resize(core_cnt);
    std::vector<uint16_t> head_rec;
    head_rec.resize(core_cnt);

    std::array<CcReq::uptr, 1024 * 4> buf;
    size_t total = 0;
    size_t loop_cnt = 0, empty_loop_cnt = 0;
    size_t pseudo_sum = 0;
    while (finish_flag->load(std::memory_order_relaxed))
    {
        size_t dispatch_cnt = 0;

        CcReq::uptr req = GetCcReq();
        size_t send_cnt = 0;
        while (req != nullptr)
        {
            req->from_core_id_ = core_id;
            req->status_ = 0;

            size_t num = num_gen(rng);
            size_t dispatch_id = num % global_queue->size();
            dispatch_cache[dispatch_id].emplace_back(std::move(req));
            ++send_cnt;

            if (send_cnt >= 512)
            {
                break;
            }

            req = GetCcReq();
        }

        for (uint16_t cid = 0; cid < core_cnt; ++cid)
        {
            auto &dispatch_vec = dispatch_cache[cid];
            if (dispatch_vec.empty())
            {
                continue;
            }

            uint16_t cnt = (*global_queue)[cid].TryEnqueueBulk(
                dispatch_vec.begin(), dispatch_vec.size(), core_id);

            dispatch_vec.erase(dispatch_vec.begin(),
                               dispatch_vec.begin() + cnt);

            dispatch_cnt += cnt;
        }

        size_t finish_req_cnt = 0;
        size_t req_cnt = 0;
        size_t batch_cnt = 0;
        do
        {
            batch_cnt = (*global_queue)[core_id].TryDequeueBulk(
                buf.begin(), buf.size(), &head_rec);
            req_cnt += batch_cnt;

            for (size_t idx = 0; idx < batch_cnt; ++idx)
            {
                CcReq::uptr &req = buf[idx];

                if (req->from_core_id_ != core_id)
                {
                    // req->data0_ = num_gen(rng);
                    // req->status_ = 1;
                    uint16_t req_core_id = req->from_core_id_;
                    dispatch_cache[req_core_id].emplace_back(std::move(req));
                }
                else
                {
                    // assert(req->status_ == 1);
                    // pseudo_sum += req->data0_;
                    cc_req_pool.emplace_back(std::move(req));
                    finish_req_cnt++;
                }
            }

        } while (batch_cnt > (buf.size() / 2));

        total += finish_req_cnt;

        if (req_cnt + dispatch_cnt == 0)
        {
            empty_loop_cnt++;
        }

        // std::string s;
        // for (uint16_t dispatch_cid = 0; dispatch_cid < core_cnt;
        // ++dispatch_cid)
        // {
        //     s.append("-");
        //     s.append(std::to_string(dispatch_cnt[dispatch_cid]));
        //     s.append("/");
        //     s.append(std::to_string(tail_rec[dispatch_cid]));
        // }

        // s.append(",deque#");
        // s.append(std::to_string(cnt));
        // for (uint16_t cid = 0; cid < core_cnt; ++cid)
        // {
        //     s.append("-");
        //     s.append(std::to_string(head_rec[cid]));
        // }

        // LOG(INFO) << "core#" << core_id << ",allocated:" << req_alloc_cnt
        //           << ",avail#" << cc_req_pool.size() << ",dispatch:" << s;

        loop_cnt++;
    }

    finish_cnt->fetch_add(total, std::memory_order_relaxed);
    LOG(INFO) << "core #" << core_id << ", pseudo sum: " << pseudo_sum
              << ", empty ratio: " << empty_loop_cnt * 100 / loop_cnt
              << ", alloc req: " << req_alloc_cnt;
}

int main()
{
    std::vector<eloq::ShardQueue<CcReq::uptr>> global_queue;

    global_queue.reserve(core_cnt);
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        global_queue.emplace_back(1024, core_cnt);
    }

    std::atomic<bool> flag{true};
    std::atomic<size_t> task_cnt{0};

    std::vector<std::thread> worker_pool;
    worker_pool.reserve(core_cnt);
    for (size_t cid = 0; cid < core_cnt; ++cid)
    {
        worker_pool.emplace_back(std::thread(
            [core_id = cid,
             core_cnt = core_cnt,
             flag_ptr = &flag,
             queue_ptr = &global_queue,
             cnt_ptr = &task_cnt]()
            { Run(core_id, core_cnt, flag_ptr, queue_ptr, cnt_ptr); }));
    }

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s * seconds);

    flag.store(false, std::memory_order_relaxed);
    for (size_t idx = 0; idx < worker_pool.size(); ++idx)
    {
        worker_pool[idx].join();
    }

    std::cout << "op throughput : "
              << (task_cnt.load(std::memory_order_relaxed) / seconds)
              << std::endl;
}