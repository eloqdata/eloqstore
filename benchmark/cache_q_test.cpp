#include <glog/logging.h>

#include <array>
#include <atomic>
#include <iostream>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "shard_queue.h"

size_t core_cnt = 4;
size_t seconds = 5;

struct CcReq
{
    using uptr = std::unique_ptr<CcReq>;

    uint64_t data0_;
    std::atomic<uint64_t> data1_;
};

thread_local std::vector<std::unique_ptr<CcReq>> cc_req_pool;
thread_local size_t req_alloc_cnt = 0;
thread_local uint64_t pseudo_sum = 0;

std::unique_ptr<CcReq> GetCcReq()
{
    if (cc_req_pool.empty())
    {
        if (req_alloc_cnt < 1024)
        {
            ++req_alloc_cnt;
            return std::make_unique<CcReq>();
        }
        else
        {
            return nullptr;
        }
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
         std::vector<eloq::ShardQueue<std::pair<CcReq::uptr, uint64_t>>>
             *global_queue,
         std::atomic<size_t> *finish_cnt,
         std::unordered_map<int, int> *map)
{
    std::random_device dev{"/dev/urandom"};
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> num_gen(0,
                                                                     10000000);

    std::vector<std::vector<std::pair<CcReq::uptr, uint64_t>>> dispatch_cache;
    dispatch_cache.resize(core_cnt);

    std::vector<uint16_t> tail_rec;
    tail_rec.resize(core_cnt);
    std::vector<uint16_t> head_rec;
    head_rec.resize(core_cnt);

    std::array<std::pair<CcReq::uptr, uint64_t>, 1024 * 4> buf;
    size_t total = 0;
    size_t loop_cnt = 0, empty_loop_cnt = 0;
    while (finish_flag->load(std::memory_order_relaxed))
    {
        for (size_t idx = 0; idx < 128; ++idx)
        {
            CcReq::uptr req = GetCcReq();
            if (req == nullptr)
            {
                break;
            }

            size_t num = num_gen(rng);
            size_t dispatch_id = num % global_queue->size();
            req->data0_ = num;

            uint64_t code = core_id << 1;
            dispatch_cache[dispatch_id].emplace_back(std::move(req), code);
            // dispatch_cnt[dispatch_id]++;
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
        }

        size_t req_cnt = 0;
        size_t batch_cnt = 0;
        do
        {
            batch_cnt = (*global_queue)[core_id].TryDequeueBulk(
                buf.begin(), buf.size() - 4, &head_rec);
            // req_cnt += batch_cnt;

            for (size_t idx = 0; idx < batch_cnt; ++idx)
            {
                __builtin_prefetch(buf[idx + 4].first.get(), 1, 0);

                auto &[req, code] = buf[idx];

                if ((code & 1) == 0)
                {
                    uint16_t from_core_id = code >> 1;
                    auto it = map->find(req->data0_);
                    if (it != map->end())
                    {
                        req->data0_ = it->second;
                    }
                    code |= 1;
                    dispatch_cache[from_core_id].emplace_back(std::move(req),
                                                              code);
                }
                else
                {
                    pseudo_sum += req->data0_;
                    cc_req_pool.emplace_back(std::move(req));
                    req_cnt++;
                }
            }

            // for (size_t idx = 0; idx < batch_cnt; ++idx)
            // {
            //     cc_req_pool.emplace_back(std::move(buf[idx]));
            // }
        } while (batch_cnt > (buf.size() / 2));

        total += req_cnt;

        if (req_cnt == 0)
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
    LOG(INFO) << "core #" << core_id << " alloc req #" << req_alloc_cnt
              << ", pseudo sum: " << pseudo_sum;
}

int main()
{
    std::vector<eloq::ShardQueue<std::pair<CcReq::uptr, uint64_t>>>
        global_queue;

    global_queue.reserve(core_cnt);
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        global_queue.emplace_back(1024, core_cnt);
    }

    std::unordered_map<int, int> map;
    for (size_t v = 0; v < 10000000; ++v)
    {
        map.try_emplace(v, v + 100);
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
             cnt_ptr = &task_cnt,
             map = &map]()
            { Run(core_id, core_cnt, flag_ptr, queue_ptr, cnt_ptr, map); }));
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
