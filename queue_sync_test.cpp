#include <array>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "shared_queue.h"

size_t core_cnt = 4;
size_t seconds = 5;

struct ThreadCounter
{
    ThreadCounter() : cnt_(0)
    {
    }

    ThreadCounter(const ThreadCounter &) = delete;
    ThreadCounter(ThreadCounter &&) = delete;

    std::atomic<size_t> cnt_{0};
    char padding_[64 - sizeof(size_t)];
};

void Run(size_t core_id,
         std::atomic<bool> *finish_flag,
         std::vector<ShardQueue<size_t>> *global_queue,
         std::atomic<size_t> *finish_cnt)
{
    std::random_device dev{"/dev/urandom"};
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> num_gen(0,
                                                                     1000000);

    std::array<size_t, 1024 * 4> buf;
    size_t total = 0;
    size_t loop_cnt = 0, empty_loop_cnt = 0;
    while (finish_flag->load(std::memory_order_relaxed))
    {
        for (size_t idx = 0; idx < 32; ++idx)
        {
            size_t num = num_gen(rng);
            size_t dispatch_id = num % global_queue->size();
            (*global_queue)[dispatch_id].Enqueue(num, core_id);
        }

        size_t cnt =
            (*global_queue)[core_id].TryDequeueBulk(buf.begin(), buf.size());
        total += cnt;

        if (cnt == 0)
        {
            empty_loop_cnt++;
        }
        else
        {
            while (cnt == buf.size())
            {
                cnt = (*global_queue)[core_id].TryDequeueBulk(buf.begin(),
                                                              buf.size());
                total += cnt;
            }
        }

        loop_cnt++;
    }

    finish_cnt->store(total, std::memory_order_relaxed);
}

int main()
{
    std::vector<ShardQueue<size_t>> global_queue;
    global_queue.reserve(core_cnt);
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        global_queue.emplace_back(core_cnt, idx);
    }

    std::atomic<bool> flag{true};
    std::vector<std::unique_ptr<ThreadCounter>> cnt_vec;
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        cnt_vec.emplace_back(std::make_unique<ThreadCounter>());
    }

    std::vector<std::thread> worker_pool;
    worker_pool.reserve(core_cnt);
    for (size_t cid = 0; cid < core_cnt; ++cid)
    {
        worker_pool.emplace_back(
            std::thread([core_id = cid,
                         flag_ptr = &flag,
                         queue_ptr = &global_queue,
                         cnt_ptr = &cnt_vec[cid]->cnt_]()
                        { Run(core_id, flag_ptr, queue_ptr, cnt_ptr); }));
    }

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s * seconds);

    flag.store(false, std::memory_order_relaxed);
    for (size_t idx = 0; idx < worker_pool.size(); ++idx)
    {
        worker_pool[idx].join();
    }

    size_t total_cnt = 0;
    for (auto &thd_cnt : cnt_vec)
    {
        size_t c = thd_cnt->cnt_.load(std::memory_order_relaxed);
        total_cnt += c;
    }

    std::cout << "op throughput : " << (total_cnt / seconds) << std::endl;
}