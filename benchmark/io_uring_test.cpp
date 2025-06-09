#include "io_uring.h"

#include <liburing.h>
#include <sys/types.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <ios>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "io_read_task.h"

static const uint32_t task_concurrency = 32;
static const uint32_t task_total = 2000000;
static const uint32_t page_cnt = 1024 * 1024;
static const uint32_t page_size = 4 * 1024;
static const uint32_t thread_cnt = 2;
std::string file_name = "/home/ljchen/test_data.dat";

#include "file_reader.h"

void InitDataFile(const std::string &file_name)
{
    std::ofstream of{file_name, std::ios::binary | std::ios::out};
    char page_buf[page_size];

    of.seekp(0);
    for (size_t pidx = 0; pidx < page_cnt; ++pidx)
    {
        memset(page_buf, 0, page_size);
        uint32_t v = pidx;
        memcpy(page_buf + (page_size - sizeof(uint32_t)), &v, sizeof(uint32_t));
        of.write(page_buf, page_size);
    }

    of.flush();
    of.close();
}

int OpenFixedFile(const std::string &file_name, io_uring *ring, int fd)
{
    // io_uring_sqe *sqe = io_uring_get_sqe(ring);
    // io_uring_prep_openat_direct(sqe,
    //                             -1,
    //                             file_name.c_str(),
    //                             0,
    //                             O_RDONLY | O_DIRECT,
    //                             IORING_FILE_INDEX_ALLOC);

    // int ret = io_uring_submit(ring);
    // if (ret == -1)
    // {
    //     throw std::runtime_error("Failed to open the fixed file in IO
    //     uring."); return 0;
    // }

    // io_uring_cqe *cqe;
    // io_uring_wait_cqe(ring, &cqe);

    // int fixed_file_idx = cqe->res;
    // io_uring_cq_advance(ring, 1);
    // return fixed_file_idx;

    int ret = io_uring_register_files_update(ring, 0, &fd, 1);
    if (ret < 0)
    {
        fprintf(stderr, "io_uring_register_files_update failed\n");
        return 1;
    }

    return 0;
}

void CloseFixedFile(int fixed_fd, io_uring *ring)
{
    io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_close_direct(sqe, fixed_fd);

    int ret = io_uring_submit(ring);
    if (ret == -1)
    {
        throw std::runtime_error("Failed to close the fixed file.");
    }

    io_uring_cqe *cqe;
    io_uring_wait_cqe(ring, &cqe);
    io_uring_cq_advance(ring, 1);
}

int RegisterOpenFile(int fd, io_uring *ring)
{
    // io_uring_sqe *sqe = io_uring_get_sqe(ring);
    // io_uring_prep_files_update(sqe, &fd, 1, IORING_FILE_INDEX_ALLOC);

    // int ret = io_uring_submit(ring);
    // if (ret == -1)
    // {
    //     throw std::runtime_error(
    //         "Failed to register the fixed file in IO uring.");
    //     return 0;
    // }

    // io_uring_cqe *cqe;
    // io_uring_wait_cqe(ring, &cqe);

    // int fixed_file_idx = cqe->res;
    // io_uring_cq_advance(ring, 1);

    // return fixed_file_idx;

    int ret = io_uring_register_files_update(ring, 0, &fd, 1);
    if (ret < 0)
    {
        fprintf(stderr, "io_uring_register_files_update failed\n");
        return 1;
    }

    return 0;
}

void ReadWorker(const std::string &file_name, int fd)
{
    size_t task_finished = 0;

    std::vector<std::unique_ptr<kvstore::IoReadTask>> task_pool;
    task_pool.reserve(task_concurrency);
    for (size_t idx = 0; idx < task_concurrency; ++idx)
    {
        task_pool.emplace_back(std::make_unique<kvstore::IoReadTask>(
            idx, page_size, task_finished, task_pool));
    }

    kvstore::IoUring read_ring{task_concurrency};
    io_uring *uring = read_ring.Get();
    io_uring_cqe *cqe_ptr = nullptr;
    int ring_ret = 1;

    auto freader = std::make_unique<kvstore::FileReader>(file_name);
    int fixed_file_idx = OpenFixedFile(file_name, uring, freader->Fd());
    // int fixed_file_idx = RegisterOpenFile(fd, uring);

    std::vector<iovec> register_buf{task_concurrency};
    for (size_t idx = 0; idx < task_pool.size(); ++idx)
    {
        assert(task_pool[idx]->TaskIdx() == idx);
        auto [buf_ptr, buf_len] = task_pool[idx]->Buffer();
        register_buf[idx].iov_base = buf_ptr;
        register_buf[idx].iov_len = buf_len;
    }

    int ret = io_uring_register_buffers(
        uring, register_buf.data(), register_buf.size());

    std::random_device dev{"/dev/urandom"};
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> page_gen(
        0, page_cnt - 1);

    uint32_t new_task = 0;

    while (task_finished < task_total)
    {
        if (ring_ret < 0)
        {
            ring_ret = io_uring_peek_cqe(uring, &cqe_ptr);
        }

        if (ring_ret == 0)
        {
            int processed{0};
            unsigned head;  // head of the ring buffer, unused
            io_uring_for_each_cqe(uring, head, cqe_ptr)
            {
                kvstore::IoReadTask *task = static_cast<kvstore::IoReadTask *>(
                    io_uring_cqe_get_data(cqe_ptr));
                int ret = cqe_ptr->res;
                task->Finish();
                ++processed;
            }
            io_uring_cq_advance(uring, processed);
            ring_ret = -1;
        }
        else if (!task_pool.empty())
        {
            while (!task_pool.empty())
            {
                uint32_t page_id = page_gen(rng);
                uint32_t page_offset = page_id << 12;
                new_task++;

                std::unique_ptr<kvstore::IoReadTask> task =
                    std::move(task_pool.back());
                task_pool.pop_back();

                kvstore::IoReadTask *rt = task.release();
                rt->Reset(fixed_file_idx, page_offset, read_ring);
                rt->Run();
            }
            io_uring_submit(uring);
            ring_ret = -1;
        }
        else
        {
            // All tasks are ongoing. Waits for any of them to finish.
            ring_ret = io_uring_wait_cqe_nr(uring, &cqe_ptr, 1);
            assert(!ring_ret);
        }
    }

    // CloseFixedFile(fixed_file_idx, uring);
}

int main()
{
    std::unique_ptr<kvstore::FileReader> freader = nullptr;

    try
    {
        freader = std::make_unique<kvstore::FileReader>(file_name);
    }
    catch (const std::runtime_error &e)
    {
        InitDataFile(file_name);
        try
        {
            freader = std::make_unique<kvstore::FileReader>(file_name);
        }
        catch (const std::runtime_error &e)
        {
            std::cout << "Failed to init data file." << std::endl;
            return 1;
        }
    }

    std::cout << "Open data file successfully, file size: " << freader->Size()
              << std::endl;

    // freader = nullptr;

    std::vector<std::thread> thd_pool;

    auto start_tp = std::chrono::high_resolution_clock::now();

    for (size_t idx = 0; idx < thread_cnt; ++idx)
    {
        thd_pool.emplace_back([&]() { ReadWorker(file_name, freader->Fd()); });
    }

    for (size_t idx = 0; idx < thread_cnt; ++idx)
    {
        thd_pool[idx].join();
    }

    auto elapse = std::chrono::high_resolution_clock::now() - start_tp;
    size_t micro_sec =
        std::chrono::duration_cast<std::chrono::milliseconds>(elapse).count();

    std::cout << "Task count: " << task_total * thread_cnt
              << ", time (ms): " << micro_sec << std::endl;
}