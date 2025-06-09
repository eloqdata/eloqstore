#pragma once

#include <liburing.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>

#include "io_uring.h"
#include "task.h"

namespace kvstore
{
class IoReadTask
{
public:
    IoReadTask(size_t pool_idx,
               size_t page_size,
               size_t &task_cnt,
               std::vector<std::unique_ptr<IoReadTask>> &task_pool)
        : pool_idx_(pool_idx), task_cnt_ref_(task_cnt), task_pool_(task_pool)
    {
        void *chunk = std::aligned_alloc(4096, page_size);
        page_buf_ = std::unique_ptr<char[]>((char *) chunk);
        page_len_ = page_size;
    }

    IoReadTask(const IoReadTask &) = delete;

    void Reset(int fd, uint32_t offset, IoUring &uring)
    {
        fd_ = fd;
        offset_ = offset;

        // Resets the last 4 bytes to 0. We will check the last 4 bytes when
        // reading the page from the disk.
        uint32_t zero = 0;
        memcpy(page_buf_.get() + (page_len_ - sizeof(uint32_t)),
               &zero,
               sizeof(uint32_t));

        status_ = TaskStatus::Idle;
        ring_ = uring.Get();
        sqe_ = io_uring_get_sqe(ring_);
    }

    void Run()
    {
        status_ = TaskStatus::Ongoing;

        // io_uring_prep_read(sqe_, fd_, page_buf_.get(), page_len_, offset_);
        io_uring_prep_read_fixed(
            sqe_, fd_, page_buf_.get(), page_len_, offset_, pool_idx_);
        sqe_->flags |= IOSQE_FIXED_FILE;
        // We are transferring the ownership of the task to io_uring. Whoever
        // calling this function should release the ownership of the task before
        // calling.
        io_uring_sqe_set_data(sqe_, this);
    }

    void Finish()
    {
        status_ = TaskStatus::Idle;
        uint32_t *int_ptr =
            (uint32_t *) (page_buf_.get() + (page_len_ - sizeof(uint32_t)));
        if (*int_ptr != (offset_ >> 12))
        {
            throw std::runtime_error("Checking failed.");
        }

        task_cnt_ref_++;
        task_pool_.emplace_back(std::unique_ptr<IoReadTask>(this));
    }

    size_t TaskIdx() const
    {
        return pool_idx_;
    }

    std::pair<char *, size_t> Buffer()
    {
        return {page_buf_.get(), page_len_};
    }

private:
    size_t const pool_idx_;
    int fd_;
    uint32_t offset_;
    std::unique_ptr<char[]> page_buf_;
    size_t page_len_;
    TaskStatus status_;

    io_uring_sqe *sqe_{nullptr};
    io_uring *ring_{nullptr};

    size_t &task_cnt_ref_;
    std::vector<std::unique_ptr<IoReadTask>> &task_pool_;
};

}  // namespace kvstore