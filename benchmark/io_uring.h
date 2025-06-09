#pragma once

#include <liburing.h>

#include <stdexcept>

namespace kvstore
{

class IoUring
{
public:
    explicit IoUring(size_t queue_size)
    {
        int ret = io_uring_queue_init(
            queue_size,
            &ring_,
            IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);

        if (ret)
        {
            throw std::runtime_error("io_uring_queue_init failed: " +
                                     std::to_string(ret));
        }

        ret = io_uring_register_ring_fd(&ring_);
        if (ret < 0)
        {
            throw std::runtime_error("io_uring_register_ring_fd failed");
        }

        ret = io_uring_register_files_sparse(&ring_, 1024);
        if (ret < 0)
        {
            throw std::runtime_error("io_uring_register_files_sparse failed");
        }
    }

    IoUring(const IoUring &) = delete;
    IoUring &operator=(const IoUring &) = delete;
    IoUring(IoUring &&) = delete;
    IoUring &operator=(IoUring &&) = delete;

    ~IoUring()
    {
        io_uring_queue_exit(&ring_);
    }

    io_uring *Get()
    {
        return &ring_;
    }

private:
    io_uring ring_;
};

}  // namespace kvstore