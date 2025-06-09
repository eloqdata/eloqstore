#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>
#include <vector>

const char *file_path = "/home/ljchen/kvstore/test_data.dat";
iovec iovecs[32];
std::vector<iovec> iovec_vec;

int get_fixed_file_by_open_direct(struct io_uring *ring)
{
    // openat direct
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    // io_uring_prep_openat_direct(sqe, -1, file_path, 0, O_RDONLY, 0);
    io_uring_prep_openat_direct(
        sqe, -1, file_path, 0, O_RDONLY, IORING_FILE_INDEX_ALLOC);
    int ret = io_uring_submit(ring);
    if (ret == -1)
    {
        printf("error");
        return 0;
    }

    // wait cqe
    struct io_uring_cqe *cqe;
    io_uring_wait_cqe(ring, &cqe);

    printf("openat_direct fixed file is %d\n", cqe->res);
    int fixed_file_idx = cqe->res;

    io_uring_cq_advance(ring, 1);
    return fixed_file_idx;
}

void read_file(struct io_uring *ring, int fd, __u8 flags)
{
    // submit with IOSQE_FIXED_FILE
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    char buf[100];

    io_uring_prep_read_fixed(sqe, fd, iovec_vec[1].iov_base, 4096, 0, 1);
    // io_uring_prep_read(sqe, fd, buf, 100, 0);
    sqe->flags = flags;

    int ret = io_uring_submit(ring);
    if (ret == -1)
    {
        printf("error");
        return;
    }

    // wait cqe
    struct io_uring_cqe *cqe;
    ret = io_uring_wait_cqe(ring, &cqe);
    if (ret == -1)
    {
        printf("error");
        return;
    }
    printf("output: %d\n", cqe->res);
    io_uring_cq_advance(ring, 1);
}

int main()
{
    // setup pipe
    int pipefd[2];
    int ret = pipe(pipefd);
    if (ret == -1)
    {
        printf("error");
        return 0;
    }

    // setup io_uring
    struct io_uring ring;
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SQPOLL;
    if (io_uring_queue_init_params(128, &ring, &params) < 0)
    {
        printf("error");
        return 0;
    }

    // register file sparse
    ret = io_uring_register_files_sparse(&ring, 1);
    if (ret == -1)
    {
        printf("error");
        return 0;
    }

    // int fixed_file_idx = get_fixed_file_by_open_direct(&ring);

    // cqe->res=-EBADF
    // read_file(&ring, fixed_file_idx, IOSQE_FIXED_FILE);

    // cqe->res>=0
    iovec_vec.resize(32);
    for (size_t idx = 0; idx < 32; ++idx)
    {
        // iovecs[idx].iov_base = std::aligned_alloc(4096, 4096);
        // iovecs[idx].iov_len = 4096;
        iovec_vec[idx].iov_base = std::aligned_alloc(4096, 4096);
        iovec_vec[idx].iov_len = 4096;
    }

    ret = io_uring_register_buffers(&ring, &iovec_vec[0], 32);
    if (ret)
    {
        fprintf(stderr, "Failed to register buffers\n");
        return -1;
    }

    int fd = open(file_path, O_RDONLY);
    read_file(&ring, fd, 0);

    for (size_t idx = 0; idx < 32; idx++)
    {
        free(iovecs[idx].iov_base);
    }

    return 0;
}