#include <liburing.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define QUEUE_DEPTH 1
#define BUFFER_SIZE 4096

int main() {
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    int fds[1];
    int ret;
    void *buf;
    
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 2000;    
    // Initialize io_uring
    ret = io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params);
    if (ret < 0) {
        perror("io_uring_queue_init");
        return 1;
    }

    // Open the file to be registered
    fds[0] = open("testfile.txt", O_RDONLY);
    if (fds[0] < 0) {
        perror("open");
        return 1;
    }

    // Register the file with io_uring
    ret = io_uring_register_files(&ring, fds, 1);
    if (ret < 0) {
        perror("io_uring_register_files");
        return 1;
    }

    // Allocate and register a buffer
    buf = mmap(NULL, BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (buf == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    struct iovec iov = {
        .iov_base = buf,
        .iov_len = BUFFER_SIZE,
    };

    ret = io_uring_register_buffers(&ring, &iov, 1);
    if (ret < 0) {
        perror("io_uring_register_buffers");
        return 1;
    }

    // Prepare read operation
    sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read_fixed(sqe, 0, buf, BUFFER_SIZE, 0, 0); // 0 is the index of the registered file and buffer
    sqe->flags |= IOSQE_FIXED_FILE;

    // Submit the read request
    io_uring_submit(&ring);

    // Poll for the completion
    while (1) {
        ret = io_uring_peek_cqe(&ring, &cqe);
        if (ret == -EAGAIN) {
            // No completion yet, continue polling
            continue;
        } else if (ret < 0) {
            perror("io_uring_peek_cqe");
            return 1;
        } else {
            break; // Completion received
        }
    }

    // Check for errors
    if (cqe->res < 0) {
        fprintf(stderr, "Error: %s\n", strerror(-cqe->res));
    } else {
        printf("Read %d bytes: %.*s\n", cqe->res, cqe->res, (char *)buf);
    }

    // Mark completion as seen
    io_uring_cqe_seen(&ring, cqe);

    // Clean up
    munmap(buf, BUFFER_SIZE);
    io_uring_unregister_buffers(&ring);
    io_uring_unregister_files(&ring);
    close(fds[0]);
    io_uring_queue_exit(&ring);

    return 0;
}