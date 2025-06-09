#include <liburing.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <iostream>
#include <future>
#include <vector>
#include <cassert>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <deque>
#include <algorithm>
#include <numeric>
#include <optional>

constexpr size_t PAGE_SIZE = 4096;

using std::vector;
using std::pair;

enum IOResult
{
    SUCCESS,
    FAIL
};

//the expected API for a file access class, read, write, read_many, write_many, and fsync
class AsyncFileBase
{
public:
    AsyncFileBase(const std::string& filename) : filename_(filename)
    {
        fd_ = open(filename_.c_str(), O_RDWR | O_CREAT | O_DIRECT , 0644);
        assert(fd_ >= 0 && "Failed to open file");
        std::cerr << " Opened file " << filename_ << " with fd " << fd_ << "\n"; 
    }

    ~AsyncFileBase()
    {
        close(fd_);
    }

    virtual void fsync_file(bool data_only = false) = 0 ;
    virtual std::future<IOResult> read_page(size_t pg_idx, char * buff) = 0;
    virtual std::future<IOResult> write_page(size_t pg_idx, char * buff) = 0;
    virtual std::future<IOResult> read_many(vector<pair<size_t, char *>> & pages) = 0;
    virtual std::future<IOResult> write_many(vector<pair<size_t, char *>> & pages) = 0;

    IOResult read_page_sync(size_t pg_idx, char * buff)
    {
        std::future<IOResult> future = read_page(pg_idx, buff);
        return future.get();
    }

    IOResult write_page_sync(size_t pg_idx, char * buff)
    {
        std::future<IOResult> future = write_page(pg_idx, buff);
        return future.get();
    }

    IOResult read_many_sync(vector<pair<size_t, char *>> & pages)
    {
        std::future<IOResult> future = read_many(pages);
        return future.get();
    }

    IOResult write_many_sync(vector<pair<size_t, char *>> & pages)
    {
        std::future<IOResult> future = write_many(pages);
        return future.get();
    }

    IOResult read_page_posix(size_t pg_idx, char * buff)
    {
        size_t read_bytes = pread(fd_, buff, PAGE_SIZE, pg_idx * PAGE_SIZE);
        assert(read_bytes == PAGE_SIZE && "Failed to read full page");
        return SUCCESS;
    }

    IOResult write_page_posix(size_t pg_idx, char * buff)
    {
        size_t written_bytes = pwrite(fd_, buff, PAGE_SIZE, pg_idx * PAGE_SIZE);
        assert(written_bytes == PAGE_SIZE && "Failed to write full page");
        return SUCCESS;
    }

    IOResult read_many_posix(vector<pair<size_t, char *>> & pages)
    {
        for (auto & page : pages)
        {
            size_t read_bytes = pread(fd_, page.second, PAGE_SIZE, page.first * PAGE_SIZE);
            assert(read_bytes == PAGE_SIZE && "Failed to read full page");
        }
        return SUCCESS;
    }

    IOResult write_many_posix(vector<pair<size_t, char *>> & pages)
    {
        for (auto & page : pages)
        {
            size_t written_bytes = pwrite(fd_, page.second, PAGE_SIZE, page.first * PAGE_SIZE);
            assert(written_bytes == PAGE_SIZE && "Failed to write full page");
        }
        return SUCCESS;
    }

protected:
    int fd_;
    std::string filename_;
};

//not really aysnc, just a wrapper around posix read and write
//always return a future object that is fulfilled
class AsyncFilePosix : public AsyncFileBase
{
public:
    AsyncFilePosix(const std::string& filename) : AsyncFileBase(filename)
    {
    }

    void fsync_file(bool data_only) override
    {
        if (data_only)
            fdatasync(fd_);
        else
            fsync(fd_);
    }

    //using posix read and write, return a future object that is always fulfilled
    std::future<IOResult> read_page(size_t pg_idx, char * buff) override
    {
        std::promise<IOResult> promise;
        std::future<IOResult> future = promise.get_future();
        if (read_page_posix(pg_idx, buff) == SUCCESS)
            promise.set_value(SUCCESS);
        else
            promise.set_value(FAIL);

        return future;
    }

    std::future<IOResult> write_page(size_t pg_idx, char * buff) override
    {
        std::promise<IOResult> promise;
        std::future<IOResult> future = promise.get_future();
        if (write_page_posix(pg_idx, buff) == SUCCESS)
            promise.set_value(SUCCESS);
        else
            promise.set_value(FAIL);
        return future;
    }

    std::future<IOResult> read_many(vector<pair<size_t, char *>> & pages) override
    {
        std::promise<IOResult> promise;
        std::future<IOResult> future = promise.get_future();
        if (read_many_posix(pages) == SUCCESS)
            promise.set_value(SUCCESS);
        else
            promise.set_value(FAIL);
        return future;
    }

    std::future<IOResult> write_many(vector<pair<size_t, char *>> & pages) override
    {
        std::promise<IOResult> promise;
        std::future<IOResult> future = promise.get_future();
        if (write_many_posix(pages) == SUCCESS)
            promise.set_value(SUCCESS);
        else
            promise.set_value(FAIL);
        return future;
    }
};

//using std::async to wrap the sychronous posix read and write, but return a future object that is fulfilled asynchronously 
//std::async starts up a thread everytime it is called, so it is not fast
class AsyncFileAsync : public AsyncFileBase
{
public:
    AsyncFileAsync(const std::string& filename) : AsyncFileBase(filename), outstanding_writes_(0), allow_writes_(true)
    {
    }

    void fsync_file(bool data_only) override
    {
        bool expected = true; 
        while (!allow_writes_.compare_exchange_weak(expected, false)) 
            std::this_thread::yield(); //already in fsync, wait for it to finish

        assert(allow_writes_ == false);
        while(outstanding_writes_ > 0)
            std::this_thread::yield();

        if (data_only)
            fdatasync(fd_);
        else
            fsync(fd_);
        allow_writes_ = true;
    }

    std::future<IOResult> read_page(size_t pg_idx, char * buff) override
    {
        return std::async(std::launch::async, [this, pg_idx, buff] {
            return read_page_posix(pg_idx, buff);
        });
    }

    std::future<IOResult> write_page(size_t pg_idx, char * buff) override
    {
        while(!allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();

        outstanding_writes_ ++;
        return std::async(std::launch::async, [this, pg_idx, buff] {
            return write_page_posix(pg_idx, buff);
        });
    }

    std::future<IOResult> read_many(vector<pair<size_t, char *>> & pages) override
    {
        return std::async(std::launch::async, [this, &pages] {
            return read_many_posix(pages);
        });
    }

    std::future<IOResult> write_many(vector<pair<size_t, char *>> & pages) override
    {
        while(!allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();

        outstanding_writes_ ++;
        return std::async(std::launch::async, [this, &pages] {
            return write_many_posix(pages);
        });
    }

private:
    std::atomic<size_t> outstanding_writes_; 
    std::atomic<bool> allow_writes_; 
};


//a slot to hold the IO operation, and a slot manager to manage the slots
struct IOSlot
{
    static const size_t INVALID_PAGE_IDX = -1;
    enum IOOp {NO_OP, READ_PAGE, WRITE_PAGE, READ_MANY, WRITE_MANY, FSYNC, FSYNC_DATA};
    IOOp op;
    size_t page_idx;
    char * buff;
    std::vector<pair<size_t, char *>> pages;
    std::atomic<size_t> num_finished; 
    std::promise<IOResult> promise;
};

class IOSlotManager
{
public:
    IOSlotManager(size_t slot_count = 128) : slots_(slot_count), free_slots_(slot_count)
    {
        for (size_t i = 0; i < slot_count; i++)
        {
            free_slots_[i] = &slots_[i];
        }
    }

    //this is for fsync 
    IOSlot * get_slot()
    {
        IOSlot * slot = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (free_slots_.empty()) {
                assert (false && " No More Slots? ");
                return nullptr;
            }
            slot = free_slots_.back();
            free_slots_.pop_back();
        }
        //re-initiate
        slot->op = IOSlot::NO_OP;
        slot->page_idx = IOSlot::INVALID_PAGE_IDX; 
        slot->buff = nullptr;
        slot->pages.clear();
        slot->promise = {}; 
        slot->num_finished = 0;
        return slot;
    }
    //this is for read/write
    IOSlot * get_slot(IOSlot::IOOp op, size_t page_idx, char * buff) {
        assert (op == IOSlot::READ_PAGE || op == IOSlot::WRITE_PAGE);
        IOSlot * slot = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (free_slots_.empty()) {
                assert (false && " No More Slots? ");
                return nullptr;
            }
            slot = free_slots_.back();
            free_slots_.pop_back();
        }
        //re-initiate
        slot->op = op;
        slot->page_idx = page_idx;
        slot->buff = buff;
        slot->pages.clear();
        slot->num_finished = 0;
        slot->promise = {}; 
        return slot;
    }
    //this is for read many /write many
    IOSlot * get_slot(IOSlot::IOOp op, vector<pair<size_t, char*>> & pages) {
        assert (op == IOSlot::READ_MANY || op == IOSlot::WRITE_MANY);
        IOSlot * slot = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (free_slots_.empty()) {
                assert (false && " No More Slots? ");
                return nullptr;
            }
            slot = free_slots_.back();
            free_slots_.pop_back();
        }
        //re-initiate
        slot->op = op;
        slot->page_idx = IOSlot::INVALID_PAGE_IDX; 
        slot->buff = nullptr;
        slot->pages = pages;
        slot->num_finished = 0;
        slot->promise = {}; 
        return slot;
    }

    void release_slot(IOSlot * slot)
    { 
        std::lock_guard<std::mutex> lock(mutex_);
        if (slot->pages.capacity() > 1024) { //capacity is not thread safe, I think
            vector<pair<size_t, char *>> empty;
            slot->pages.swap(empty);        //so that we release the extra memory
        }
        free_slots_.push_back(slot);
    }

private:
    std::vector<IOSlot> slots_; 
    std::vector<IOSlot *> free_slots_; 
    std::mutex mutex_;
};


//using a fixed number of threads in a pool to handle IO operations. Ops are inserted into a queue first
//and the threads will pick up the ops from the queue
class AsyncFileThreadPool : public AsyncFileBase
{
public:
    AsyncFileThreadPool(const std::string& filename, size_t num_threads = std::thread::hardware_concurrency()) : AsyncFileBase(filename), slot_manager_(128)
    {
        allow_writes_ = true;
        outstanding_writes_ = 0;
        stop_ = false;

        for (size_t i = 0; i < num_threads; i++)
        {
            threads_.emplace_back([this] {
                thread_fun();
            });
        }
    }
    ~AsyncFileThreadPool()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        condition_.notify_all();
        for (auto & thread : threads_)
            thread.join();
    }
    //have to wait till all outstanding writes are processed before fsync to maintain semantic
    void fsync_file(bool data_only) override
    {
        bool expected = true; 
        while (!allow_writes_.compare_exchange_weak(expected, false)) 
            std::this_thread::yield(); //already in fsync, wait for the previous one to complete

        while(outstanding_writes_ > 0)
            std::this_thread::yield();
        if (data_only)
            fdatasync(fd_);
        else
            fsync(fd_);
        allow_writes_ = true;
    }

    std::future<IOResult> read_page(size_t pg_idx, char * buff) override
    {
        IOSlot * slot = slot_manager_.get_slot(IOSlot::READ_PAGE, pg_idx, buff);
        std::future<IOResult> future = slot->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(slot);
        }
        condition_.notify_one();
        return future;
    }

    std::future<IOResult> write_page(size_t pg_idx, char * buff) override
    {
        while(! allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();
        outstanding_writes_ ++;
        IOSlot * slot = slot_manager_.get_slot(IOSlot::WRITE_PAGE, pg_idx, buff);
        std::future<IOResult> future = slot->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(slot);
        }
        condition_.notify_one();
        return future;
    }

    std::future<IOResult> read_many(vector<pair<size_t, char *>> & pages) override
    {
        IOSlot * slot = slot_manager_.get_slot(IOSlot::READ_MANY, pages);
        std::future<IOResult> future = slot->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(slot);
        }
        condition_.notify_one();
        return future;
    }

    std::future<IOResult> write_many(vector<pair<size_t, char *>> & pages) override
    {
        while(! allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();
        outstanding_writes_ ++;
        IOSlot * slot = slot_manager_.get_slot(IOSlot::WRITE_MANY, pages);
        std::future<IOResult> future = slot->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(slot);
        }
        condition_.notify_one();
        return future;
    }

private:
    void thread_fun(void)
    {
        while (!stop_)
        {
            IOSlot * item = nullptr;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                condition_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty())
                    return;
                item = tasks_.front();
                tasks_.pop();
            }
            switch(item->op)
            {
                case IOSlot::READ_PAGE:
                {
                    IOResult result = read_page_posix(item->page_idx, item->buff);
                    if(result == SUCCESS)
                        item->promise.set_value(SUCCESS);
                    else
                        item->promise.set_value(FAIL);
                    break;
                }
                case IOSlot::WRITE_PAGE:
                {
                    IOResult result = write_page_posix(item->page_idx, item->buff);
                    if(result == SUCCESS)
                        item->promise.set_value(SUCCESS);
                    else
                        item->promise.set_value(FAIL);
                    outstanding_writes_ --;
                    break;
                }
                case IOSlot::READ_MANY:
                {
                    IOResult result = read_many_posix(item->pages);
                    if(result == SUCCESS)
                        item->promise.set_value(SUCCESS);
                    else
                        item->promise.set_value(FAIL);
                    break;
                }
                case IOSlot::WRITE_MANY:
                {
                    IOResult result = write_many_posix(item->pages);
                    if(result == SUCCESS)
                        item->promise.set_value(SUCCESS);
                    else
                        item->promise.set_value(FAIL);
                    outstanding_writes_ --;
                    break;
                }
                case IOSlot::FSYNC:
                case IOSlot::FSYNC_DATA:
                default:
                    assert(false && "Unknown IO Operation");
            }
            slot_manager_.release_slot(item);
        }
    }

private:
    std::vector<std::thread> threads_;
    std::queue<IOSlot *> tasks_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool stop_;
    std::atomic<bool> allow_writes_; 
    std::atomic<size_t> outstanding_writes_;
    IOSlotManager slot_manager_;
};

//using io_uring to perform async read and write, 
//using an event loop to poll for completion
class AsyncFileIOUring : public AsyncFileBase
{
public:
    AsyncFileIOUring(const std::string& filename, size_t queue_size = 32) : AsyncFileBase(filename), io_slot_manager_(128)
    {
        stop_polling_ = false;
        outstanding_writes_ = 0;
        allow_writes_ = true;
        pending_ops_ = 0;

        int r = io_uring_queue_init(queue_size, &ring_, 0);
        if (r != 0) {
            std::cerr << "Failed to initialize io_uring: " << strerror(-r) << std::endl;
            exit(1);
        }

        poll_thread_ = std::thread(&AsyncFileIOUring::poll_for_completion, this);
    }

    ~AsyncFileIOUring()
    {
        stop_polling_ = true;
        poll_thread_.join();
        io_uring_queue_exit(&ring_);
    }

    void fsync_file(bool data_only) override
    {
        bool expected = true; 
        while (!allow_writes_.compare_exchange_weak(expected, false)) //already in fsync, return;
            std::this_thread::yield();

        while(outstanding_writes_ > 0)
            std::this_thread::yield();

        //will this work? 
        // if (data_only)
        //     fdatasync(fd_);
        // else
        //     fsync(fd_);

        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::NO_OP, 0, nullptr);
        
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if (data_only) {
            io_uring_prep_fsync(sqe, fd_, IORING_FSYNC_DATASYNC);
            io_slot->op = IOSlot::FSYNC_DATA;
        }
        else {
            io_uring_prep_fsync(sqe, fd_, 0);
            io_slot->op = IOSlot::FSYNC;
        }
        //make sure pending op is increased when submitting fsync op
        submit_io_op(sqe, io_slot);
        io_slot->promise.get_future().get();
        allow_writes_ = true;
        io_slot_manager_.release_slot(io_slot);
    }

    std::future<IOResult> read_page(size_t pg_idx, char * buff) override
    {
        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::READ_PAGE, pg_idx, buff);
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_read(sqe, fd_, buff, PAGE_SIZE, pg_idx * PAGE_SIZE);
        submit_io_op(sqe, io_slot);
        return io_slot->promise.get_future();
    }

    std::future<IOResult> write_page(size_t pg_idx, char * buff) override
    {
        while(!allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();

        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::WRITE_PAGE, pg_idx, buff);
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_write(sqe, fd_, buff, PAGE_SIZE, pg_idx * PAGE_SIZE);
        outstanding_writes_ ++;
        submit_io_op(sqe, io_slot);
        return io_slot->promise.get_future();
    }

    std::future<IOResult> read_many(vector<pair<size_t, char *>> & pages) override
    {
        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::READ_MANY, pages);
        bool submitted = false;
        for (auto & page : pages)
        {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            io_uring_prep_read(sqe, fd_, page.second, PAGE_SIZE, page.first * PAGE_SIZE);
            io_uring_sqe_set_data(sqe, io_slot);
            pending_ops_ ++;
            if (pending_ops_ >= 32) { //too many pending ops
                io_uring_submit(&ring_);
                submitted = true;
                while(pending_ops_ >= 32) //wait for the batch to complete
                    std::this_thread::yield();
            }
            else
                submitted = false;
        }
        if (submitted == false)
            io_uring_submit(&ring_);
        return io_slot->promise.get_future();
    }

    std::future<IOResult> write_many(vector<pair<size_t, char *>> & pages) override
    {
        while(!allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();

        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::WRITE_MANY, pages);
        bool submitted = false;
        for (auto & page : pages)
        {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            io_uring_prep_write(sqe, fd_, page.second, PAGE_SIZE, page.first * PAGE_SIZE);
            io_uring_sqe_set_data(sqe, io_slot);
            pending_ops_ ++;
            if (pending_ops_ >= 32) { //submit in batches of 32
                io_uring_submit(&ring_);
                submitted = true;
                while(pending_ops_ >= 32) //wait for the batch to complete
                    std::this_thread::yield();
            }
            else
                submitted = false;
        }
        outstanding_writes_ ++;
        if (submitted == false)
            io_uring_submit(&ring_);
        return io_slot->promise.get_future();
    }

private:
    void submit_io_op(io_uring_sqe * sqe, IOSlot * io_slot) 
    {
        pending_ops_ ++;
        io_uring_sqe_set_data(sqe, io_slot);
        io_uring_submit(&ring_);
    }

    void poll_for_completion() {
        std::chrono::time_point<std::chrono::high_resolution_clock> idle_start = std::chrono::high_resolution_clock::now();
        while (!stop_polling_) {
            struct io_uring_cqe *cqe;
            int ret = io_uring_peek_cqe(&ring_, &cqe);
            if (ret == -EAGAIN) {
                std::this_thread::yield();
                if (std::chrono::high_resolution_clock::now() - idle_start > std::chrono::microseconds(1000 * 10)) {
                    std::this_thread::sleep_for(std::chrono::microseconds(1000 * 20));
                    idle_start = std::chrono::high_resolution_clock::now();
                }
                continue;
            }
            else if (ret < 0) {
                std::cerr << "Error waiting for completion: " << strerror(-ret) << std::endl;
                continue;
            }

            void * user_data = io_uring_cqe_get_data(cqe);
            assert (user_data != nullptr && " ??? ");
            IOSlot * io_slot = reinterpret_cast<IOSlot *>(user_data);
            switch(io_slot->op)
            {
                case IOSlot::READ_PAGE:
                    io_slot->promise.set_value(SUCCESS);
                    break;
                case IOSlot::WRITE_PAGE:
                    outstanding_writes_ --; 
                    io_slot->promise.set_value(SUCCESS);
                    break;
                case IOSlot::READ_MANY:
                    if (++io_slot->num_finished == io_slot->pages.size())
                        io_slot->promise.set_value(SUCCESS);
                    break;
                case IOSlot::WRITE_MANY:
                    if (++io_slot->num_finished == io_slot->pages.size()) {
                        outstanding_writes_ --;
                        io_slot->promise.set_value(SUCCESS);
                    }
                    break;
                case IOSlot::FSYNC:
                case IOSlot::FSYNC_DATA:
                    io_slot->promise.set_value(SUCCESS);
                    break;
                default:
                    assert(false && "Unknown IO Operation");
            }
            io_uring_cqe_seen(&ring_, cqe);
            pending_ops_ --;
            io_slot_manager_.release_slot(io_slot);
            idle_start = std::chrono::high_resolution_clock::now();
        }
    }

    struct io_uring ring_;
    std::thread poll_thread_;
    bool stop_polling_;
    IOSlotManager io_slot_manager_;
    std::atomic<size_t> outstanding_writes_; 
    std::atomic<bool> allow_writes_; 
    std::atomic<size_t> pending_ops_;
};


//using io_uring, optiized with polling thread, registered file and buffers
class AsyncFileIOUringOptimized : public AsyncFileBase
{
public:
    AsyncFileIOUringOptimized(const std::string& filename, size_t queue_size = 32)  : AsyncFileBase(filename), io_slot_manager_(128)
    {
        stop_polling_ = false;
        outstanding_writes_ = 0;
        allow_writes_ = true;
        pending_ops_ = 0;

        int r = io_uring_queue_init(queue_size, &ring_, IORING_SETUP_SQPOLL);
        if (r != 0) {
            std::cerr << "Failed to initialize io_uring: " << strerror(-r) << std::endl;
            exit(1);
        }
        r = io_uring_register_files(&ring_, &fd_, 1);
        if (r != 0) {
            std::cerr << "Failed to register file to io_uring: " << strerror(-r) << std::endl;
            exit(1);
        }
        poll_thread_ = std::thread(&AsyncFileIOUringOptimized::poll_for_completion, this);
    }

    ~AsyncFileIOUringOptimized()
    {
        stop_polling_ = true;
        poll_thread_.join();
        io_uring_queue_exit(&ring_);
    }

    void register_uring_buffer(char * buff, size_t n_pages)
    {
        //create a io_vec, fill in the buff and its size
        assert(((size_t)buff & (PAGE_SIZE - 1)) == 0); //buff must be aligned
        struct iovec iov;
        iov.iov_base = buff;
        iov.iov_len = n_pages * PAGE_SIZE;
        io_uring_register_buffers(&ring_, &iov, 1);
    }

    void fsync_file(bool data_only) override
    {
        bool expected = true; 
        while (!allow_writes_.compare_exchange_weak(expected, false)) //already in fsync, return;
            std::this_thread::yield();

        while(outstanding_writes_ > 0)
            std::this_thread::yield();

        //will this work? 
        // if (data_only)
        //     fdatasync(fd_);
        // else
        //     fsync(fd_);

        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::NO_OP, 0, nullptr);
        
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if (data_only) {
            io_uring_prep_fsync(sqe, fd_, IORING_FSYNC_DATASYNC);
            io_slot->op = IOSlot::FSYNC_DATA;
        }
        else {
            io_uring_prep_fsync(sqe, fd_, 0);
            io_slot->op = IOSlot::FSYNC;
        }
        //make sure pending op is increased when submitting fsync op
        submit_io_op(sqe, io_slot);
        io_slot->promise.get_future().get();
        allow_writes_ = true;
        io_slot_manager_.release_slot(io_slot);
    }

    std::future<IOResult> read_page(size_t pg_idx, char * buff) override
    {
        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::READ_PAGE, pg_idx, buff);
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_read_fixed(sqe, fd_, buff, PAGE_SIZE, pg_idx * PAGE_SIZE, 0);
        submit_io_op(sqe, io_slot);
        return io_slot->promise.get_future();
    }

    std::future<IOResult> write_page(size_t pg_idx, char * buff) override
    {
        while(!allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();

        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::WRITE_PAGE, pg_idx, buff);
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_write_fixed(sqe, fd_, buff, PAGE_SIZE, pg_idx * PAGE_SIZE, 0);
        outstanding_writes_ ++;
        submit_io_op(sqe, io_slot);
        return io_slot->promise.get_future();
    }

    std::future<IOResult> read_many(vector<pair<size_t, char *>> & pages) override
    {
        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::READ_MANY, pages);
        bool submitted = false;
        for (auto & page : pages)
        {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            io_uring_prep_read_fixed(sqe, fd_, page.second, PAGE_SIZE, page.first * PAGE_SIZE, 0);
            io_uring_sqe_set_data(sqe, io_slot);
            pending_ops_ ++;
            if (pending_ops_ >= 32) { //too many pending ops
                io_uring_submit(&ring_);
                submitted = true;
                while(pending_ops_ >= 32) //wait for the batch to complete
                    std::this_thread::yield();
            }
            else
                submitted = false;
        }
        if (submitted == false)
            io_uring_submit(&ring_);
        return io_slot->promise.get_future();
    }

    std::future<IOResult> write_many(vector<pair<size_t, char *>> & pages) override
    {
        while(!allow_writes_) //fsync is in progress, wait for it to finish
            std::this_thread::yield();

        IOSlot * io_slot = io_slot_manager_.get_slot(IOSlot::WRITE_MANY, pages);
        bool submitted = false;
        for (auto & page : pages)
        {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            io_uring_prep_write_fixed(sqe, fd_, page.second, PAGE_SIZE, page.first * PAGE_SIZE, 0);
            io_uring_sqe_set_data(sqe, io_slot);
            pending_ops_ ++;
            if (pending_ops_ >= 32) { //submit in batches of 32
                io_uring_submit(&ring_);
                submitted = true;
                while(pending_ops_ >= 32) //wait for the batch to complete
                    std::this_thread::yield();
            }
            else
                submitted = false;
        }
        outstanding_writes_ ++;
        if (submitted == false)
            io_uring_submit(&ring_);
        return io_slot->promise.get_future();
    }

private:
    void submit_io_op(io_uring_sqe * sqe, IOSlot * io_slot) 
    {
        pending_ops_ ++;
        io_uring_sqe_set_data(sqe, io_slot);
        io_uring_submit(&ring_);
    }

    void poll_for_completion() {
        std::chrono::time_point<std::chrono::high_resolution_clock> idle_start = std::chrono::high_resolution_clock::now();
        while (!stop_polling_) {
            struct io_uring_cqe *cqe;
            int ret = io_uring_peek_cqe(&ring_, &cqe);
            if (ret == -EAGAIN) {
                std::this_thread::yield();
                if (std::chrono::high_resolution_clock::now() - idle_start > std::chrono::microseconds(1000 * 10)) {
                    std::this_thread::sleep_for(std::chrono::microseconds(1000 * 20));
                    idle_start = std::chrono::high_resolution_clock::now();
                }
                continue;
            }
            else if (ret < 0) {
                std::cerr << "Error waiting for completion: " << strerror(-ret) << std::endl;
                continue;
            }

            void * user_data = io_uring_cqe_get_data(cqe);
            assert (user_data != nullptr && " ??? ");
            IOSlot * io_slot = reinterpret_cast<IOSlot *>(user_data);
            switch(io_slot->op)
            {
                case IOSlot::READ_PAGE:
                    io_slot->promise.set_value(SUCCESS);
                    break;
                case IOSlot::WRITE_PAGE:
                    outstanding_writes_ --; 
                    io_slot->promise.set_value(SUCCESS);
                    break;
                case IOSlot::READ_MANY:
                    if (++io_slot->num_finished == io_slot->pages.size())
                        io_slot->promise.set_value(SUCCESS);
                    break;
                case IOSlot::WRITE_MANY:
                    if (++io_slot->num_finished == io_slot->pages.size()) {
                        outstanding_writes_ --;
                        io_slot->promise.set_value(SUCCESS);
                    }
                    break;
                case IOSlot::FSYNC:
                case IOSlot::FSYNC_DATA:
                    io_slot->promise.set_value(SUCCESS);
                    break;
                default:
                    assert(false && "Unknown IO Operation");
            }
            io_uring_cqe_seen(&ring_, cqe);
            pending_ops_ --;
            io_slot_manager_.release_slot(io_slot);
            idle_start = std::chrono::high_resolution_clock::now();
        }
    }

    struct io_uring ring_;
    std::thread poll_thread_;
    bool stop_polling_;
    IOSlotManager io_slot_manager_;
    std::atomic<size_t> outstanding_writes_; 
    std::atomic<bool> allow_writes_; 
    std::atomic<size_t> pending_ops_;
    //char * uring_buffer_start_; 
};



class PagePool
{
public:
    PagePool(size_t page_count = 1024) : total_pages_(page_count)
    {
        //align the pool to page size
        pool_ = (char *)aligned_alloc(PAGE_SIZE, page_count * PAGE_SIZE);
        //fill the freePages_ with the pages
        freePages_.reserve(page_count);
        for (size_t i = 0; i < page_count; i++)
            freePages_.push_back(pool_ + i * PAGE_SIZE);
    }

    ~PagePool()
    {       
        free(pool_);
    }

    char * get_page(void)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        char * page = nullptr;
        if (freePages_.size() > 0)
        {
            page = freePages_.back();
            freePages_.pop_back();
        }
        else {
            assert (false && "No More Pages?");
        }
        return page;
    }

    void free_page(char * page)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        //page falls within the pool
        assert(page >= pool_ && page < pool_ + total_pages_ * PAGE_SIZE);
        freePages_.push_back(page);
    }

private:
    char * pool_;
    size_t total_pages_; 
    std::vector<char *> freePages_;
    std::mutex mutex_;
};


struct IOTask
{
    bool is_read;
    bool processed;
    size_t page_id;
    char * page_data;
    std::future<IOResult> future_obj;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
};

void test_io(size_t num_pages, size_t num_ops, size_t max_num_outstanding, double read_fraction)
{
    const char * filename = "testfile.bin";
    //the first 8 bytes of the page is filled with the page integer
    std::vector<size_t> page_integer(num_pages); 
    
    //1. initialize the file
    int fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert(fd >= 0 && "Failed to open file");
    std::vector<int> page_content(PAGE_SIZE / sizeof(int));
    for (size_t page_id = 0; page_id < num_pages; ++page_id) {
        page_integer[page_id] = rand();
        page_content[0] = page_integer[page_id];
        ssize_t written = pwrite(fd, page_content.data(), PAGE_SIZE, page_id * PAGE_SIZE);
        assert(written == PAGE_SIZE && "Failed to write full page");
    }
    close(fd);
    std::cout << "File filled with integers up to page count: " << num_pages << std::endl;

        //2. Create AsyncFileIO object and Memory Pool
    //AsyncFilePosix file_io(filename);
    //AsyncFileAsync file_io(filename);
    //AsyncFileThreadPool file_io(filename);
    //AsyncFileIOUring file_io(filename);
    AsyncFileIOUringOptimized file_io(filename);
    std::deque<IOTask> io_tasks;
    PagePool page_pool; 

    //3. Start IO operations
    double total_io_time = 0.0;
    double max_io_time = 0.0;
    size_t num_issued_ops = 0; 
    size_t num_outstanding = 0;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time = std::chrono::high_resolution_clock::now();
    while(num_issued_ops < num_ops)
    {
        //3.1 Issue IO operations
        while (num_issued_ops < num_ops && num_outstanding < max_num_outstanding)
        {
            size_t page_id = rand() % num_pages;
            char * page_data = page_pool.get_page();
            if(page_data == nullptr)
                assert(false && "No more Pages");
            if (static_cast<double>(rand()) / RAND_MAX < read_fraction) 
            {
                //read
                auto read_future = file_io.read_page(page_id, page_data);
                io_tasks.push_back(IOTask{true, false, page_id, page_data, std::move(read_future), std::chrono::high_resolution_clock::now()});
            }
            else {
                //write
                page_integer[page_id] = rand();
                int* int_ptr = reinterpret_cast<int*>(page_data);
                int_ptr[0] = page_integer[page_id];
                auto write_future = file_io.write_page(page_id, page_data);
                io_tasks.push_back(IOTask{false, false, page_id, page_data, std::move(write_future), std::chrono::high_resolution_clock::now()});
            }
            num_issued_ops++;
            num_outstanding ++;
        }

        //3.2 process completed IO operations
        for (auto & io_task : io_tasks)
        {
            if (io_task.processed)
                continue;
            if (io_task.future_obj.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
            {
                std::chrono::duration<double> elapsed = std::chrono::high_resolution_clock::now() - io_task.start_time;
                double time_diff = elapsed.count();
                total_io_time += time_diff;
                if (max_io_time < time_diff)
                    max_io_time = time_diff;

                IOResult r = io_task.future_obj.get();
                assert (r == SUCCESS && "IO Failed");
                if (io_task.is_read) {
                    //verify the content
                    int* int_ptr = reinterpret_cast<int*>(io_task.page_data);
                    if (int_ptr[0] != page_integer[io_task.page_id]) 
                        assert(false && "Read data not checking out?");
                }
                page_pool.free_page(io_task.page_data);
                io_task.processed = true;
                num_outstanding--;
            }
        }
        //3.3 remove processed IO tasks at the top of the queue
        while (!io_tasks.empty() && io_tasks.front().processed)
            io_tasks.pop_front();
    }
    std::chrono::duration<double> total_time = std::chrono::high_resolution_clock::now() - start_time;
    std::cout << "Total Pages " << num_pages << " Total IOs " << num_ops << " IOPS" << std::endl;
    std::cout << "Throughput " << num_ops / total_time.count() << std::endl; 
    std::cout << "Average IO Latency " << total_io_time/ num_ops * 1000.0 * 1000.0 << "us, max IO Latency " << max_io_time * 1000.0 * 1000.0  << "us" << std::endl;

};

int main() {
    test_io (1024 * 1024, 100 * 1000, 30, 1.0);
    return 0;
}

