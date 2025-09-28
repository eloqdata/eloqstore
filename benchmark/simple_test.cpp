#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../eloq_store.h"

const size_t KEY_LEN_MIN = 16;
const size_t KEY_LEN_MAX = 16;
const size_t VALUE_LEN_MIN = 16;
const size_t VALUE_LEN_MAX = 16;

class ThreadPool
{
public:
    // the constructor just launches some amount of workers
    ThreadPool(size_t threads) : stop(false)
    {
        for (size_t i = 0; i < threads; ++i)
            workers.emplace_back(
                [this]
                {
                    for (;;)
                    {
                        std::function<void()> task;

                        {
                            std::unique_lock<std::mutex> lock(
                                this->queue_mutex);
                            this->condition.wait(
                                lock,
                                [this]
                                { return this->stop || !this->tasks.empty(); });
                            if (this->stop && this->tasks.empty())
                                return;
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                        }

                        task();
                    }
                });
    }

    // add new work item to the pool
    template <class F, class... Args>
    auto enqueue(F &&f, Args &&...args)
        -> std::future<typename std::result_of<F(Args...)>::type>
    {
        using return_type = typename std::result_of<F(Args...)>::type;

        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);

            // don't allow enqueueing after stopping the pool
            if (stop)
                throw std::runtime_error("enqueue on stopped ThreadPool");

            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    // the destructor joins all threads
    ~ThreadPool()
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers)
            worker.join();
    }

private:
    // need to keep track of threads so we can join them
    std::vector<std::thread> workers;
    // the task queue
    std::queue<std::function<void()>> tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

uint32_t hash(uint32_t x)
{
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

// a key is 10 bytes long, plus zeros following the number (so that the number
// can be compared and is sorted), the length is deterministic between
// key_len_min and key_len_max
std::string make_key(int number,
                     size_t key_len_min = 16,
                     size_t key_len_max = 16)
{
    assert(key_len_min <= key_len_max);
    size_t hash_it = hash(number);
    size_t len = key_len_min + hash_it % (key_len_max - key_len_min + 1);

    assert(len >= 10);
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << number;
    std::string s = ss.str();
    if (s.size() < len)
        s += '_';
    s.resize(len, '0');
    return s;
}

size_t get_num_from_key(const std::string &key)
{
    assert(key.size() >= 10);
    return std::stoul(key.substr(0, 10));
}

// a value is prefixed with 0 strings, so we can always check if the value is
// not corrupted
std::string make_value(size_t number,
                       size_t value_len_min = 16,
                       size_t value_len_max = 16)
{
    assert(value_len_min <= value_len_max);
    std::random_device rd;
    std::mt19937 gen(rd());
    size_t len = value_len_min + gen() % (value_len_max - value_len_min + 1);
    std::stringstream ss;
    ss << std::setw(len) << std::setfill('0') << number;
    return ss.str();
}

size_t get_num_from_value(const std::string &value)
{
    assert(value.size() >= 10);
    // the last 10 bytes are the number
    return std::stoul(value.substr(value.size() - 10));
}

// Populate database with N key-value pairs in random order
void populate_db(eloqstore::EloqStore &store,
                 const eloqstore::TableIdent &tbl_id,
                 size_t N,
                 size_t key_len_min = 16,
                 size_t key_len_max = 16,
                 size_t value_len_min = 16,
                 size_t value_len_max = 16)
{
    std::cout << "Generating and inserting " << N << " key-value pairs..."
              << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    // Create vector of numbers and shuffle them as keys
    std::vector<size_t> rand_keys(N);
    for (size_t i = 0; i < N; i++)
    {
        rand_keys[i] = i;
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    // std::shuffle(rand_keys.begin(), rand_keys.end(), gen);

    const size_t BATCH_SIZE = 1000;
    size_t index = 0;
    while (index < N)
    {
        eloqstore::BatchWriteRequest req;
        std::vector<eloqstore::WriteDataEntry> entries;

        size_t current_batch = std::min(BATCH_SIZE, N - index);
        // sort the current batch by key, because the database expects sorted
        // keys for batch updates
        std::sort(rand_keys.begin() + index,
                  rand_keys.begin() + index + current_batch);
        for (size_t j = 0; j < current_batch; ++j)
        {
            size_t current_num = rand_keys[index + j];
            std::string key =
                make_key(current_num,
                         key_len_min + rd() % (key_len_max - key_len_min + 1));
            std::string value = make_value(
                current_num,
                value_len_min + rd() % (value_len_max - value_len_min + 1));
            entries.emplace_back(key, value, 1, eloqstore::WriteOp::Upsert);
        }
        index += current_batch;
        req.SetArgs(tbl_id, std::move(entries));
        store.ExecSync(&req);
        assert(req.Error() == eloqstore::KvError::NoError);
        if (index % 100000 == 0)
            std::cout << "\tInserted " << index << " Entries" << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Insertion completed in " << duration.count() << "ms"
              << std::endl;
}

void perform_random_lookups_sync(eloqstore::EloqStore &store,
                                 const eloqstore::TableIdent &tbl_id,
                                 size_t N,
                                 size_t num_lookups)
{
    std::cout << "Performing " << num_lookups << " random lookups..."
              << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, N - 1);
    size_t index = 0;
    while (index < num_lookups)
    {
        size_t key_val = dis(gen);
        std::string key = make_key(key_val, KEY_LEN_MIN, KEY_LEN_MAX);
        eloqstore::ReadRequest *req = new eloqstore::ReadRequest();
        req->SetArgs(tbl_id, key);
        store.ExecSync(req);
        assert(req->Error() == eloqstore::KvError::NoError);
        int value = get_num_from_value(req->value_);
        assert(value == key_val);
        delete req;
        index++;
        if (index % 10000 == 0)
        {
            std::cout << "\tLookup " << index << " Keys" << std::endl;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Scans completed in " << duration.count() << "ms" << std::endl;
}

// Perform random lookups
void perform_random_lookups(eloqstore::EloqStore &store,
                            const eloqstore::TableIdent &tbl_id,
                            size_t N,
                            size_t num_lookups,
                            size_t batch_size_max = 50,
                            size_t sleep_between_batch = 0)
{
    std::cout << "Performing " << num_lookups << " random lookups..."
              << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, N - 1);
    size_t index = 0;
    while (index < num_lookups)
    {
        std::vector<std::pair<eloqstore::ReadRequest *, std::string>> requests;
        size_t batch_size = rd() % batch_size_max + 1;
        batch_size = std::min(batch_size, num_lookups - index);
        for (size_t i = 0; i < batch_size; ++i)
        {
            size_t key_val = dis(gen);
            std::string key = make_key(key_val, KEY_LEN_MIN, KEY_LEN_MAX);
            eloqstore::ReadRequest *req = new eloqstore::ReadRequest();
            requests.emplace_back(req, std::move(key));
            requests.back().first->SetArgs(tbl_id, requests.back().second);
            bool r = store.ExecAsyn(requests.back().first);
            assert(r);
        }
        if (sleep_between_batch > 0)
        {
            size_t sleep_time = rd() % sleep_between_batch;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        }
        for (size_t i = 0; i < batch_size; ++i)
        {
            eloqstore::ReadRequest *req = requests[i].first;
            req->Wait();
            assert(req->Error() == eloqstore::KvError::NoError);
            int value = get_num_from_value(req->value_);
            assert(value == get_num_from_key(std::string(req->Key())));
            delete req;
        }
        requests.clear();
        index += batch_size;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Lookups completed in " << duration.count() << "ms"
              << std::endl;
}

// Perform range scans
void perform_range_scans(eloqstore::EloqStore &store,
                         const eloqstore::TableIdent &tbl_id,
                         size_t N,
                         size_t num_scans,
                         size_t range_size,
                         size_t batch_size_max = 10,
                         size_t sleep_between_batch = 0)
{
    std::cout << "Performing " << num_scans << " range scans..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::random_device rd;
    std::mt19937 gen(rd());
    size_t index = 0;
    struct Scanner
    {
        eloqstore::ScanRequest req_;
        std::string begin_key_;
        std::string end_key_;
    };
    std::vector<Scanner> scanners(batch_size_max);
    while (index < num_scans)
    {
        const size_t batch_size = std::min(batch_size_max, num_scans - index);
        for (size_t i = 0; i < batch_size; ++i)
        {
            Scanner &scanner = scanners[i];
            size_t scan_size = range_size;
            size_t start_num = gen() % (N - scan_size);
            scanner.begin_key_ = make_key(start_num);
            scanner.end_key_ = make_key(start_num + scan_size);

            scanner.req_.SetArgs(tbl_id, scanner.begin_key_, scanner.end_key_);
            bool r = store.ExecAsyn(&scanner.req_);
            assert(r);
        }
        if (sleep_between_batch > 0)
        {
            size_t sleep_time = rd() % sleep_between_batch;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        }
        for (size_t i = 0; i < batch_size; ++i)
        {
            Scanner &scanner = scanners[i];
            scanner.req_.Wait();
            assert(scanner.req_.Error() == eloqstore::KvError::NoError);
            size_t start_num = get_num_from_key(scanner.begin_key_);
            size_t end_num = get_num_from_key(scanner.end_key_);
            assert(scanner.req_.Entries().size() == end_num - start_num);
            for (const eloqstore::KvEntry entry : scanner.req_.Entries())
            {
                int key = get_num_from_key(entry.key_);
                int value = get_num_from_value(entry.value_);
                assert(value == key);
            }
        }
        index += batch_size;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Scans completed in " << duration.count() << "ms" << std::endl;
}

void do_random_lookups(eloqstore::EloqStore &store,
                       const eloqstore::TableIdent &tbl_id,
                       size_t N,
                       int num_read_threads)
{
    const size_t NUM_LOOKUPS = 1000000;  // Number of random lookups
    std::vector<std::thread> threads;
    std::cout << "Number of threads: " << num_read_threads << std::endl;
    std::cout << "Random lookups" << std::endl;
    std::cout << "Number of lookups Per thread: " << NUM_LOOKUPS << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_read_threads; ++i)
    {
        threads.emplace_back(
            [&]() { perform_random_lookups(store, tbl_id, N, NUM_LOOKUPS); });
    }
    for (int i = 0; i < num_read_threads; ++i)
    {
        threads[i].join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    std::cout << "Time taken: " << duration.count() << "ms"
              << " Throughput: "
              << NUM_LOOKUPS * num_read_threads * 1000.0 / duration.count()
              << " lookups/s" << std::endl;
}

void do_range_scans(eloqstore::EloqStore &store,
                    const eloqstore::TableIdent &tbl_id,
                    size_t N,
                    int num_read_threads)
{
    const size_t NUM_SCANS = 10000;  // Number of range scans
    const size_t SCAN_RANGE = 1000;  // Number of items in each scan

    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Range scans" << std::endl;
    for (int i = 0; i < num_read_threads; ++i)
    {
        threads.emplace_back(
            [&]() {
                perform_range_scans(
                    store, tbl_id, N, NUM_SCANS, SCAN_RANGE, 100);
            });
    }
    for (int i = 0; i < num_read_threads; ++i)
    {
        threads[i].join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    size_t kvs_per_sec =
        NUM_SCANS * SCAN_RANGE * num_read_threads * 1000.0 / duration.count();
    size_t kib_per_sec = (kvs_per_sec * 32) >> 10;
    std::cout << "Time taken: " << duration.count() << "ms"
              << " Throughput: " << kvs_per_sec << " entries/s"
              << ", " << kib_per_sec << " KB/s" << std::endl;
}

int main(int argc, char *argv[])
{
    const size_t N = 1000000;  // Number of key-value pairs
    int num_read_threads = 4;
    // Initialize store
    eloqstore::KvOptions opts;
    opts.skip_verify_checksum = true;
    opts.store_path = {"/mnt/ramdisk/eloq_store_perf"};
    eloqstore::TableIdent tbl_id("perf_test", 1);

    eloqstore::EloqStore store(opts);
    eloqstore::KvError err = store.Start();
    assert(err == eloqstore::KvError::NoError);

    // based on command arguments, we run different tests:
    std::cout << "Starting EloqStore with options: " << std::endl;
    std::cout << "Store path: " << opts.store_path[0] << std::endl;
    std::cout << "Number of threads: " << opts.num_threads << std::endl;

    if (argc > 1)
    {
        std::cout << "Running tests based on command arguments..." << std::endl;
        if (std::string(argv[1]) == "populate")
        {
            // Populate database
            populate_db(store, tbl_id, N);
        }
        else if (std::string(argv[1]) == "lookup")
        {
            do_random_lookups(store, tbl_id, N, num_read_threads);
        }
        else if (std::string(argv[1]) == "scan")
        {
            do_range_scans(store, tbl_id, N, num_read_threads);
        }
        else
        {
            std::cerr << "Unknown command: " << argv[1] << std::endl;
            return 1;
        }
        store.Stop();
        return 0;
    }
    else
    {
        // Populate database
        populate_db(store, tbl_id, N);
        do_random_lookups(store, tbl_id, N, num_read_threads);
        do_range_scans(store, tbl_id, N, num_read_threads);
    }
    store.Stop();
    return 0;
}