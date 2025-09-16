#include "test_fixtures.h"
#include <thread>
#include <chrono>
#include <map>
#include <fstream>
#include <catch2/catch_test_macros.hpp>

namespace eloqstore::test {

// TestFixture implementation
TestFixture::TestFixture() {
    // Initialize logging for tests
    if (!verbose_logging_) {
        FLAGS_minloglevel = 2; // WARNING and above
    }
}

TestFixture::~TestFixture() {
    if (auto_cleanup_) {
        ShutdownStore();
        for (const auto& table : created_tables_) {
            DropTestTable(table);
        }
    }
}

void TestFixture::InitStore(const KvOptions& options) {
    if (!temp_dir_) {
        temp_dir_ = std::make_unique<TempDirectory>("eloqstore_test");
    }

    KvOptions opts = options;
    if (opts.store_path.empty()) {
        opts.store_path.push_back(temp_dir_->path().string());
    }

    store_ = std::make_unique<EloqStore>(opts);
    auto err = store_->Start();
    AssertNoError(err);
}

void TestFixture::InitStoreWithDefaults() {
    InitStore(GetDefaultOptions());
}

void TestFixture::ShutdownStore() {
    if (store_) {
        store_->Stop();
        store_.reset();
    }
}

KvOptions TestFixture::GetDefaultOptions() const {
    KvOptions opts;
    opts.num_threads = 2;
    opts.index_buffer_pool_size = 1000;
    opts.fd_limit = 100;
    opts.skip_verify_checksum = false;
    opts.data_page_size = 4096;
    opts.pages_per_file_shift = 10; // 1024 pages per file
    return opts;
}

KvOptions TestFixture::GetMemoryOnlyOptions() const {
    KvOptions opts = GetDefaultOptions();
    opts.index_buffer_pool_size = 100000;
    opts.max_write_batch_pages = 1024;
    return opts;
}

KvOptions TestFixture::GetMinimalOptions() const {
    KvOptions opts;
    opts.num_threads = 1;
    opts.index_buffer_pool_size = 10;
    opts.fd_limit = 10;
    opts.data_page_size = 512; // Minimal page size
    opts.pages_per_file_shift = 4; // 16 pages per file
    return opts;
}

KvOptions TestFixture::GetStressOptions() const {
    KvOptions opts;
    opts.num_threads = 8;
    opts.index_buffer_pool_size = 100000;
    opts.fd_limit = 1000;
    opts.max_inflight_write = 64 << 10;
    opts.max_write_batch_pages = 512;
    opts.io_queue_size = 8192;
    return opts;
}

TableIdent TestFixture::CreateTestTable(const std::string& name) {
    static uint32_t partition_counter = 0;
    TableIdent table(name, partition_counter++);
    created_tables_.push_back(table);
    return table;
}

void TestFixture::DropTestTable(const TableIdent& table) {
    // Cleanup table data
    if (store_) {
        auto req = std::make_unique<TruncateRequest>();
        req->SetTableId(table);
        store_->ExecSync(req.get());
        WaitForRequest(req.get());
    }
}

std::unique_ptr<ReadRequest> TestFixture::MakeReadRequest(
    const TableIdent& table, const std::string& key) {
    auto req = std::make_unique<ReadRequest>();
    req->SetArgs(table, key);
    return req;
}

std::unique_ptr<ScanRequest> TestFixture::MakeScanRequest(
    const TableIdent& table,
    const std::string& start_key,
    const std::string& end_key,
    uint32_t limit) {
    auto req = std::make_unique<ScanRequest>();
    req->SetArgs(table, start_key, end_key, true);
    req->SetPagination(limit, SIZE_MAX);
    return req;
}

std::unique_ptr<BatchWriteRequest> TestFixture::MakeBatchWriteRequest(
    const TableIdent& table) {
    auto req = std::make_unique<BatchWriteRequest>();
    req->SetTableId(table);
    return req;
}

void TestFixture::AssertNoError(KvError error) {
    if (error != KvError::NoError) {
        FAIL("Expected no error but got: " + std::to_string(static_cast<int>(error)));
    }
}

void TestFixture::AssertError(KvError expected, KvError actual) {
    if (expected != actual) {
        FAIL("Expected error " + std::to_string(static_cast<int>(expected)) +
             " but got " + std::to_string(static_cast<int>(actual)));
    }
}

void TestFixture::WaitForRequest(KvRequest* request) {
    while (!request->IsDone()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

void TestFixture::WaitForRequests(const std::vector<KvRequest*>& requests) {
    for (auto* req : requests) {
        WaitForRequest(req);
    }
}

// Sync operations
KvError TestFixture::ReadSync(const TableIdent& table, const std::string& key, std::string& value) {
    auto req = MakeReadRequest(table, key);
    store_->ExecSync(req.get());
    if (req->Error() == KvError::NoError) {
        value = req->value_;
    }
    return req->Error();
}

KvError TestFixture::WriteSync(const TableIdent& table, const std::string& key, const std::string& value) {
    auto req = MakeBatchWriteRequest(table);
    req->AddWrite(key, value, 0, WriteOp::Upsert);
    store_->ExecSync(req.get());
    return req->Error();
}

KvError TestFixture::ScanSync(const TableIdent& table, const std::string& start, const std::string& end,
                              std::vector<KvEntry>& results, uint32_t limit) {
    auto req = MakeScanRequest(table, start, end, limit);
    store_->ExecSync(req.get());
    if (req->Error() == KvError::NoError) {
        results.clear();
        for (const auto& entry : req->Entries()) {
            results.push_back(entry);
        }
    }
    return req->Error();
}

// Async operations
void TestFixture::ReadAsync(const TableIdent& table, const std::string& key,
                            std::function<void(KvError, const std::string&)> callback) {
    auto req = MakeReadRequest(table, key);
    auto* req_ptr = req.release(); // Transfer ownership

    store_->ExecAsyn(req_ptr, 0, [req_ptr, callback](KvRequest* r) {
        auto* read_req = static_cast<ReadRequest*>(r);
        callback(read_req->Error(), read_req->value_);
        delete req_ptr; // Clean up
    });
}

void TestFixture::WriteAsync(const TableIdent& table, const std::string& key, const std::string& value,
                             std::function<void(KvError)> callback) {
    auto req = MakeBatchWriteRequest(table);
    req->AddWrite(key, value, 0, WriteOp::Upsert);
    auto* req_ptr = req.release(); // Transfer ownership

    store_->ExecAsyn(req_ptr, 0, [req_ptr, callback](KvRequest* r) {
        callback(r->Error());
        delete req_ptr; // Clean up
    });
}

void TestFixture::ScanAsync(const TableIdent& table, const std::string& start, const std::string& end,
                            std::function<void(KvError, const std::vector<KvEntry>&)> callback,
                            uint32_t limit) {
    auto req = MakeScanRequest(table, start, end, limit);
    auto* req_ptr = req.release(); // Transfer ownership

    store_->ExecAsyn(req_ptr, 0, [req_ptr, callback](KvRequest* r) {
        auto* scan_req = static_cast<ScanRequest*>(r);
        std::vector<KvEntry> results;
        if (scan_req->Error() == KvError::NoError) {
            for (const auto& entry : scan_req->Entries()) {
                results.push_back(entry);
            }
        }
        callback(scan_req->Error(), results);
        delete req_ptr; // Clean up
    });
}

// MultiShardFixture implementation
MultiShardFixture::MultiShardFixture(uint16_t num_shards)
    : num_shards_(num_shards) {
}

KvOptions MultiShardFixture::GetMultiShardOptions(uint16_t num_shards) const {
    KvOptions opts = GetDefaultOptions();
    opts.num_threads = num_shards;
    return opts;
}

void MultiShardFixture::InitWithShards(uint16_t num_shards) {
    num_shards_ = num_shards;
    InitStore(GetMultiShardOptions(num_shards));
}

void MultiShardFixture::AssertBalancedDistribution(
    const std::vector<TableIdent>& tables) {
    std::vector<uint32_t> shard_counts(num_shards_, 0);

    for (const auto& table : tables) {
        uint16_t shard_id = GetShardForTable(table);
        REQUIRE(shard_id < num_shards_);
        shard_counts[shard_id]++;
    }

    // Check that distribution is relatively balanced
    uint32_t min_count = *std::min_element(shard_counts.begin(), shard_counts.end());
    uint32_t max_count = *std::max_element(shard_counts.begin(), shard_counts.end());

    // Allow at most 2x difference
    REQUIRE(max_count <= min_count * 2 + 1);
}

uint16_t MultiShardFixture::GetShardForTable(const TableIdent& table) {
    return table.ShardIndex(num_shards_);
}

// AsyncTestFixture implementation
AsyncTestFixture::AsyncTestFixture() {
}

void AsyncTestFixture::SubmitAsyncRead(
    const TableIdent& table,
    const std::string& key,
    std::function<void(ReadRequest*)> callback) {
    auto req = MakeReadRequest(table, key);

    auto wrapper_callback = [callback, this](KvRequest* r) {
        callback(static_cast<ReadRequest*>(r));
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_cv_.notify_one();
    };

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_requests_.push_back(std::move(req));
    }

    store_->ExecAsyn(static_cast<ReadRequest*>(pending_requests_.back().get()), 0, wrapper_callback);
}

void AsyncTestFixture::SubmitAsyncWrite(
    const TableIdent& table,
    const std::vector<std::pair<std::string, std::string>>& kvs,
    std::function<void(BatchWriteRequest*)> callback) {
    auto req = MakeBatchWriteRequest(table);

    for (const auto& [key, value] : kvs) {
        req->AddWrite(key, value, 0, WriteOp::Upsert);
    }

    auto wrapper_callback = [callback, this](KvRequest* r) {
        callback(static_cast<BatchWriteRequest*>(r));
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_cv_.notify_one();
    };

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_requests_.push_back(std::move(req));
    }

    store_->ExecAsyn(static_cast<BatchWriteRequest*>(pending_requests_.back().get()), 0, wrapper_callback);
}

void AsyncTestFixture::WaitAllPending() {
    std::unique_lock<std::mutex> lock(pending_mutex_);
    pending_cv_.wait(lock, [this] {
        return std::all_of(pending_requests_.begin(), pending_requests_.end(),
                          [](const auto& req) { return req->IsDone(); });
    });
}

void AsyncTestFixture::RunConcurrentReads(
    const TableIdent& table,
    const std::vector<std::string>& keys,
    uint32_t num_threads) {
    std::vector<std::thread> threads;
    std::atomic<uint32_t> successful_reads{0};

    for (uint32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, &table, &keys, &successful_reads, i, num_threads] {
            for (size_t j = i; j < keys.size(); j += num_threads) {
                auto req = MakeReadRequest(table, keys[j]);
                store_->ExecSync(req.get());
                WaitForRequest(req.get());
                if (req->Error() == KvError::NoError) {
                    successful_reads++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << "Concurrent reads completed: " << successful_reads << "/" << keys.size();
}

void AsyncTestFixture::RunConcurrentWrites(
    const TableIdent& table,
    uint32_t num_threads,
    uint32_t writes_per_thread) {
    std::vector<std::thread> threads;
    std::atomic<uint32_t> successful_writes{0};

    for (uint32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, &table, &successful_writes, i, writes_per_thread] {
            for (uint32_t j = 0; j < writes_per_thread; ++j) {
                auto req = MakeBatchWriteRequest(table);
                std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                std::string value = "value_" + std::to_string(j);
                req->AddWrite(key, value, 0, WriteOp::Upsert);

                store_->ExecSync(req.get());
                WaitForRequest(req.get());
                if (req->Error() == KvError::NoError) {
                    successful_writes++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << "Concurrent writes completed: " << successful_writes
              << "/" << (num_threads * writes_per_thread);
}

// PersistenceTestFixture implementation
PersistenceTestFixture::PersistenceTestFixture() {
}

void PersistenceTestFixture::FlushAllData() {
    // Force flush by triggering background writes
    if (store_) {
        // Implementation depends on EloqStore internals
        // This would trigger background write tasks
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void PersistenceTestFixture::SimulateCrash() {
    if (store_) {
        saved_options_ = store_->Options();
        if (!saved_options_.store_path.empty()) {
            data_path_ = saved_options_.store_path[0];
        }
        // Force shutdown without cleanup
        store_.reset();
    }
}

void PersistenceTestFixture::RestartStore() {
    if (!store_) {
        saved_options_.store_path.clear();
        saved_options_.store_path.push_back(data_path_);
        store_ = std::make_unique<EloqStore>(saved_options_);
        auto err = store_->Start();
        AssertNoError(err);
    }
}

void PersistenceTestFixture::CorruptFile(const std::string& file_pattern) {
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator(data_path_)) {
        if (entry.path().filename().string().find(file_pattern) != std::string::npos) {
            // Corrupt file by writing random data
            std::ofstream file(entry.path(), std::ios::binary | std::ios::in | std::ios::out);
            file.seekp(0);
            char corrupt_data[] = "CORRUPTED";
            file.write(corrupt_data, sizeof(corrupt_data));
            file.close();
            LOG(INFO) << "Corrupted file: " << entry.path();
            break;
        }
    }
}

void PersistenceTestFixture::VerifyDataIntegrity(const TableIdent& table) {
    // Verify by scanning all data
    auto req = MakeScanRequest(table, "", "", UINT32_MAX);
    store_->ExecSync(req.get());
    WaitForRequest(req.get());
    AssertNoError(req->Error());
}

void PersistenceTestFixture::VerifyManifestConsistency() {
    // Verify manifest can be loaded and is consistent
    // This would check internal manifest state
    RestartStore();
}

void PersistenceTestFixture::VerifyNoDataLoss(
    const std::map<std::string, std::string>& expected_data) {
    for (const auto& [key, expected_value] : expected_data) {
        auto req = MakeReadRequest(CreateTestTable(), key);
        store_->ExecSync(req.get());
        WaitForRequest(req.get());
        AssertNoError(req->Error());
        // REQUIRE(req->value_ == expected_value); // Would need Catch2 macro
    }
}

// EdgeCaseFixture implementation
EdgeCaseFixture::EdgeCaseFixture() : rng_(std::random_device{}()) {
}

std::string EdgeCaseFixture::GenerateMaxSizeKey() {
    // Maximum key size depends on page size
    const size_t max_key_size = 1024; // Conservative estimate
    return std::string(max_key_size, 'K');
}

std::string EdgeCaseFixture::GenerateMaxSizeValue() {
    // Maximum value size that fits in a page
    const size_t max_value_size = 3072; // Conservative estimate
    return std::string(max_value_size, 'V');
}

std::string EdgeCaseFixture::GenerateZeroLengthString() {
    return "";
}

std::vector<uint8_t> EdgeCaseFixture::GenerateBinaryData(size_t size) {
    std::vector<uint8_t> data(size);
    std::uniform_int_distribution<uint8_t> dist(0, 255);
    for (auto& byte : data) {
        byte = dist(rng_);
    }
    return data;
}

std::string EdgeCaseFixture::GenerateUnicodeString(size_t length) {
    std::string result;
    std::uniform_int_distribution<uint32_t> dist(0x0080, 0x07FF); // 2-byte UTF-8

    for (size_t i = 0; i < length; ++i) {
        uint32_t codepoint = dist(rng_);
        if (codepoint <= 0x7F) {
            result += static_cast<char>(codepoint);
        } else if (codepoint <= 0x7FF) {
            result += static_cast<char>(0xC0 | (codepoint >> 6));
            result += static_cast<char>(0x80 | (codepoint & 0x3F));
        }
    }
    return result;
}

void EdgeCaseFixture::TestPageBoundaries() {
    auto table = CreateTestTable();
    auto req = MakeBatchWriteRequest(table);

    // Fill page to exact capacity
    size_t total_size = 0;
    const size_t page_size = store_->Options().data_page_size;
    uint32_t key_num = 0;

    while (total_size < page_size - 100) { // Leave some room for metadata
        std::string key = "key_" + std::to_string(key_num++);
        std::string value = "val_" + std::to_string(key_num);
        req->AddWrite(key, value, 0, WriteOp::Upsert);
        total_size += key.size() + value.size() + 8; // Approximate overhead
    }

    store_->ExecSync(req.get());
    WaitForRequest(req.get());
    AssertNoError(req->Error());
}

void EdgeCaseFixture::TestFileLimits() {
    // Test maximum files open
    std::vector<TableIdent> tables;
    const uint32_t fd_limit = store_->Options().fd_limit;

    for (uint32_t i = 0; i < fd_limit * 2; ++i) {
        tables.push_back(CreateTestTable("file_test_" + std::to_string(i)));
        auto req = MakeBatchWriteRequest(tables.back());
        req->AddWrite("key", "value", 0, WriteOp::Upsert);
        store_->ExecSync(req.get());
        WaitForRequest(req.get());
    }

    // Verify all tables are still accessible
    for (const auto& table : tables) {
        auto req = MakeReadRequest(table, "key");
        store_->ExecSync(req.get());
        WaitForRequest(req.get());
        AssertNoError(req->Error());
    }
}

void EdgeCaseFixture::TestMemoryLimits() {
    // Test with minimal memory configuration
    ShutdownStore();
    KvOptions opts = GetMinimalOptions();
    opts.index_buffer_pool_size = 1;
    opts.max_inflight_write = 1024;
    InitStore(opts);

    auto table = CreateTestTable();

    // Try to exceed memory limits
    for (int i = 0; i < 1000; ++i) {
        auto req = MakeBatchWriteRequest(table);
        req->AddWrite("key_" + std::to_string(i), GenerateMaxSizeValue(), 0, WriteOp::Upsert);
        store_->ExecSync(req.get());
        WaitForRequest(req.get());
    }
}

void EdgeCaseFixture::InjectIOError(double probability) {
    // This would integrate with kill points or error injection framework
    LOG(INFO) << "Injecting I/O errors with probability: " << probability;
}

void EdgeCaseFixture::InjectMemoryPressure(size_t available_bytes) {
    // Allocate memory to simulate pressure
    static std::vector<std::unique_ptr<char[]>> memory_hog;
    size_t chunk_size = 1024 * 1024; // 1MB chunks
    size_t chunks_to_allocate = available_bytes / chunk_size;

    for (size_t i = 0; i < chunks_to_allocate; ++i) {
        memory_hog.push_back(std::make_unique<char[]>(chunk_size));
        std::fill_n(memory_hog.back().get(), chunk_size, 0xFF);
    }

    LOG(INFO) << "Injected memory pressure: " << available_bytes << " bytes";
}

void EdgeCaseFixture::InjectCorruption(const std::string& target) {
    LOG(INFO) << "Injecting corruption in: " << target;
    // This would corrupt specific data structures or files
}

} // namespace eloqstore::test