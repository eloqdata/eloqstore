#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <random>
#include <filesystem>
#include <condition_variable>
#include <mutex>
#include <glog/logging.h>

#include "eloq_store.h"
#include "kv_options.h"
#include "types.h"
#include "temp_directory.h"

namespace eloqstore::test {

/**
 * Base test fixture for all EloqStore tests
 */
class TestFixture {
public:
    TestFixture();
    virtual ~TestFixture();

    // Store lifecycle management
    void InitStore(const KvOptions& options);
    void InitStoreWithDefaults();
    void ShutdownStore();

    // Store access
    EloqStore* GetStore() const { return store_.get(); }

    // Helper methods
    KvOptions GetDefaultOptions() const;
    KvOptions GetMemoryOnlyOptions() const;
    KvOptions GetMinimalOptions() const;
    KvOptions GetStressOptions() const;

    // Table management
    TableIdent CreateTestTable(const std::string& name = "test_table");
    void DropTestTable(const TableIdent& table);

    // Request helpers
    std::unique_ptr<ReadRequest> MakeReadRequest(const TableIdent& table, const std::string& key);
    std::unique_ptr<ScanRequest> MakeScanRequest(const TableIdent& table,
                                                  const std::string& start_key,
                                                  const std::string& end_key,
                                                  uint32_t limit = 100);
    std::unique_ptr<BatchWriteRequest> MakeBatchWriteRequest(const TableIdent& table);

    // Sync operations
    KvError ReadSync(const TableIdent& table, const std::string& key, std::string& value);
    KvError WriteSync(const TableIdent& table, const std::string& key, const std::string& value);
    KvError ScanSync(const TableIdent& table, const std::string& start, const std::string& end,
                     std::vector<KvEntry>& results, uint32_t limit = 100);

    // Async operations
    void ReadAsync(const TableIdent& table, const std::string& key,
                   std::function<void(KvError, const std::string&)> callback);
    void WriteAsync(const TableIdent& table, const std::string& key, const std::string& value,
                    std::function<void(KvError)> callback);
    void ScanAsync(const TableIdent& table, const std::string& start, const std::string& end,
                   std::function<void(KvError, const std::vector<KvEntry>&)> callback,
                   uint32_t limit = 100);

    // Assertion helpers
    void AssertNoError(KvError error);
    void AssertError(KvError expected, KvError actual);
    void WaitForRequest(KvRequest* request);
    void WaitForRequests(const std::vector<KvRequest*>& requests);

protected:
    std::unique_ptr<EloqStore> store_;
    std::unique_ptr<TempDirectory> temp_dir_;
    std::vector<TableIdent> created_tables_;

    // Test configuration
    bool auto_cleanup_ = true;
    bool verbose_logging_ = false;
};

/**
 * Fixture for testing with multiple shards
 */
class MultiShardFixture : public TestFixture {
public:
    MultiShardFixture(uint16_t num_shards = 4);

    KvOptions GetMultiShardOptions(uint16_t num_shards) const;
    void InitWithShards(uint16_t num_shards);

    // Shard-specific operations
    void AssertBalancedDistribution(const std::vector<TableIdent>& tables);
    uint16_t GetShardForTable(const TableIdent& table);

private:
    uint16_t num_shards_;
};

/**
 * Fixture for testing async operations
 */
class AsyncTestFixture : public TestFixture {
public:
    AsyncTestFixture();

    // Async operation helpers
    void SubmitAsyncRead(const TableIdent& table, const std::string& key,
                         std::function<void(ReadRequest*)> callback);
    void SubmitAsyncWrite(const TableIdent& table,
                          const std::vector<std::pair<std::string, std::string>>& kvs,
                          std::function<void(BatchWriteRequest*)> callback);
    void WaitAllPending();

    // Concurrent operation helpers
    void RunConcurrentReads(const TableIdent& table,
                            const std::vector<std::string>& keys,
                            uint32_t num_threads);
    void RunConcurrentWrites(const TableIdent& table,
                             uint32_t num_threads,
                             uint32_t writes_per_thread);

private:
    std::vector<std::unique_ptr<KvRequest>> pending_requests_;
    std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
};

/**
 * Fixture for testing storage persistence
 */
class PersistenceTestFixture : public TestFixture {
public:
    PersistenceTestFixture();

    // Persistence operations
    void FlushAllData();
    void SimulateCrash();
    void RestartStore();
    void CorruptFile(const std::string& file_pattern);

    // Verification helpers
    void VerifyDataIntegrity(const TableIdent& table);
    void VerifyManifestConsistency();
    void VerifyNoDataLoss(const std::map<std::string, std::string>& expected_data);

private:
    KvOptions saved_options_;
    std::string data_path_;
};

/**
 * Fixture for edge case testing
 */
class EdgeCaseFixture : public TestFixture {
public:
    EdgeCaseFixture();

    // Edge case generators
    std::string GenerateMaxSizeKey();
    std::string GenerateMaxSizeValue();
    std::string GenerateZeroLengthString();
    std::vector<uint8_t> GenerateBinaryData(size_t size);
    std::string GenerateUnicodeString(size_t length);

    // Boundary testing
    void TestPageBoundaries();
    void TestFileLimits();
    void TestMemoryLimits();

    // Error injection
    void InjectIOError(double probability);
    void InjectMemoryPressure(size_t available_bytes);
    void InjectCorruption(const std::string& target);

private:
    std::mt19937 rng_;
};

} // namespace eloqstore::test