#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>
#include <set>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "eloq_store.h"
#include "types.h"
#include "kv_options.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/random_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

namespace bfs = boost::filesystem;

class RecoveryTestFixture {
public:
    RecoveryTestFixture() : data_dir_("/mnt/ramdisk/recovery_test_" + GenerateTestId()) {
        bfs::create_directories(data_dir_);
        backup_dir_ = data_dir_ + "_backup";
        bfs::create_directories(backup_dir_);
    }

    ~RecoveryTestFixture() {
        Cleanup();
    }

    void StartStore(bool clean_start = false) {
        if (clean_start) {
            CleanDataDir();
        }

        opts_.store_path = {data_dir_};
        opts_.num_threads = 2;
        opts_.data_page_size = 4096;

        store_ = std::make_unique<EloqStore>(opts_);
        auto err = store_->Start();
        if (err != KvError::NoError) {
            throw std::runtime_error("Failed to start store");
        }
    }

    void StopStore() {
        if (store_) {
            store_->Stop();
            store_.reset();
        }
    }

    void BackupDataDir() {
        if (bfs::exists(backup_dir_)) {
            bfs::remove_all(backup_dir_);
        }
        bfs::create_directories(backup_dir_);

        // Copy all files from data_dir to backup_dir
        for (const auto& entry : bfs::directory_iterator(data_dir_)) {
            bfs::copy(entry.path(), backup_dir_ / entry.path().filename());
        }
    }

    void RestoreDataDir() {
        if (bfs::exists(data_dir_)) {
            bfs::remove_all(data_dir_);
        }
        bfs::create_directories(data_dir_);

        // Copy all files from backup_dir to data_dir
        for (const auto& entry : bfs::directory_iterator(backup_dir_)) {
            bfs::copy(entry.path(), data_dir_ / entry.path().filename());
        }
    }

    void CorruptFile(const std::string& pattern, size_t corruption_offset, size_t corruption_size) {
        for (const auto& entry : bfs::directory_iterator(data_dir_)) {
            std::string filename = entry.path().filename().string();
            if (filename.find(pattern) != std::string::npos) {
                CorruptSpecificFile(entry.path().string(), corruption_offset, corruption_size);
                break;
            }
        }
    }

    void SimulateCrash() {
        // Fork a child process to simulate crash
        pid_t pid = fork();
        if (pid == 0) {
            // Child process - simulate crash by killing itself
            raise(SIGKILL);
        } else if (pid > 0) {
            // Parent process - wait for child to crash
            int status;
            waitpid(pid, &status, 0);
        }
    }

    bool WriteData(const TableIdent& table, const std::map<std::string, std::string>& data) {
        auto req = std::make_unique<BatchWriteRequest>();
        req->SetTableId(table);

        for (const auto& [key, value] : data) {
            req->AddWrite(key, value, 0, WriteOp::Upsert);
        }

        store_->ExecSync(req.get());
        return req->Error() == KvError::NoError;
    }

    std::map<std::string, std::string> ReadAllData(const TableIdent& table) {
        std::map<std::string, std::string> result;

        auto req = std::make_unique<ScanRequest>();
        req->SetTableId(table);
        req->SetArgs(table, "", "");
        req->SetPagination(UINT32_MAX, SIZE_MAX);

        store_->ExecSync(req.get());

        if (req->Error() == KvError::NoError) {
            auto entries = req->Entries();
            for (const auto& kv : entries) {
                result[std::string(kv.key_)] = std::string(kv.value_);
            }
        }

        return result;
    }

    bool VerifyData(const TableIdent& table, const std::map<std::string, std::string>& expected) {
        auto actual = ReadAllData(table);

        if (actual.size() != expected.size()) {
            LOG(ERROR) << "Size mismatch: expected=" << expected.size()
                      << " actual=" << actual.size();
            return false;
        }

        for (const auto& [key, value] : expected) {
            auto it = actual.find(key);
            if (it == actual.end()) {
                LOG(ERROR) << "Missing key: " << key;
                return false;
            }
            if (it->second != value) {
                LOG(ERROR) << "Value mismatch for key " << key
                          << ": expected=" << value << " actual=" << it->second;
                return false;
            }
        }

        return true;
    }

private:
    std::string GenerateTestId() {
        static std::atomic<int> counter{0};
        return std::to_string(std::time(nullptr)) + "_" + std::to_string(counter++);
    }

    void CleanDataDir() {
        if (bfs::exists(data_dir_)) {
            bfs::remove_all(data_dir_);
        }
        bfs::create_directories(data_dir_);
    }

    void Cleanup() {
        StopStore();
        try {
            if (bfs::exists(data_dir_)) {
                bfs::remove_all(data_dir_);
            }
            if (bfs::exists(backup_dir_)) {
                bfs::remove_all(backup_dir_);
            }
        } catch (...) {
            // Ignore cleanup errors
        }
    }

    void CorruptSpecificFile(const std::string& filepath, size_t offset, size_t size) {
        std::fstream file(filepath, std::ios::binary | std::ios::in | std::ios::out);
        if (!file) return;

        file.seekp(offset);
        std::vector<char> garbage(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (auto& byte : garbage) {
            byte = static_cast<char>(dis(gen));
        }

        file.write(garbage.data(), garbage.size());
        file.close();
    }

    std::string data_dir_;
    std::string backup_dir_;
    KvOptions opts_;
    std::unique_ptr<EloqStore> store_;
};

TEST_CASE("Recovery after clean shutdown", "[persistence]") {
    RecoveryTestFixture fixture;
    TableIdent table{"test_table", 1};
    RandomGenerator gen(42);

    SECTION("Simple data persistence") {
        std::map<std::string, std::string> test_data;
        for (int i = 0; i < 10; ++i) {
            test_data["key_" + std::to_string(i)] = gen.GetValue(100, 1000);
        }

        // Write data and shutdown cleanly
        fixture.StartStore(true);
        REQUIRE(fixture.WriteData(table, test_data));
        fixture.StopStore();

        // Restart and verify data
        fixture.StartStore();
        REQUIRE(fixture.VerifyData(table, test_data));
    }

}

// TODO: Add more recovery tests once the basic API is fully working
// - Large dataset persistence
// - Multiple restart cycles
// - Crash scenarios
// - Corrupted file recovery
// - Incomplete operations
// - Long-term persistence stability