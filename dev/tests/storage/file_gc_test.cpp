#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <thread>
#include <chrono>
#include <set>
#include <random>

#include "file_gc.h"
#include "kv_options.h"
#include "types.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/temp_directory.h"

using namespace eloqstore;
using namespace eloqstore::test;
namespace fs = std::filesystem;

class FileGCTestFixture {
public:
    FileGCTestFixture() {
        InitOptions();
        CreateTestDirectory();
    }

    ~FileGCTestFixture() {
        CleanupTestDirectory();
    }

    void InitOptions() {
        options_ = std::make_unique<KvOptions>();
        options_->page_size = 4096;
        options_->pages_per_file = 256;
        options_->local_data_dirs = {test_dir_.string()};
    }

    void CreateTestDirectory() {
        test_dir_ = fs::temp_directory_path() / ("file_gc_test_" + std::to_string(getpid()));
        fs::create_directories(test_dir_);
    }

    void CleanupTestDirectory() {
        fs::remove_all(test_dir_);
    }

    void CreateTestFiles(const TableIdent& table, const std::set<FileId>& file_ids) {
        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        fs::create_directories(table_dir);

        for (FileId id : file_ids) {
            fs::path file_path = table_dir / std::to_string(id);
            std::ofstream file(file_path);
            file << "test data for file " << id;
            file.close();
        }
    }

    std::set<FileId> GetExistingFiles(const TableIdent& table) {
        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        std::set<FileId> files;

        if (fs::exists(table_dir)) {
            for (const auto& entry : fs::directory_iterator(table_dir)) {
                if (entry.is_regular_file()) {
                    try {
                        FileId id = std::stoull(entry.path().filename().string());
                        files.insert(id);
                    } catch (...) {
                        // Ignore non-numeric files
                    }
                }
            }
        }

        return files;
    }

protected:
    fs::path test_dir_;
    std::unique_ptr<KvOptions> options_;
};

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_GetRetainedFiles", "[file-gc][unit]") {
    SECTION("Empty mapping table") {
        std::vector<uint64_t> mapping_table;
        std::unordered_set<FileId> retained;

        GetRetainedFiles(retained, mapping_table, 8);  // 256 pages per file

        REQUIRE(retained.empty());
    }

    SECTION("Single file referenced") {
        std::vector<uint64_t> mapping_table;
        // Add file page IDs that map to file 0
        for (int i = 0; i < 10; ++i) {
            mapping_table.push_back(i);  // Pages 0-9 are in file 0
        }

        std::unordered_set<FileId> retained;
        GetRetainedFiles(retained, mapping_table, 8);

        REQUIRE(retained.size() == 1);
        REQUIRE(retained.count(0) == 1);
    }

    SECTION("Multiple files referenced") {
        std::vector<uint64_t> mapping_table;
        // Add pages from different files
        mapping_table.push_back(100);   // File 0
        mapping_table.push_back(300);   // File 1
        mapping_table.push_back(600);   // File 2
        mapping_table.push_back(1000);  // File 3

        std::unordered_set<FileId> retained;
        GetRetainedFiles(retained, mapping_table, 8);

        REQUIRE(retained.size() == 4);
        REQUIRE(retained.count(0) == 1);
        REQUIRE(retained.count(1) == 1);
        REQUIRE(retained.count(2) == 1);
        REQUIRE(retained.count(3) == 1);
    }

    SECTION("Invalid page IDs") {
        std::vector<uint64_t> mapping_table;
        mapping_table.push_back(MaxFilePageId);  // Invalid
        mapping_table.push_back(100);            // Valid

        std::unordered_set<FileId> retained;
        GetRetainedFiles(retained, mapping_table, 8);

        // Should only include valid file
        REQUIRE(retained.count(0) == 1);
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_BasicOperations", "[file-gc][unit]") {
    FileGarbageCollector gc(options_.get());

    SECTION("Start and stop") {
        gc.Start(2);  // Start with 2 workers
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        gc.Stop();
    }

    SECTION("Add single task") {
        gc.Start(1);

        TableIdent table("test_table", 1);
        std::unordered_set<FileId> retained = {1, 2, 3};

        bool added = gc.AddTask(table, 1000, 10, retained);
        REQUIRE(added == true);

        gc.Stop();
    }

    SECTION("Add multiple tasks") {
        gc.Start(2);

        for (int i = 0; i < 10; ++i) {
            TableIdent table("table_" + std::to_string(i), i);
            std::unordered_set<FileId> retained = {i, i + 1};

            bool added = gc.AddTask(table, 1000 + i, 100, retained);
            REQUIRE(added == true);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        gc.Stop();
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_Execute", "[file-gc][unit]") {
    TableIdent table("gc_test", 1);

    SECTION("Clean unreferenced files") {
        // Create test files 0-9
        std::set<FileId> all_files = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        CreateTestFiles(table, all_files);

        // Only retain files 2, 5, 7
        std::unordered_set<FileId> retained = {2, 5, 7};

        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 10, retained);

        // Should succeed
        REQUIRE(err == KvError::NoError);

        // Check remaining files
        auto remaining = GetExistingFiles(table);
        REQUIRE(remaining == std::set<FileId>{2, 5, 7});
    }

    SECTION("No files to clean") {
        // Create only retained files
        std::set<FileId> files = {1, 2, 3};
        CreateTestFiles(table, files);

        std::unordered_set<FileId> retained = {1, 2, 3};

        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 5, retained);

        REQUIRE(err == KvError::NoError);

        // All files should remain
        auto remaining = GetExistingFiles(table);
        REQUIRE(remaining == files);
    }

    SECTION("Clean all files") {
        // Create files but retain none
        std::set<FileId> files = {1, 2, 3, 4, 5};
        CreateTestFiles(table, files);

        std::unordered_set<FileId> retained;  // Empty

        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 10, retained);

        REQUIRE(err == KvError::NoError);

        // All files should be deleted
        auto remaining = GetExistingFiles(table);
        REQUIRE(remaining.empty());
    }

    SECTION("Non-existent directory") {
        TableIdent missing_table("missing", 999);
        std::unordered_set<FileId> retained = {1, 2};

        fs::path table_dir = test_dir_ / (missing_table.name + "_" + std::to_string(missing_table.partition));
        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 10, retained);

        // Should handle gracefully
        REQUIRE((err == KvError::NoError || err == KvError::FileNotFound));
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_ConcurrentTasks", "[file-gc][stress]") {
    FileGarbageCollector gc(options_.get());
    gc.Start(4);  // 4 worker threads

    const int num_tables = 20;
    std::vector<TableIdent> tables;

    // Create tables and files
    for (int i = 0; i < num_tables; ++i) {
        TableIdent table("concurrent_" + std::to_string(i), i);
        tables.push_back(table);

        std::set<FileId> files;
        for (FileId j = 0; j < 10; ++j) {
            files.insert(j);
        }
        CreateTestFiles(table, files);
    }

    // Submit GC tasks concurrently
    std::vector<std::thread> submitters;
    for (int i = 0; i < num_tables; ++i) {
        submitters.emplace_back([&gc, &tables, i]() {
            std::unordered_set<FileId> retained;
            // Retain only even numbered files
            for (FileId j = 0; j < 10; j += 2) {
                retained.insert(j);
            }

            bool added = gc.AddTask(tables[i], 1000 + i, 100, retained);
            REQUIRE(added == true);
        });
    }

    for (auto& t : submitters) {
        t.join();
    }

    // Wait for GC to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    gc.Stop();

    // Verify some tables were cleaned
    for (const auto& table : tables) {
        auto remaining = GetExistingFiles(table);
        // Should have cleaned odd-numbered files
        for (FileId id : remaining) {
            REQUIRE(id % 2 == 0);
        }
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_GcTask", "[file-gc][unit]") {
    using GcTask = FileGarbageCollector::GcTask;

    SECTION("Normal task") {
        TableIdent table("test", 1);
        std::unordered_set<FileId> retained = {1, 2, 3};

        GcTask task(table, 1000, 100, retained);

        REQUIRE(task.IsStopSignal() == false);
        REQUIRE(task.tbl_id_.name == "test");
        REQUIRE(task.tbl_id_.partition == 1);
        REQUIRE(task.mapping_ts_ == 1000);
        REQUIRE(task.max_file_id_ == 100);
        REQUIRE(task.retained_files_.size() == 3);
    }

    SECTION("Stop signal task") {
        GcTask stop_task;
        // Default constructed task might be used as stop signal
        // Check implementation for exact stop signal detection
    }

    SECTION("Empty retained set") {
        TableIdent table("empty", 1);
        std::unordered_set<FileId> empty_retained;

        GcTask task(table, 2000, 50, empty_retained);

        REQUIRE(task.retained_files_.empty());
        REQUIRE(task.IsStopSignal() == false);
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_EdgeCases", "[file-gc][edge-case]") {
    SECTION("Maximum file ID") {
        FileGarbageCollector gc(options_.get());
        gc.Start(1);

        TableIdent table("max_file", 1);
        std::unordered_set<FileId> retained = {MaxFileId - 1};

        bool added = gc.AddTask(table, 1000, MaxFileId, retained);
        REQUIRE(added == true);

        gc.Stop();
    }

    SECTION("Very large retained set") {
        FileGarbageCollector gc(options_.get());
        gc.Start(2);

        TableIdent table("large_retained", 1);
        std::unordered_set<FileId> retained;

        // Add 10000 retained files
        for (FileId i = 0; i < 10000; ++i) {
            retained.insert(i);
        }

        bool added = gc.AddTask(table, 1000, 20000, retained);
        REQUIRE(added == true);

        gc.Stop();
    }

    SECTION("Rapid start-stop") {
        FileGarbageCollector gc(options_.get());

        for (int i = 0; i < 10; ++i) {
            gc.Start(2);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            gc.Stop();
        }
    }

    SECTION("Files with invalid names") {
        TableIdent table("invalid_files", 1);
        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        fs::create_directories(table_dir);

        // Create files with non-numeric names
        std::ofstream(table_dir / "not_a_number.txt") << "data";
        std::ofstream(table_dir / "manifest") << "data";
        std::ofstream(table_dir / ".hidden") << "data";

        // Create valid files
        std::ofstream(table_dir / "1") << "data";
        std::ofstream(table_dir / "2") << "data";

        std::unordered_set<FileId> retained = {1};

        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 10, retained);

        REQUIRE(err == KvError::NoError);

        // Should only affect numeric files
        REQUIRE(fs::exists(table_dir / "1"));
        REQUIRE(!fs::exists(table_dir / "2"));
        REQUIRE(fs::exists(table_dir / "not_a_number.txt"));
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_Performance", "[file-gc][benchmark]") {
    SECTION("Large directory scan") {
        TableIdent table("perf_test", 1);

        // Create many files
        std::set<FileId> files;
        for (FileId i = 0; i < 1000; ++i) {
            files.insert(i);
        }
        CreateTestFiles(table, files);

        // Retain only 10% of files
        std::unordered_set<FileId> retained;
        for (FileId i = 0; i < 100; ++i) {
            retained.insert(i * 10);
        }

        Timer timer;
        timer.Start();

        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));
        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 1000, retained);

        timer.Stop();

        REQUIRE(err == KvError::NoError);

        double files_per_sec = 900 / timer.ElapsedSeconds();  // 900 files deleted
        LOG(INFO) << "GC throughput: " << files_per_sec << " files/sec";

        // Verify cleanup
        auto remaining = GetExistingFiles(table);
        REQUIRE(remaining.size() == 100);
    }

    SECTION("Concurrent GC operations") {
        FileGarbageCollector gc(options_.get());
        gc.Start(8);  // Many workers

        Timer timer;
        const int num_tasks = 100;

        timer.Start();

        for (int i = 0; i < num_tasks; ++i) {
            TableIdent table("concurrent_" + std::to_string(i), i);
            std::unordered_set<FileId> retained = {1, 2, 3};

            gc.AddTask(table, 1000 + i, 100, retained);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        gc.Stop();

        timer.Stop();

        double tasks_per_sec = num_tasks / timer.ElapsedSeconds();
        LOG(INFO) << "GC task throughput: " << tasks_per_sec << " tasks/sec";
    }
}

TEST_CASE_METHOD(FileGCTestFixture, "FileGC_ErrorHandling", "[file-gc][unit]") {
    SECTION("Permission denied") {
        // This test would require setting up files with restricted permissions
        // Platform-specific implementation needed
    }

    SECTION("Corrupted file system") {
        // Simulate file system issues
        TableIdent table("corrupted", 1);
        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));

        // Create directory as a file (invalid)
        std::ofstream(table_dir) << "not a directory";

        std::unordered_set<FileId> retained = {1};

        KvError err = FileGarbageCollector::Execute(
            options_.get(), table_dir, 1000, 10, retained);

        REQUIRE(err != KvError::NoError);
    }

    SECTION("Race with file creation") {
        TableIdent table("race_test", 1);
        std::set<FileId> initial_files = {1, 2, 3, 4, 5};
        CreateTestFiles(table, initial_files);

        std::unordered_set<FileId> retained = {1, 2};
        fs::path table_dir = test_dir_ / (table.name + "_" + std::to_string(table.partition));

        // Start GC in background
        std::thread gc_thread([this, &table_dir, &retained]() {
            FileGarbageCollector::Execute(
                options_.get(), table_dir, 1000, 10, retained);
        });

        // Concurrently create new files
        std::thread create_thread([this, &table]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            std::set<FileId> new_files = {10, 11, 12};
            CreateTestFiles(table, new_files);
        });

        gc_thread.join();
        create_thread.join();

        // New files should not be deleted
        auto remaining = GetExistingFiles(table);
        REQUIRE(remaining.count(10) == 1);
        REQUIRE(remaining.count(11) == 1);
        REQUIRE(remaining.count(12) == 1);
    }
}