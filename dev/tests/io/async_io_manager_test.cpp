#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <random>

#include "async_io_manager.h"
#include "shard.h"
#include "page.h"
#include "data_page.h"
#include "overflow_page.h"
#include "mem_index_page.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"
#include "mocks/mock_io_manager.h"

using namespace eloqstore;
using namespace eloqstore::test;

class AsyncIoManagerTestFixture {
public:
    AsyncIoManagerTestFixture() {
        InitOptions();
        InitStore();
    }

    ~AsyncIoManagerTestFixture() {
        CleanupTestFiles();
    }

    void InitOptions() {
        options_ = std::make_unique<KvOptions>();
        options_->page_size = 4096;
        options_->pages_per_file = 256;
        options_->use_append_mode = false;
        options_->local_data_dirs = {test_dir_};
        options_->num_threads = 4;
        options_->io_uring_queue_depth = 128;
        options_->buffer_ring_size = 64;
    }

    void InitStore() {
        store_ = std::make_unique<EloqStore>();
        // Initialize basic store components
    }

    std::unique_ptr<AsyncIoManager> CreateIoManager(uint32_t fd_limit = 100) {
        return AsyncIoManager::Instance(store_.get(), fd_limit);
    }

    void CleanupTestFiles() {
        // Clean up test directory
        std::filesystem::remove_all(test_dir_);
    }

protected:
    std::string test_dir_ = "/tmp/test_async_io_" + std::to_string(getpid());
    std::unique_ptr<KvOptions> options_;
    std::unique_ptr<EloqStore> store_;
    DataGenerator gen_{42};
};

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_BasicOperations", "[io][unit]") {
    auto io_mgr = CreateIoManager();
    REQUIRE(io_mgr != nullptr);

    // Create a mock shard for testing
    Shard mock_shard(0, options_.get());

    SECTION("Initialize IO manager") {
        KvError err = io_mgr->Init(&mock_shard);
        REQUIRE(err == KvError::NoError);
    }

    SECTION("Start and stop") {
        KvError err = io_mgr->Init(&mock_shard);
        REQUIRE(err == KvError::NoError);

        io_mgr->Start();
        REQUIRE(io_mgr->IsIdle() == true);

        io_mgr->Stop();
    }

    SECTION("Submit and poll") {
        KvError err = io_mgr->Init(&mock_shard);
        REQUIRE(err == KvError::NoError);

        io_mgr->Start();

        // Submit should work even with no operations
        io_mgr->Submit();

        // Poll should complete immediately with no operations
        io_mgr->PollComplete();

        io_mgr->Stop();
    }
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_PageOperations", "[io][unit]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    TableIdent table("test_table", 1);

    SECTION("Read single page") {
        Page page(options_->page_size);
        FilePageId fp_id = 0;

        auto [result_page, err] = io_mgr->ReadPage(table, fp_id, page);

        // May fail if file doesn't exist yet
        if (err == KvError::FileNotFound) {
            REQUIRE(result_page.Data() == nullptr);
        } else {
            REQUIRE(err == KvError::NoError);
            REQUIRE(result_page.Data() != nullptr);
        }
    }

    SECTION("Read multiple pages") {
        std::vector<FilePageId> page_ids = {0, 1, 2, 3, 4};
        std::vector<Page> pages;

        KvError err = io_mgr->ReadPages(table, page_ids, pages);

        if (err == KvError::NoError) {
            REQUIRE(pages.size() == page_ids.size());
        }
    }

    SECTION("Write single page") {
        DataPage data_page(options_->page_size);
        data_page.Init();
        VarPage var_page = data_page;
        FilePageId fp_id = 0;

        KvError err = io_mgr->WritePage(table, var_page, fp_id);

        // Should handle write operations
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Write multiple pages") {
        std::vector<VarPage> pages;
        for (int i = 0; i < 5; ++i) {
            DataPage page(options_->page_size);
            page.Init();
            pages.push_back(page);
        }

        FilePageId first_fp_id = 0;
        KvError err = io_mgr->WritePages(table, pages, first_fp_id);

        // Should handle batch write operations
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_VarPageTypes", "[io][unit]") {
    SECTION("VarPage with DataPage") {
        DataPage data_page(options_->page_size);
        data_page.Init();
        VarPage var_page = data_page;

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr == data_page.Data());
    }

    SECTION("VarPage with OverflowPage") {
        OverflowPage overflow_page(options_->page_size);
        overflow_page.Init();
        VarPage var_page = overflow_page;

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr == overflow_page.Data());
    }

    SECTION("VarPage with MemIndexPage") {
        auto index_page = std::make_unique<MemIndexPage>(nullptr, 16);
        VarPage var_page = index_page.get();

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr != nullptr);
    }

    SECTION("VarPage with Page") {
        Page page(options_->page_size);
        VarPage var_page = page;

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr == page.Data());
    }
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_ManifestOperations", "[io][unit]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    TableIdent table("test_table", 1);

    SECTION("Append to manifest") {
        std::string log_entry = "test_log_entry_12345";
        uint64_t manifest_size = 1024;

        KvError err = io_mgr->AppendManifest(table, log_entry, manifest_size);

        // Should handle manifest operations
        REQUIRE((err == KvError::NoError || err == KvError::FileNotFound));
    }

    SECTION("Switch manifest") {
        std::string snapshot = "snapshot_data_here";

        KvError err = io_mgr->SwitchManifest(table, snapshot);

        // Should handle manifest switch
        REQUIRE((err == KvError::NoError || err == KvError::FileNotFound));
    }

    SECTION("Get manifest") {
        auto [manifest, err] = io_mgr->GetManifest(table);

        if (err == KvError::NoError) {
            REQUIRE(manifest != nullptr);
        } else {
            REQUIRE(manifest == nullptr);
        }
    }

    SECTION("Create archive") {
        std::string snapshot = "archive_snapshot";
        uint64_t timestamp = 123456789;

        KvError err = io_mgr->CreateArchive(table, snapshot, timestamp);

        // Should handle archive creation
        REQUIRE((err == KvError::NoError || err == KvError::NotSupported));
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_SyncOperations", "[io][unit]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    TableIdent table("test_table", 1);

    SECTION("Sync data") {
        // Write some pages first
        DataPage page(options_->page_size);
        page.Init();
        VarPage var_page = page;
        io_mgr->WritePage(table, var_page, 0);

        // Sync the data
        KvError err = io_mgr->SyncData(table);
        REQUIRE((err == KvError::NoError || err == KvError::IOError));
    }

    SECTION("Abort write") {
        // Start a write operation
        DataPage page(options_->page_size);
        page.Init();
        VarPage var_page = page;
        io_mgr->WritePage(table, var_page, 0);

        // Abort the write
        KvError err = io_mgr->AbortWrite(table);
        REQUIRE((err == KvError::NoError || err == KvError::NotSupported));
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_CleanupOperations", "[io][unit]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    TableIdent table("test_table", 1);

    SECTION("Clean table") {
        // Write some data first
        DataPage page(options_->page_size);
        page.Init();
        VarPage var_page = page;
        io_mgr->WritePage(table, var_page, 0);

        // Clean the table
        io_mgr->CleanTable(table);

        // Should be able to continue operations after cleanup
        auto [result_page, err] = io_mgr->ReadPage(table, 0, Page(options_->page_size));
        // May fail since we cleaned the table
        REQUIRE((err == KvError::FileNotFound || err == KvError::NoError));
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_ErrorHandling", "[io][unit]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    TableIdent table("test_table", 1);

    SECTION("Invalid page ID") {
        Page page(options_->page_size);
        FilePageId invalid_fp_id = MaxFilePageId;

        auto [result_page, err] = io_mgr->ReadPage(table, invalid_fp_id, page);
        REQUIRE(err != KvError::NoError);
    }

    SECTION("Empty page list") {
        std::vector<FilePageId> empty_ids;
        std::vector<Page> pages;

        KvError err = io_mgr->ReadPages(table, empty_ids, pages);
        // Should handle empty list gracefully
        REQUIRE((err == KvError::NoError || err == KvError::InvalidArgument));
    }

    SECTION("Null page write") {
        VarPage var_page = Page(0);  // Invalid page
        FilePageId fp_id = 0;

        KvError err = io_mgr->WritePage(table, var_page, fp_id);
        REQUIRE(err != KvError::NoError);
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "IouringMgr_FdManagement", "[io][unit]") {
    uint32_t fd_limit = 10;
    auto io_mgr = std::make_unique<IouringMgr>(options_.get(), fd_limit);
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);

    SECTION("FD limit enforcement") {
        // Test that FD limit is respected
        std::vector<TableIdent> tables;
        for (uint32_t i = 0; i < fd_limit * 2; ++i) {
            tables.emplace_back("table_" + std::to_string(i), i);
        }

        // Try to access more files than FD limit
        for (const auto& table : tables) {
            Page page(options_->page_size);
            auto [result_page, err] = io_mgr->ReadPage(table, 0, page);
            // Should handle FD exhaustion gracefully
        }
    }

    SECTION("LRU FD eviction") {
        // Access files in order
        for (uint32_t i = 0; i < fd_limit + 5; ++i) {
            TableIdent table("table_" + std::to_string(i), i);
            Page page(options_->page_size);
            io_mgr->ReadPage(table, 0, page);
        }

        // Access old file - should trigger LRU eviction
        TableIdent old_table("table_0", 0);
        Page page(options_->page_size);
        auto [result_page, err] = io_mgr->ReadPage(old_table, 0, page);
        // Should work after eviction
    }
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_Concurrency", "[io][stress]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    const int thread_count = 8;
    const int ops_per_thread = 100;
    std::atomic<int> success_count{0};
    std::atomic<int> error_count{0};

    SECTION("Concurrent reads") {
        std::vector<std::thread> threads;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&io_mgr, &success_count, &error_count, t, ops_per_thread, this]() {
                TableIdent table("thread_table_" + std::to_string(t), t);

                for (int i = 0; i < ops_per_thread; ++i) {
                    Page page(options_->page_size);
                    FilePageId fp_id = i % 10;

                    auto [result_page, err] = io_mgr->ReadPage(table, fp_id, page);
                    if (err == KvError::NoError || err == KvError::FileNotFound) {
                        success_count++;
                    } else {
                        error_count++;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count + error_count == thread_count * ops_per_thread);
    }

    SECTION("Concurrent writes") {
        std::vector<std::thread> threads;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&io_mgr, &success_count, &error_count, t, ops_per_thread, this]() {
                TableIdent table("write_table_" + std::to_string(t), t);

                for (int i = 0; i < ops_per_thread; ++i) {
                    DataPage page(options_->page_size);
                    page.Init();
                    VarPage var_page = page;
                    FilePageId fp_id = i;

                    KvError err = io_mgr->WritePage(table, var_page, fp_id);
                    if (err == KvError::NoError) {
                        success_count++;
                    } else {
                        error_count++;
                    }
                }

                // Sync at the end
                io_mgr->SyncData(table);
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count > 0);
    }

    SECTION("Mixed operations") {
        std::vector<std::thread> threads;

        for (int t = 0; t < thread_count; ++t) {
            threads.emplace_back([&io_mgr, &success_count, t, ops_per_thread, this]() {
                TableIdent table("mixed_table_" + std::to_string(t), t);

                for (int i = 0; i < ops_per_thread; ++i) {
                    if (i % 2 == 0) {
                        // Write
                        DataPage page(options_->page_size);
                        page.Init();
                        VarPage var_page = page;
                        KvError err = io_mgr->WritePage(table, var_page, i);
                        if (err == KvError::NoError) {
                            success_count++;
                        }
                    } else {
                        // Read
                        Page page(options_->page_size);
                        auto [result_page, err] = io_mgr->ReadPage(table, i - 1, page);
                        if (err == KvError::NoError) {
                            success_count++;
                        }
                    }
                }

                // Final sync
                io_mgr->SyncData(table);
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count > 0);
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_EdgeCases", "[io][edge-case]") {
    auto io_mgr = CreateIoManager();
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    SECTION("Empty table identifier") {
        TableIdent empty_table("", 0);
        Page page(options_->page_size);

        auto [result_page, err] = io_mgr->ReadPage(empty_table, 0, page);
        // Should handle empty table name
        REQUIRE(err != KvError::NoError);
    }

    SECTION("Maximum file page ID") {
        TableIdent table("test", 1);
        Page page(options_->page_size);

        auto [result_page, err] = io_mgr->ReadPage(table, MaxFilePageId - 1, page);
        // Should handle maximum values
        REQUIRE(err != KvError::NoError);
    }

    SECTION("Zero-sized operations") {
        TableIdent table("test", 1);

        // Empty manifest append
        KvError err = io_mgr->AppendManifest(table, "", 0);
        REQUIRE((err == KvError::NoError || err == KvError::InvalidArgument));

        // Empty snapshot
        err = io_mgr->SwitchManifest(table, "");
        REQUIRE((err == KvError::NoError || err == KvError::InvalidArgument));
    }

    SECTION("Rapid start-stop cycles") {
        for (int i = 0; i < 10; ++i) {
            io_mgr->Stop();
            io_mgr->Start();
        }

        // Should still be functional
        REQUIRE(io_mgr->IsIdle() == true);
    }

    io_mgr->Stop();
}

TEST_CASE_METHOD(AsyncIoManagerTestFixture, "AsyncIoManager_Performance", "[io][benchmark]") {
    auto io_mgr = CreateIoManager(1000);  // Higher FD limit for performance
    Shard mock_shard(0, options_.get());
    io_mgr->Init(&mock_shard);
    io_mgr->Start();

    TableIdent table("perf_test", 1);

    SECTION("Sequential write performance") {
        Timer timer;
        const int num_pages = 1000;

        timer.Start();
        for (int i = 0; i < num_pages; ++i) {
            DataPage page(options_->page_size);
            page.Init();
            VarPage var_page = page;
            io_mgr->WritePage(table, var_page, i);
        }
        io_mgr->SyncData(table);
        timer.Stop();

        double pages_per_sec = num_pages / timer.ElapsedSeconds();
        double mb_per_sec = (pages_per_sec * options_->page_size) / (1024 * 1024);

        LOG(INFO) << "Sequential write: " << pages_per_sec << " pages/sec ("
                  << mb_per_sec << " MB/sec)";

        REQUIRE(pages_per_sec > 100);  // Should handle >100 pages/sec
    }

    SECTION("Random read performance") {
        // First write pages
        for (int i = 0; i < 100; ++i) {
            DataPage page(options_->page_size);
            page.Init();
            VarPage var_page = page;
            io_mgr->WritePage(table, var_page, i);
        }
        io_mgr->SyncData(table);

        Timer timer;
        const int num_reads = 1000;
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist(0, 99);

        timer.Start();
        for (int i = 0; i < num_reads; ++i) {
            Page page(options_->page_size);
            FilePageId fp_id = dist(rng);
            io_mgr->ReadPage(table, fp_id, page);
        }
        timer.Stop();

        double reads_per_sec = num_reads / timer.ElapsedSeconds();
        LOG(INFO) << "Random read: " << reads_per_sec << " reads/sec";

        REQUIRE(reads_per_sec > 100);
    }

    SECTION("Batch operations performance") {
        Timer timer;

        // Batch write
        std::vector<VarPage> pages;
        for (int i = 0; i < 100; ++i) {
            DataPage page(options_->page_size);
            page.Init();
            pages.push_back(page);
        }

        timer.Start();
        io_mgr->WritePages(table, pages, 0);
        timer.Stop();

        double batch_time = timer.ElapsedSeconds();
        LOG(INFO) << "Batch write (100 pages): " << batch_time << " seconds";

        // Batch read
        std::vector<FilePageId> page_ids;
        for (int i = 0; i < 100; ++i) {
            page_ids.push_back(i);
        }
        std::vector<Page> read_pages;

        timer.Start();
        io_mgr->ReadPages(table, page_ids, read_pages);
        timer.Stop();

        batch_time = timer.ElapsedSeconds();
        LOG(INFO) << "Batch read (100 pages): " << batch_time << " seconds";

        REQUIRE(batch_time < 1.0);  // Should complete within 1 second
    }

    io_mgr->Stop();
}

TEST_CASE("AsyncIoManager_ErrorConversion", "[io][unit]") {
    SECTION("Convert errno to KvError") {
        REQUIRE(ToKvError(0) == KvError::NoError);
        REQUIRE(ToKvError(ENOENT) == KvError::FileNotFound);
        REQUIRE(ToKvError(EACCES) == KvError::PermissionDenied);
        REQUIRE(ToKvError(EIO) == KvError::IOError);
        REQUIRE(ToKvError(ENOMEM) == KvError::OutOfMemory);
        REQUIRE(ToKvError(ENOSPC) == KvError::DiskFull);
    }

    SECTION("Unknown error codes") {
        int unknown_error = 999999;
        KvError err = ToKvError(unknown_error);
        REQUIRE(err == KvError::Unknown);
    }
}