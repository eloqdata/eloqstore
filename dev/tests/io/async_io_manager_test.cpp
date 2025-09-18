#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <random>
#include <filesystem>

#include "async_io_manager.h"
#include "shard.h"
#include "page.h"
#include "data_page.h"
#include "mem_index_page.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

class SimpleAsyncIoManagerTestFixture {
public:
    SimpleAsyncIoManagerTestFixture() {
        InitOptions();
        InitStore();
    }

    ~SimpleAsyncIoManagerTestFixture() {
        CleanupTestFiles();
    }

    void InitOptions() {
        options_ = std::make_unique<KvOptions>();
        options_->data_page_size = 4096;
        options_->pages_per_file_shift = 8;  // 2^8 = 256
        options_->data_append_mode = false;
        options_->store_path = {test_dir_};
        options_->num_threads = 4;
        options_->io_queue_size = 128;
        options_->buf_ring_size = 64;
    }

    void InitStore() {
        store_ = std::make_unique<EloqStore>(*options_);
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

TEST_CASE_METHOD(SimpleAsyncIoManagerTestFixture, "AsyncIoManager_Basic", "[io][unit]") {
    auto io_mgr = AsyncIoManager::Instance(store_.get(), 100);
    REQUIRE(io_mgr != nullptr);

    SECTION("Create IO manager instance") {
        // Basic instance creation test
        REQUIRE(io_mgr.get() != nullptr);
    }
}

TEST_CASE_METHOD(SimpleAsyncIoManagerTestFixture, "AsyncIoManager_VarPageTypes", "[io][unit]") {
    SECTION("VarPage with DataPage") {
        DataPage data_page(0, Page(true));
        VarPage var_page = std::move(data_page);

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr != nullptr);
    }

    SECTION("VarPage with OverflowPage") {
        OverflowPage overflow_page(0, Page(true));
        VarPage var_page = std::move(overflow_page);

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr != nullptr);
    }

    SECTION("VarPage with MemIndexPage") {
        auto index_page = std::make_unique<MemIndexPage>(true);
        VarPage var_page = index_page.get();

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr != nullptr);
    }

    SECTION("VarPage with Page") {
        Page page(true);
        VarPage var_page = std::move(page);

        char* ptr = VarPagePtr(var_page);
        REQUIRE(ptr != nullptr);
    }
}

TEST_CASE("AsyncIoManager_ErrorConversion", "[io][unit]") {
    SECTION("Convert errno to KvError") {
        REQUIRE(ToKvError(0) == KvError::NoError);
        REQUIRE(ToKvError(ENOENT) == KvError::NotFound);
        REQUIRE(ToKvError(EACCES) == KvError::NoPermission);
        REQUIRE(ToKvError(EIO) == KvError::IoFail);
        REQUIRE(ToKvError(ENOMEM) == KvError::OutOfMem);
        REQUIRE(ToKvError(ENOSPC) == KvError::OutOfSpace);
    }

    SECTION("Unknown error codes") {
        int unknown_error = 999999;
        KvError err = ToKvError(unknown_error);
        REQUIRE(err == KvError::IoFail);
    }
}