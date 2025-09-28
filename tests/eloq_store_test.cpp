#include "eloq_store.h"

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

#include "common.h"
#include "test_utils.h"

namespace fs = std::filesystem;

eloqstore::KvOptions CreateValidOptions(const fs::path &test_dir)
{
    eloqstore::KvOptions options;
    options.store_path = {test_dir};
    options.num_threads = 2;
    options.data_page_size = 4096;
    options.coroutine_stack_size = 8192;
    options.overflow_pointers = 4;
    options.max_write_batch_pages = 16;
    options.fd_limit = 100;
    return options;
}

fs::path CreateTestDir(const std::string &suffix = "")
{
    fs::path test_dir = fs::temp_directory_path() / ("eloqstore_test" + suffix);
    fs::create_directories(test_dir);
    return test_dir;
}

void CleanupTestDir(const fs::path &test_dir)
{
    if (fs::exists(test_dir))
    {
        fs::remove_all(test_dir);
    }
}
TEST_CASE("EloqStore ValidateOptions validates all parameters", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_validate_options");
    auto options = CreateValidOptions(test_dir);

    // Test valid configuration
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == true);

    // Test data_page_size that is not page-aligned
    options.data_page_size = 4097;  // not page-aligned
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options.data_page_size = 4096;  // restore valid value

    // Test coroutine_stack_size that is not page-aligned
    options.coroutine_stack_size = 8193;  // not page-aligned
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options.coroutine_stack_size = 8192;  // restore valid value

    // Test invalid overflow_pointers
    options.overflow_pointers = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options.overflow_pointers = 4;  // restore valid value

    // Test invalid max_write_batch_pages
    options.max_write_batch_pages = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options.max_write_batch_pages = 16;  // restore valid value

    // Test cloud storage configuration - local space limit
    options.local_space_limit =
        0;  // must set local space limit in cloud storage mode
    options.cloud_store_path = "test";
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options.local_space_limit = 1024 * 1024 * 1024;
    options.cloud_store_path = "";

    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == true);

    CleanupTestDir(test_dir);
}

TEST_CASE("EloqStore Start validates local store paths", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_start_store_space");
    auto options = CreateValidOptions(test_dir);

    // test safe path
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::NoError);
        store.Stop();
    }

    // test the non exist path
    fs::path nonexistent_path =
        fs::temp_directory_path() / "nonexistent_eloqstore_test";
    options.store_path = {nonexistent_path};
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::NoError);
        REQUIRE(fs::exists(nonexistent_path));
        REQUIRE(fs::is_directory(nonexistent_path));
        store.Stop();
    }

    // the path is file
    fs::path file_path = fs::temp_directory_path() / "eloqstore_file_test";
    std::ofstream file(file_path);
    file.close();
    options.store_path = {file_path};
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::InvalidArgs);
        store.Stop();
    }
    fs::remove(file_path);

    // not directory
    auto test_dir_with_file = CreateTestDir("_with_file");
    fs::path file_in_dir = test_dir_with_file / "not_a_directory.txt";
    std::ofstream file_in_dir_stream(file_in_dir);
    file_in_dir_stream.close();
    options.store_path = {test_dir_with_file};
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::InvalidArgs);
        store.Stop();
    }

    CleanupTestDir(test_dir);
    CleanupTestDir(nonexistent_path);
    CleanupTestDir(test_dir_with_file);
}

// test the basic life cycle
TEST_CASE("EloqStore basic lifecycle management", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_lifecycle");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    REQUIRE(store.IsStopped());

    auto err = store.Start();
    REQUIRE(err == eloqstore::KvError::NoError);
    REQUIRE_FALSE(store.IsStopped());

    store.Stop();
    REQUIRE(store.IsStopped());

    CleanupTestDir(test_dir);
}

// test repeat start
TEST_CASE("EloqStore handles multiple start calls", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_multi_start");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    // first start
    auto err1 = store.Start();
    REQUIRE(err1 == eloqstore::KvError::NoError);
    REQUIRE_FALSE(store.IsStopped());

    // the second should be safe
    auto err2 = store.Start();

    store.Stop();
    CleanupTestDir(test_dir);
}

// test repeat stop
TEST_CASE("EloqStore handles multiple stop calls", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_multi_stop");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    auto err = store.Start();
    REQUIRE(err == eloqstore::KvError::NoError);

    // first stop
    store.Stop();
    REQUIRE(store.IsStopped());

    // the second time should be safe
    REQUIRE_NOTHROW(store.Stop());
    REQUIRE(store.IsStopped());

    CleanupTestDir(test_dir);
}

TEST_CASE("EloqStore handles requests when stopped", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_stopped_requests");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    REQUIRE(store.IsStopped());

    eloqstore::ReadRequest request;
    eloqstore::TableIdent tbl_id("test_table", 0);
    request.SetArgs(tbl_id, "test_key");

    store.ExecSync(&request);
    REQUIRE(request.Error() == eloqstore::KvError::NotRunning);

    CleanupTestDir(test_dir);
}