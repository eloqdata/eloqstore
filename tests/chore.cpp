#include <glog/logging.h>

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

#include "common.h"
#include "test_utils.h"

using namespace eloqstore;
using test_util::MapVerifier;
TEST_CASE("file size tests - different page sizes", "[chore][file_size]")
{
    // Test different page sizes (must be power of 2 and aligned)
    std::vector<uint16_t> page_sizes = {
        1 << 12,  // 4KB
        1 << 13,  // 8KB
        1 << 14,  // 16KB
        1 << 15,  // 32KB
    };

    for (uint16_t page_size : page_sizes)
    {
        KvOptions opts = default_opts;
        opts.data_page_size = page_size;
        opts.pages_per_file_shift = 10;  // 1024 pages per file
        opts.data_append_mode = true;

        EloqStore *store = InitStore(opts);

        // Write some data to trigger file creation
        MapVerifier tester(test_tbl_id, store, false);
        tester.Upsert(0, 100);  // Write 100 entries

        REQUIRE(ValidateFileSizes(opts));
    }
}

TEST_CASE("file size tests - different file sizes", "[chore][file_size]")
{
    // Test different file sizes through pages_per_file_shift
    std::vector<std::pair<uint8_t, std::string>> file_configs = {
        {8, "1MB"},     // 2^8 = 256 pages * 4KB = 1MB
        {10, "4MB"},    // 2^10 = 1024 pages * 4KB = 4MB
        {12, "16MB"},   // 2^12 = 4096 pages * 4KB = 16MB
        {14, "64MB"},   // 2^14 = 16384 pages * 4KB = 64MB
        {16, "256MB"},  // 2^16 = 65536 pages * 4KB = 256MB
        {18, "1GB"},    // 2^18 = 262144 pages * 4KB = 1GB
    };

    for (auto [shift, size_desc] : file_configs)
    {
        KvOptions opts = default_opts;
        opts.data_page_size = 1 << 12;  // 4KB
        opts.pages_per_file_shift = shift;
        opts.data_append_mode = true;

        EloqStore *store = InitStore(opts);

        // Write data to trigger file creation
        MapVerifier tester(test_tbl_id, store, false);
        tester.Upsert(0, 50);  // Write 50 entries

        // Log expected file size
        size_t expected_file_size = opts.DataFileSize();

        REQUIRE(ValidateFileSizes(opts));
    }
}

TEST_CASE("file size tests - massive data injection", "[chore][file_size]")
{
    KvOptions opts = default_opts;
    opts.data_page_size = 1 << 12;   // 4KB
    opts.pages_per_file_shift = 16;  // 256MB files
    opts.data_append_mode = true;
    opts.manifest_limit = 16 << 20;  // 16MB manifest limit

    EloqStore *store = InitStore(opts);

    // Inject massive amount of data to test file size limits
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(1024);  // 1KB values

    // Write data in batches to fill multiple files
    const int batch_size = 1000;
    const int num_batches = 10;

    for (int batch = 0; batch < num_batches; batch++)
    {
        uint64_t start_key = batch * batch_size;
        uint64_t end_key = start_key + batch_size;

        tester.Upsert(start_key, end_key);

        // Validate file sizes after each batch
        if ((batch + 1) % 3 == 0)  // Check every 3 batches
        {
            REQUIRE(ValidateFileSizes(opts));
        }
    }

    REQUIRE(ValidateFileSizes(opts));
}

TEST_CASE("file size tests - extreme page count per file", "[chore][file_size]")
{
    // Test with maximum reasonable pages_per_file_shift
    std::vector<std::pair<uint8_t, std::string>> extreme_configs = {
        {19, "2GB"}, {20, "4GB"}, {21, "8GB"}};

    for (auto [shift, size_desc] : extreme_configs)
    {
        KvOptions opts = default_opts;
        opts.data_page_size = 1 << 12;  // 4KB
        opts.pages_per_file_shift = shift;
        opts.data_append_mode = true;

        EloqStore *store = InitStore(opts);

        // Write minimal data just to create the file structure
        MapVerifier tester(test_tbl_id, store, false);
        tester.Upsert(0, 10);  // Write only 10 entries

        // Log expected vs actual
        size_t expected_file_size = opts.DataFileSize();

        REQUIRE(ValidateFileSizes(opts));
    }
}

TEST_CASE("file size tests - mixed page and file size combinations",
          "[chore][file_size]")
{
    // Test various combinations of page size and file size
    std::vector<std::tuple<uint16_t, uint8_t, std::string>> combinations = {
        {1 << 12, 10, "4KB pages, 4MB files"},     // 4KB * 1024 = 4MB
        {1 << 13, 11, "8KB pages, 16MB files"},    // 8KB * 2048 = 16MB
        {1 << 14, 12, "16KB pages, 64MB files"},   // 16KB * 4096 = 64MB
        {1 << 15, 13, "32KB pages, 256MB files"},  // 32KB * 8192 = 256MB
    };

    for (auto [page_size, shift, desc] : combinations)
    {
        KvOptions opts = default_opts;
        opts.data_page_size = page_size;
        opts.pages_per_file_shift = shift;
        opts.data_append_mode = true;

        EloqStore *store = InitStore(opts);

        // Write data proportional to page size
        MapVerifier tester(test_tbl_id, store, false);
        tester.SetValueSize(page_size / 8);  // Value size = 1/8 of page size
        tester.Upsert(0, 100);

        // Log configuration
        size_t expected_file_size = opts.DataFileSize();

        // Validate
        REQUIRE(ValidateFileSizes(opts));
    }
}
