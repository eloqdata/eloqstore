#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"

using namespace test_util;
namespace fs = std::filesystem;
namespace chrono = std::chrono;

// Local mode options for GC testing
const eloqstore::KvOptions local_gc_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .store_path = {"/tmp/test-gc-local"},
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

// Cloud mode options for GC testing
const eloqstore::KvOptions cloud_gc_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-gc-cloud"},
    .cloud_store_path = "eloqstore/gc-test",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

// Archive options for testing archive behavior
const eloqstore::KvOptions archive_gc_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // send archive request immediately
    .file_amplify_factor = 2,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-gc-archive"},
    .cloud_store_path = "eloqstore/gc-archive-test",
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};

// Helper function to check if local partition directory exists
bool CheckLocalPartitionExists(const eloqstore::KvOptions &opts,
                               const eloqstore::TableIdent &tbl_id)
{
    for (const std::string &store_path : opts.store_path)
    {
        fs::path partition_path = fs::path(store_path) / tbl_id.ToString();
        if (fs::exists(partition_path))
        {
            return true;
        }
    }
    return false;
}

// Helper function to check if cloud partition directory exists
bool CheckCloudPartitionExists(const eloqstore::KvOptions &opts,
                               const eloqstore::TableIdent &tbl_id)
{
    if (opts.cloud_store_path.empty())
    {
        return false;
    }

    std::vector<std::string> cloud_files =
        ListCloudFiles(opts, opts.cloud_store_path, tbl_id.ToString());

    LOG(INFO) << "CheckCloudPartitionExists, cloud_files size: "
              << cloud_files.size();
    for (const std::string &file : cloud_files)
    {
        LOG(INFO) << "CheckCloudPartitionExists, cloud_file: " << file;
    }
    // return !cloud_files.empty();
    // Exclude CURRENT_TERM file, because it never be deleted during GC.
    if (cloud_files.size() == 1)
    {
        REQUIRE(cloud_files[0] == eloqstore::CurrentTermFileName);
    }
    return cloud_files.size() > 1;
}

// Helper function to wait for GC to complete
void WaitForGC(int seconds = 1)
{
    std::this_thread::sleep_for(chrono::seconds(seconds));
}

// TEST_CASE("local mode truncate directory cleanup", "[gc][local]")
// {
//     eloqstore::EloqStore *store = InitStore(local_gc_opts);
//     eloqstore::TableIdent tbl_id = {"gc_test", 1};
//     MapVerifier tester(tbl_id, store, false);
//     tester.SetValueSize(1000);

//     // Write some data to create partition directory
//     tester.Upsert(0, 100);
//     tester.Validate();

//     // Verify partition directory exists
//     REQUIRE(CheckLocalPartitionExists(local_gc_opts, tbl_id));

//     // Truncate the partition using MapVerifier (delete all data)
//     tester.Truncate(0, true);  // Delete all data

//     // Wait for GC to process
//     WaitForGC();

//     // Verify partition directory is removed
//     REQUIRE_FALSE(CheckLocalPartitionExists(local_gc_opts, tbl_id));
// }

// TEST_CASE("local mode repeated write and truncate", "[gc][local]")
// {
//     CleanupStore(local_gc_opts);

//     eloqstore::EloqStore *store = InitStore(local_gc_opts);
//     eloqstore::TableIdent tbl_id = {"gc_repeat", 1};
//     MapVerifier tester(tbl_id, store, false);
//     tester.SetValueSize(1000);

//     // Repeat write and truncate operations
//     for (int i = 0; i < 3; i++)
//     {
//         // Write data
//         tester.Upsert(i * 100, (i + 1) * 100);
//         tester.Validate();

//         // Verify partition directory exists
//         REQUIRE(CheckLocalPartitionExists(local_gc_opts, tbl_id));

//         // Truncate using MapVerifier (delete all data)
//         tester.Truncate(0, true);  // Delete all data

//         // Wait for GC
//         WaitForGC();

//         // Verify directory is cleaned up
//         REQUIRE_FALSE(CheckLocalPartitionExists(local_gc_opts, tbl_id));
//     }
// }

// TEST_CASE("local mode delete all data cleanup", "[gc][local]")
// {
//     eloqstore::EloqStore *store = InitStore(local_gc_opts);
//     eloqstore::TableIdent tbl_id = {"gc_delete_all", 1};
//     MapVerifier tester(tbl_id, store, false);
//     tester.SetValueSize(1000);

//     // Write some data
//     tester.Upsert(0, 100);
//     tester.Validate();

//     // Verify partition directory exists
//     REQUIRE(CheckLocalPartitionExists(local_gc_opts, tbl_id));

//     // Delete all data
//     tester.Delete(0, 100);
//     tester.Validate();

//     // Wait for GC to process
//     WaitForGC();

//     // Verify partition directory is removed after deleting all data
//     REQUIRE_FALSE(CheckLocalPartitionExists(local_gc_opts, tbl_id));
// }

TEST_CASE("cloud mode truncate remote directory cleanup", "[gc][cloud]")
{
    eloqstore::EloqStore *store = InitStore(cloud_gc_opts);
    eloqstore::TableIdent tbl_id = {"gc_cloud_truncate", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    // Write some data to create cloud files
    tester.Upsert(0, 100);
    tester.Validate();

    // Verify cloud partition exists
    REQUIRE(CheckCloudPartitionExists(cloud_gc_opts, tbl_id));

    // Truncate the partition using MapVerifier (delete all data)
    tester.Truncate(0, true);  // Delete all data

    // // Wait for cloud GC to process
    WaitForGC(2);  // Cloud operations may take longer

    // Verify cloud partition directory is removed
    REQUIRE_FALSE(CheckCloudPartitionExists(cloud_gc_opts, tbl_id));
}

TEST_CASE("cloud mode delete all data remote cleanup", "[gc][cloud]")
{
    CleanupStore(cloud_gc_opts);

    eloqstore::EloqStore *store = InitStore(cloud_gc_opts);
    eloqstore::TableIdent tbl_id = {"gc_cloud_delete", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    // Write some data
    tester.Upsert(0, 100);
    tester.Validate();

    // Verify cloud partition exists
    REQUIRE(CheckCloudPartitionExists(cloud_gc_opts, tbl_id));

    // Delete all data
    tester.Delete(0, 100);
    tester.Validate();

    // // Wait for cloud GC to process
    WaitForGC(2);

    // Verify cloud partition directory is removed
    REQUIRE_FALSE(CheckCloudPartitionExists(cloud_gc_opts, tbl_id));

    CleanupStore(cloud_gc_opts);
}

TEST_CASE("archive prevents data deletion after truncate", "[gc][archive]")
{
    CleanupStore(archive_gc_opts);

    eloqstore::EloqStore *store = InitStore(archive_gc_opts);
    eloqstore::TableIdent tbl_id = {"gc_archive_test", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    // Write initial data
    tester.Upsert(0, 100);
    tester.Validate();

    // Create archive
    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(tbl_id);
    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    // Write more data after archive
    tester.Upsert(100, 200);
    tester.Validate();

    // Verify cloud partition exists
    REQUIRE(CheckCloudPartitionExists(archive_gc_opts, tbl_id));

    // Truncate after archive using MapVerifier (delete all data)
    tester.Truncate(0, true);  // Delete all data

    // // Wait for GC
    WaitForGC(2);

    // Data should NOT be deleted because archive exists
    if (!archive_gc_opts.cloud_store_path.empty())
    {
        // For cloud mode, check that some files still exist (archive files)
        std::vector<std::string> remaining_files =
            ListCloudFiles(archive_gc_opts,
                           archive_gc_opts.cloud_store_path,
                           tbl_id.ToString());

        // Should have at least the archive manifest file
        bool has_archive = false;
        for (const auto &file : remaining_files)
        {
            if (eloqstore::IsArchiveFile(file))
            {
                has_archive = true;
                break;
            }
        }
        REQUIRE(has_archive);
    }
    else
    {
        // For local mode, check that partition directory still exists with
        // archive
        REQUIRE(CheckLocalPartitionExists(archive_gc_opts, tbl_id));

        // Verify archive file exists in local directory
        fs::path partition_path =
            fs::path(archive_gc_opts.store_path[0]) / tbl_id.ToString();
        bool archive_found = false;

        if (fs::exists(partition_path))
        {
            for (const auto &entry : fs::directory_iterator(partition_path))
            {
                if (entry.is_regular_file())
                {
                    std::string filename = entry.path().filename().string();
                    if (eloqstore::IsArchiveFile(filename))
                    {
                        archive_found = true;
                        break;
                    }
                }
            }
        }
        REQUIRE(archive_found);
    }

    CleanupStore(archive_gc_opts);
}

TEST_CASE("cloud mode repeated truncate with directory purge", "[gc][cloud]")
{
    CleanupStore(cloud_gc_opts);

    eloqstore::EloqStore *store = InitStore(cloud_gc_opts);
    eloqstore::TableIdent tbl_id = {"gc_cloud_repeat", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    // Repeat write and truncate to test directory purge functionality
    for (int i = 0; i < 2; i++)
    {
        LOG(INFO) << "Repeat truncate iteration " << i;
        // Write data
        tester.Upsert(i * 100, (i + 1) * 100);
        tester.Validate();

        // Verify cloud partition exists
        REQUIRE(CheckCloudPartitionExists(cloud_gc_opts, tbl_id));

        // Truncate using MapVerifier (delete all data)
        tester.Truncate(0, true);  // Delete all data

        // // Wait for cloud GC with directory purge
        WaitForGC(2);

        // Verify cloud directory is completely removed
        REQUIRE_FALSE(CheckCloudPartitionExists(cloud_gc_opts, tbl_id));
    }

    CleanupStore(cloud_gc_opts);
}
