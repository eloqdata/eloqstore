#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"
#include "utils.h"

using namespace test_util;
namespace fs = std::filesystem;
namespace chrono = std::chrono;
using std::chrono_literals::operator""ms;
using std::chrono_literals::operator""s;

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
    .cloud_endpoint = "http://127.0.0.1:9900",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

// Archive options for testing archive behavior
const eloqstore::KvOptions archive_gc_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // disable automatic archive scheduling
    .file_amplify_factor = 2,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-gc-archive"},
    .cloud_store_path = "eloqstore/gc-archive-test",
    .cloud_endpoint = "http://127.0.0.1:9900",
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

std::vector<std::string> ListLocalPartitionFiles(
    const eloqstore::KvOptions &opts, const eloqstore::TableIdent &tbl_id)
{
    std::vector<std::string> result;
    for (const std::string &store_path : opts.store_path)
    {
        fs::path partition_path = fs::path(store_path) / tbl_id.ToString();
        if (!fs::exists(partition_path))
        {
            continue;
        }
        for (const auto &entry : fs::directory_iterator(partition_path))
        {
            result.push_back(entry.path().filename().string());
        }
    }
    return result;
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
    // Exclude CURRENT_TERM.<branch> files, because they are never deleted
    // during GC.
    std::string_view branch_name;
    cloud_files.erase(
        std::remove_if(
            cloud_files.begin(),
            cloud_files.end(),
            [&branch_name](const std::string &file)
            { return eloqstore::ParseCurrentTermFilename(file, branch_name); }),
        cloud_files.end());
    return !cloud_files.empty();
}

// Helper function to wait for GC to complete
void WaitForGC(int seconds = 1)
{
    std::this_thread::sleep_for(chrono::seconds(seconds));
}

namespace
{
template <typename Pred>
bool WaitForCondition(std::chrono::milliseconds timeout,
                      std::chrono::milliseconds step,
                      Pred &&pred)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }
        std::this_thread::sleep_for(step);
    }
    return pred();
}
}  // namespace

TEST_CASE("local mode truncate removes local partition directory",
          "[gc][local]")
{
    CleanupStore(local_gc_opts);

    eloqstore::EloqStore *store = InitStore(local_gc_opts);
    eloqstore::TableIdent tbl_id = {"gc_test", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    tester.Upsert(0, 100);
    tester.Validate();
    REQUIRE(CheckLocalPartitionExists(local_gc_opts, tbl_id));

    tester.Truncate(0, true);

    REQUIRE(WaitForCondition(
        3s,
        20ms,
        [&]() { return !CheckLocalPartitionExists(local_gc_opts, tbl_id); }));

    store->Stop();
    CleanupStore(local_gc_opts);
}

TEST_CASE("local mode truncate removes local partition directory after wait",
          "[gc][local]")
{
    CleanupStore(local_gc_opts);

    eloqstore::EloqStore *store = InitStore(local_gc_opts);
    eloqstore::TableIdent tbl_id = {"gc_local_dir_cleanup", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    tester.Upsert(0, 100);
    tester.Validate();
    REQUIRE(CheckLocalPartitionExists(local_gc_opts, tbl_id));

    tester.Truncate(0, true);
    WaitForGC(2);

    REQUIRE_FALSE(CheckLocalPartitionExists(local_gc_opts, tbl_id));

    store->Stop();
    CleanupStore(local_gc_opts);
}

TEST_CASE("cloud mode truncate remote directory cleanup", "[gc][cloud]")
{
    eloqstore::KvOptions options = cloud_gc_opts;
    options.fd_limit += utils::CountUsedFD();

    eloqstore::EloqStore *store = InitStore(options);
    eloqstore::TableIdent tbl_id = {"gc_cloud_truncate", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    // Write some data to create cloud files
    tester.Upsert(0, 100);
    tester.Validate();

    // Verify cloud partition exists
    REQUIRE(CheckCloudPartitionExists(options, tbl_id));

    // Truncate the partition using MapVerifier (delete all data)
    tester.Truncate(0, true);  // Delete all data

    // // Wait for cloud GC to process
    WaitForGC(2);  // Cloud operations may take longer

    // Verify cloud partition directory is removed
    REQUIRE_FALSE(CheckCloudPartitionExists(options, tbl_id));
}

TEST_CASE("cloud mode truncate removes local partition directory after wait",
          "[gc][cloud]")
{
    CleanupStore(cloud_gc_opts);

    eloqstore::KvOptions options = cloud_gc_opts;
    options.fd_limit += utils::CountUsedFD();

    eloqstore::EloqStore *store = InitStore(options);
    eloqstore::TableIdent tbl_id = {"gc_cloud_local_dir_cleanup", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    tester.Upsert(0, 100);
    tester.Validate();
    REQUIRE(CheckLocalPartitionExists(options, tbl_id));

    tester.Truncate(0, true);
    WaitForGC(2);

    REQUIRE_FALSE(CheckLocalPartitionExists(options, tbl_id));

    store->Stop();
    CleanupStore(options);
}

TEST_CASE("cloud mode delete all data remote cleanup", "[gc][cloud]")
{
    CleanupStore(cloud_gc_opts);

    eloqstore::KvOptions options = cloud_gc_opts;
    options.fd_limit += utils::CountUsedFD();

    eloqstore::EloqStore *store = InitStore(options);
    eloqstore::TableIdent tbl_id = {"gc_cloud_delete", 1};
    MapVerifier tester(tbl_id, store, false);
    tester.SetValueSize(1000);

    // Write some data
    tester.Upsert(0, 100);
    tester.Validate();

    // Verify cloud partition exists
    REQUIRE(CheckCloudPartitionExists(options, tbl_id));

    // Delete all data
    tester.Delete(0, 100);
    tester.Validate();

    // // Wait for cloud GC to process
    WaitForGC(2);

    // Verify cloud partition directory is removed
    REQUIRE_FALSE(CheckCloudPartitionExists(options, tbl_id));

    CleanupStore(cloud_gc_opts);
}

TEST_CASE("archive prevents data deletion after truncate", "[gc][archive]")
{
    CleanupStore(archive_gc_opts);

    eloqstore::KvOptions options = archive_gc_opts;
    options.fd_limit += utils::CountUsedFD();

    eloqstore::EloqStore *store = InitStore(options);
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
    REQUIRE(CheckCloudPartitionExists(options, tbl_id));

    // Truncate after archive using MapVerifier (delete all data)
    tester.Truncate(0, true);  // Delete all data

    // // Wait for GC
    WaitForGC(2);

    // Data should NOT be deleted because archive exists
    if (!options.cloud_store_path.empty())
    {
        // For cloud mode, check that some files still exist (archive files)
        std::vector<std::string> remaining_files = ListCloudFiles(
            options, options.cloud_store_path, tbl_id.ToString());

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
        REQUIRE(CheckLocalPartitionExists(options, tbl_id));

        // Verify archive file exists in local directory
        fs::path partition_path =
            fs::path(options.store_path[0]) / tbl_id.ToString();
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

    eloqstore::KvOptions options = cloud_gc_opts;
    options.fd_limit += utils::CountUsedFD();

    eloqstore::EloqStore *store = InitStore(options);
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
        REQUIRE(CheckCloudPartitionExists(options, tbl_id));

        // Truncate using MapVerifier (delete all data)
        tester.Truncate(0, true);  // Delete all data

        // // Wait for cloud GC with directory purge
        WaitForGC(2);

        // Verify cloud directory is completely removed
        REQUIRE_FALSE(CheckCloudPartitionExists(options, tbl_id));
    }

    CleanupStore(cloud_gc_opts);
}
