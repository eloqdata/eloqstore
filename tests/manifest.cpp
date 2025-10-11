#include <algorithm>
#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>

#include "kv_options.h"
#include "test_utils.h"
#include "tests/common.h"
#include "utils.h"

using namespace test_util;
namespace fs = std::filesystem;

TEST_CASE("simple manifest recovery", "[manifest]")
{
    eloqstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    verifier.NewMapping();
    verifier.NewMapping();
    verifier.UpdateMapping();
    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();

    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();
}

TEST_CASE("medium manifest recovery", "[manifest]")
{
    eloqstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    for (int i = 0; i < 100; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();
    }
    verifier.Verify();

    verifier.Snapshot();
    verifier.Verify();

    for (int i = 0; i < 10; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();

        verifier.Verify();
    }
}

TEST_CASE("detect manifest corruption", "[manifest]")
{
    // TODO:
}

TEST_CASE("create archives", "[archive]")
{
    eloqstore::EloqStore *store = InitStore(archive_opts);

    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(10000);

    // Write some data first
    tester.WriteRnd(0, 1000, 50, 80);
    tester.Validate();

    // Manually create and execute ArchiveRequest
    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(test_tbl_id);

    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);

    // Wait for archive to complete
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    // Verify archive file exists
    const fs::path partition_path =
        fs::path(test_path) / test_tbl_id.ToString();
    bool archive_found = false;

    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (filename.find("manifest_") == 0)
            {
                archive_found = true;
                break;
            }
        }
    }

    REQUIRE(archive_found);

    // Test multiple archives for the same partition
    for (int i = 0; i < 3; i++)
    {
        // Write more data
        tester.WriteRnd(1000 + i * 100, 1100 + i * 100, 50, 80);

        // Create another archive
        eloqstore::ArchiveRequest another_req;
        another_req.SetTableId(test_tbl_id);

        ok = store->ExecAsyn(&another_req);
        REQUIRE(ok);

        another_req.Wait();
        REQUIRE(another_req.Error() == eloqstore::KvError::NoError);
    }

    // Count archive files
    int archive_count = 0;
    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (filename.find("manifest_") == 0)
            {
                archive_count++;
            }
        }
    }

    // Should have multiple archives (exact count depends on retention policy)
    REQUIRE(archive_count >= 1);

    tester.Validate();
}

TEST_CASE("easy rollback to archive", "[archive]")
{
    eloqstore::EloqStore *store = InitStore(archive_opts);
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(1000);

    tester.Upsert(0, 10);
    tester.Validate();

    auto old_dataset = tester.DataSet();
    REQUIRE(old_dataset.size() == 10);

    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(test_tbl_id);

    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);

    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    tester.Upsert(10, 20);
    tester.Validate();

    auto full_dataset = tester.DataSet();
    REQUIRE(full_dataset.size() == 20);

    store->Stop();

    std::string archive_file;
    const fs::path partition_path =
        fs::path(test_path) / test_tbl_id.ToString();
    std::string manifest_path = (partition_path / "manifest").string();

    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (filename.find("manifest_") == 0)
            {
                archive_file = entry.path().string();
                break;
            }
        }
    }

    REQUIRE(!archive_file.empty());

    std::string backup_manifest = manifest_path + "_backup";
    fs::copy_file(manifest_path, backup_manifest);

    // roll back archive to manifest
    fs::copy_file(
        archive_file, manifest_path, fs::copy_options::overwrite_existing);

    LOG(INFO) << "roll back to archive: " << archive_file;
    store->Start();

    tester.SwitchDataSet(old_dataset);
    tester.Validate();

    store->Stop();

    // roll back to full dataset
    fs::copy_file(
        backup_manifest, manifest_path, fs::copy_options::overwrite_existing);
    fs::remove(backup_manifest);

    LOG(INFO) << "roll back to full dataset";
    store->Start();

    tester.SwitchDataSet(full_dataset);
    tester.Validate();
}

TEST_CASE("enhanced rollback with mix operations", "[archive]")
{
    eloqstore::EloqStore *store = InitStore(archive_opts);
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(2000);

    // Phase 1: Initial data with mixed operations
    tester.Upsert(0, 1000);     // Write 1000 entries
    tester.Delete(200, 400);    // Delete some entries
    tester.Upsert(1000, 1500);  // Add more entries
    tester.WriteRnd(
        1500, 2000, 30, 70);  // Random write with 30% delete probability
    tester.Validate();

    auto phase1_dataset = tester.DataSet();
    LOG(INFO) << "Phase 1 dataset size: " << phase1_dataset.size();

    // Create archive after phase 1
    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(test_tbl_id);
    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    // Phase 2: More complex operations
    tester.Delete(0, 100);                // Delete from beginning
    tester.Upsert(2000, 2500);            // Add new range
    tester.Delete(1200, 1300);            // Delete from middle
    tester.WriteRnd(2500, 3000, 50, 80);  // More random operations

    // Mixed read/write operations like batch_write.cpp
    for (int i = 0; i < 10; i++)
    {
        tester.WriteRnd(3000 + i * 100, 3100 + i * 100, 25, 60);
        // Verify with reads
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = std::rand() % 2000;
            tester.Scan(start, start + 50);
            tester.Read(std::rand() % 3000);
            tester.Floor(std::rand() % 3000);
        }
    }
    tester.Validate();

    auto phase2_dataset = tester.DataSet();
    LOG(INFO) << "Phase 2 dataset size: " << phase2_dataset.size();

    store->Stop();

    // Find and rollback to archive
    std::string archive_file;
    const fs::path partition_path =
        fs::path(test_path) / test_tbl_id.ToString();
    std::string manifest_path = (partition_path / "manifest").string();

    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (filename.find("manifest_") == 0)
            {
                archive_file = entry.path().string();
                break;
            }
        }
    }
    REQUIRE(!archive_file.empty());

    std::string backup_manifest = manifest_path + "_backup";
    fs::copy_file(manifest_path, backup_manifest);
    fs::copy_file(
        archive_file, manifest_path, fs::copy_options::overwrite_existing);

    LOG(INFO) << "Rollback to archive: " << archive_file;
    store->Start();

    // Verify rollback to phase 1 state
    tester.SwitchDataSet(phase1_dataset);
    tester.Validate();

    store->Stop();

    // Restore to phase 2 state
    fs::copy_file(
        backup_manifest, manifest_path, fs::copy_options::overwrite_existing);
    fs::remove(backup_manifest);
    store->Start();

    tester.SwitchDataSet(phase2_dataset);
    tester.Validate();
}

TEST_CASE("manifest deletion on rootmeta eviction", "[manifest][eviction]")
{
    eloqstore::KvOptions opts = {
        .index_buffer_pool_size = 15,  // small value
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .data_append_mode = true,
    };

    eloqstore::EloqStore *store = InitStore(opts);

    eloqstore::TableIdent partition_a{"test_table", 1};
    eloqstore::TableIdent partition_b{"test_table", 2};

    MapVerifier verifier_a(partition_a, store, false);
    MapVerifier verifier_b(partition_b, store, false);

    const fs::path partition_a_path =
        fs::path(test_path) / partition_a.ToString();
    const std::string manifest_path = (partition_a_path / "manifest").string();

    LOG(INFO) << "Testing manifest deletion for partition: "
              << partition_a.ToString();
    LOG(INFO) << "Manifest path: " << manifest_path;

    LOG(INFO) << "Phase 1: Writing data to partition A";
    verifier_a.Upsert(0, 100);
    verifier_a.Validate();

    REQUIRE(fs::exists(manifest_path));
    LOG(INFO) << "Manifest file created successfully";

    LOG(INFO) << "Phase 2: Truncating partition A";
    verifier_a.Truncate(0, true);  // delete_all = true

    REQUIRE(fs::exists(manifest_path));
    LOG(INFO) << "Manifest still exists after truncate (expected)";

    LOG(INFO) << "Phase 3: Writing data to partition B to trigger eviction";

    bool manifest_deleted = false;
    int iteration = 0;
    const int max_iterations = 100;

    for (iteration = 0; iteration < max_iterations; iteration++)
    {
        // each time a batch of data is written
        int start = iteration * 50;
        int end = start + 50;

        LOG(INFO) << "Iteration " << iteration << ": Writing range [" << start
                  << ", " << end << ")";
        verifier_b.Upsert(start, end);

        // check whether the manifest has been deleted
        if (!fs::exists(manifest_path))
        {
            manifest_deleted = true;
            LOG(INFO) << "Manifest deleted after " << iteration
                      << " iterations";
            break;
        }

        if (iteration % 20 == 19)
        {
            LOG(INFO) << "Completed " << (iteration + 1)
                      << " iterations, manifest still exists";
        }
    }

    // manifest should be deleted
    REQUIRE(manifest_deleted);
    REQUIRE(!fs::exists(manifest_path));

    // check that the partition directory exists but is empty
    LOG(INFO) << "Checking partition directory state after manifest deletion";
    REQUIRE(fs::exists(partition_a_path));
    REQUIRE(fs::is_directory(partition_a_path));

    // verify directory is empty
    bool directory_empty = true;
    int file_count = 0;
    for (const auto &entry : fs::directory_iterator(partition_a_path))
    {
        file_count++;
        directory_empty = false;
        LOG(INFO) << "Found unexpected file in partition directory: "
                  << entry.path().filename().string();
    }

    REQUIRE(directory_empty);
    LOG(INFO)
        << "SUCCESS: Partition directory exists but is empty (file count: "
        << file_count << ")";

    LOG(INFO) << "SUCCESS: Manifest file successfully deleted after "
              << iteration << " iterations";

    LOG(INFO) << "Validating partition B data integrity";
    verifier_b.Validate();

    // test remove manifest and then batch write
    verifier_a.Upsert(0, 100);
    verifier_a.Validate();
}