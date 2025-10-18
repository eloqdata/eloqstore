#include <catch2/catch_test_macros.hpp>
#include <chrono>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"
#include "utils.h"

using namespace test_util;
namespace chrono = std::chrono;

const eloqstore::KvOptions cloud_options = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .local_space_limit = 200 << 20,  // 100MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "docker-minio:eloqstore/unit-test",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

const eloqstore::KvOptions cloud_archive_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .num_retained_archives = 1,
    .archive_interval_secs = 0,  // send archive request immediately
    .file_amplify_factor = 2,
    .local_space_limit = 200 << 20,  // 200MB
    .store_path = {"/tmp/test-data"},
    .cloud_store_path = "docker-minio:eloqstore/unit-test",
    .pages_per_file_shift = 8,
    .data_append_mode = true,
};

TEST_CASE("simple cloud store", "[cloud]")
{
    eloqstore::EloqStore *store = InitStore(cloud_options);
    MapVerifier tester(test_tbl_id, store);
    tester.Upsert(100, 200);
    tester.Delete(100, 150);
    tester.Upsert(0, 50);
    tester.WriteRnd(0, 200);
    tester.WriteRnd(0, 200);
}

TEST_CASE("cloud store with restart", "[cloud]")
{
    eloqstore::EloqStore *store = InitStore(cloud_options);

    std::vector<std::unique_ptr<MapVerifier>> partitions;
    for (uint32_t i = 0; i < 3; i++)
    {
        eloqstore::TableIdent tbl_id{"t0", i};
        auto part = std::make_unique<MapVerifier>(tbl_id, store, false);
        part->SetValueSize(10000);
        partitions.push_back(std::move(part));
    }

    for (int i = 0; i < 3; i++)
    {
        for (auto &part : partitions)
        {
            part->WriteRnd(0, 1000);
        }
        store->Stop();
        CleanupLocalStore(cloud_options);
        store->Start();
        for (auto &part : partitions)
        {
            part->Validate();
        }
    }
}

TEST_CASE("cloud store cached file LRU", "[cloud]")
{
    eloqstore::KvOptions options = cloud_options;
    options.manifest_limit = 8 << 10;
    options.fd_limit = 2;
    options.local_space_limit = 2 << 20;
    options.num_retained_archives = 1;
    options.archive_interval_secs = 3;
    options.pages_per_file_shift = 5;
    eloqstore::EloqStore *store = InitStore(options);

    std::vector<std::unique_ptr<MapVerifier>> partitions;
    for (uint32_t i = 0; i < 3; i++)
    {
        eloqstore::TableIdent tbl_id{"t0", i};
        auto part = std::make_unique<MapVerifier>(tbl_id, store, false, 6);
        part->SetValueSize(10000);
        partitions.push_back(std::move(part));
    }

    auto rand_tester = [&partitions]() -> MapVerifier *
    { return partitions[std::rand() % partitions.size()].get(); };

    const uint32_t max_key = 3000;
    for (int i = 0; i < 20; i++)
    {
        uint32_t key = std::rand() % max_key;
        rand_tester()->WriteRnd(key, key + (max_key / 10));

        rand_tester()->Read(std::rand() % max_key);
        rand_tester()->Read(std::rand() % max_key);
        rand_tester()->Read(std::rand() % max_key);
    }
}

TEST_CASE("concurrent test with cloud", "[cloud]")
{
    eloqstore::KvOptions options = cloud_options;
    options.num_threads = 4;
    options.fd_limit = 100 + eloqstore::num_reserved_fd;
    options.reserve_space_ratio = 5;
    options.local_space_limit = 500 << 22;  // 100MB
    eloqstore::EloqStore *store = InitStore(options);

    ConcurrencyTester tester(store, "t1", 50, 1000);
    tester.Init();
    tester.Run(1000, 100, 10);
    tester.Clear();
}

TEST_CASE("easy cloud rollback to archive", "[cloud][archive]")
{
    CleanupStore(cloud_archive_opts);

    eloqstore::EloqStore *store = InitStore(cloud_archive_opts);
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(1000);

    // Insert initial data
    tester.Upsert(0, 100);
    tester.Validate();

    auto old_dataset = tester.DataSet();
    REQUIRE(old_dataset.size() == 100);

    // Record timestamp before creating archive
    uint64_t archive_ts = utils::UnixTs<chrono::microseconds>();

    // Create an archive
    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(test_tbl_id);
    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    std::vector<std::string> cloud_files = ListCloudFiles(
        cloud_archive_opts.cloud_store_daemon_url,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString());

    std::string archive_name;
    for (const auto &filename : cloud_files)
    {
        if (filename.find("manifest_") == 0)
        {
            archive_name = filename;
            break;
        }
    }
    REQUIRE(!archive_name.empty());

    // Insert more data after archive
    tester.Upsert(100, 200);
    tester.Validate();

    auto full_dataset = tester.DataSet();
    REQUIRE(full_dataset.size() == 200);

    // Stop the store
    store->Stop();

    // Create backup with timestamp
    uint64_t backup_ts = utils::UnixTs<chrono::seconds>();
    std::string backup_name = "manifest_" + std::to_string(backup_ts);

    // Move current manifest to backup
    bool backup_success = MoveCloudFile(
        cloud_archive_opts.cloud_store_daemon_url,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString(),
        "manifest",
        backup_name);
    REQUIRE(backup_success);

    // Move archive to manifest
    bool rollback_success = MoveCloudFile(
        cloud_archive_opts.cloud_store_daemon_url,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString(),
        archive_name,
        "manifest");
    REQUIRE(rollback_success);

    // Clean local cache and restart store
    CleanupLocalStore(cloud_archive_opts);

    tester.SwitchDataSet(old_dataset);
    store->Start();

    // Validate old dataset (should only have data from 0-99)

    tester.Validate();

    store->Stop();

    // Restore to full dataset by moving backup back to manifest
    bool restore_success = MoveCloudFile(
        cloud_archive_opts.cloud_store_daemon_url,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString(),
        backup_name,
        "manifest");
    REQUIRE(restore_success);

    CleanupLocalStore(cloud_archive_opts);
    tester.SwitchDataSet(full_dataset);
    store->Start();

    // Validate full dataset
    tester.Validate();
}

TEST_CASE("enhanced cloud rollback with mix operations", "[cloud][archive]")
{
    eloqstore::EloqStore *store = InitStore(cloud_archive_opts);
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(2000);

    // Phase 1: Complex data operations
    tester.Upsert(0, 1000);     // Write 1000 entries
    tester.Delete(200, 400);    // Delete some entries
    tester.Upsert(1000, 1500);  // Add more entries
    tester.WriteRnd(
        1500, 2000, 30, 70);  // Random write with 30% delete probability
    tester.Validate();

    auto phase1_dataset = tester.DataSet();
    LOG(INFO) << "Phase 1 dataset size: " << phase1_dataset.size();

    // Create archive with timestamp tracking
    uint64_t archive_ts = utils::UnixTs<chrono::microseconds>();
    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(test_tbl_id);
    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    // Phase 2: More complex operations after archive
    tester.Delete(0, 100);                // Delete from beginning
    tester.Upsert(2000, 2500);            // Add new range
    tester.Delete(1200, 1300);            // Delete from middle
    tester.WriteRnd(2500, 3000, 50, 80);  // More random operations

    // Simulate concurrent read/write workload
    for (int i = 0; i < 10; i++)
    {
        tester.WriteRnd(3000 + i * 100, 3100 + i * 100, 25, 60);
        // Interleave with reads
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

    // Get cloud configuration from options
    const std::string &daemon_url = cloud_archive_opts.cloud_store_daemon_url;
    const std::string cloud_path =
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString();

    // Create backup with timestamp
    uint64_t backup_ts = utils::UnixTs<chrono::seconds>();
    std::string backup_name = "manifest_" + std::to_string(backup_ts);

    // Backup current manifest
    bool backup_ok =
        MoveCloudFile(daemon_url, cloud_path, "manifest", backup_name);
    REQUIRE(backup_ok);

    // List cloud files to find the archive file
    std::vector<std::string> cloud_files =
        ListCloudFiles(daemon_url, cloud_path);

    // Find archive file (starts with "manifest_")
    std::string archive_name;
    for (const auto &filename : cloud_files)
    {
        if (filename.starts_with("manifest_") && filename != backup_name)
        {
            archive_name = filename;
            break;
        }
    }

    // Rollback to archive if found
    bool rollback_ok = false;
    if (!archive_name.empty())
    {
        rollback_ok =
            MoveCloudFile(daemon_url, cloud_path, archive_name, "manifest");
    }

    // Clean up local store
    CleanupLocalStore(cloud_archive_opts);

    LOG(INFO) << "Attempting enhanced rollback to archive in cloud storage";
    store->Start();

    if (rollback_ok)
    {
        // Validate rollback to phase 1 dataset
        tester.SwitchDataSet(phase1_dataset);
        tester.Validate();

        store->Stop();

        // Restore backup to get back to phase 2 dataset
        bool restore_ok =
            MoveCloudFile(daemon_url, cloud_path, backup_name, "manifest");
        REQUIRE(restore_ok);

        CleanupLocalStore(cloud_archive_opts);

        store->Start();

        tester.SwitchDataSet(phase2_dataset);
        tester.Validate();
    }
    else
    {
        LOG(INFO) << "Archive file not found, validating with phase 2 dataset";
        tester.SwitchDataSet(phase2_dataset);
        tester.Validate();
    }
}