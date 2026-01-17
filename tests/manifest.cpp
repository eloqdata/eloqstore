#include <algorithm>
#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string_view>
#include <thread>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"
#include "tests/common.h"
#include "utils.h"

using namespace test_util;
using namespace eloqstore;
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
            if (eloqstore::IsArchiveFile(filename))
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
            if (eloqstore::IsArchiveFile(filename))
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
    std::string manifest_path =
        (partition_path / eloqstore::ManifestFileName(0)).string();

    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (eloqstore::IsArchiveFile(filename))
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
    std::string manifest_path =
        (partition_path / eloqstore::ManifestFileName(0)).string();

    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (eloqstore::IsArchiveFile(filename))
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
    auto RunManifestEvictionTest = [&](const eloqstore::KvOptions &opts,
                                       std::string_view mode,
                                       bool has_archive)
    {
        INFO("manifest-eviction mode=" << mode
                                       << " has_archive=" << has_archive);

        eloqstore::KvOptions case_opts = opts;
        case_opts.buffer_pool_size = 15 * 4 * KB;
        case_opts.file_amplify_factor = 2;
        case_opts.data_append_mode = true;
        if (case_opts.store_path.empty())
        {
            case_opts.store_path = {test_path};
        }

        eloqstore::EloqStore *store = InitStore(case_opts);
        std::string table_prefix = "manifest-evict-";
        table_prefix.append(has_archive ? "archive-" : "plain-");
        table_prefix.append(mode);

        eloqstore::TableIdent partition_a{table_prefix, 1};
        eloqstore::TableIdent partition_b{table_prefix, 2};

        MapVerifier verifier_a(partition_a, store, false);
        MapVerifier verifier_b(partition_b, store, false);

        const std::string base_path = case_opts.store_path.empty()
                                          ? std::string(test_path)
                                          : case_opts.store_path[0];
        const fs::path partition_a_path =
            fs::path(base_path) / partition_a.ToString();
        const fs::path manifest_path =
            partition_a_path / eloqstore::ManifestFileName(0);

        verifier_a.Upsert(0, 100);
        verifier_a.Validate();
        REQUIRE(fs::exists(manifest_path));

        fs::path archive_path;
        if (has_archive)
        {
            archive_path = partition_a_path / ArchiveName(0, 123456789);
            fs::copy_file(manifest_path,
                          archive_path,
                          fs::copy_options::overwrite_existing);
            REQUIRE(fs::exists(archive_path));
        }

        verifier_a.Truncate(0, true);
        verifier_a.Validate();
        REQUIRE(fs::exists(manifest_path));

        bool manifest_deleted = false;
        const int max_iterations = 100;

        for (int iteration = 0; iteration < max_iterations; iteration++)
        {
            int start_key = iteration * 50;
            int end_key = start_key + 50;
            verifier_b.Upsert(start_key, end_key);
            verifier_b.Validate();

            if (!fs::exists(manifest_path))
            {
                manifest_deleted = true;
                break;
            }
        }

        if (has_archive)
        {
            REQUIRE_FALSE(manifest_deleted);
            REQUIRE(fs::exists(manifest_path));
            REQUIRE(fs::exists(partition_a_path));
            if (!archive_path.empty())
            {
                REQUIRE(fs::exists(archive_path));
                fs::remove(archive_path);
            }
        }
        else
        {
            REQUIRE(manifest_deleted);
            REQUIRE_FALSE(fs::exists(manifest_path));
            REQUIRE_FALSE(fs::exists(partition_a_path));
        }

        verifier_b.Validate();
        verifier_a.Upsert(0, 100);
        verifier_a.Validate();
    };

    RunManifestEvictionTest(default_opts, "local", false);
    RunManifestEvictionTest(default_opts, "local", true);
}
