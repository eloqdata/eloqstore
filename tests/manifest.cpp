#include <algorithm>
#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "kv_options.h"
#include "replayer.h"
#include "test_utils.h"
#include "utils.h"

using namespace test_util;
using namespace eloqstore;
namespace fs = std::filesystem;

namespace
{
std::vector<uint64_t> CollectArchiveTimestamps(const fs::path &partition_path)
{
    std::vector<uint64_t> timestamps;
    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        std::string filename = entry.path().filename().string();
        if (!eloqstore::IsArchiveFile(filename))
        {
            continue;
        }
        auto [type, suffix] = eloqstore::ParseFileName(filename);
        uint64_t term = 0;
        std::optional<uint64_t> ts;
        REQUIRE(eloqstore::ParseManifestFileSuffix(suffix, term, ts));
        REQUIRE(ts.has_value());
        timestamps.push_back(*ts);
    }
    return timestamps;
}
}  // namespace

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

TEST_CASE("global archive shares timestamp and filters partitions",
          "[archive][global]")
{
    eloqstore::KvOptions opts = archive_opts;
    const std::string tbl_name = "global_archive";

    std::vector<eloqstore::TableIdent> partitions;
    for (uint32_t i = 0; i < 3; ++i)
    {
        partitions.emplace_back(tbl_name, i);
    }

    const std::unordered_set<uint32_t> included_ids = {
        partitions[0].partition_id_,
        partitions[2].partition_id_,
    };
    opts.partition_filter =
        [included_ids](const eloqstore::TableIdent &tbl) -> bool
    { return included_ids.count(tbl.partition_id_) != 0; };

    eloqstore::EloqStore *store = InitStore(opts);
    std::vector<std::unique_ptr<MapVerifier>> writers;
    writers.reserve(partitions.size());
    for (const auto &tbl_id : partitions)
    {
        auto writer = std::make_unique<MapVerifier>(tbl_id, store, false);
        writer->SetAutoClean(false);
        writer->SetValueSize(256);
        writer->WriteRnd(0, 200);
        writer->Validate();
        writers.push_back(std::move(writer));
    }

    constexpr uint64_t kSnapshotTs = 123456789;
    eloqstore::GlobalArchiveRequest global_req;
    global_req.SetSnapshotTimestamp(kSnapshotTs);
    store->ExecSync(&global_req);
    REQUIRE(global_req.Error() == eloqstore::KvError::NoError);

    for (const auto &tbl_id : partitions)
    {
        const fs::path partition_path = fs::path(test_path) / tbl_id.ToString();
        auto timestamps = CollectArchiveTimestamps(partition_path);
        if (included_ids.count(tbl_id.partition_id_) != 0)
        {
            REQUIRE(timestamps.size() == 1);
            REQUIRE(timestamps.front() == kSnapshotTs);
        }
        else
        {
            REQUIRE(timestamps.empty());
        }
    }
}

TEST_CASE("global archive handles more partitions than max_archive_tasks",
          "[archive][global]")
{
    eloqstore::KvOptions opts = archive_opts;
    opts.max_archive_tasks = 2;
    const std::string tbl_name = "global_archive_many";
    const uint32_t partition_count =
        static_cast<uint32_t>(opts.max_archive_tasks) + 3;

    std::vector<eloqstore::TableIdent> partitions;
    partitions.reserve(partition_count);
    for (uint32_t i = 0; i < partition_count; ++i)
    {
        partitions.emplace_back(tbl_name, i);
    }

    eloqstore::EloqStore *store = InitStore(opts);
    std::vector<std::unique_ptr<MapVerifier>> writers;
    writers.reserve(partitions.size());
    for (const auto &tbl_id : partitions)
    {
        auto writer = std::make_unique<MapVerifier>(tbl_id, store, false);
        writer->SetAutoClean(false);
        writer->SetValueSize(256);
        writer->WriteRnd(0, 200);
        writer->Validate();
        writers.push_back(std::move(writer));
    }

    constexpr uint64_t kSnapshotTs = 987654321;
    eloqstore::GlobalArchiveRequest global_req;
    global_req.SetSnapshotTimestamp(kSnapshotTs);
    store->ExecSync(&global_req);
    REQUIRE(global_req.Error() == eloqstore::KvError::NoError);

    for (const auto &tbl_id : partitions)
    {
        const fs::path partition_path = fs::path(test_path) / tbl_id.ToString();
        auto timestamps = CollectArchiveTimestamps(partition_path);
        REQUIRE(timestamps.size() == 1);
        REQUIRE(timestamps.front() == kSnapshotTs);
    }
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

TEST_CASE("rootmeta eviction with small cache across partitions",
          "[manifest][rootmeta_eviction]")
{
    eloqstore::KvOptions opts = append_opts;
    opts.num_threads = 1;
    opts.root_meta_cache_size = 256;
    opts.init_page_count = 8;
    opts.data_page_size = 4096;
    const uint32_t value_size = 256;
    const uint64_t batch_keys = 200;

    eloqstore::EloqStore *store = InitStore(opts);

    constexpr uint32_t partitions = 20;
    std::vector<std::unique_ptr<MapVerifier>> verifiers;
    verifiers.reserve(partitions);

    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        eloqstore::TableIdent tbl_id{"rootmeta", pid};
        auto verifier = std::make_unique<MapVerifier>(tbl_id, store, true);
        verifier->SetValueSize(value_size);
        verifier->SetAutoClean(false);
        verifier->Upsert(0, batch_keys);
        verifier->Read(0);
        verifier->Read(1);
        verifiers.emplace_back(std::move(verifier));
    }
    for (uint32_t pid = 0; pid < partitions; ++pid)
    {
        verifiers[pid]->Read(0);
        verifiers[pid]->Read(2);
    }
}

TEST_CASE("manifest tolerates trailing corruption", "[manifest]")
{
    eloqstore::KvOptions opts;
    opts.init_page_count = 32;
    test_util::ManifestVerifier verifier(opts);

    verifier.NewMapping();
    verifier.NewMapping();
    verifier.Finish();
    std::string manifest_snapshot = verifier.ManifestContent();

    verifier.UpdateMapping();
    verifier.Finish();
    std::string manifest_prefix = verifier.ManifestContent();

    verifier.UpdateMapping();
    verifier.Finish();
    std::string manifest_full = verifier.ManifestContent();

    REQUIRE(manifest_prefix.size() > manifest_snapshot.size());
    REQUIRE(manifest_full.size() > manifest_prefix.size());

    auto replay_manifest = [&](const std::string &manifest)
    {
        eloqstore::Replayer replayer(&opts);
        eloqstore::MemStoreMgr::Manifest file(manifest);
        auto err = replayer.Replay(&file);
        REQUIRE(err == eloqstore::KvError::NoError);
        return std::tuple{replayer.root_,
                          replayer.ttl_root_,
                          replayer.mapping_tbl_,
                          replayer.max_fp_id_,
                          replayer.file_size_};
    };

    const auto baseline = replay_manifest(manifest_prefix);

    SECTION("checksum mismatch on trailing record")
    {
        std::string corrupted = manifest_full;
        corrupted[manifest_prefix.size()] ^= 0x1;

        const auto result = replay_manifest(corrupted);
        CHECK(std::get<0>(result) == std::get<0>(baseline));
        CHECK(std::get<1>(result) == std::get<1>(baseline));
        CHECK(std::get<2>(result) == std::get<2>(baseline));
        CHECK(std::get<3>(result) == std::get<3>(baseline));
        CHECK(std::get<4>(result) == manifest_prefix.size());
    }

    SECTION("truncated trailing record")
    {
        REQUIRE(manifest_full.size() > manifest_prefix.size() + 1);
        std::string truncated = manifest_full;
        truncated.resize(truncated.size() - 1);

        const auto result = replay_manifest(truncated);
        CHECK(std::get<0>(result) == std::get<0>(baseline));
        CHECK(std::get<1>(result) == std::get<1>(baseline));
        CHECK(std::get<2>(result) == std::get<2>(baseline));
        CHECK(std::get<3>(result) == std::get<3>(baseline));
        CHECK(std::get<4>(result) == manifest_full.size());
    }

    SECTION("snapshot corruption remains fatal")
    {
        std::string corrupted_snapshot = manifest_snapshot;
        corrupted_snapshot[0] ^= 0x1;

        eloqstore::Replayer replayer(&opts);
        eloqstore::MemStoreMgr::Manifest file(corrupted_snapshot);
        auto err = replayer.Replay(&file);
        CHECK(err == eloqstore::KvError::Corrupted);
    }
}
