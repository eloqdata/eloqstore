#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"
#include "utils.h"

using namespace test_util;
namespace chrono = std::chrono;

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

TEST_CASE("cloud prewarm downloads while shards idle", "[cloud][prewarm]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.local_space_limit = 64ULL << 22;
    eloqstore::EloqStore *store = InitStore(options);

    MapVerifier writer(test_tbl_id, store);
    writer.SetValueSize(4096);
    writer.WriteRnd(0, 8000);
    writer.SetAutoClean(false);
    writer.Validate();

    store->Stop();
    CleanupLocalStore(options);

    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const fs::path partition_path =
        fs::path(options.store_path[0]) / test_tbl_id.ToString();

    LOG(INFO) << "aa";
    REQUIRE(WaitForCondition(3s,
                             10ms,
                             [&]() {
                                 return fs::exists(partition_path) &&
                                        !fs::is_empty(partition_path);
                             }));

    const uint64_t initial_size = DirectorySize(partition_path);
    for (int i = 0; i < 5; ++i)
    {
        auto begin = std::chrono::steady_clock::now();
        writer.Read(0);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin);
        REQUIRE(elapsed.count() < 1000);
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));

    REQUIRE(WaitForCondition(10s,
                             20ms,
                             [&]()
                             {
                                 return fs::exists(partition_path) &&
                                        DirectorySize(partition_path) >=
                                            initial_size +
                                                options.DataFileSize();
                             }));

    writer.Validate();
    store->Stop();
}

TEST_CASE("cloud prewarm respects cache budget", "[cloud][prewarm]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.local_space_limit = 2ULL << 30;

    eloqstore::EloqStore *store = InitStore(options);
    eloqstore::TableIdent tbl_id{"prewarm", 0};
    MapVerifier writer(tbl_id, store);
    writer.SetValueSize(16 << 10);
    writer.Upsert(0, 500);
    writer.Validate();

    auto baseline_dataset = writer.DataSet();
    REQUIRE_FALSE(baseline_dataset.empty());

    store->Stop();

    auto remote_bytes =
        GetCloudSize(options.cloud_store_daemon_url, options.cloud_store_path);
    REQUIRE(remote_bytes.has_value());

    CleanupLocalStore(options);

    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const auto partition_path =
        fs::path(options.store_path[0]) / tbl_id.ToString();

    uint64_t limit_bytes = options.local_space_limit;
    uint64_t expected_target = std::min(limit_bytes, remote_bytes.value());

    uint64_t local_size = 0;
    auto size_reached = [&]() -> bool
    {
        local_size = DirectorySize(partition_path);
        if (expected_target == 0)
        {
            return local_size == 0;
        }
        double ratio = static_cast<double>(local_size) /
                       static_cast<double>(expected_target);
        return ratio >= 0.9;
    };

    REQUIRE(WaitForCondition(20s, 100ms, size_reached));

    if (expected_target == 0)
    {
        REQUIRE(local_size == 0);
    }
    else
    {
        double ratio = static_cast<double>(local_size) /
                       static_cast<double>(expected_target);
        REQUIRE(ratio >= 0.9);
    }
}

TEST_CASE("cloud prewarm honors partition filter", "[cloud][prewarm]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.local_space_limit = 2ULL << 30;

    const std::string tbl_name = "prewarm_filter";
    std::vector<eloqstore::TableIdent> partitions;
    for (uint32_t i = 0; i < 3; ++i)
    {
        partitions.emplace_back(tbl_name, i);
    }

    const std::unordered_set<eloqstore::TableIdent> included = {
        partitions[0],
        partitions[1],
    };
    options.prewarm_filter =
        [included](const eloqstore::TableIdent &tbl) -> bool
    { return included.count(tbl) != 0; };

    eloqstore::EloqStore *store = InitStore(options);
    for (const auto &tbl_id : partitions)
    {
        MapVerifier writer(tbl_id, store);
        writer.SetAutoClean(false);
        writer.SetValueSize(4096);
        writer.WriteRnd(0, 4000);
        writer.Validate();
    }

    store->Stop();
    CleanupLocalStore(options);
    REQUIRE(store->Start() == eloqstore::KvError::NoError);

    REQUIRE(WaitForCondition(
        12s,
        20ms,
        [&]()
        {
            for (const auto &tbl_id : included)
            {
                const fs::path partition_path =
                    fs::path(options.store_path[0]) / tbl_id.ToString();
                if (!fs::exists(partition_path) || fs::is_empty(partition_path))
                {
                    return false;
                }
            }
            return true;
        }));

    for (const auto &tbl_id : partitions)
    {
        const fs::path partition_path =
            fs::path(options.store_path[0]) / tbl_id.ToString();
        if (included.count(tbl_id) != 0)
        {
            REQUIRE(fs::exists(partition_path));
            REQUIRE(!fs::is_empty(partition_path));
        }
        else
        {
            REQUIRE_FALSE(fs::exists(partition_path));
        }
    }
}

TEST_CASE("cloud gc preserves archived data after truncate",
          "[cloud][archive][gc]")
{
    eloqstore::EloqStore *store = InitStore(cloud_archive_opts);
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(1500);

    tester.Upsert(0, 200);
    tester.Validate();

    auto baseline_dataset = tester.DataSet();
    REQUIRE_FALSE(baseline_dataset.empty());

    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(test_tbl_id);
    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    const std::string &daemon_url = cloud_archive_opts.cloud_store_daemon_url;
    const std::string &cloud_root = cloud_archive_opts.cloud_store_path;
    const std::string partition = test_tbl_id.ToString();
    const std::string partition_remote = cloud_root + "/" + partition;

    std::vector<std::string> cloud_files =
        ListCloudFiles(daemon_url, cloud_root, partition);
    REQUIRE_FALSE(cloud_files.empty());

    std::string archive_name;
    std::string protected_data_file;
    for (const std::string &filename : cloud_files)
    {
        if (filename.rfind("manifest_", 0) == 0)
        {
            archive_name = filename;
        }
        else if (protected_data_file.empty() && filename.rfind("data_", 0) == 0)
        {
            protected_data_file = filename;
        }
    }
    REQUIRE(!archive_name.empty());
    REQUIRE(!protected_data_file.empty());

    store->Stop();
    CleanupLocalStore(cloud_archive_opts);

    store->Start();
    tester.Validate();

    tester.Upsert(0, 200);
    tester.Upsert(0, 200);
    tester.Upsert(200, 260);
    tester.Validate();

    tester.Truncate(0, true);
    tester.Validate();
    REQUIRE(tester.DataSet().empty());

    std::vector<std::string> files_after_gc =
        ListCloudFiles(daemon_url, cloud_root, partition);
    REQUIRE(std::find(files_after_gc.begin(),
                      files_after_gc.end(),
                      protected_data_file) != files_after_gc.end());

    store->Stop();

    uint64_t backup_ts = utils::UnixTs<chrono::seconds>();
    std::string backup_name = "manifest_" + std::to_string(backup_ts);

    bool backup_ok =
        MoveCloudFile(daemon_url, partition_remote, "manifest", backup_name);
    REQUIRE(backup_ok);

    bool rollback_ok =
        MoveCloudFile(daemon_url, partition_remote, archive_name, "manifest");
    REQUIRE(rollback_ok);

    CleanupLocalStore(cloud_archive_opts);

    tester.SwitchDataSet(baseline_dataset);
    store->Start();
    tester.Validate();
    store->Stop();

    bool restore_archive =
        MoveCloudFile(daemon_url, partition_remote, "manifest", archive_name);
    REQUIRE(restore_archive);

    bool restore_manifest =
        MoveCloudFile(daemon_url, partition_remote, backup_name, "manifest");
    REQUIRE(restore_manifest);

    CleanupLocalStore(cloud_archive_opts);

    const std::map<std::string, eloqstore::KvEntry> empty_dataset;
    tester.SwitchDataSet(empty_dataset);
    store->Start();
    tester.Validate();
    store->Stop();
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
