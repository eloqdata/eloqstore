#include <algorithm>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
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
    tester.Clean();
    tester.SetAutoClean(false);
    store->Stop();
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

namespace
{
void WriteBatches(MapVerifier &writer,
                  uint64_t &next_key,
                  size_t entries_per_batch,
                  size_t batches)
{
    for (size_t i = 0; i < batches; ++i)
    {
        writer.Upsert(next_key, next_key + entries_per_batch);
        next_key += entries_per_batch;
    }
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
    writer.SetValueSize(40960);
    writer.WriteRnd(0, 8000);
    writer.SetAutoClean(false);
    writer.Validate();

    store->Stop();
    CleanupLocalStore(options);

    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const fs::path partition_path =
        fs::path(options.store_path[0]) / test_tbl_id.ToString();

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

TEST_CASE("cloud prewarm supports writes after restart", "[cloud][prewarm]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    eloqstore::EloqStore *store = InitStore(options);

    eloqstore::TableIdent tbl_id{"prewarm_single_write", 0};
    MapVerifier writer(tbl_id, store);
    writer.SetAutoClean(false);
    writer.Upsert(0, 1);
    writer.Validate();

    store->Stop();
    CleanupLocalStore(options);

    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const fs::path partition_path =
        fs::path(options.store_path[0]) / tbl_id.ToString();

    REQUIRE(WaitForCondition(5s,
                             20ms,
                             [&]() {
                                 return fs::exists(partition_path) &&
                                        !fs::is_empty(partition_path);
                             }));

    writer.Upsert(1, 2);
    writer.Validate();

    store->Stop();
}

TEST_CASE("cloud reopen waits on evicting cached file", "[cloud][gc]")
{
    using namespace std::chrono_literals;

    eloqstore::KvOptions options = cloud_options;
    // Force frequent eviction: allow only ~3 data files worth of cache.
    options.local_space_limit = options.DataFileSize() * 10;
    eloqstore::TableIdent tbl_id{"cloud-wait", 0};
    eloqstore::EloqStore *store = InitStore(options);

    MapVerifier writer(tbl_id, store);
    writer.SetValueSize(32 * 1024);  // Large values to quickly fill files.
    writer.SetAutoClean(false);
    writer.Upsert(0, 200);
    writer.Validate();

    std::atomic<bool> stop_reader{false};
    std::atomic<bool> reader_failed{false};
    std::thread reader(
        [&]()
        {
            while (!stop_reader.load(std::memory_order_relaxed))
            {
                eloqstore::ReadRequest req;
                req.SetArgs(tbl_id, Key(0));
                store->ExecSync(&req);
                if (req.Error() != eloqstore::KvError::NoError)
                {
                    reader_failed.store(true, std::memory_order_relaxed);
                    break;
                }
                std::this_thread::sleep_for(2ms);
            }
        });

    // Produce more files to trigger cache eviction while reads keep reopening
    // the oldest file (where key 0 lives).
    for (int i = 0; i < 4; ++i)
    {
        writer.Upsert(10000 + i * 500, 10000 + (i + 1) * 500);
        std::this_thread::sleep_for(50ms);
    }

    stop_reader.store(true, std::memory_order_relaxed);
    reader.join();

    writer.Validate();
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

    auto remote_bytes = GetCloudSize(options, options.cloud_store_path);
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

    writer.Clean();
    writer.SetAutoClean(false);
    store->Stop();
}

TEST_CASE("cloud reuse cache enforces budgets across restarts",
          "[cloud][cache]")
{
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.allow_reuse_local_caches = true;
    options.local_space_limit = 40ULL << 20;

    const std::string suffix = "reuse-cache";
    const std::string local_base = options.store_path[0];
    options.store_path = {local_base + std::string("/") + suffix};
    options.cloud_store_path.push_back('/');
    options.cloud_store_path += suffix;

    CleanupStore(options);

    auto store = std::make_unique<eloqstore::EloqStore>(options);
    REQUIRE(store->Start() == eloqstore::KvError::NoError);

    eloqstore::TableIdent tbl_id{"reuse-cache", 0};
    MapVerifier writer(tbl_id, store.get());
    writer.SetAutoClean(false);
    writer.SetAutoValidate(false);
    writer.SetValueSize(64 << 10);

    const size_t entries_per_batch = 32;
    const size_t batches_per_phase = 16;
    uint64_t next_key = 0;
    const fs::path partition_path =
        fs::path(options.store_path[0]) / tbl_id.ToString();

    // Initial fill to create ~32MB of cached data.
    WriteBatches(writer, next_key, entries_per_batch, batches_per_phase);
    REQUIRE(WaitForCondition(
        chrono::milliseconds(10000),
        chrono::milliseconds(100),
        [&]() { return DirectorySize(partition_path) >= (32ULL << 20); }));

    // Restart with the same budget and ensure writing more data never exceeds
    // the 40MB limit.
    store->Stop();
    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store.get());

    WriteBatches(writer, next_key, entries_per_batch, batches_per_phase);

    REQUIRE(WaitForCondition(chrono::milliseconds(10000),
                             chrono::milliseconds(100),
                             [&]() {
                                 return DirectorySize(partition_path) <=
                                        options.local_space_limit;
                             }));

    store->Stop();

    // Tighten the budget to 20MB and verify restore trims/existing files and
    // future writes respect the new limit.
    options.local_space_limit = 20ULL << 20;
    auto trimmed_store = std::make_unique<eloqstore::EloqStore>(options);
    REQUIRE(trimmed_store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(trimmed_store.get());

    WriteBatches(writer, next_key, entries_per_batch, batches_per_phase / 2);

    REQUIRE(WaitForCondition(chrono::milliseconds(10000),
                             chrono::milliseconds(100),
                             [&]() {
                                 return DirectorySize(partition_path) <=
                                        options.local_space_limit;
                             }));

    trimmed_store->Stop();
    CleanupStore(options);
}

TEST_CASE("cloud prewarm honors partition filter", "[cloud][prewarm]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.local_space_limit = 2ULL << 30;

    const std::string tbl_name = "partition_filter";
    std::vector<eloqstore::TableIdent> partitions;
    for (uint32_t i = 0; i < 3; ++i)
    {
        partitions.emplace_back(tbl_name, i);
    }

    const std::unordered_set<eloqstore::TableIdent> included = {
        partitions[0],
        partitions[1],
    };
    options.partition_filter =
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

TEST_CASE("cloud prewarm handles pagination with 2000+ files",
          "[cloud][prewarm][pagination]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.pages_per_file_shift = 1;          // Small files: 2 pages per file
    options.local_space_limit = 100ULL << 20;  // 100MB cache
    options.num_threads = 1;                   // Single shard for simplicity

    eloqstore::EloqStore *store = InitStore(options);

    // Create 2000 small records to generate many data files
    eloqstore::TableIdent tbl_id{"pagination_test", 0};
    MapVerifier writer(tbl_id, store);
    writer.SetAutoClean(false);
    writer.SetValueSize(10024);  // 10KB values

    // Write 2000 records with small page size = many files
    // With pages_per_file_shift=1 and small writes, this creates 1000+ files
    writer.Upsert(0, 2000);
    writer.Validate();

    // Count cloud files before restart
    auto cloud_files_before =
        ListCloudFiles(options, options.cloud_store_path, tbl_id.ToString());

    LOG(INFO) << "Created " << cloud_files_before.size()
              << " files in cloud storage";

    // Require at least 1000 files to properly test pagination
    // (S3 ListObjectsV2 default max-keys is 1000)
    REQUIRE(cloud_files_before.size() >= 1000);

    store->Stop();
    CleanupLocalStore(options);

    // Restart with prewarm enabled
    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const fs::path partition_path =
        fs::path(options.store_path[0]) / tbl_id.ToString();

    // Wait for prewarm to download files
    // With 2000+ files, this tests pagination robustness
    REQUIRE(WaitForCondition(
        60s,
        100ms,
        [&]()
        {
            if (!fs::exists(partition_path))
            {
                return false;
            }

            // Count local files
            size_t local_file_count = 0;
            for (const auto &entry : fs::directory_iterator(partition_path))
            {
                if (entry.is_regular_file())
                {
                    local_file_count++;
                }
            }

            // Check if we've downloaded most files
            // Allow some margin since manifests/metadata files differ
            return local_file_count >= (cloud_files_before.size() * 0.9);
        }));

    // Verify data integrity after prewarm
    writer.Validate();

    // Verify no files were missed due to pagination
    size_t final_local_count = 0;
    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (entry.is_regular_file())
        {
            final_local_count++;
        }
    }

    LOG(INFO) << "Prewarmed " << final_local_count << " files from "
              << cloud_files_before.size() << " cloud files";

    // Should have downloaded nearly all files
    REQUIRE(final_local_count >= (cloud_files_before.size() * 0.9));

    store->Stop();
}

TEST_CASE("cloud prewarm queue management with producer blocking",
          "[cloud][prewarm][queue]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.pages_per_file_shift = 1;          // Small files
    options.local_space_limit = 200ULL << 20;  // 200MB cache
    options.num_threads = 1;                   // Single shard
    options.prewarm_task_count = 1;  // Single consumer for controlled draining

    eloqstore::EloqStore *store = InitStore(options);

    // Create 3000 records to generate ~1500+ files
    // This forces multiple producer blocking cycles
    eloqstore::TableIdent tbl_id{"queue_test", 0};
    MapVerifier writer(tbl_id, store);
    writer.SetAutoClean(false);
    writer.SetValueSize(10024);
    writer.Upsert(0, 3000);
    writer.Validate();

    auto cloud_files =
        ListCloudFiles(options, options.cloud_store_path, tbl_id.ToString());

    LOG(INFO) << "Created " << cloud_files.size()
              << " files for queue management test";
    REQUIRE(cloud_files.size() >= 1500);

    store->Stop();
    CleanupLocalStore(options);

    // Enable debug logging if available
    // export GLOG_v=1 before running to see queue state logs

    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const fs::path partition_path =
        fs::path(options.store_path[0]) / tbl_id.ToString();

    // Monitor prewarm progress
    size_t prev_size = 0;
    size_t stuck_count = 0;

    bool completed = WaitForCondition(
        120s,  // Longer timeout for large dataset
        500ms,
        [&]()
        {
            if (!fs::exists(partition_path))
            {
                return false;
            }

            size_t current_size = DirectorySize(partition_path);

            // Check for progress
            if (current_size > prev_size)
            {
                LOG(INFO) << "Prewarm progress: " << current_size
                          << " bytes downloaded";
                prev_size = current_size;
                stuck_count = 0;
            }
            else
            {
                stuck_count++;
                // Allow up to 10 checks (~5 seconds) without progress
                // Producer blocking is normal, but should resume
                if (stuck_count > 10)
                {
                    LOG(WARNING) << "Prewarm appears stuck at " << current_size
                                 << " bytes";
                }
            }

            // Check if we've downloaded enough data
            // 3000 records * 512 bytes = ~1.5MB minimum
            return current_size >= (1ULL << 20);  // 1MB
        });

    REQUIRE(completed);

    // Verify data integrity
    writer.Validate();

    // Check completion logs
    // Should see "Prewarm completed successfully" in logs
    // Should show file counts matching cloud files

    store->Stop();
}

TEST_CASE("cloud prewarm aborts gracefully when disk fills",
          "[cloud][prewarm][disk]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    eloqstore::KvOptions options = cloud_options;
    options.prewarm_cloud_cache = true;
    options.pages_per_file_shift = 1;  // Small files
    // VERY LIMITED cache to force disk full condition
    options.local_space_limit = 5ULL << 20;  // Only 5MB cache
    options.num_threads = 1;
    options.prewarm_task_count = 2;  // Multiple consumers

    eloqstore::EloqStore *store = InitStore(options);

    // Create enough data to exceed cache limit
    eloqstore::TableIdent tbl_id{"disk_abort_test", 0};
    MapVerifier writer(tbl_id, store);
    writer.SetAutoClean(false);
    writer.SetValueSize(10024);  // 10KB values
    writer.Upsert(0, 2000);      // ~20MB of data
    writer.Validate();

    auto cloud_files =
        ListCloudFiles(options, options.cloud_store_path, tbl_id.ToString());

    LOG(INFO) << "Created " << cloud_files.size()
              << " files for disk abort test";

    auto cloud_size_opt = GetCloudSize(options, options.cloud_store_path);
    REQUIRE(cloud_size_opt.has_value());
    uint64_t cloud_size = cloud_size_opt.value();

    LOG(INFO) << "Cloud storage size: " << cloud_size
              << " bytes, local limit: " << options.local_space_limit
              << " bytes";

    // Ensure cloud data exceeds local cache limit
    REQUIRE(cloud_size > options.local_space_limit);

    store->Stop();
    CleanupLocalStore(options);

    // Restart with prewarm - should abort due to disk full
    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    writer.SetStore(store);

    const fs::path partition_path =
        fs::path(options.store_path[0]) / tbl_id.ToString();

    // Wait for prewarm to hit disk limit and abort
    bool hit_limit = WaitForCondition(
        30s,
        100ms,
        [&]()
        {
            if (!fs::exists(partition_path))
            {
                return false;
            }

            uint64_t local_size = DirectorySize(partition_path);

            // Check if we've hit the cache limit
            // Prewarm should abort around the limit
            return local_size >= (options.local_space_limit * 0.8);
        });

    // Should hit disk limit (not just complete normally)
    REQUIRE(hit_limit);

    // Wait a bit more to ensure prewarm has fully aborted
    std::this_thread::sleep_for(2s);

    uint64_t final_size = DirectorySize(partition_path);

    LOG(INFO) << "Final cache size after abort: " << final_size
              << " bytes (limit: " << options.local_space_limit << ")";

    // Verify cache didn't exceed limit significantly
    // Allow 20% margin for in-flight downloads
    REQUIRE(final_size <= (options.local_space_limit * 1.2));

    // Check logs for abort message
    // Should see "out of local disk space during prewarm" in INFO logs
    // Should show "Pulled X files in Y seconds before running out of space"

    // Store should still be functional
    // Validate with whatever data was prewarmed
    writer.Validate();

    store->Stop();
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

    const std::string &cloud_root = cloud_archive_opts.cloud_store_path;
    const std::string partition = test_tbl_id.ToString();
    const std::string partition_remote = cloud_root + "/" + partition;

    std::vector<std::string> cloud_files =
        ListCloudFiles(cloud_archive_opts, cloud_root, partition);
    REQUIRE_FALSE(cloud_files.empty());

    std::string archive_name;
    std::string protected_data_file;
    for (const std::string &filename : cloud_files)
    {
        if (eloqstore::IsArchiveFile(filename))
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
        ListCloudFiles(cloud_archive_opts, cloud_root, partition);
    REQUIRE(std::find(files_after_gc.begin(),
                      files_after_gc.end(),
                      protected_data_file) != files_after_gc.end());

    store->Stop();

    uint64_t backup_ts = utils::UnixTs<chrono::seconds>();
    // Use ArchiveName to generate a valid archive-like filename. This ensures
    // it won't be treated as a current manifest during selection.
    std::string backup_name = eloqstore::ArchiveName(0, backup_ts);

    bool backup_ok = MoveCloudFile(cloud_archive_opts,
                                   partition_remote,
                                   eloqstore::ManifestFileName(0),
                                   backup_name);
    REQUIRE(backup_ok);

    bool rollback_ok = MoveCloudFile(cloud_archive_opts,
                                     partition_remote,
                                     archive_name,
                                     eloqstore::ManifestFileName(0));
    REQUIRE(rollback_ok);

    CleanupLocalStore(cloud_archive_opts);

    tester.SwitchDataSet(baseline_dataset);
    store->Start();
    tester.Validate();
    store->Stop();

    bool restore_archive = MoveCloudFile(cloud_archive_opts,
                                         partition_remote,
                                         eloqstore::ManifestFileName(0),
                                         archive_name);
    REQUIRE(restore_archive);

    bool restore_manifest = MoveCloudFile(cloud_archive_opts,
                                          partition_remote,
                                          backup_name,
                                          eloqstore::ManifestFileName(0));
    REQUIRE(restore_manifest);

    CleanupLocalStore(cloud_archive_opts);

    const std::map<std::string, eloqstore::KvEntry> empty_dataset;
    tester.SwitchDataSet(empty_dataset);
    store->Start();
    tester.Validate();
    store->Stop();
}

TEST_CASE("cloud global archive shares timestamp and filters partitions",
          "[cloud][archive][global]")
{
    using namespace std::chrono_literals;

    eloqstore::KvOptions options = cloud_archive_opts;
    const std::string tbl_name = "global_archive_cloud";
    constexpr uint32_t kListPageSize = 1000;
    constexpr uint32_t kPartitionCount = kListPageSize + 5;
    constexpr uint32_t kPartitionBase = 1000;
    static_assert(kPartitionCount > kListPageSize);

    std::vector<eloqstore::TableIdent> partitions;
    partitions.reserve(kPartitionCount);
    for (uint32_t i = 0; i < kPartitionCount; ++i)
    {
        partitions.emplace_back(tbl_name, kPartitionBase + i);
    }

    const uint32_t first_partition_id = kPartitionBase;
    const uint32_t second_page_partition_id =
        kPartitionBase + kPartitionCount - 1;
    const uint32_t excluded_first_id = kPartitionBase + 1;
    const uint32_t excluded_second_id = kPartitionBase + kPartitionCount - 2;
    const std::unordered_set<uint32_t> included_ids = {
        first_partition_id,
        second_page_partition_id,
    };
    options.partition_filter =
        [included_ids](const eloqstore::TableIdent &tbl) -> bool
    { return included_ids.count(tbl.partition_id_) != 0; };

    eloqstore::EloqStore *store = InitStore(options);
    std::vector<std::unique_ptr<MapVerifier>> writers;
    writers.reserve(partitions.size());
    for (const auto &tbl_id : partitions)
    {
        auto writer = std::make_unique<MapVerifier>(tbl_id, store, false);
        writer->SetAutoClean(false);
        writer->SetValueSize(128);
        writer->Upsert(0, 1);
        writer->Validate();
        writers.push_back(std::move(writer));
    }

    const std::string &cloud_root = options.cloud_store_path;
    auto list_partitions = [&]()
    {
        std::unordered_set<uint32_t> listed;
        auto files = ListCloudFiles(options, cloud_root);
        for (const auto &name : files)
        {
            size_t slash = name.find('/');
            if (slash == std::string::npos)
            {
                continue;
            }
            eloqstore::TableIdent tbl_id =
                eloqstore::TableIdent::FromString(name.substr(0, slash));
            if (!tbl_id.IsValid() || tbl_id.tbl_name_ != tbl_name)
            {
                continue;
            }
            listed.insert(tbl_id.partition_id_);
        }
        return listed;
    };

    std::unordered_set<uint32_t> listed_partitions;
    REQUIRE(WaitForCondition(60s,
                             500ms,
                             [&]()
                             {
                                 listed_partitions = list_partitions();
                                 if (listed_partitions.size() !=
                                     kPartitionCount)
                                 {
                                     return false;
                                 }
                                 for (uint32_t id = kPartitionBase;
                                      id < kPartitionBase + kPartitionCount;
                                      ++id)
                                 {
                                     if (listed_partitions.count(id) == 0)
                                     {
                                         return false;
                                     }
                                 }
                                 return true;
                             }));
    REQUIRE(listed_partitions.size() == kPartitionCount);
    REQUIRE(listed_partitions.count(first_partition_id) != 0);
    REQUIRE(listed_partitions.count(second_page_partition_id) != 0);

    constexpr uint64_t kSnapshotTs = 987654321;
    eloqstore::GlobalArchiveRequest global_req;
    global_req.SetSnapshotTimestamp(kSnapshotTs);
    store->ExecSync(&global_req);
    REQUIRE(global_req.Error() == eloqstore::KvError::NoError);

    auto collect_archive_timestamps = [&](const eloqstore::TableIdent &tbl_id)
    {
        std::vector<uint64_t> timestamps;
        auto files = ListCloudFiles(options, cloud_root, tbl_id.ToString());
        for (const auto &filename : files)
        {
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
    };

    auto wait_for_archive = [&](const eloqstore::TableIdent &tbl_id)
    {
        return WaitForCondition(
            20s,
            200ms,
            [&]() { return !collect_archive_timestamps(tbl_id).empty(); });
    };

    const std::vector<eloqstore::TableIdent> included_partitions = {
        {tbl_name, first_partition_id},
        {tbl_name, second_page_partition_id},
    };
    const std::vector<eloqstore::TableIdent> excluded_partitions = {
        {tbl_name, excluded_first_id},
        {tbl_name, excluded_second_id},
    };

    for (const auto &tbl_id : included_partitions)
    {
        REQUIRE(wait_for_archive(tbl_id));
        auto timestamps = collect_archive_timestamps(tbl_id);
        REQUIRE_FALSE(timestamps.empty());
        for (uint64_t ts : timestamps)
        {
            REQUIRE(ts == kSnapshotTs);
        }
    }

    for (const auto &tbl_id : excluded_partitions)
    {
        auto timestamps = collect_archive_timestamps(tbl_id);
        REQUIRE(timestamps.empty());
    }
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

    for (auto &part : partitions)
    {
        part->Clean();
        part->SetAutoClean(false);
    }
    store->Stop();
}

TEST_CASE("cloud reopen refreshes manifest via archive swap", "[cloud][reopen]")
{
    eloqstore::KvOptions options = cloud_archive_opts;
    options.store_path = {"/tmp/test-data-reopen"};
    options.cloud_store_path += "/reopen-refresh";

    CleanupStore(options);

    eloqstore::TableIdent tbl_id{"reopen", 0};
    eloqstore::EloqStore *store = InitStore(options);
    MapVerifier verifier(tbl_id, store, false);

    // Version 1 data and archive snapshot.
    verifier.Upsert(0, 50);
    auto v1_dataset = verifier.DataSet();
    REQUIRE_FALSE(v1_dataset.empty());

    eloqstore::ArchiveRequest archive_req;
    archive_req.SetTableId(tbl_id);
    bool ok = store->ExecAsyn(&archive_req);
    REQUIRE(ok);
    archive_req.Wait();
    REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

    // Version 2 data (current manifest).
    verifier.Upsert(100, 120);
    verifier.Upsert(130, 140);

    const std::string &cloud_root = options.cloud_store_path;
    const std::string partition = tbl_id.ToString();
    const std::string partition_remote = cloud_root + "/" + partition;
    std::vector<std::string> cloud_files =
        ListCloudFiles(options, cloud_root, partition);
    REQUIRE_FALSE(cloud_files.empty());

    std::string current_manifest;
    std::string archive_manifest;
    for (const std::string &filename : cloud_files)
    {
        if (eloqstore::IsArchiveFile(filename))
        {
            archive_manifest = filename;
        }
        else if (filename.rfind(eloqstore::FileNameManifest, 0) == 0)
        {
            current_manifest = filename;
        }
    }
    REQUIRE_FALSE(current_manifest.empty());
    REQUIRE_FALSE(archive_manifest.empty());

    uint64_t term = eloqstore::ManifestTermFromFilename(current_manifest);
    REQUIRE(term >= 0);

    uint64_t backup_ts = utils::UnixTs<chrono::seconds>();
    std::string backup_manifest = eloqstore::ArchiveName(term, backup_ts);

    // Move current manifest aside, then promote archive manifest.
    REQUIRE(MoveCloudFile(
        options, partition_remote, current_manifest, backup_manifest));
    REQUIRE(MoveCloudFile(
        options, partition_remote, archive_manifest, current_manifest));

    // Reopen to pull refreshed manifest and verify version rollback.
    eloqstore::ReopenRequest reopen_req;
    reopen_req.SetArgs(tbl_id);
    store->ExecSync(&reopen_req);
    REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

    verifier.SwitchDataSet(v1_dataset);
    verifier.Validate();

    verifier.SetAutoClean(false);
    store->Stop();
    CleanupStore(options);
}

TEST_CASE("cloud reopen refreshes local manifest from remote",
          "[cloud][reopen]")
{
    eloqstore::KvOptions options = cloud_archive_opts;
    options.store_path = {"/tmp/test-data-reopen-local"};
    options.cloud_store_path += "/reopen-local";
    options.prewarm_cloud_cache = false;
    options.allow_reuse_local_caches = true;

    CleanupStore(options);

    eloqstore::TableIdent tbl_id{"reopen_local", 0};
    eloqstore::EloqStore *store = InitStore(options);
    MapVerifier verifier(tbl_id, store, false);

    // Version 1 data, keep a local backup.
    verifier.Upsert(0, 50);
    auto v1_dataset = verifier.DataSet();
    REQUIRE_FALSE(v1_dataset.empty());

    // Stop to ensure local files are durable before backup.
    store->Stop();

    const std::string backup_root = "/tmp/test-data-reopen-local-backup";
    const std::string manifest_name = eloqstore::ManifestFileName(0);
    uint64_t v1_manifest_size = 0;
    std::filesystem::remove_all(backup_root);
    std::filesystem::create_directories(backup_root);
    for (const auto &path : options.store_path)
    {
        std::filesystem::path src = path;
        std::filesystem::path dst =
            std::filesystem::path(backup_root) / src.filename();
        std::filesystem::remove_all(dst);
        std::filesystem::copy(
            src,
            dst,
            std::filesystem::copy_options::recursive |
                std::filesystem::copy_options::overwrite_existing);
    }
    {
        std::filesystem::path v1_manifest =
            std::filesystem::path(backup_root) /
            std::filesystem::path(options.store_path.front()).filename() /
            tbl_id.ToString() / manifest_name;
        v1_manifest_size = std::filesystem::file_size(v1_manifest);
        REQUIRE(v1_manifest_size > 0);
    }

    // Restart to write version 2 data (remote is newer).
    REQUIRE(store->Start() == eloqstore::KvError::NoError);

    // Version 2 data (remote is newer).
    verifier.Upsert(100, 120);
    verifier.Upsert(130, 140);
    auto v2_dataset = verifier.DataSet();
    REQUIRE(v2_dataset.size() > v1_dataset.size());

    // Stop, replace local with older snapshot.
    store->Stop();
    uint64_t v2_manifest_size = 0;
    {
        std::filesystem::path v2_manifest =
            std::filesystem::path(options.store_path.front()) /
            tbl_id.ToString() / manifest_name;
        v2_manifest_size = std::filesystem::file_size(v2_manifest);
        REQUIRE(v2_manifest_size >= v1_manifest_size);
    }
    for (const auto &path : options.store_path)
    {
        std::filesystem::remove_all(path);
        std::filesystem::path src = std::filesystem::path(backup_root) /
                                    std::filesystem::path(path).filename();
        std::filesystem::copy(
            src,
            path,
            std::filesystem::copy_options::recursive |
                std::filesystem::copy_options::overwrite_existing);
    }
    auto clear_data_files = [&](const eloqstore::TableIdent &table_id)
    {
        for (const auto &path : options.store_path)
        {
            std::filesystem::path part_path =
                std::filesystem::path(path) / table_id.ToString();
            if (!std::filesystem::exists(part_path))
            {
                continue;
            }
            for (const auto &ent :
                 std::filesystem::directory_iterator(part_path))
            {
                if (!ent.is_regular_file())
                {
                    continue;
                }
                auto [type, suffix] =
                    eloqstore::ParseFileName(ent.path().filename().string());
                if (type == eloqstore::FileNameData)
                {
                    std::filesystem::remove(ent.path());
                }
            }
        }
    };
    clear_data_files(tbl_id);

    // Restart without prewarm so it doesn't auto-download.
    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    {
        std::filesystem::path restored_manifest =
            std::filesystem::path(options.store_path.front()) /
            tbl_id.ToString() / manifest_name;
        REQUIRE(std::filesystem::file_size(restored_manifest) ==
                v1_manifest_size);
    }

    // Reopen should refresh local manifest to latest remote.
    eloqstore::ReopenRequest reopen_req;
    reopen_req.SetArgs(tbl_id);
    store->ExecSync(&reopen_req);
    REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);
    {
        std::filesystem::path refreshed_manifest =
            std::filesystem::path(options.store_path.front()) /
            tbl_id.ToString() / manifest_name;
        REQUIRE(std::filesystem::file_size(refreshed_manifest) ==
                v2_manifest_size);
    }

    verifier.SwitchDataSet(v2_dataset);
    verifier.Validate();

    verifier.SetAutoClean(false);
    store->Stop();
    CleanupStore(options);
}

TEST_CASE("cloud global reopen refreshes local manifests", "[cloud][reopen]")
{
    eloqstore::KvOptions options = cloud_archive_opts;
    options.store_path = {"/tmp/test-data-reopen-global"};
    options.cloud_store_path += "/reopen-global";
    options.prewarm_cloud_cache = false;
    options.allow_reuse_local_caches = true;

    CleanupStore(options);

    const std::string tbl_name = "reopen_global";
    const std::vector<eloqstore::TableIdent> tbl_ids = {
        {tbl_name, 0},
        {tbl_name, 1},
    };

    eloqstore::EloqStore *store = InitStore(options);
    std::vector<std::unique_ptr<MapVerifier>> verifiers;
    verifiers.reserve(tbl_ids.size());
    for (const auto &tbl_id : tbl_ids)
    {
        verifiers.emplace_back(
            std::make_unique<MapVerifier>(tbl_id, store, false));
    }

    // Version 1 data, keep a local backup.
    std::vector<std::map<std::string, eloqstore::KvEntry>> v1_datasets;
    v1_datasets.reserve(verifiers.size());
    std::vector<uint64_t> v1_manifest_sizes;
    v1_manifest_sizes.reserve(verifiers.size());
    for (auto &verifier : verifiers)
    {
        verifier->Upsert(0, 50);
        v1_datasets.push_back(verifier->DataSet());
        REQUIRE_FALSE(v1_datasets.back().empty());
    }

    // Stop to ensure local files are durable before backup.
    store->Stop();

    const std::string backup_root = "/tmp/test-data-reopen-global-backup";
    const std::string manifest_name = eloqstore::ManifestFileName(0);
    std::filesystem::remove_all(backup_root);
    std::filesystem::create_directories(backup_root);
    for (const auto &path : options.store_path)
    {
        std::filesystem::path src = path;
        std::filesystem::path dst =
            std::filesystem::path(backup_root) / src.filename();
        std::filesystem::remove_all(dst);
        std::filesystem::copy(
            src,
            dst,
            std::filesystem::copy_options::recursive |
                std::filesystem::copy_options::overwrite_existing);
    }
    for (const auto &tbl_id : tbl_ids)
    {
        std::filesystem::path v1_manifest =
            std::filesystem::path(backup_root) /
            std::filesystem::path(options.store_path.front()).filename() /
            tbl_id.ToString() / manifest_name;
        uint64_t size = std::filesystem::file_size(v1_manifest);
        v1_manifest_sizes.push_back(size);
        REQUIRE(size > 0);
    }

    // Restart to write version 2 data (remote is newer).
    REQUIRE(store->Start() == eloqstore::KvError::NoError);

    // Version 2 data (remote is newer).
    std::vector<std::map<std::string, eloqstore::KvEntry>> v2_datasets;
    v2_datasets.reserve(verifiers.size());
    std::vector<uint64_t> v2_manifest_sizes;
    v2_manifest_sizes.reserve(verifiers.size());
    for (auto &verifier : verifiers)
    {
        verifier->Upsert(100, 120);
        verifier->Upsert(130, 140);
        v2_datasets.push_back(verifier->DataSet());
        REQUIRE(v2_datasets.back().size() > 0);
    }

    store->Stop();
    for (const auto &tbl_id : tbl_ids)
    {
        std::filesystem::path v2_manifest =
            std::filesystem::path(options.store_path.front()) /
            tbl_id.ToString() / manifest_name;
        uint64_t size = std::filesystem::file_size(v2_manifest);
        v2_manifest_sizes.push_back(size);
    }
    for (const auto &path : options.store_path)
    {
        std::filesystem::remove_all(path);
        std::filesystem::path src = std::filesystem::path(backup_root) /
                                    std::filesystem::path(path).filename();
        std::filesystem::copy(
            src,
            path,
            std::filesystem::copy_options::recursive |
                std::filesystem::copy_options::overwrite_existing);
    }
    auto clear_partition_data_files = [&](const eloqstore::TableIdent &table_id)
    {
        for (const auto &path : options.store_path)
        {
            std::filesystem::path part_path =
                std::filesystem::path(path) / table_id.ToString();
            if (!std::filesystem::exists(part_path))
            {
                continue;
            }
            for (const auto &ent :
                 std::filesystem::directory_iterator(part_path))
            {
                if (!ent.is_regular_file())
                {
                    continue;
                }
                auto [type, suffix] =
                    eloqstore::ParseFileName(ent.path().filename().string());
                if (type == eloqstore::FileNameData)
                {
                    std::filesystem::remove(ent.path());
                }
            }
        }
    };
    for (const auto &tbl_id : tbl_ids)
    {
        clear_partition_data_files(tbl_id);
    }

    REQUIRE(store->Start() == eloqstore::KvError::NoError);
    for (size_t i = 0; i < tbl_ids.size(); ++i)
    {
        std::filesystem::path restored_manifest =
            std::filesystem::path(options.store_path.front()) /
            tbl_ids[i].ToString() / manifest_name;
        REQUIRE(std::filesystem::file_size(restored_manifest) ==
                v1_manifest_sizes[i]);
    }

    eloqstore::GlobalReopenRequest reopen_req;
    store->ExecSync(&reopen_req);
    REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);
    for (size_t i = 0; i < tbl_ids.size(); ++i)
    {
        std::filesystem::path refreshed_manifest =
            std::filesystem::path(options.store_path.front()) /
            tbl_ids[i].ToString() / manifest_name;
        REQUIRE(std::filesystem::file_size(refreshed_manifest) ==
                v2_manifest_sizes[i]);
    }

    for (size_t i = 0; i < verifiers.size(); ++i)
    {
        verifiers[i]->SwitchDataSet(v2_datasets[i]);
        verifiers[i]->Validate();
    }

    for (auto &verifier : verifiers)
    {
        verifier->SetAutoClean(false);
    }
    store->Stop();
    CleanupStore(options);
}

TEST_CASE("cloud store cached file LRU", "[cloud]")
{
    eloqstore::KvOptions options = cloud_options;
    options.manifest_limit = 8 << 10;
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

    for (auto &part : partitions)
    {
        part->Clean();
        part->SetAutoClean(false);
    }
    store->Stop();
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
        cloud_archive_opts,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString());

    std::string archive_name;
    for (const auto &filename : cloud_files)
    {
        if (eloqstore::IsArchiveFile(filename))
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
    std::string backup_name = eloqstore::ArchiveName(0, backup_ts);

    // Move current manifest to backup
    bool backup_success = MoveCloudFile(
        cloud_archive_opts,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString(),
        eloqstore::ManifestFileName(0),
        backup_name);
    REQUIRE(backup_success);

    // Move archive to manifest
    bool rollback_success = MoveCloudFile(
        cloud_archive_opts,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString(),
        archive_name,
        eloqstore::ManifestFileName(0));
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
        cloud_archive_opts,
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString(),
        backup_name,
        eloqstore::ManifestFileName(0));
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
    const std::string cloud_path =
        cloud_archive_opts.cloud_store_path + "/" + test_tbl_id.ToString();

    // Create backup with timestamp
    uint64_t backup_ts = utils::UnixTs<chrono::seconds>();
    std::string backup_name = eloqstore::ArchiveName(0, backup_ts);

    // Backup current manifest
    bool backup_ok = MoveCloudFile(cloud_archive_opts,
                                   cloud_path,
                                   eloqstore::ManifestFileName(0),
                                   backup_name);
    REQUIRE(backup_ok);

    // List cloud files to find the archive file
    std::vector<std::string> cloud_files =
        ListCloudFiles(cloud_archive_opts, cloud_path);

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
        rollback_ok = MoveCloudFile(cloud_archive_opts,
                                    cloud_path,
                                    archive_name,
                                    eloqstore::ManifestFileName(0));
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
        bool restore_ok = MoveCloudFile(cloud_archive_opts,
                                        cloud_path,
                                        backup_name,
                                        eloqstore::ManifestFileName(0));
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

    tester.Clean();
    tester.SetAutoClean(false);
    store->Stop();
}

TEST_CASE("archive triggers with cloud-only partitions", "[cloud][archive]")
{
    using namespace std::chrono_literals;
    constexpr uint32_t kPartitionCount = 10;
    const std::string tbl_name = "remote_archive";

    eloqstore::KvOptions options = cloud_archive_opts;
    options.num_threads = 1;  // single shard handles all partitions
    options.prewarm_cloud_cache =
        false;                          // keep local cache empty after restart
    options.archive_interval_secs = 1;  // trigger archiver quickly
    options.local_space_limit = 1LL << 40;  // 1TB
    options.fd_limit += utils::CountUsedFD();

    eloqstore::EloqStore *store = InitStore(options);

    std::vector<std::unique_ptr<eloqstore::BatchWriteRequest>> writes;
    writes.reserve(kPartitionCount);
    for (uint32_t pid = 0; pid < kPartitionCount; ++pid)
    {
        auto req = std::make_unique<eloqstore::BatchWriteRequest>();
        req->SetTableId({tbl_name, pid});
        req->AddWrite(test_util::Key(0),
                      test_util::Value(pid, 8),
                      utils::UnixTs<chrono::microseconds>(),
                      eloqstore::WriteOp::Upsert);
        REQUIRE(store->ExecAsyn(req.get()));
        writes.emplace_back(std::move(req));
    }
    for (auto &req : writes)
    {
        req->Wait();
        REQUIRE(req->Error() == eloqstore::KvError::NoError);
    }

    store->Stop();
    CleanupLocalStore(options);
    REQUIRE(store->Start() == eloqstore::KvError::NoError);

    std::unordered_set<uint32_t> pending;
    for (uint32_t pid = 0; pid < kPartitionCount; ++pid)
    {
        pending.insert(pid);
    }

    bool archived = WaitForCondition(
        60s,
        2s,
        [&]()
        {
            for (auto it = pending.begin(); it != pending.end();)
            {
                eloqstore::TableIdent tid{tbl_name, *it};
                auto files = ListCloudFiles(
                    options, options.cloud_store_path, tid.ToString());
                bool has_archive = std::any_of(
                    files.begin(),
                    files.end(),
                    [](const auto &f) { return f.rfind("manifest_", 0) == 0; });
                if (has_archive)
                {
                    it = pending.erase(it);
                }
                else
                {
                    ++it;
                }
            }
            return pending.empty();
        });
    REQUIRE(archived);

    store->Stop();
    CleanupStore(options);
}
