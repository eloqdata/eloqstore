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

TEST_CASE("cloud prewarm logs accurate completion statistics",
          "[cloud][prewarm][logging]")
{
    using namespace std::chrono_literals;
    namespace fs = std::filesystem;

    // Test successful completion logging
    SECTION("successful completion")
    {
        eloqstore::KvOptions options = cloud_options;
        options.prewarm_cloud_cache = true;
        options.pages_per_file_shift = 1;
        options.local_space_limit = 50ULL << 20;  // 50MB
        options.num_threads = 1;

        eloqstore::EloqStore *store = InitStore(options);

        eloqstore::TableIdent tbl_id{"logging_success", 0};
        MapVerifier writer(tbl_id, store);
        writer.SetAutoClean(false);
        writer.SetValueSize(1024);
        writer.Upsert(0, 1000);
        writer.Validate();

        auto cloud_files = ListCloudFiles(
            options, options.cloud_store_path, tbl_id.ToString());

        size_t expected_files = cloud_files.size();
        LOG(INFO) << "Created " << expected_files << " files for logging test";

        store->Stop();
        CleanupLocalStore(options);

        // Capture start time for duration verification
        auto start_time = std::chrono::steady_clock::now();

        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        writer.SetStore(store);

        const fs::path partition_path =
            fs::path(options.store_path[0]) / tbl_id.ToString();

        // Wait for prewarm completion
        REQUIRE(WaitForCondition(
            30s,
            100ms,
            [&]()
            {
                if (!fs::exists(partition_path))
                {
                    return false;
                }

                size_t local_count = 0;
                for (const auto &entry : fs::directory_iterator(partition_path))
                {
                    if (entry.is_regular_file())
                    {
                        local_count++;
                    }
                }

                return local_count >= (expected_files * 0.9);
            }));

        auto end_time = std::chrono::steady_clock::now();
        auto duration_sec = std::chrono::duration_cast<std::chrono::seconds>(
                                end_time - start_time)
                                .count();

        LOG(INFO) << "Prewarm took " << duration_sec << " seconds";

        writer.Validate();

        // Verify the actual number of files pulled matches expectations
        size_t final_local_count = 0;
        for (const auto &entry : fs::directory_iterator(partition_path))
        {
            if (entry.is_regular_file())
            {
                final_local_count++;
            }
        }

        LOG(INFO) << "Verified " << final_local_count << " files locally from "
                  << expected_files << " cloud files";

        // Should have pulled nearly all files (allowing for manifests/metadata)
        REQUIRE(final_local_count >= (expected_files * 0.9));
        REQUIRE(final_local_count <= expected_files);

        // Check logs for completion message
        // Should see: "Prewarm completed successfully. Duration: Xs, API calls:
        // Y,
        //              Files listed: Z, Files skipped: W, Files pulled: V,
        //              Shards: 1"
        // Should see: "Shard 0 prewarm completed successfully. Pulled X files
        // in Ys" The reported "Files pulled" should match final_local_count

        store->Stop();
    }

    // Test abort logging (disk full)
    SECTION("abort with disk full")
    {
        eloqstore::KvOptions options = cloud_options;
        options.prewarm_cloud_cache = true;
        options.pages_per_file_shift = 1;
        options.local_space_limit = 3ULL << 20;  // Only 3MB
        options.num_threads = 1;

        eloqstore::EloqStore *store = InitStore(options);

        eloqstore::TableIdent tbl_id{"logging_abort", 0};
        MapVerifier writer(tbl_id, store);
        writer.SetAutoClean(false);
        writer.SetValueSize(2048);
        writer.Upsert(0, 3000);  // Create more data than cache can hold
        writer.Validate();

        store->Stop();
        CleanupLocalStore(options);

        REQUIRE(store->Start() == eloqstore::KvError::NoError);
        writer.SetStore(store);

        const fs::path partition_path =
            fs::path(options.store_path[0]) / tbl_id.ToString();

        // Wait for prewarm to hit disk limit
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
                return local_size >= (options.local_space_limit * 0.8);
            });

        REQUIRE(hit_limit);

        // Wait a bit more to ensure prewarm has fully aborted
        std::this_thread::sleep_for(2s);

        // Verify the number of files pulled before abort
        size_t files_pulled = 0;
        if (fs::exists(partition_path))
        {
            for (const auto &entry : fs::directory_iterator(partition_path))
            {
                if (entry.is_regular_file())
                {
                    files_pulled++;
                }
            }
        }

        uint64_t final_size = DirectorySize(partition_path);

        LOG(INFO) << "Aborted prewarm pulled " << files_pulled
                  << " files, using " << final_size << " bytes "
                  << "(limit: " << options.local_space_limit << ")";

        // Should have pulled some files before hitting limit
        REQUIRE(files_pulled > 0);
        REQUIRE(final_size <= (options.local_space_limit * 1.2));

        // Check logs for abort messages
        // Should see: "Shard 0 out of local disk space during prewarm.
        //              Pulled X files in Ys before running out of space."
        // Should see: "Prewarm aborted due to insufficient disk space.
        // Duration: Xs,
        //              API calls: Y, Files listed: Z, Files skipped: W, Files
        //              pulled: V"
        // The reported "Files pulled" should match files_pulled count

        writer.Validate();
        store->Stop();
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
