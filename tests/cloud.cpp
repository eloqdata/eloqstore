#include <catch2/catch_test_macros.hpp>
#include <filesystem>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"

using namespace test_util;

const eloqstore::KvOptions cloud_options = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .num_gc_threads = 0,
    .local_space_limit = 100 << 20,  // 100MB
    .store_path = {"./test-data"},
    .cloud_store_path = "docker-minio:eloqstore/unit-test",
    .pages_per_file_shift = 8,  // 1MB per datafile
    .data_append_mode = true,
};

void SetStorePathS3(eloqstore::KvOptions &opts)
{
    // Add username to cloud path to avoid conflicts between users.
    opts.cloud_store_path = "eloq-s3:eloqstore-test";
    opts.cloud_store_path.push_back('/');
    opts.cloud_store_path.append(getlogin());
}

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
        for (const std::string &db_path : cloud_options.store_path)
        {
            std::filesystem::remove_all(db_path);
        }
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
    options.fd_limit = 20 + eloqstore::num_reserved_fd;
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
    options.rclone_threads = 8;
    options.fd_limit = 100 + eloqstore::num_reserved_fd;
    options.reserve_space_ratio = 5;
    options.local_space_limit = 500 << 22;  // 100MB
    eloqstore::EloqStore *store = InitStore(options);

    ConcurrencyTester tester(store, "t1", 50, 1000);
    tester.Init();
    tester.Run(1000, 100, 10);
    tester.Clear();
}