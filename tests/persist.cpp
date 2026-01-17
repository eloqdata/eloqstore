#include <glog/logging.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common.h"
#include "error.h"
#include "kv_options.h"
#include "test_utils.h"
#include "types.h"
#include "utils.h"

using namespace test_util;

namespace fs = std::filesystem;

TEST_CASE("simple persist", "[persist]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(100, 200);
    verify.Delete(100, 150);
    verify.Upsert(0, 50);
    verify.WriteRnd(0, 200);
    verify.WriteRnd(0, 200);
}

TEST_CASE("complex persist", "[persist]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store);
    for (int i = 0; i < 5; i++)
    {
        verify.WriteRnd(0, 2000);
    }
}

TEST_CASE("persist with restart", "[persist]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 3; i++)
    {
        eloqstore::TableIdent tbl_id{"t1", i};
        tbls.push_back(std::make_unique<MapVerifier>(tbl_id, store));
    }

    for (int i = 0; i < 5; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->WriteRnd(0, 1000);
        }
        store->Stop();
        store->Start();
    }
}

TEST_CASE("drop table clears all partitions", "[persist][droptable]")
{
    const std::string tbl_name = "drop-table-persist";
    const std::vector<uint32_t> partitions = {0, 1, 2};
    constexpr size_t kNumKeysPerPartition = 16;

    auto run_drop_table_case =
        [&](const eloqstore::KvOptions &opts, std::string_view mode)
    {
        INFO("drop-table mode=" << mode);
        eloqstore::EloqStore *store = InitStore(opts);
        auto table_ident = [&tbl_name](uint32_t partition)
        { return eloqstore::TableIdent{tbl_name, partition}; };
        std::vector<std::vector<std::string>> expected_keys(partitions.size());

        for (int round = 0; round < 3; ++round)
        {
            uint64_t ts = static_cast<uint64_t>(round + 1);
            for (size_t idx = 0; idx < partitions.size(); ++idx)
            {
                uint32_t partition = partitions[idx];
                expected_keys[idx].clear();
                std::vector<eloqstore::WriteDataEntry> entries;
                entries.reserve(kNumKeysPerPartition);
                uint32_t base_key =
                    static_cast<uint32_t>(round * 1000 + partition * 100);
                for (size_t i = 0; i < kNumKeysPerPartition; ++i)
                {
                    uint32_t key_id = base_key + static_cast<uint32_t>(i);
                    std::string key = test_util::Key(key_id);
                    std::string value = test_util::Value(key_id, 32);
                    expected_keys[idx].push_back(key);
                    entries.emplace_back(std::move(key),
                                         std::move(value),
                                         ts,
                                         eloqstore::WriteOp::Upsert);
                }

                eloqstore::BatchWriteRequest write_req;
                write_req.SetArgs(table_ident(partition), std::move(entries));
                store->ExecSync(&write_req);
                REQUIRE(write_req.Error() == eloqstore::KvError::NoError);
            }

            for (size_t idx = 0; idx < partitions.size(); ++idx)
            {
                uint32_t partition = partitions[idx];
                eloqstore::ScanRequest scan_req;
                uint32_t base_key =
                    static_cast<uint32_t>(round * 1000 + partition * 100);
                std::string begin_key = test_util::Key(base_key);
                std::string end_key = test_util::Key(
                    base_key + static_cast<uint32_t>(kNumKeysPerPartition));
                scan_req.SetArgs(table_ident(partition), begin_key, end_key);
                store->ExecSync(&scan_req);
                REQUIRE(scan_req.Error() == eloqstore::KvError::NoError);

                std::unordered_set<std::string> actual_keys;
                auto entries = scan_req.Entries();
                for (const auto &entry : entries)
                {
                    actual_keys.insert(entry.key_);
                }
                REQUIRE(actual_keys.size() == expected_keys[idx].size());
                for (const auto &key : expected_keys[idx])
                {
                    REQUIRE(actual_keys.count(key) == 1);
                }
            }

            eloqstore::DropTableRequest drop_req;
            drop_req.SetArgs(tbl_name);
            store->ExecSync(&drop_req);
            REQUIRE(drop_req.Error() == eloqstore::KvError::NoError);

            for (size_t idx = 0; idx < partitions.size(); ++idx)
            {
                uint32_t partition = partitions[idx];
                eloqstore::ScanRequest scan_req;
                uint32_t base_key =
                    static_cast<uint32_t>(round * 1000 + partition * 100);
                std::string begin_key = test_util::Key(base_key);
                std::string end_key = test_util::Key(
                    base_key + static_cast<uint32_t>(kNumKeysPerPartition));
                scan_req.SetArgs(table_ident(partition), begin_key, end_key);
                store->ExecSync(&scan_req);
                REQUIRE(scan_req.Error() == eloqstore::KvError::NoError);
                REQUIRE(scan_req.Entries().empty());

                for (const auto &key : expected_keys[idx])
                {
                    eloqstore::ReadRequest read_req;
                    read_req.SetArgs(table_ident(partition), key);
                    store->ExecSync(&read_req);
                    REQUIRE(read_req.Error() == eloqstore::KvError::NotFound);
                }
            }
        }

        store->Stop();
        CleanupStore(opts);
    };

    run_drop_table_case(default_opts, "local");
    run_drop_table_case(cloud_options, "cloud");
}

TEST_CASE("simple LRU for opened fd", "[persist]")
{
    eloqstore::KvOptions options{
        .fd_limit = 20 + eloqstore::num_reserved_fd,
        .store_path = {test_path},
        .data_page_size = static_cast<uint16_t>(eloqstore::page_align),
        .pages_per_file_shift = 1,
    };
    eloqstore::EloqStore *store = InitStore(options);

    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(1, 5000);
    verify.Upsert(5000, 10000);
    verify.Upsert(1, 10000);
}

TEST_CASE("complex LRU for opened fd", "[persist]")
{
    eloqstore::KvOptions options{
        .fd_limit = 20 + eloqstore::num_reserved_fd,
        .store_path = {test_path},
        .data_page_size = static_cast<uint16_t>(eloqstore::page_align),
        .pages_per_file_shift = 1,
    };
    eloqstore::EloqStore *store = InitStore(options);

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 10; i++)
    {
        eloqstore::TableIdent tbl_id{"t1", i};
        tbls.push_back(std::make_unique<MapVerifier>(tbl_id, store));
    }

    for (uint32_t i = 0; i < 3; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->Upsert(0, 5000);
        }
    }
}

TEST_CASE("detect corrupted page", "[persist][checksum]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    eloqstore::TableIdent tbl_id = {"detect-corrupted", 1};
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        for (size_t idx = 0; idx < 10; ++idx)
        {
            entries.emplace_back(
                Key(idx), std::to_string(idx), 1, eloqstore::WriteOp::Upsert);
        }
        eloqstore::BatchWriteRequest req;
        req.SetArgs(tbl_id, std::move(entries));
        store->ExecSync(&req);
    }

    // corrupt it
    std::string datafile = std::string(test_path) + '/' + tbl_id.ToString() +
                           '/' + eloqstore::DataFileName(0, 0);
    std::fstream file(datafile,
                      std::ios::binary | std::ios::out | std::ios::in);
    REQUIRE(file);
    char c;
    file.seekg(10, std::ios::beg);
    file.read(&c, 1);
    REQUIRE(file);
    c += 1;
    file.seekp(10, std::ios::beg);
    file.write(&c, 1);
    REQUIRE(file);
    file.sync();
    REQUIRE(file);
    file.close();

    {
        eloqstore::ScanRequest req;
        req.SetArgs(tbl_id, Key(0), Key(10));
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::Corrupted);
    }

    {
        // can't read success if the target key locate on the same page
        eloqstore::ReadRequest req;
        req.SetArgs(tbl_id, Key(0));
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::Corrupted);
    }
}

TEST_CASE("overflow kv", "[persist][overflow_kv]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);

    const eloqstore::TableIdent tbl_id("overflow", 0);
    const uint32_t biggest = (128 << 20);
    MapVerifier verifier(tbl_id, store);

    eloqstore::BatchWriteRequest write_req;
    write_req.SetTableId(tbl_id);

    for (uint32_t sz = 1; sz <= biggest; sz <<= 1)
    {
        write_req.AddWrite(
            Key(sz), Value(sz, sz), 1, eloqstore::WriteOp::Upsert);
    }
    verifier.ExecWrite(&write_req);
    write_req.batch_.clear();

    verifier.Read(Key(1 << 20));

    verifier.Scan(Key(2 << 10), Key(16 << 20));

    for (uint32_t i : {1, 100, 1024, 5000, 131072})
    {
        write_req.AddWrite(
            Key(i), Value(i + 1, i), 2, eloqstore::WriteOp::Upsert);
    }
    verifier.ExecWrite(&write_req);

    verifier.Read(Key(5000));
}

TEST_CASE("random overflow kv", "[persist][overflow_kv]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verifier(test_tbl_id, store);
    verifier.SetValueSize(5000);
    verifier.WriteRnd(1, 100);
    verifier.SetValueSize(10000);
    verifier.WriteRnd(1, 100);
    verifier.SetValueSize(1 << 20);
    verifier.WriteRnd(1, 100, 20, 10);
    verifier.WriteRnd(1, 100, 20, 10);
    verifier.SetValueSize(5000);
    verifier.WriteRnd(1, 100);
}

TEST_CASE("concurrency with overflow kv", "[overflow_kv]")
{
    eloqstore::KvOptions options{
        .num_threads = 4,
        .store_path = {test_path},
    };
    eloqstore::EloqStore *store = InitStore(options);
    ConcurrencyTester tester(store, "t0", 10, 1000, 4, 50000);
    tester.Init();
    tester.Run(100, 100, 100);
}

TEST_CASE("easy append only mode", "[persist][append]")
{
    eloqstore::KvOptions options{
        .store_path = {test_path},
        .data_append_mode = true,
    };
    eloqstore::EloqStore *store = InitStore(options);

    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(1000);

    verify.WriteRnd(0, 1000);
    verify.WriteRnd(1000, 2000);
    verify.WriteRnd(500, 1500);
}

TEST_CASE("hard append only mode", "[persist][append]")
{
    eloqstore::KvOptions options{
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };
    eloqstore::EloqStore *store = InitStore(options);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(10000);

    for (int i = 0; i < 50; i += 5)
    {
        verify.WriteRnd(0, 1000, i);
    }
    verify.Validate();
}

TEST_CASE("file garbage collector", "[GC]")
{
    eloqstore::KvOptions options{
        .num_retained_archives = 0,
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };
    eloqstore::EloqStore *store = InitStore(options);
    const fs::path dir_path = fs::path(test_path) / test_tbl_id.ToString();

    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(10000);

    tester.Upsert(0, 1000);
    tester.Upsert(0, 1000);
    size_t max_cnt = utils::DirEntryCount(dir_path);
    for (int i = 0; i < 20; i++)
    {
        tester.Upsert(500, 1000);
        // Do validate after each write to try to ensure the file GC is
        // finished.
        tester.Validate();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        size_t cnt = utils::DirEntryCount(dir_path);
        CHECK(cnt <= max_cnt);
    }
}

TEST_CASE("append mode with restart", "[persist]")
{
    eloqstore::KvOptions options{
        .num_retained_archives = 0,
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };

    eloqstore::EloqStore *store = InitStore(options);

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 3; i++)
    {
        eloqstore::TableIdent tbl_id{"t1", i};
        auto tester = std::make_unique<MapVerifier>(tbl_id, store, false);
        tester->SetValueSize(10000);
        tbls.push_back(std::move(tester));
    }

    for (int i = 0; i < 5; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->WriteRnd(0, 1000, 10, 90);
        }
        store->Stop();
        store->Start();
        for (auto &tbl : tbls)
        {
            tbl->Validate();
        }
    }
}

TEST_CASE("append mode survives compression toggles across restarts",
          "[persist]")
{
    eloqstore::KvOptions base_opts = append_opts;
    base_opts.enable_compression = false;
    CleanupStore(base_opts);

    auto start_store = [&](bool enable_compression)
    {
        eloqstore::KvOptions opts = base_opts;
        opts.enable_compression = enable_compression;
        auto new_store = std::make_unique<eloqstore::EloqStore>(opts);
        REQUIRE(new_store->Start() == eloqstore::KvError::NoError);
        return new_store;
    };

    auto store = start_store(false);
    MapVerifier verify(test_tbl_id, store.get(), false);
    verify.SetAutoClean(false);
    verify.SetValueSize(1024);

    verify.Upsert(0, 100);
    verify.Validate();

    store->Stop();
    store = start_store(true);
    verify.SetStore(store.get());
    verify.Upsert(100, 200);
    verify.Validate();

    store->Stop();
    store = start_store(false);
    verify.SetStore(store.get());
    verify.Scan(0, 200);
    verify.Validate();

    store->Stop();
    store = start_store(true);
    verify.SetStore(store.get());
    verify.Scan(0, 200);
    verify.Validate();

    store->Stop();
    store.reset();
    CleanupStore(base_opts);
}
