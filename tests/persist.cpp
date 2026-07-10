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
#include "fail_point.h"
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
        REQUIRE(store->Start(eloqstore::MainBranchName, 0) ==
                eloqstore::KvError::NoError);
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
                REQUIRE(scan_req.Error() == eloqstore::KvError::NotFound);
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
    std::string datafile =
        std::string(test_path) + '/' + tbl_id.ToString() + '/' +
        eloqstore::BranchDataFileName(0, eloqstore::MainBranchName, 0);
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
    // Each partition is seeded with seg_size*seg_count = 4000 x 50KB overflow
    // values (~200MB), written independently, and InitStore wipes the store so
    // the seed is rewritten every run. Seeding dominated wall time and blew the
    // ctest timeout, so keep the partition count small -- the overflow
    // read/write concurrency coverage is preserved with fewer partitions.
    ConcurrencyTester tester(store, "t0", 3, 1000, 4, 50000);
    tester.Init();
    tester.Run(100, 100, 100);
}

// ProcessReq's ListObject case downcasts io_mgr_ to
// CloudStoreMgr with no mode check. In a non-cloud store io_mgr_ is not a
// CloudStoreMgr, so the cast + AcquireCloudSlot/GetObjectStore reads past the
// object -> out-of-bounds / UB. It must return InvalidArgs instead (like the
// sibling ListStandbyPartition request).
TEST_CASE("ListObject on a non-cloud store returns InvalidArgs", "[persist]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    std::vector<std::string> objects;
    eloqstore::ListObjectRequest req(&objects);
    req.SetRemotePath("any/prefix");
    store->ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::InvalidArgs);
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
        REQUIRE(store->Start(eloqstore::MainBranchName, 0) ==
                eloqstore::KvError::NoError);
        for (auto &tbl : tbls)
        {
            tbl->Validate();
        }
    }
}

TEST_CASE("write an overflow page without a write buffer pool (cloud)",
          "[persist][overflow_kv][cloud]")
{
    eloqstore::KvOptions options = cloud_options;
    options.buffer_pool_size = 1 * eloqstore::MB;  // 1MB buffer pool
    options.pages_per_file_shift = 0;              // one 4K page per data file
    eloqstore::EloqStore *store = InitStore(options);

    const eloqstore::TableIdent tbl_id{"overflow-no-wbuf-cloud", 0};

    MapVerifier verify(tbl_id, store);
    verify.SetValueSize(20 * 1024);  // 20K value (stored via overflow pages)

    // Write the 20K key-value once. MapVerifier::Upsert asserts the write
    // returns NoError and validates the readback.
    constexpr uint64_t kKey = 1;
    verify.Upsert(kKey);

    store->Stop();
}

// A write task that aborts after advancing the BranchFileMapping file-id
// high-water must roll that advance back. Otherwise the next write to the same
// table allocates a lower file id than the stale recorded max and trips the
// ascending-order CHECK in SetBranchFileIdTerm (which aborts the process). The
// SyncFiles fail point forces the abort deterministically; the tiny data files
// (one 4K page each) make the first write span several files so its high-water
// is well past file 0.
TEST_CASE("write task abort rolls back branch file-id high-water (cloud)",
          "[persist][overflow_kv][cloud][abort]")
{
    eloqstore::KvOptions options = cloud_options;
    options.buffer_pool_size = 1 * eloqstore::MB;  // no write-buffer pool
    options.pages_per_file_shift = 0;              // one 4K page per data file
    eloqstore::EloqStore *store = InitStore(options);

    const eloqstore::TableIdent tbl_id{"abort-rollback-cloud", 0};

    auto write = [&](uint64_t key, uint32_t value_len, uint64_t ts)
    {
        eloqstore::BatchWriteRequest req;
        req.SetTableId(tbl_id);
        req.AddWrite(
            Key(key), Value(key, value_len), ts, eloqstore::WriteOp::Upsert);
        store->ExecSync(&req);
        return req.Error();
    };

    // Arm the flush fail point: the first SyncFiles returns Corrupted, aborting
    // the write task after it has stamped file ids well past file 0.
    eloqstore::FailPoint::GetInstance().ArmOnce("SyncFiles");
    REQUIRE(write(1, 20 * 1024, 1) == eloqstore::KvError::Corrupted);
    eloqstore::FailPoint::GetInstance().Disarm();

    // The next write must succeed: Abort rolled the high-water back to its
    // pre-task value. Without the rollback it regresses and aborts the process.
    REQUIRE(write(2, 64, 2) == eloqstore::KvError::NoError);
}

// A write whose merged data flush fails must not leak the cache page it is
// holding. With enable_data_page_cache, each leaf data page is promoted into a
// fresh MemCachedPage before being written (write_task.cpp WritePage). When a
// subsequent page forces AppendWritePage to flush the buffered batch and that
// flush fails, the buggy error return drops the current promote page's only
// Handle -- which merely unpins a page that is detached (not on the free list
// nor the active LRU), permanently orphaning that buffer-pool slot. Under a
// tiny pool, repeated failures stuff every slot with orphans until no page can
// be allocated at all, so a later healthy write can never rebuild its index and
// fails with OutOfMem forever.
//
// The pool is sized to hold 8 cache pages (64KB * (1 - 0.5) / 4KB); one page
// per data file (shift 0) makes every leaf after the first target a new file,
// so each failing write flushes the prior leaf and leaks the current one. The
// SubmitMergedWrite fail point forces exactly one flush failure per armed
// write.
TEST_CASE("failed merged write must not leak the held cache page",
          "[persist][append][abort]")
{
    eloqstore::KvOptions options = append_opts;  // data_append_mode = true
    options.pages_per_file_shift = 0;            // one 4K page per data file
    options.enable_data_page_cache = true;  // promote leaves -> the leak site
    options.data_page_size = 4 * eloqstore::KB;
    // Write buffer holds 2 pages, so a buffered leaf does not auto-flush before
    // the next (different-file) leaf forces the flush we want to fail.
    options.write_buffer_size = 8 * eloqstore::KB;
    options.write_buffer_ratio = 0.5;  // 32KB write buffers, 32KB cache
    options.buffer_pool_size = 64 * eloqstore::KB;  // cache limit = 8 pages
    options.auto_oom_retry_times = 0;  // let OutOfMem surface immediately

    eloqstore::EloqStore *store = InitStore(options);
    const eloqstore::TableIdent tbl_id{"merged-write-leak", 0};

    auto write_batch = [&](uint64_t base, uint64_t count, uint64_t ts)
    {
        eloqstore::BatchWriteRequest req;
        req.SetTableId(tbl_id);
        for (uint64_t k = 0; k < count; ++k)
        {
            req.AddWrite(Key(base + k),
                         Value(base + k, 512),
                         ts,
                         eloqstore::WriteOp::Upsert);
        }
        store->ExecSync(&req);
        return req.Error();
    };

    // Each failing write spans several 4K leaf pages (>=2 files, shift 0): the
    // first leaf is promoted and buffered, the second forces AppendWritePage to
    // flush it, the injected failure aborts, and the buggy code leaks the
    // second leaf's held promote page. After ~7 failures every cache slot but
    // one is orphaned; from then on promotion just fails and the write degrades
    // to the un-promoted path (no further leak), so the failures keep returning
    // the injected error rather than OutOfMem.
    for (int i = 0; i < 32; ++i)
    {
        eloqstore::FailPoint::GetInstance().ArmOnce("SubmitMergedWrite");
        eloqstore::KvError err =
            write_batch(/*base=*/i * 100000, /*count=*/40, /*ts=*/i + 1);
        eloqstore::FailPoint::GetInstance().Disarm();
        REQUIRE(err == eloqstore::KvError::Corrupted);
    }

    // A healthy write whose B-tree is deep enough to buffer more than one index
    // page at once (Pop flushes the previous index page -- still pinned in the
    // write buffer -- then allocates the next). On fixed code the intact pool
    // absorbs this and the write commits; on buggy code the orphaned slots
    // leave no page to allocate for the second index page and it fails with
    // OutOfMem.
    REQUIRE(write_batch(/*base=*/90000000, /*count=*/4000, /*ts=*/1000) ==
            eloqstore::KvError::NoError);

    store->Stop();
}

// Sibling of the above for the index-build path: when Pop's FinishIndexPage /
// FlushIndexPage fails while prev_handle still holds the freshly allocated
// index page, the buggy code drops that handle without freeing it, orphaning
// the cache slot. The FlushIndexPage fail point forces exactly that: the data
// leaves flush fine, then the root index page's flush fails with prev_handle
// held. Each failure leaks one slot until the pool is exhausted and a later
// healthy write can no longer allocate an index page.
TEST_CASE("failed index-page flush must not leak the held cache page",
          "[persist][append][abort]")
{
    eloqstore::KvOptions options = append_opts;  // data_append_mode = true
    options.pages_per_file_shift = 0;            // one 4K page per data file
    options.enable_data_page_cache = true;
    options.data_page_size = 4 * eloqstore::KB;
    options.write_buffer_size = 8 * eloqstore::KB;
    options.write_buffer_ratio = 0.5;               // 32KB write buffers
    options.buffer_pool_size = 64 * eloqstore::KB;  // cache limit = 8 pages
    options.auto_oom_retry_times = 0;

    eloqstore::EloqStore *store = InitStore(options);
    const eloqstore::TableIdent tbl_id{"index-flush-leak", 0};

    auto write_batch = [&](uint64_t base, uint64_t count, uint64_t ts)
    {
        eloqstore::BatchWriteRequest req;
        req.SetTableId(tbl_id);
        for (uint64_t k = 0; k < count; ++k)
        {
            req.AddWrite(Key(base + k),
                         Value(base + k, 512),
                         ts,
                         eloqstore::WriteOp::Upsert);
        }
        store->ExecSync(&req);
        return req.Error();
    };

    // Each armed write fails at the index-page flush while Pop holds the index
    // page; on buggy code that page leaks. Once the pool fills the writes start
    // failing earlier with OutOfMem instead, so assert only that they fail.
    for (int i = 0; i < 32; ++i)
    {
        eloqstore::FailPoint::GetInstance().ArmOnce("FlushIndexPage");
        eloqstore::KvError err =
            write_batch(/*base=*/i * 100000, /*count=*/40, /*ts=*/i + 1);
        eloqstore::FailPoint::GetInstance().Disarm();
        REQUIRE(err != eloqstore::KvError::NoError);
    }

    // On fixed code no slot leaked, so this healthy write commits; on buggy
    // code the orphaned slots leave no page for the index build and it
    // OutOfMems.
    REQUIRE(write_batch(/*base=*/90000000, /*count=*/40, /*ts=*/1000) ==
            eloqstore::KvError::NoError);

    store->Stop();
}

// Third leak site: the non-append write path (no write-buffer pool), where the
// promoted page is moved into IouringMgr::WritePage and a synchronous failure
// (modeled by the fail point, in reality an OpenOrCreateFD error) returned
// before the page is submitted. IouringMgr::WritePage now takes the page by
// reference and only consumes it on success, so WriteTask::WritePage can
// release it on error; the buggy code dropped the moved-in page and leaked it.
TEST_CASE("failed non-append write must not leak the held cache page",
          "[persist][abort]")
{
    eloqstore::KvOptions options = default_opts;  // data_append_mode = false
    options.enable_data_page_cache = true;        // promote leaves -> leak site
    options.data_page_size = 4 * eloqstore::KB;
    options.buffer_pool_size = 64 * eloqstore::KB;
    options.auto_oom_retry_times = 0;

    eloqstore::EloqStore *store = InitStore(options);
    const eloqstore::TableIdent tbl_id{"non-append-leak", 0};

    auto write_batch = [&](uint64_t base, uint64_t count, uint64_t ts)
    {
        eloqstore::BatchWriteRequest req;
        req.SetTableId(tbl_id);
        for (uint64_t k = 0; k < count; ++k)
        {
            req.AddWrite(Key(base + k),
                         Value(base + k, 512),
                         ts,
                         eloqstore::WriteOp::Upsert);
        }
        store->ExecSync(&req);
        return req.Error();
    };

    // Each armed write fails at the first page's WritePage while a promoted
    // page is held; on buggy code that page leaks. Once the pool is full
    // promotion stops (the write degrades to the un-promoted path), so no
    // further leak.
    for (int i = 0; i < 32; ++i)
    {
        eloqstore::FailPoint::GetInstance().ArmOnce("WritePageBeforeSubmit");
        eloqstore::KvError err =
            write_batch(/*base=*/i * 100000, /*count=*/40, /*ts=*/i + 1);
        eloqstore::FailPoint::GetInstance().Disarm();
        REQUIRE(err != eloqstore::KvError::NoError);
    }

    // On fixed code no slot leaked, so this healthy write commits; on buggy
    // code the orphaned slots leave no page to allocate and it OutOfMems.
    REQUIRE(write_batch(/*base=*/90000000, /*count=*/40, /*ts=*/1000) ==
            eloqstore::KvError::NoError);

    store->Stop();
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
        REQUIRE(new_store->Start(eloqstore::MainBranchName, 0) ==
                eloqstore::KvError::NoError);
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
