/**
 * @file ds_shards.cpp
 * @brief One store hosting several data store shards: AttachShard,
 * DetachShard, per-shard terms and term files, the admission gate and the
 * detach drain.
 */
#include <algorithm>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "eloq_store.h"

namespace
{
using eloqstore::DsShardId;
using eloqstore::EloqStore;
using eloqstore::KvError;
using eloqstore::TableIdent;

const TableIdent kPartitionA{"ds_shard_tbl", 1};
const TableIdent kPartitionB{"ds_shard_tbl", 2};

/** @brief Options for a cloud store under its own cloud and local paths. */
eloqstore::KvOptions ShardTestOptions(const std::string &local_path)
{
    eloqstore::KvOptions options = cloud_options;
    options.store_path = {local_path};
    options.cloud_store_path += "/ds-shards";
    options.allow_reuse_local_caches = true;
    return options;
}

/** @brief Upserts one key for a data store shard; returns the error. */
KvError Write(EloqStore *store,
              DsShardId ds_shard_id,
              const TableIdent &tbl_id,
              const std::string &key,
              const std::string &value)
{
    eloqstore::BatchWriteRequest req;
    req.SetDsShard(ds_shard_id);
    req.SetTableId(tbl_id);
    req.AddWrite(key, value, 1, eloqstore::WriteOp::Upsert);
    store->ExecSync(&req);
    return req.Error();
}

/** @brief Reads one key for a data store shard; returns the error. */
KvError Read(EloqStore *store,
             DsShardId ds_shard_id,
             const TableIdent &tbl_id,
             const std::string &key,
             std::string *value = nullptr)
{
    eloqstore::ReadRequest req;
    req.SetDsShard(ds_shard_id);
    req.SetArgs(tbl_id, key);
    store->ExecSync(&req);
    if (value != nullptr && req.Error() == KvError::NoError)
    {
        *value = req.value_;
    }
    return req.Error();
}

bool Contains(const std::vector<std::string> &names, const std::string &name)
{
    return std::find(names.begin(), names.end(), name) != names.end();
}
}  // namespace

TEST_CASE("two data store shards are served in their own terms",
          "[cloud][ds_shard]")
{
    eloqstore::KvOptions options = ShardTestOptions("/tmp/test-ds-shards");
    CleanupStore(options);

    EloqStore store(options);
    REQUIRE(store.StartInstance() == KvError::NoError);
    REQUIRE(store.HostedShards().empty());
    // Nothing is attached: no request is admitted.
    REQUIRE(Write(&store, 1, kPartitionA, "k", "v") == KvError::NotRunning);

    REQUIRE(store.AttachShard(1, 5) == KvError::NoError);
    REQUIRE(store.AttachShard(2, 9) == KvError::NoError);
    REQUIRE(store.HostedShards() == std::vector<DsShardId>{1, 2});
    REQUIRE(store.ShardTerm(1) == 5);
    REQUIRE(store.ShardTerm(2) == 9);
    // Attaching again, or an id out of range, is refused.
    REQUIRE(store.AttachShard(1, 6) == KvError::InvalidArgs);
    REQUIRE(store.AttachShard(EloqStore::kMaxDsShards, 1) ==
            KvError::InvalidArgs);

    REQUIRE(Write(&store, 1, kPartitionA, "a", "va") == KvError::NoError);
    REQUIRE(Write(&store, 2, kPartitionB, "b", "vb") == KvError::NoError);
    std::string value;
    REQUIRE(Read(&store, 1, kPartitionA, "a", &value) == KvError::NoError);
    REQUIRE(value == "va");
    REQUIRE(Read(&store, 2, kPartitionB, "b", &value) == KvError::NoError);
    REQUIRE(value == "vb");
    // A request naming no shard works for the first one attached.
    REQUIRE(store.DefaultDsShard() == 1);
    REQUIRE(Read(&store, eloqstore::kUnsetDsShard, kPartitionA, "a") ==
            KvError::NoError);

    // Each shard has its own term file, and each partition's manifest is
    // named with its shard's term.
    const auto root_files = ListCloudFiles(options, options.cloud_store_path);
    REQUIRE(Contains(root_files, "CURRENT_TERM_main_1"));
    REQUIRE(Contains(root_files, "CURRENT_TERM_main_2"));
    REQUIRE(
        Contains(ListCloudFiles(
                     options, options.cloud_store_path, kPartitionA.ToString()),
                 "manifest_main_5"));
    REQUIRE(
        Contains(ListCloudFiles(
                     options, options.cloud_store_path, kPartitionB.ToString()),
                 "manifest_main_9"));

    // Detaching one shard refuses its requests and leaves the other alone.
    REQUIRE(store.DetachShard(1) == KvError::NoError);
    REQUIRE(store.HostedShards() == std::vector<DsShardId>{2});
    REQUIRE(store.ShardState(1) == EloqStore::DsShardState::Detached);
    REQUIRE(Read(&store, 1, kPartitionA, "a") == KvError::NotRunning);
    REQUIRE(Read(&store, 2, kPartitionB, "b", &value) == KvError::NoError);
    REQUIRE(value == "vb");
    REQUIRE(store.DetachShard(1) == KvError::NoError);

    // Attached again at a newer term, the shard's partition is taken over:
    // its data is there, and its manifest is promoted to the new term.
    REQUIRE(store.AttachShard(1, 6) == KvError::NoError);
    REQUIRE(Read(&store, 1, kPartitionA, "a", &value) == KvError::NoError);
    REQUIRE(value == "va");
    REQUIRE(Write(&store, 1, kPartitionA, "a2", "va2") == KvError::NoError);
    REQUIRE(
        Contains(ListCloudFiles(
                     options, options.cloud_store_path, kPartitionA.ToString()),
                 "manifest_main_6"));

    store.Stop();
    REQUIRE(store.HostedShards().empty());
    CleanupStore(options);
}

TEST_CASE("an expired term fails one data store shard only",
          "[cloud][ds_shard]")
{
    eloqstore::KvOptions first_options =
        ShardTestOptions("/tmp/test-ds-shards-first");
    eloqstore::KvOptions second_options =
        ShardTestOptions("/tmp/test-ds-shards-second");
    CleanupStore(first_options);
    CleanupLocalStore(second_options);

    // The first node hosts shards 1 and 2 at term 5.
    {
        EloqStore first(first_options);
        REQUIRE(first.StartInstance() == KvError::NoError);
        REQUIRE(first.AttachShard(1, 5) == KvError::NoError);
        REQUIRE(first.AttachShard(2, 5) == KvError::NoError);
        REQUIRE(Write(&first, 1, kPartitionA, "a", "va") == KvError::NoError);
        REQUIRE(Write(&first, 2, kPartitionB, "b", "vb") == KvError::NoError);
        first.Stop();
    }

    // A second node takes shard 1 over at term 7 and writes to it.
    {
        EloqStore second(second_options);
        REQUIRE(second.StartInstance() == KvError::NoError);
        REQUIRE(second.AttachShard(1, 7) == KvError::NoError);
        std::string value;
        REQUIRE(Read(&second, 1, kPartitionA, "a", &value) == KvError::NoError);
        REQUIRE(value == "va");
        REQUIRE(Write(&second, 1, kPartitionA, "a7", "va7") ==
                KvError::NoError);
        second.Stop();
    }

    // Back on the first node, shard 1 at term 5 is stale; shard 2 is not.
    {
        EloqStore first(first_options);
        REQUIRE(first.StartInstance() == KvError::NoError);
        REQUIRE(first.AttachShard(2, 5) == KvError::NoError);
        REQUIRE(first.AttachShard(1, 5) == KvError::ExpiredTerm);
        REQUIRE(first.HostedShards() == std::vector<DsShardId>{2});
        REQUIRE(first.ShardState(1) == EloqStore::DsShardState::Detached);
        REQUIRE(Read(&first, 1, kPartitionA, "a") == KvError::NotRunning);
        std::string value;
        REQUIRE(Read(&first, 2, kPartitionB, "b", &value) == KvError::NoError);
        REQUIRE(value == "vb");
        // At a term above the second node's, shard 1 can be taken back.
        REQUIRE(first.AttachShard(1, 8) == KvError::NoError);
        REQUIRE(Read(&first, 1, kPartitionA, "a7", &value) == KvError::NoError);
        REQUIRE(value == "va7");
        first.Stop();
    }

    CleanupStore(first_options);
    CleanupLocalStore(second_options);
}

TEST_CASE("detaching a data store shard drains its running requests",
          "[cloud][ds_shard]")
{
    eloqstore::KvOptions options =
        ShardTestOptions("/tmp/test-ds-shards-drain");
    CleanupStore(options);

    EloqStore store(options);
    REQUIRE(store.StartInstance() == KvError::NoError);
    REQUIRE(store.AttachShard(1, 5) == KvError::NoError);
    REQUIRE(store.AttachShard(2, 5) == KvError::NoError);
    for (int i = 0; i < 200; ++i)
    {
        REQUIRE(Write(&store, 1, kPartitionA, "k" + std::to_string(i), "v") ==
                KvError::NoError);
    }
    REQUIRE(Write(&store, 2, kPartitionB, "b", "vb") == KvError::NoError);

    // Reads of shard 1 keep arriving while it is detached. Every one of them
    // completes, either served or refused; none hangs.
    constexpr int kReads = 2000;
    std::vector<std::unique_ptr<eloqstore::ReadRequest>> reads;
    reads.reserve(kReads);
    std::atomic<int> completed{0};
    std::atomic<int> served{0};
    std::atomic<int> refused{0};
    std::atomic<int> other{0};
    auto on_done = [&](eloqstore::KvRequest *req)
    {
        switch (req->Error())
        {
        case KvError::NoError:
            ++served;
            break;
        case KvError::NotRunning:
            ++refused;
            break;
        default:
            ++other;
            break;
        }
        ++completed;
    };
    std::thread reader(
        [&]
        {
            for (int i = 0; i < kReads; ++i)
            {
                auto req = std::make_unique<eloqstore::ReadRequest>();
                req->SetDsShard(1);
                req->SetArgs(kPartitionA, "k" + std::to_string(i % 200));
                if (!store.ExecAsyn(req.get(), 0, on_done))
                {
                    // Refused at the gate: nothing will call back.
                    ++refused;
                    ++completed;
                }
                reads.push_back(std::move(req));
            }
        });
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    REQUIRE(store.DetachShard(1) == KvError::NoError);
    reader.join();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (completed.load() < kReads &&
           std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    REQUIRE(completed.load() == kReads);
    REQUIRE(other.load() == 0);
    REQUIRE(served.load() + refused.load() == kReads);
    REQUIRE(refused.load() > 0);

    // The other shard was never disturbed, and the detached one comes back.
    std::string value;
    REQUIRE(Read(&store, 2, kPartitionB, "b", &value) == KvError::NoError);
    REQUIRE(value == "vb");
    REQUIRE(store.AttachShard(1, 6) == KvError::NoError);
    REQUIRE(Read(&store, 1, kPartitionA, "k0", &value) == KvError::NoError);
    REQUIRE(value == "v");

    store.Stop();
    CleanupStore(options);
}

TEST_CASE("only cloud mode hosts a second data store shard", "[ds_shard]")
{
    // Start attaches shard 0 with the default options' term 0.
    EloqStore *store = InitStore(default_opts);
    REQUIRE(store->HostedShards() == std::vector<DsShardId>{0});
    REQUIRE(store->DefaultDsShard() == 0);
    REQUIRE(store->AttachShard(1, 0) == KvError::InvalidArgs);
    REQUIRE(Write(store, 0, test_tbl_id, "a", "va") == KvError::NoError);

    REQUIRE(store->DetachShard(0) == KvError::NoError);
    REQUIRE(store->HostedShards().empty());
    REQUIRE(Read(store, 0, test_tbl_id, "a") == KvError::NotRunning);
    // With the default shard gone, a request naming none is refused too.
    REQUIRE(Read(store, eloqstore::kUnsetDsShard, test_tbl_id, "a") ==
            KvError::NotRunning);

    REQUIRE(store->AttachShard(0, 0) == KvError::NoError);
    std::string value;
    REQUIRE(Read(store, 0, test_tbl_id, "a", &value) == KvError::NoError);
    REQUIRE(value == "va");
    store->Stop();
    CleanupStore(default_opts);
}
