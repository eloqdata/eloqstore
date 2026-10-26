/**
 * @file ds_shards.cpp
 * @brief One store hosting several data store shards: AttachShard,
 * DetachShard, per-shard terms and term files, the admission gate, the
 * detach drain, and store-level requests confined to their shard's
 * partitions.
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
    // Partition N belongs to data store shard N: kPartitionA to shard 1,
    // kPartitionB to shard 2.
    options.partition_filter =
        [](DsShardId ds_shard_id, const TableIdent &tbl_id)
    { return static_cast<DsShardId>(tbl_id.partition_id_) == ds_shard_id; };
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

/**
 * @brief Stops and joins a test's worker thread on every exit path, so that a
 * failed REQUIRE unwinds past the thread instead of terminating the process
 * on a joinable one. Declare it after the store, so it goes first.
 */
struct ThreadJoiner
{
    std::thread &thread_;
    std::atomic<bool> *stop_;
    ~ThreadJoiner()
    {
        if (stop_ != nullptr)
        {
            stop_->store(true);
        }
        if (thread_.joinable())
        {
            thread_.join();
        }
    }
};

/** @brief "k" followed by @p i zero-padded to three digits. */
std::string PaddedKey(int i)
{
    std::string digits = std::to_string(i);
    return "k" + std::string(3 - digits.size(), '0') + digits;
}

/**
 * @brief Counts a partition's archives of one term in the cloud: the files
 * named manifest_main_<term>_<tag>, which the manifest itself,
 * manifest_main_<term>, does not match.
 * @param options The store's options, for the cloud path.
 * @param tbl_id The partition.
 * @param term The term the archives are named with.
 * @return How many there are.
 */
size_t CountArchives(const eloqstore::KvOptions &options,
                     const TableIdent &tbl_id,
                     uint64_t term)
{
    const std::string prefix = "manifest_main_" + std::to_string(term) + "_";
    size_t count = 0;
    for (const std::string &name :
         ListCloudFiles(options, options.cloud_store_path, tbl_id.ToString()))
    {
        if (name.rfind(prefix, 0) == 0)
        {
            ++count;
        }
    }
    return count;
}

/**
 * @brief Polls a condition every 200 ms until it holds or a timeout passes.
 * @param timeout How long to wait at most.
 * @param pred The condition.
 * @return Whether it held.
 */
template <typename Pred>
bool WaitUntil(std::chrono::milliseconds timeout, Pred &&pred)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}
}  // namespace

TEST_CASE("two data store shards are served in their own terms",
          "[cloud][ds_shard]")
{
    eloqstore::KvOptions options = ShardTestOptions("/tmp/test-ds-shards");
    CleanupStore(options);

    {
        // Hosting a second shard needs the partition filter.
        eloqstore::KvOptions unfiltered = options;
        unfiltered.partition_filter = nullptr;
        EloqStore store(unfiltered);
        REQUIRE(store.StartInstance() == KvError::NoError);
        REQUIRE(store.AttachShard(1, 5) == KvError::NoError);
        REQUIRE(store.AttachShard(2, 9) == KvError::InvalidArgs);
        REQUIRE(store.HostedShards() == std::vector<DsShardId>{1});
        store.Stop();
    }

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

    // Reads of shard 1 keep arriving until the gate refuses one, which the
    // detach below makes it do. Every one of them completes, either served
    // or refused; none hangs.
    constexpr int kMaxReads = 20000;
    std::vector<std::unique_ptr<eloqstore::ReadRequest>> reads;
    reads.reserve(kMaxReads);
    std::atomic<int> submitted{0};
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
            for (int i = 0; i < kMaxReads; ++i)
            {
                auto req = std::make_unique<eloqstore::ReadRequest>();
                req->SetDsShard(1);
                req->SetArgs(kPartitionA, "k" + std::to_string(i % 200));
                ++submitted;
                const bool accepted = store.ExecAsyn(req.get(), 0, on_done);
                reads.push_back(std::move(req));
                if (!accepted)
                {
                    // Refused at the gate: nothing will call back, and the
                    // detach has begun.
                    ++refused;
                    ++completed;
                    break;
                }
            }
        });
    ThreadJoiner join_reader{reader, nullptr};
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    REQUIRE(store.DetachShard(1) == KvError::NoError);
    reader.join();
    const int total = submitted.load();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (completed.load() < total &&
           std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    REQUIRE(completed.load() == total);
    REQUIRE(other.load() == 0);
    REQUIRE(served.load() + refused.load() == total);
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

TEST_CASE("detaching a data store shard waits for its background work",
          "[cloud][ds_shard]")
{
    // Overwriting the same keys round after round exceeds the file
    // amplification, so the write tasks keep queuing compactions, and the
    // sealed files queue file GC. That work is queued by shard 1's tasks and
    // runs as shard 1's own, in shard 1's term: a detach fails what has not
    // started and waits for what runs, instead of pulling the partition from
    // under it, and nothing runs outside the shard's term.
    eloqstore::KvOptions options = ShardTestOptions("/tmp/test-ds-shards-bg");
    CleanupStore(options);

    EloqStore store(options);
    REQUIRE(store.StartInstance() == KvError::NoError);
    REQUIRE(store.AttachShard(1, 5) == KvError::NoError);
    REQUIRE(store.AttachShard(2, 5) == KvError::NoError);
    REQUIRE(Write(&store, 2, kPartitionB, "b", "vb") == KvError::NoError);

    constexpr int kKeys = 200;
    std::atomic<bool> stop{false};
    std::atomic<int> served{0};
    std::atomic<int> refused{0};
    std::atomic<int> other{0};
    std::thread writer(
        [&]
        {
            for (int round = 0; !stop.load(); ++round)
            {
                eloqstore::BatchWriteRequest req;
                req.SetDsShard(1);
                req.SetTableId(kPartitionA);
                const std::string value(1000,
                                        static_cast<char>('a' + round % 26));
                for (int i = 0; i < kKeys; ++i)
                {
                    // A batch's keys are sorted: zero-padded, so numeric
                    // order is key order.
                    req.AddWrite(PaddedKey(i),
                                 value,
                                 static_cast<uint64_t>(round + 1),
                                 eloqstore::WriteOp::Upsert);
                }
                store.ExecSync(&req);
                switch (req.Error())
                {
                case KvError::NoError:
                    ++served;
                    break;
                case KvError::NotRunning:
                    ++refused;
                    // Do not spin while the shard is detached.
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                    break;
                default:
                    ++other;
                    break;
                }
            }
        });

    ThreadJoiner join_writer{writer, &stop};

    // Detach and re-attach the shard under the storm, each time at a newer
    // term: each detach meets writes, compactions and file GC queued or in
    // flight.
    uint64_t term = 5;
    for (int cycle = 0; cycle < 5; ++cycle)
    {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(cycle == 0 ? 1000 : 400));
        REQUIRE(store.DetachShard(1) == KvError::NoError);
        REQUIRE(Read(&store, 1, kPartitionA, PaddedKey(0)) ==
                KvError::NotRunning);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        REQUIRE(store.AttachShard(1, ++term) == KvError::NoError);
    }
    stop.store(true);
    writer.join();
    REQUIRE(other.load() == 0);
    REQUIRE(served.load() > 0);
    REQUIRE(refused.load() > 0);

    // The other shard was never disturbed, and the partition holds what its
    // last served round wrote.
    std::string value;
    REQUIRE(Read(&store, 2, kPartitionB, "b", &value) == KvError::NoError);
    REQUIRE(value == "vb");
    REQUIRE(Read(&store, 1, kPartitionA, PaddedKey(0), &value) ==
            KvError::NoError);
    REQUIRE(value.size() == 1000);
    REQUIRE(Read(&store, 1, kPartitionA, PaddedKey(kKeys - 1), &value) ==
            KvError::NoError);
    REQUIRE(value.size() == 1000);

    // Every file the storm and its background work put in the cloud is named
    // with one of the terms the shard held: the data and segment files, and
    // the manifests, end in their term.
    const auto files = ListCloudFiles(
        options, options.cloud_store_path, kPartitionA.ToString());
    REQUIRE(!files.empty());
    for (const std::string &name : files)
    {
        const size_t sep = name.rfind('_');
        REQUIRE(sep != std::string::npos);
        const uint64_t file_term = std::stoull(name.substr(sep + 1));
        INFO(name);
        REQUIRE(file_term >= 5);
        REQUIRE(file_term <= term);
    }

    store.Stop();
    CleanupStore(options);
}

TEST_CASE("each data store shard is archived on its own schedule",
          "[cloud][ds_shard][archive]")
{
    eloqstore::KvOptions options =
        ShardTestOptions("/tmp/test-ds-shards-archive");
    options.num_retained_archives = 100;
    options.archive_interval_secs = 1;
    CleanupStore(options);
    const std::chrono::seconds timeout(20);

    EloqStore store(options);
    REQUIRE(store.StartInstance() == KvError::NoError);
    REQUIRE(store.AttachShard(1, 5) == KvError::NoError);
    REQUIRE(store.AttachShard(2, 9) == KvError::NoError);
    REQUIRE(Write(&store, 1, kPartitionA, "a", "va") == KvError::NoError);
    REQUIRE(Write(&store, 2, kPartitionB, "b", "vb") == KvError::NoError);

    // Each shard's archives are named with its own term and cover only its
    // own partitions.
    REQUIRE(WaitUntil(timeout,
                      [&]
                      {
                          return CountArchives(options, kPartitionA, 5) > 0 &&
                                 CountArchives(options, kPartitionB, 9) > 0;
                      }));
    REQUIRE(CountArchives(options, kPartitionA, 9) == 0);
    REQUIRE(CountArchives(options, kPartitionB, 5) == 0);

    // A detach ends the shard's schedule; the other shard's goes on. The
    // detach drains an archive of the shard in flight, so nothing of shard 1
    // lands after it returns.
    REQUIRE(store.DetachShard(1) == KvError::NoError);
    const size_t a_archives = CountArchives(options, kPartitionA, 5);
    const size_t b_archives = CountArchives(options, kPartitionB, 9);
    REQUIRE(WaitUntil(
        timeout,
        [&]
        { return CountArchives(options, kPartitionB, 9) >= b_archives + 2; }));
    REQUIRE(CountArchives(options, kPartitionA, 5) == a_archives);

    // Attached again at a newer term, the shard starts a fresh schedule, and
    // its archives carry the new term.
    REQUIRE(store.AttachShard(1, 6) == KvError::NoError);
    REQUIRE(Write(&store, 1, kPartitionA, "a6", "va6") == KvError::NoError);
    REQUIRE(WaitUntil(
        timeout, [&] { return CountArchives(options, kPartitionA, 6) > 0; }));

    store.Stop();
    CleanupStore(options);
}

TEST_CASE("dropping a table drops its data store shard's partitions only",
          "[cloud][ds_shard]")
{
    eloqstore::KvOptions options = ShardTestOptions("/tmp/test-ds-shards-drop");
    CleanupStore(options);

    EloqStore store(options);
    REQUIRE(store.StartInstance() == KvError::NoError);
    REQUIRE(store.AttachShard(1, 5) == KvError::NoError);
    REQUIRE(store.AttachShard(2, 5) == KvError::NoError);
    REQUIRE(Write(&store, 1, kPartitionA, "a", "va") == KvError::NoError);
    REQUIRE(Write(&store, 2, kPartitionB, "b", "vb") == KvError::NoError);

    // The table has a partition in each shard. Dropped for shard 1, it loses
    // shard 1's partition only.
    eloqstore::DropTableRequest drop;
    drop.SetDsShard(1);
    drop.SetArgs(kPartitionA.tbl_name_);
    store.ExecSync(&drop);
    REQUIRE(drop.Error() == KvError::NoError);
    REQUIRE(Read(&store, 1, kPartitionA, "a") == KvError::NotFound);
    std::string value;
    REQUIRE(Read(&store, 2, kPartitionB, "b", &value) == KvError::NoError);
    REQUIRE(value == "vb");

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
