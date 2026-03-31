#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>  // NOLINT(build/c++17)
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "eloq_store.h"
#include "test_utils.h"
#include "types.h"

namespace fs = std::filesystem;
namespace chrono = std::chrono;
using std::chrono_literals::operator""ms;
using std::chrono_literals::operator""s;
using test_util::Key;
using test_util::Scan;
using test_util::Value;

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

void UpsertRange(eloqstore::EloqStore &store,
                 const eloqstore::TableIdent &tbl_id,
                 uint64_t begin,
                 uint64_t end)
{
    std::vector<eloqstore::WriteDataEntry> entries;
    for (uint64_t i = begin; i < end; ++i)
    {
        entries.emplace_back(Key(i), Value(i), 0, eloqstore::WriteOp::Upsert);
    }
    eloqstore::BatchWriteRequest req;
    req.SetArgs(tbl_id, std::move(entries));
    store.ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

void DeleteRange(eloqstore::EloqStore &store,
                 const eloqstore::TableIdent &tbl_id,
                 uint64_t begin,
                 uint64_t end)
{
    std::vector<eloqstore::WriteDataEntry> entries;
    for (uint64_t i = begin; i < end; ++i)
    {
        entries.emplace_back(Key(i), "", 0, eloqstore::WriteOp::Delete);
    }
    eloqstore::BatchWriteRequest req;
    req.SetArgs(tbl_id, std::move(entries));
    store.ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

void VerifyRange(eloqstore::EloqStore &store,
                 const eloqstore::TableIdent &tbl_id,
                 uint64_t begin,
                 uint64_t end,
                 bool expect_exists)
{
    for (uint64_t i = begin; i < end; ++i)
    {
        eloqstore::ReadRequest req;
        req.SetArgs(tbl_id, Key(i));
        store.ExecSync(&req);
        if (expect_exists)
        {
            REQUIRE(req.Error() == eloqstore::KvError::NoError);
            REQUIRE(req.value_ == Value(i));
        }
        else
        {
            REQUIRE(req.Error() == eloqstore::KvError::NotFound);
        }
    }
}
}  // namespace

TEST_CASE("standby reopen a partition with nonexistent dir", "[standby]")
{
    fs::path dir = fs::temp_directory_path() / fs::path("standby-test");
    fs::path remote_master_dir = dir / "remote-master";
    fs::path local_dir = dir / "local";
    fs::remove_all(dir);
    fs::create_directories(local_dir);
    fs::create_directories(remote_master_dir);

    eloqstore::KvOptions opts;
    opts.store_path = {local_dir.string()};
    opts.enable_local_standby = true;
    opts.standby_master_store_paths = {};
    opts.pages_per_file_shift = 0;

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};

    eloqstore::EloqStore store(opts);
    REQUIRE(store.Start("main", 1) == eloqstore::KvError::NoError);
    UpsertRange(store, tbl_id, 0, 5000);
    VerifyRange(store, tbl_id, 0, 5000, true);
    store.Stop();

    opts.standby_master_addr = "local";
    opts.standby_master_store_paths = {remote_master_dir.string()};
    eloqstore::EloqStore standby(opts);
    REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);
    eloqstore::GlobalReopenRequest reopen_req;
    reopen_req.SetTag(std::to_string(1001));
    standby.ExecSync(&reopen_req);
    REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

    VerifyRange(standby, tbl_id, 0, 5000, false);

    standby.Stop();

    fs::remove_all(dir);
}

TEST_CASE("standby reopen a partition with empty dir", "[standby]")
{
    fs::path dir = fs::temp_directory_path() / fs::path("standby-test");
    fs::path remote_master_dir = dir / "remote-master";
    fs::path local_dir = dir / "local";

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};

    fs::remove_all(dir);
    fs::create_directories(remote_master_dir / tbl_id.ToString());
    fs::create_directories(local_dir);

    eloqstore::KvOptions opts;
    opts.store_path = {local_dir.string()};
    opts.enable_local_standby = true;
    opts.standby_master_store_paths = {};
    opts.pages_per_file_shift = 0;

    eloqstore::EloqStore store(opts);
    REQUIRE(store.Start("main", 1) == eloqstore::KvError::NoError);
    UpsertRange(store, tbl_id, 0, 5000);
    VerifyRange(store, tbl_id, 0, 5000, true);
    store.Stop();

    opts.standby_master_addr = "local";
    opts.standby_master_store_paths = {remote_master_dir.string()};
    eloqstore::EloqStore standby(opts);
    REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);
    eloqstore::GlobalReopenRequest reopen_req;
    reopen_req.SetTag(std::to_string(1001));
    standby.ExecSync(&reopen_req);
    REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

    VerifyRange(standby, tbl_id, 0, 5000, false);

    standby.Stop();

    fs::remove_all(dir);
}

TEST_CASE("standby reopen loads archived master data into empty standby",
          "[standby]")
{
    fs::path dir = fs::temp_directory_path() / fs::path("standby-test");
    fs::path master_dir = dir / "master";
    fs::path standby_dir = dir / "standby";
    fs::remove_all(dir);
    fs::create_directories(master_dir);
    fs::create_directories(standby_dir);

    eloqstore::KvOptions master_opts;
    master_opts.store_path = {master_dir.string()};
    master_opts.enable_local_standby = true;
    master_opts.standby_master_store_paths = {};
    master_opts.pages_per_file_shift = 0;

    eloqstore::KvOptions standby_opts = master_opts;
    standby_opts.store_path = {standby_dir.string()};
    standby_opts.standby_master_addr = "local";
    standby_opts.standby_master_store_paths = {master_dir.string()};

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);
        UpsertRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, true);

        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(1001));
        master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(1001));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, true);

        standby.Stop();
    }

    fs::remove_all(dir);
}

TEST_CASE("cloud reopen clears local-only partition when remote is empty",
          "[standby][cloud]")
{
    fs::path temp_root =
        fs::temp_directory_path() / fs::path("standby-cloud-clean-test");
    fs::path local_dir = temp_root / "local";
    fs::remove_all(temp_root);
    fs::create_directories(local_dir);

    std::string cloud_storage_path = cloud_archive_opts.cloud_store_path + "/" +
                                     temp_root.filename().string();

    eloqstore::KvOptions local_opts = cloud_archive_opts;
    local_opts.store_path = {local_dir.string()};
    local_opts.cloud_store_path.clear();
    local_opts.pages_per_file_shift = 0;
    local_opts.allow_reuse_local_caches = true;

    eloqstore::KvOptions cloud_opts = cloud_archive_opts;
    cloud_opts.store_path = {local_dir.string()};
    cloud_opts.cloud_store_path = cloud_storage_path;
    cloud_opts.pages_per_file_shift = 0;
    cloud_opts.allow_reuse_local_caches = true;

    CleanupStore(cloud_opts);

    eloqstore::TableIdent tbl_id{"standby_cloud_tbl", 0};
    const fs::path partition_path = local_dir / tbl_id.ToString();

    {
        eloqstore::EloqStore store(local_opts);
        REQUIRE(store.Start("main", 1) == eloqstore::KvError::NoError);
        UpsertRange(store, tbl_id, 0, 5000);
        VerifyRange(store, tbl_id, 0, 5000, true);
        store.Stop();
    }

    {
        eloqstore::EloqStore store(cloud_opts);
        REQUIRE(store.Start("main", 1) == eloqstore::KvError::NoError);

        eloqstore::GlobalReopenRequest reopen_req;
        store.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(store, tbl_id, 0, 5000, false);
        REQUIRE(WaitForCondition(
            5s, 100ms, [&]() { return !fs::exists(partition_path); }));

        store.Stop();
    }

    CleanupStore(cloud_opts);
    fs::remove_all(temp_root);
}

TEST_CASE("standby rsync replica follows master changes", "[standby]")
{
    fs::path temp_root = fs::temp_directory_path() / fs::path("standby-test");
    fs::path master_dir = temp_root / "master";
    fs::path new_master_dir = temp_root / "new_master";
    fs::path standby_dir = temp_root / "standby";
    fs::remove_all(temp_root);
    fs::create_directories(master_dir);
    fs::create_directories(standby_dir);

    eloqstore::KvOptions master_opts;
    master_opts.store_path = {master_dir.string()};
    master_opts.enable_local_standby = true;
    master_opts.standby_master_store_paths = {};
    master_opts.pages_per_file_shift = 0;

    eloqstore::KvOptions new_master_opts = master_opts;
    new_master_opts.store_path = {new_master_dir.string()};

    eloqstore::KvOptions standby_opts;
    standby_opts.store_path = {standby_dir.string()};
    standby_opts.enable_local_standby = true;
    standby_opts.standby_master_addr = "local";
    standby_opts.standby_master_store_paths = {master_dir.string()};
    standby_opts.pages_per_file_shift = 0;

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};
    const fs::path partition_path = standby_dir / tbl_id.ToString();

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);
        UpsertRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, true);

        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(1001));
        master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        // load manifest and index pages into memory to test online reopen
        Scan(&standby, tbl_id, 0, 10000);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(1001));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, true);

        standby.Stop();
    }

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);

        DeleteRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, false);

        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(2002));
        master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        // load manifest and index pages into memory to test online reopen
        Scan(&standby, tbl_id, 0, 10000);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(2002));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        REQUIRE(WaitForCondition(
            10s, 100ms, [&]() { return !fs::exists(partition_path); }));

        eloqstore::ReadRequest read_req;
        read_req.SetArgs(tbl_id, Key(0));
        standby.ExecSync(&read_req);
        REQUIRE(read_req.Error() == eloqstore::KvError::NotFound);

        VerifyRange(standby, tbl_id, 0, 5000, false);

        standby.Stop();
    }

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);
        UpsertRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, true);

        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(3003));
        master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        // load manifest and index pages into memory to test online reopen
        Scan(&standby, tbl_id, 0, 10000);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(3003));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, true);

        standby.Stop();
    }

    {
        eloqstore::EloqStore new_master(new_master_opts);
        REQUIRE(new_master.Start("main", 2) == eloqstore::KvError::NoError);
        UpsertRange(new_master, tbl_id, 5001, 10000);
        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(2002));
        new_master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        new_master.Stop();
    }

    {
        standby_opts.standby_master_store_paths = {new_master_dir.string()};
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 2) == eloqstore::KvError::NoError);

        Scan(&standby, tbl_id, 0, 10000);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(2002));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        REQUIRE(WaitForCondition(
            10s,
            100ms,
            [&]()
            {
                if (!fs::exists(partition_path))
                {
                    return false;
                }

                bool has_manifest = false;
                for (const auto &entry : fs::directory_iterator(partition_path))
                {
                    if (!entry.is_regular_file())
                    {
                        continue;
                    }

                    const std::string filename =
                        entry.path().filename().string();
                    const auto parsed = eloqstore::ParseFileName(filename);
                    const auto &type = parsed.first;
                    if (type == eloqstore::FileNameManifest)
                    {
                        has_manifest = true;
                        continue;
                    }

                    if (type == eloqstore::FileNameData)
                    {
                        eloqstore::FileId file_id = 0;
                        std::string_view branch_name;
                        uint64_t term = 0;
                        if (!eloqstore::ParseDataFileSuffix(
                                parsed.second, file_id, branch_name, term) ||
                            term != 2)
                        {
                            return false;
                        }
                    }
                }

                return has_manifest;
            }));

        VerifyRange(standby, tbl_id, 0, 5000, false);
        VerifyRange(standby, tbl_id, 5001, 10000, true);

        standby.Stop();
    }

    fs::remove_all(temp_root);
}

TEST_CASE("standby replica follows cloud-mode master", "[standby][cloud]")
{
    fs::path temp_root = fs::temp_directory_path() / "standby-cloud-test";
    fs::path master_dir = temp_root / "master";
    fs::path new_master_dir = temp_root / "new_master";
    fs::path standby_dir = temp_root / "standby";
    fs::remove_all(temp_root);

    std::string cloud_storage_path = cloud_archive_opts.cloud_store_path + "/" +
                                     temp_root.filename().string();

    eloqstore::KvOptions master_opts = cloud_archive_opts;
    master_opts.store_path = {master_dir.string()};
    master_opts.cloud_store_path = cloud_storage_path;
    master_opts.allow_reuse_local_caches = true;
    master_opts.pages_per_file_shift = 0;

    eloqstore::KvOptions standby_opts = cloud_archive_opts;
    standby_opts.store_path = {standby_dir.string()};
    standby_opts.cloud_store_path = cloud_storage_path;
    standby_opts.allow_reuse_local_caches = true;
    standby_opts.pages_per_file_shift = 0;

    eloqstore::KvOptions new_master_opts = master_opts;
    new_master_opts.store_path = {new_master_dir.string()};

    CleanupStore(master_opts);
    CleanupStore(standby_opts);
    CleanupStore(new_master_opts);

    eloqstore::TableIdent tbl_id{"standby_cloud_tbl", 0};
    const fs::path partition_path = standby_dir / tbl_id.ToString();

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);
        UpsertRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, true);

        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(1001));
        master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        DeleteRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, false);

        master.Stop();
    }

    {
        CleanupLocalStore(standby_opts);
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        // Load the latest cloud state into memory before reopening to the
        // archived snapshot.
        VerifyRange(standby, tbl_id, 0, 5000, false);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(1001));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, true);

        standby.Stop();
    }

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);

        DeleteRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, false);

        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(2002));
        master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        master.Stop();
    }

    {
        CleanupLocalStore(standby_opts);
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, false);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(2002));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);
        REQUIRE(WaitForCondition(
            10s,
            100ms,
            [&]()
            {
                if (!fs::exists(partition_path))
                {
                    return false;
                }

                bool has_manifest = false;
                for (const auto &entry : fs::directory_iterator(partition_path))
                {
                    if (!entry.is_regular_file())
                    {
                        continue;
                    }

                    const std::string filename =
                        entry.path().filename().string();
                    const auto parsed = eloqstore::ParseFileName(filename);
                    const auto &type = parsed.first;
                    if (type == eloqstore::FileNameManifest)
                    {
                        has_manifest = true;
                        continue;
                    }

                    if (type == eloqstore::FileNameData)
                    {
                        return false;
                    }
                }

                return has_manifest;
            }));

        eloqstore::ReadRequest read_req;
        read_req.SetArgs(tbl_id, Key(0));
        standby.ExecSync(&read_req);
        REQUIRE(read_req.Error() == eloqstore::KvError::NotFound);

        VerifyRange(standby, tbl_id, 0, 5000, false);

        standby.Stop();
    }

    {
        eloqstore::EloqStore new_master(new_master_opts);
        REQUIRE(new_master.Start("main", 2) == eloqstore::KvError::NoError);
        UpsertRange(new_master, tbl_id, 5001, 10000);
        eloqstore::GlobalArchiveRequest archive_req;
        archive_req.SetTag(std::to_string(2002));
        new_master.ExecSync(&archive_req);
        REQUIRE(archive_req.Error() == eloqstore::KvError::NoError);

        DeleteRange(new_master, tbl_id, 5001, 10000);
        VerifyRange(new_master, tbl_id, 5001, 10000, false);

        new_master.Stop();
    }

    {
        standby_opts.standby_master_store_paths = {new_master_dir.string()};
        CleanupLocalStore(standby_opts);
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 2) == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, false);
        VerifyRange(standby, tbl_id, 5001, 10000, false);

        eloqstore::GlobalReopenRequest reopen_req;
        reopen_req.SetTag(std::to_string(2002));
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        REQUIRE(WaitForCondition(
            10s,
            100ms,
            [&]()
            {
                if (!fs::exists(partition_path))
                {
                    return false;
                }

                bool has_manifest = false;
                for (const auto &entry : fs::directory_iterator(partition_path))
                {
                    if (!entry.is_regular_file())
                    {
                        continue;
                    }

                    const std::string filename =
                        entry.path().filename().string();
                    const auto parsed = eloqstore::ParseFileName(filename);
                    const auto &type = parsed.first;
                    if (type == eloqstore::FileNameManifest)
                    {
                        has_manifest = true;
                        continue;
                    }

                    if (type == eloqstore::FileNameData)
                    {
                        eloqstore::FileId file_id = 0;
                        std::string_view branch_name;
                        uint64_t term = 0;
                        if (eloqstore::ParseDataFileSuffix(
                                parsed.second, file_id, branch_name, term) &&
                            term == 1)
                        {
                            return false;
                        }
                    }
                }

                return has_manifest;
            }));

        VerifyRange(standby, tbl_id, 0, 5000, false);
        VerifyRange(standby, tbl_id, 5001, 10000, true);

        standby.Stop();
    }

    CleanupStore(master_opts);
    CleanupStore(standby_opts);
    CleanupStore(new_master_opts);
    fs::remove_all(temp_root);
}
