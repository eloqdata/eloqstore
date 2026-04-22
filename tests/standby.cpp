#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>  // NOLINT(build/c++17)
#include <functional>
#include <optional>
#include <sstream>
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
constexpr std::string_view kStaleManifestTableName =
    "standby_cloud_stale_manifest";
constexpr std::string_view kStaleManifestTempRootName =
    "standby-cloud-stale-manifest-test";
constexpr uint64_t kStaleManifestPhaseBegin = 0;
constexpr uint64_t kStaleManifestPhaseEnd = 20000;

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

void ReopenStore(eloqstore::EloqStore &store,
                 std::optional<std::string> tag = std::nullopt)
{
    eloqstore::GlobalReopenRequest reopen_req;
    if (tag.has_value())
    {
        reopen_req.SetTag(std::move(*tag));
    }
    store.ExecSync(&reopen_req);
    REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);
}

std::vector<std::string> ListDataFiles(const std::vector<std::string> &files)
{
    std::vector<std::string> data_files;
    for (const std::string &filename : files)
    {
        const auto [type, suffix] = eloqstore::ParseFileName(filename);
        (void) suffix;
        if (type == eloqstore::FileNameData)
        {
            data_files.push_back(filename);
        }
    }
    return data_files;
}

void RemoveLocalNonManifestFiles(const fs::path &partition_path)
{
    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }

        const std::string filename = entry.path().filename().string();
        const auto [type, suffix] = eloqstore::ParseFileName(filename);
        (void) suffix;
        std::string_view branch_name;
        const bool is_current_term =
            eloqstore::ParseCurrentTermFilename(filename, branch_name);
        if (type != eloqstore::FileNameManifest && !is_current_term)
        {
            fs::remove(entry.path());
        }
    }
}

eloqstore::KvError ScanRange(eloqstore::EloqStore &store,
                             const eloqstore::TableIdent &tbl_id,
                             uint64_t begin,
                             uint64_t end)
{
    eloqstore::ScanRequest req;
    req.SetArgs(tbl_id, Key(begin), Key(end));
    store.ExecSync(&req);
    return req.Error();
}

std::vector<std::string> ListLocalDataFiles(const fs::path &partition_path)
{
    std::vector<std::string> files;
    if (!fs::exists(partition_path))
    {
        return files;
    }

    for (const auto &entry : fs::directory_iterator(partition_path))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }

        const std::string filename = entry.path().filename().string();
        const auto [type, suffix] = eloqstore::ParseFileName(filename);
        (void) suffix;
        if (type == eloqstore::FileNameData)
        {
            files.push_back(filename);
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

void ReadOneKey(eloqstore::EloqStore &store,
                const eloqstore::TableIdent &tbl_id,
                uint64_t key)
{
    eloqstore::ReadRequest req;
    req.SetArgs(tbl_id, Key(key));
    store.ExecSync(&req);
    REQUIRE(req.Error() == eloqstore::KvError::NoError);
}

void RunStandbyProcessHelper()
{
    const fs::path self_path = fs::read_symlink("/proc/self/exe");
    auto quote = [](std::string_view value)
    {
        std::string out = "\"";
        out.append(value);
        out.push_back('"');
        return out;
    };
    std::ostringstream cmd;
    cmd << quote(self_path.string()) << ' ' << quote("[standby-subproc]");
    REQUIRE(std::system(cmd.str().c_str()) == 0);
}
}  // namespace

TEST_CASE("standby subprocess master writer", "[.][standby-subproc]")
{
    const fs::path temp_root =
        fs::temp_directory_path() / std::string(kStaleManifestTempRootName);
    const fs::path master_dir = temp_root / "master";

    eloqstore::KvOptions opts = cloud_options;
    opts.store_path = {master_dir.string()};
    opts.cloud_store_path = cloud_options.cloud_store_path + "/" +
                            std::string(kStaleManifestTempRootName);
    opts.allow_reuse_local_caches = true;
    opts.pages_per_file_shift = 1;

    const eloqstore::TableIdent tbl_id{std::string(kStaleManifestTableName), 0};

    eloqstore::EloqStore store(opts);
    REQUIRE(store.Start("main", 1) == eloqstore::KvError::NoError);
    UpsertRange(
        store, tbl_id, kStaleManifestPhaseBegin, kStaleManifestPhaseEnd);
    store.Stop();
}

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
    const fs::path partition_path = local_dir / tbl_id.ToString();

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
    REQUIRE(WaitForCondition(
        5s, 100ms, [&]() { return !fs::exists(partition_path); }));

    standby.Stop();

    fs::remove_all(dir);
}

TEST_CASE("standby reopen a partition with empty dir", "[standby]")
{
    fs::path dir = fs::temp_directory_path() / fs::path("standby-test");
    fs::path remote_master_dir = dir / "remote-master";
    fs::path local_dir = dir / "local";

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};
    const fs::path partition_path = local_dir / tbl_id.ToString();

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
    REQUIRE(WaitForCondition(
        5s, 100ms, [&]() { return !fs::exists(partition_path); }));

    standby.Stop();

    fs::remove_all(dir);
}

TEST_CASE("standby reopen without tag with nonexistent dir", "[standby]")
{
    fs::path dir =
        fs::temp_directory_path() / fs::path("standby-latest-missing-test");
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
    const fs::path partition_path = local_dir / tbl_id.ToString();

    eloqstore::EloqStore store(opts);
    REQUIRE(store.Start("main", 1) == eloqstore::KvError::NoError);
    UpsertRange(store, tbl_id, 0, 5000);
    VerifyRange(store, tbl_id, 0, 5000, true);
    store.Stop();

    opts.standby_master_addr = "local";
    opts.standby_master_store_paths = {remote_master_dir.string()};
    eloqstore::EloqStore standby(opts);
    REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);
    ReopenStore(standby);
    VerifyRange(standby, tbl_id, 0, 5000, false);
    REQUIRE(WaitForCondition(
        5s, 100ms, [&]() { return !fs::exists(partition_path); }));
    standby.Stop();

    fs::remove_all(dir);
}

TEST_CASE("standby reopen without tag with empty dir", "[standby]")
{
    fs::path dir =
        fs::temp_directory_path() / fs::path("standby-latest-empty-test");
    fs::path remote_master_dir = dir / "remote-master";
    fs::path local_dir = dir / "local";

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};
    const fs::path partition_path = local_dir / tbl_id.ToString();

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
    ReopenStore(standby);
    VerifyRange(standby, tbl_id, 0, 5000, false);
    REQUIRE(WaitForCondition(
        5s, 100ms, [&]() { return !fs::exists(partition_path); }));
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

TEST_CASE("standby reopen without tag follows latest master manifest",
          "[standby]")
{
    fs::path temp_root =
        fs::temp_directory_path() / fs::path("standby-latest-test");
    fs::path master_dir = temp_root / "master";
    fs::path standby_dir = temp_root / "standby";
    fs::remove_all(temp_root);
    fs::create_directories(master_dir);
    fs::create_directories(standby_dir);

    eloqstore::KvOptions master_opts;
    master_opts.store_path = {master_dir.string()};
    master_opts.enable_local_standby = true;
    master_opts.standby_master_store_paths = {};
    master_opts.pages_per_file_shift = 0;

    eloqstore::KvOptions standby_opts;
    standby_opts.store_path = {standby_dir.string()};
    standby_opts.enable_local_standby = true;
    standby_opts.standby_master_addr = "local";
    standby_opts.standby_master_store_paths = {master_dir.string()};
    standby_opts.pages_per_file_shift = 0;

    eloqstore::TableIdent tbl_id{"standby_tbl", 0};

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);
        UpsertRange(master, tbl_id, 0, 5000);
        VerifyRange(master, tbl_id, 0, 5000, true);
        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        eloqstore::GlobalReopenRequest reopen_req;
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 5000, true);

        standby.Stop();
    }

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);

        DeleteRange(master, tbl_id, 0, 2500);
        UpsertRange(master, tbl_id, 5000, 7500);

        VerifyRange(master, tbl_id, 0, 2500, false);
        VerifyRange(master, tbl_id, 2500, 5000, true);
        VerifyRange(master, tbl_id, 5000, 7500, true);

        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

        // Load current state before reopening to latest to exercise online
        // reopen against an already-open replica.
        Scan(&standby, tbl_id, 0, 8000);

        eloqstore::GlobalReopenRequest reopen_req;
        standby.ExecSync(&reopen_req);
        REQUIRE(reopen_req.Error() == eloqstore::KvError::NoError);

        VerifyRange(standby, tbl_id, 0, 2500, false);
        VerifyRange(standby, tbl_id, 2500, 5000, true);
        VerifyRange(standby, tbl_id, 5000, 7500, true);

        standby.Stop();
    }

    fs::remove_all(temp_root);
}

TEST_CASE(
    "standby reopen without tag loads latest master data into empty standby",
    "[standby]")
{
    fs::path dir =
        fs::temp_directory_path() / fs::path("standby-latest-empty-load-test");
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
        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);
        ReopenStore(standby);
        VerifyRange(standby, tbl_id, 0, 5000, true);
        standby.Stop();
    }

    fs::remove_all(dir);
}

TEST_CASE(
    "standby rsync replica without tag follows master changes and term switch",
    "[standby]")
{
    fs::path temp_root =
        fs::temp_directory_path() / fs::path("standby-latest-term-test");
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
        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);
        ReopenStore(standby);
        VerifyRange(standby, tbl_id, 0, 5000, true);
        standby.Stop();
    }

    {
        eloqstore::EloqStore master(master_opts);
        REQUIRE(master.Start("main", 1) == eloqstore::KvError::NoError);
        DeleteRange(master, tbl_id, 0, 5000);
        master.Stop();
    }

    {
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);
        Scan(&standby, tbl_id, 0, 10000);
        ReopenStore(standby);
        VerifyRange(standby, tbl_id, 0, 5000, false);
        REQUIRE(WaitForCondition(
            10s, 100ms, [&]() { return !fs::exists(partition_path); }));
        standby.Stop();
    }

    {
        eloqstore::EloqStore new_master(new_master_opts);
        REQUIRE(new_master.Start("main", 2) == eloqstore::KvError::NoError);
        UpsertRange(new_master, tbl_id, 5001, 10000);
        new_master.Stop();
    }

    {
        standby_opts.standby_master_store_paths = {new_master_dir.string()};
        eloqstore::EloqStore standby(standby_opts);
        REQUIRE(standby.Start("main", 2) == eloqstore::KvError::NoError);
        Scan(&standby, tbl_id, 0, 10000);
        ReopenStore(standby);
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

    std::string cloud_storage_path =
        cloud_options.cloud_store_path + "/" + temp_root.filename().string();

    eloqstore::KvOptions master_opts = cloud_options;
    master_opts.store_path = {master_dir.string()};
    master_opts.cloud_store_path = cloud_storage_path;
    master_opts.allow_reuse_local_caches = true;
    master_opts.pages_per_file_shift = 1;

    eloqstore::KvOptions standby_opts = cloud_archive_opts;
    standby_opts.store_path = {standby_dir.string()};
    standby_opts.cloud_store_path = cloud_storage_path;
    standby_opts.allow_reuse_local_caches = true;
    standby_opts.pages_per_file_shift = 1;

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

TEST_CASE(
    "standby cloud scan succeeds via auto reopen after stale local manifest "
    "and cloud gc",
    "[standby][cloud][gc]")
{
    constexpr uint64_t kPhase2Rounds = 3;

    fs::path temp_root =
        fs::temp_directory_path() / std::string(kStaleManifestTempRootName);
    fs::path master_dir = temp_root / "master";
    fs::path standby_dir = temp_root / "standby";
    fs::remove_all(temp_root);

    std::string cloud_storage_path =
        cloud_options.cloud_store_path + "/" + temp_root.filename().string();

    eloqstore::KvOptions master_opts = cloud_options;
    master_opts.store_path = {master_dir.string()};
    master_opts.cloud_store_path = cloud_storage_path;
    master_opts.allow_reuse_local_caches = true;
    master_opts.pages_per_file_shift = 1;

    eloqstore::KvOptions standby_opts = master_opts;
    standby_opts.store_path = {standby_dir.string()};

    CleanupStore(master_opts);
    CleanupStore(standby_opts);

    const eloqstore::TableIdent tbl_id{std::string(kStaleManifestTableName), 0};
    const fs::path standby_partition_path = standby_dir / tbl_id.ToString();
    std::vector<std::string> old_cloud_data_files;

    RunStandbyProcessHelper();
    old_cloud_data_files = ListDataFiles(ListCloudFiles(
        master_opts, master_opts.cloud_store_path, tbl_id.ToString()));
    std::sort(old_cloud_data_files.begin(), old_cloud_data_files.end());
    REQUIRE_FALSE(old_cloud_data_files.empty());

    CleanupLocalStore(standby_opts);
    eloqstore::EloqStore standby(standby_opts);
    REQUIRE(standby.Start("main", 1) == eloqstore::KvError::NoError);

    ReopenStore(standby);
    ReadOneKey(standby, tbl_id, kStaleManifestPhaseBegin);
    const auto local_data_files = ListLocalDataFiles(standby_partition_path);
    REQUIRE_FALSE(local_data_files.empty());
    REQUIRE(local_data_files.size() < old_cloud_data_files.size());

    std::vector<std::string> old_remote_only_files;
    std::set_difference(old_cloud_data_files.begin(),
                        old_cloud_data_files.end(),
                        local_data_files.begin(),
                        local_data_files.end(),
                        std::back_inserter(old_remote_only_files));
    REQUIRE_FALSE(old_remote_only_files.empty());

    REQUIRE(fs::exists(standby_partition_path));
    RemoveLocalNonManifestFiles(standby_partition_path);

    bool has_local_manifest = false;
    bool has_non_manifest_file = false;
    for (const auto &entry : fs::directory_iterator(standby_partition_path))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }

        const std::string filename = entry.path().filename().string();
        const auto [type, suffix] = eloqstore::ParseFileName(filename);
        (void) suffix;
        std::string_view branch_name;
        const bool is_current_term =
            eloqstore::ParseCurrentTermFilename(filename, branch_name);
        if (type == eloqstore::FileNameManifest)
        {
            has_local_manifest = true;
        }
        else if (!is_current_term)
        {
            has_non_manifest_file = true;
        }
    }
    REQUIRE(has_local_manifest);
    REQUIRE_FALSE(has_non_manifest_file);

    bool removed_old_remote_only_file = false;
    for (uint64_t round = 0;
         round < kPhase2Rounds && !removed_old_remote_only_file;
         ++round)
    {
        RunStandbyProcessHelper();

        removed_old_remote_only_file = WaitForCondition(
            15s,
            100ms,
            [&]()
            {
                auto refreshed_cloud_data_files =
                    ListDataFiles(ListCloudFiles(master_opts,
                                                 master_opts.cloud_store_path,
                                                 tbl_id.ToString()));
                if (refreshed_cloud_data_files.empty())
                {
                    return false;
                }
                std::sort(refreshed_cloud_data_files.begin(),
                          refreshed_cloud_data_files.end());

                return std::any_of(old_remote_only_files.begin(),
                                   old_remote_only_files.end(),
                                   [&](const std::string &filename)
                                   {
                                       return !std::binary_search(
                                           refreshed_cloud_data_files.begin(),
                                           refreshed_cloud_data_files.end(),
                                           filename);
                                   });
            });
    }
    REQUIRE(removed_old_remote_only_file);

    const eloqstore::KvError scan_err = ScanRange(
        standby, tbl_id, kStaleManifestPhaseBegin, kStaleManifestPhaseEnd);
    REQUIRE(scan_err == eloqstore::KvError::NoError);

    standby.Stop();

    CleanupStore(master_opts);
    CleanupStore(standby_opts);
    fs::remove_all(temp_root);
}
