#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "../include/common.h"
#include "../include/types.h"
#include "common.h"
#include "eloq_store.h"
#include "kv_options.h"
#include "test_utils.h"

using ::CleanupLocalStore;
using ::InitStore;
using test_util::MapVerifier;
namespace fs = std::filesystem;
namespace chrono = std::chrono;

// Options for branch GC testing – use a dedicated path to avoid interference.
const eloqstore::KvOptions branch_gc_opts = {
    .manifest_limit = 1 << 20,
    .fd_limit = 30 + eloqstore::num_reserved_fd,
    .store_path = {"/tmp/test-branch-gc"},
    .pages_per_file_shift = 8,  // 1 MB per data file
    .data_append_mode = true,
};

// A distinct table id so each test case gets its own namespace.
static const eloqstore::TableIdent bgc_tbl_id = {"bgc", 0};

// Wait for GC to process – synchronous writes already trigger GC inline, but
// a small sleep adds extra safety for any async book-keeping.
static void WaitForGC(int seconds = 2)
{
    std::this_thread::sleep_for(chrono::seconds(seconds));
}

// Count the number of data files present in the table directory.
static size_t CountDataFiles(const eloqstore::KvOptions &opts,
                             const eloqstore::TableIdent &tbl_id)
{
    fs::path dir = fs::path(opts.store_path[0]) / tbl_id.ToString();
    if (!fs::exists(dir))
    {
        return 0;
    }
    size_t count = 0;
    for (const auto &entry : fs::directory_iterator(dir))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        std::string name = entry.path().filename().string();
        auto [type, suffix] = eloqstore::ParseFileName(name);
        if (type == eloqstore::FileNameData)
        {
            ++count;
        }
    }
    return count;
}

// ---------------------------------------------------------------------------
// Test 1 [regression] – baseline: no branch → delete all → GC must collect
// ---------------------------------------------------------------------------
TEST_CASE("gc baseline: no branch, delete all triggers data file cleanup",
          "[branch-gc][regression]")
{
    CleanupLocalStore(branch_gc_opts);
    eloqstore::EloqStore *store = InitStore(branch_gc_opts);

    MapVerifier verify(bgc_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.SetAutoClean(false);

    verify.Upsert(0, 50);

    // Verify data files exist.
    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) > 0);

    // Delete all data – GC is triggered synchronously inside ExecSync.
    verify.Delete(0, 50);

    WaitForGC();

    // No branch was created, so GC should have deleted all data files.
    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) == 0);

    store->Stop();
}

// ---------------------------------------------------------------------------
// Test 2 [active branch protects] – branch manifest prevents deletion
// ---------------------------------------------------------------------------
TEST_CASE("gc branch protection: active branch keeps data files alive",
          "[branch-gc]")
{
    CleanupLocalStore(branch_gc_opts);
    eloqstore::EloqStore *store = InitStore(branch_gc_opts);

    MapVerifier verify(bgc_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.SetAutoClean(false);

    verify.Upsert(0, 50);

    // Create a branch – this persists a copy of the manifest.
    eloqstore::CreateBranchRequest create_req;
    create_req.SetTableId(bgc_tbl_id);
    create_req.branch_name = "feature";
    store->ExecSync(&create_req);
    REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

    size_t files_before = CountDataFiles(branch_gc_opts, bgc_tbl_id);
    REQUIRE(files_before > 0);

    // Delete all data from main – GC runs but the branch manifest still
    // references the data files, so they must NOT be deleted.
    verify.Delete(0, 50);

    WaitForGC();

    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) == files_before);

    store->Stop();
}

// ---------------------------------------------------------------------------
// Test 3 [deleted branch no protection] – deleted branch → GC collects
// ---------------------------------------------------------------------------
TEST_CASE("gc branch protection: deleted branch allows data file cleanup",
          "[branch-gc]")
{
    CleanupLocalStore(branch_gc_opts);
    eloqstore::EloqStore *store = InitStore(branch_gc_opts);

    MapVerifier verify(bgc_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.SetAutoClean(false);

    verify.Upsert(0, 50);

    // Create then immediately delete the branch.
    eloqstore::CreateBranchRequest create_req;
    create_req.SetTableId(bgc_tbl_id);
    create_req.branch_name = "feature";
    store->ExecSync(&create_req);
    REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(bgc_tbl_id);
    delete_req.branch_name = "feature";
    store->ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    // Delete all data – no branch manifest left, so GC should collect.
    verify.Delete(0, 50);

    WaitForGC();

    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) == 0);

    store->Stop();
}

// ---------------------------------------------------------------------------
// Test 4 [multiple branches protect] – any live branch prevents deletion
// ---------------------------------------------------------------------------
TEST_CASE("gc branch protection: multiple active branches keep data files",
          "[branch-gc]")
{
    CleanupLocalStore(branch_gc_opts);
    eloqstore::EloqStore *store = InitStore(branch_gc_opts);

    MapVerifier verify(bgc_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.SetAutoClean(false);

    verify.Upsert(0, 50);

    eloqstore::CreateBranchRequest req1;
    req1.SetTableId(bgc_tbl_id);
    req1.branch_name = "feature1";
    store->ExecSync(&req1);
    REQUIRE(req1.Error() == eloqstore::KvError::NoError);

    eloqstore::CreateBranchRequest req2;
    req2.SetTableId(bgc_tbl_id);
    req2.branch_name = "feature2";
    store->ExecSync(&req2);
    REQUIRE(req2.Error() == eloqstore::KvError::NoError);

    size_t files_before = CountDataFiles(branch_gc_opts, bgc_tbl_id);
    REQUIRE(files_before > 0);

    verify.Delete(0, 50);

    WaitForGC();

    // Both branches still hold manifests – data files must survive.
    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) == files_before);

    store->Stop();
}

// ---------------------------------------------------------------------------
// Test 5 [one of two branches deleted] – remaining branch still protects
// ---------------------------------------------------------------------------
TEST_CASE("gc branch protection: one deleted branch, one live still protects",
          "[branch-gc]")
{
    CleanupLocalStore(branch_gc_opts);
    eloqstore::EloqStore *store = InitStore(branch_gc_opts);

    MapVerifier verify(bgc_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.SetAutoClean(false);

    verify.Upsert(0, 50);

    eloqstore::CreateBranchRequest req1;
    req1.SetTableId(bgc_tbl_id);
    req1.branch_name = "feature1";
    store->ExecSync(&req1);
    REQUIRE(req1.Error() == eloqstore::KvError::NoError);

    eloqstore::CreateBranchRequest req2;
    req2.SetTableId(bgc_tbl_id);
    req2.branch_name = "feature2";
    store->ExecSync(&req2);
    REQUIRE(req2.Error() == eloqstore::KvError::NoError);

    // Delete only feature1; feature2 remains.
    eloqstore::DeleteBranchRequest del_req;
    del_req.SetTableId(bgc_tbl_id);
    del_req.branch_name = "feature1";
    store->ExecSync(&del_req);
    REQUIRE(del_req.Error() == eloqstore::KvError::NoError);

    size_t files_before = CountDataFiles(branch_gc_opts, bgc_tbl_id);
    REQUIRE(files_before > 0);

    verify.Delete(0, 50);

    WaitForGC();

    // feature2 is still alive – data files must NOT be deleted.
    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) == files_before);

    store->Stop();
}

// ---------------------------------------------------------------------------
// Test 6 [corrupt manifest] – warn-and-skip path: GC must survive a branch
// manifest that cannot be replayed and must still protect files held by a
// valid sibling branch manifest.
// ---------------------------------------------------------------------------
TEST_CASE("gc corrupt manifest: warn-and-skip keeps store alive", "[branch-gc]")
{
    CleanupLocalStore(branch_gc_opts);
    eloqstore::EloqStore *store = InitStore(branch_gc_opts);

    MapVerifier verify(bgc_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.SetAutoClean(false);

    verify.Upsert(0, 50);

    // Create two branches that each snapshot the same data files.
    eloqstore::CreateBranchRequest req_good;
    req_good.SetTableId(bgc_tbl_id);
    req_good.branch_name = "good";
    store->ExecSync(&req_good);
    REQUIRE(req_good.Error() == eloqstore::KvError::NoError);

    eloqstore::CreateBranchRequest req_corrupt;
    req_corrupt.SetTableId(bgc_tbl_id);
    req_corrupt.branch_name = "corrupt";
    store->ExecSync(&req_corrupt);
    REQUIRE(req_corrupt.Error() == eloqstore::KvError::NoError);

    // Overwrite manifest_corrupt_0 with garbage bytes of the same size.
    // The file was written by eloqstore with O_DIRECT (page-aligned size), so
    // preserving the size lets ReadFile succeed via O_DIRECT – but the content
    // is 0xFF throughout, which causes Replayer::Replay to fail with
    // KvError::Corrupted, exercising the warn-and-skip path in
    // AugmentRetainedFilesFromBranchManifests (file_gc.cpp lines 369-377).
    fs::path table_path =
        fs::path(branch_gc_opts.store_path[0]) / bgc_tbl_id.ToString();
    fs::path corrupt_manifest = table_path / "manifest_corrupt_0";
    size_t manifest_size = fs::file_size(corrupt_manifest);
    {
        std::vector<char> garbage(manifest_size, static_cast<char>(0xFF));
        std::ofstream of(corrupt_manifest,
                         std::ios::binary | std::ios::out | std::ios::trunc);
        of.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
    }

    size_t files_before = CountDataFiles(branch_gc_opts, bgc_tbl_id);
    REQUIRE(files_before > 0);

    // Delete all data – this triggers GC.  GC encounters manifest_corrupt_0,
    // logs a warning, and skips it.  The "good" branch manifest (and the active
    // branch's on-disk manifest) still protect the data files.
    verify.Delete(0, 50);

    WaitForGC();

    // Store must still be running (no crash on corrupt manifest) and the data
    // files referenced by "good" must remain.
    REQUIRE(CountDataFiles(branch_gc_opts, bgc_tbl_id) == files_before);

    store->Stop();
}
