#include <glog/logging.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common.h"
#include "eloq_store.h"
#include "kv_options.h"
#include "test_utils.h"

using namespace test_util;

namespace fs = std::filesystem;

TEST_CASE("create branch from main", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::CreateBranchRequest req;
    req.SetTableId(test_tbl_id);
    req.SetArgs("feature1");
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_feature1_0"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature1"));

    store->Stop();
}

TEST_CASE("create branch - invalid branch name", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::CreateBranchRequest req;
    req.SetTableId(test_tbl_id);
    req.branch_name = "invalid_branch";  // underscore not allowed
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::InvalidArgs);

    store->Stop();
}

TEST_CASE("create branch - uppercase normalized to lowercase", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::CreateBranchRequest req;
    req.SetTableId(test_tbl_id);
    req.branch_name = "FeatureBranch";
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_featurebranch_0"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.featurebranch"));

    store->Stop();
}

TEST_CASE("create multiple branches from main", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::CreateBranchRequest req1;
    req1.SetTableId(test_tbl_id);
    req1.branch_name = "feature1";
    store->ExecSync(&req1);
    REQUIRE(req1.Error() == eloqstore::KvError::NoError);

    eloqstore::CreateBranchRequest req2;
    req2.SetTableId(test_tbl_id);
    req2.branch_name = "feature2";
    store->ExecSync(&req2);
    REQUIRE(req2.Error() == eloqstore::KvError::NoError);

    eloqstore::CreateBranchRequest req3;
    req3.SetTableId(test_tbl_id);
    req3.branch_name = "hotfix";
    store->ExecSync(&req3);
    REQUIRE(req3.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_feature1_0"));
    REQUIRE(fs::exists(table_path / "manifest_feature2_0"));
    REQUIRE(fs::exists(table_path / "manifest_hotfix_0"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature1"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature2"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.hotfix"));

    store->Stop();
}

TEST_CASE("delete branch", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::CreateBranchRequest create_req;
    create_req.SetTableId(test_tbl_id);
    create_req.branch_name = "feature1";
    store->ExecSync(&create_req);
    REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_feature1_0"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature1"));

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "feature1";
    store->ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    REQUIRE(!fs::exists(table_path / "manifest_feature1_0"));
    REQUIRE(!fs::exists(table_path / "CURRENT_TERM.feature1"));

    store->Stop();
}

TEST_CASE("delete main branch should fail", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = eloqstore::MainBranchName;
    store->ExecSync(&delete_req);

    REQUIRE(delete_req.Error() == eloqstore::KvError::InvalidArgs);

    store->Stop();
}

TEST_CASE("delete non-existent branch", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "nonexistent";
    store->ExecSync(&delete_req);

    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    store->Stop();
}

TEST_CASE("create branch - already exists returns AlreadyExists", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    // First creation must succeed.
    eloqstore::CreateBranchRequest req1;
    req1.SetTableId(test_tbl_id);
    req1.branch_name = "feature1";
    store->ExecSync(&req1);
    REQUIRE(req1.Error() == eloqstore::KvError::NoError);

    // Second creation for the same branch must be rejected.
    eloqstore::CreateBranchRequest req2;
    req2.SetTableId(test_tbl_id);
    req2.branch_name = "feature1";
    store->ExecSync(&req2);
    REQUIRE(req2.Error() == eloqstore::KvError::AlreadyExists);

    store->Stop();
}

TEST_CASE("global create branch - creates manifest on single partition",
          "[branch][global]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("feature1", eloqstore::MainBranchName);
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_feature1_0"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature1"));

    store->Stop();
}

TEST_CASE("global create branch - creates manifests on all partitions",
          "[branch][global]")
{
    static const eloqstore::TableIdent tbl_p1 = {"t0", 1};

    eloqstore::EloqStore *store = InitStore(default_opts);

    // Write to two partitions so both directories appear on disk.
    MapVerifier verify0(test_tbl_id, store, false);
    verify0.SetAutoClean(false);
    verify0.Upsert(0, 100);

    MapVerifier verify1(tbl_p1, store, false);
    verify1.SetAutoClean(false);
    verify1.Upsert(0, 100);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("feature1", eloqstore::MainBranchName);
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::NoError);

    // Both partition directories must have the branch manifest files.
    for (const eloqstore::TableIdent &tbl_id : {test_tbl_id, tbl_p1})
    {
        fs::path table_path = fs::path(test_path) / tbl_id.ToString();
        REQUIRE(fs::exists(table_path / "manifest_feature1_0"));
        REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature1"));
    }

    store->Stop();
}

TEST_CASE("global create branch - invalid branch name returns InvalidArgs",
          "[branch][global]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("bad_name", eloqstore::MainBranchName);  // underscore not allowed
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::InvalidArgs);

    store->Stop();
}

TEST_CASE("global create branch - already exists returns AlreadyExists",
          "[branch][global]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    // First global create must succeed.
    eloqstore::GlobalCreateBranchRequest req1;
    req1.SetArgs("feature1", eloqstore::MainBranchName);
    store->ExecSync(&req1);
    REQUIRE(req1.Error() == eloqstore::KvError::NoError);

    // Second global create for the same branch must be rejected.
    eloqstore::GlobalCreateBranchRequest req2;
    req2.SetArgs("feature1", eloqstore::MainBranchName);
    store->ExecSync(&req2);
    REQUIRE(req2.Error() == eloqstore::KvError::AlreadyExists);

    store->Stop();
}

TEST_CASE("global create branch - no-op on empty store", "[branch][global]")
{
    // InitStore cleans up the store directory and starts fresh with no data.
    // There are no partition subdirectories, so the handler returns NoError
    // immediately without fanning out any sub-requests.
    eloqstore::EloqStore *store = InitStore(default_opts);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("feature1", eloqstore::MainBranchName);
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::NoError);

    store->Stop();
}

TEST_CASE("delete branch removes all term manifests", "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    // Create branch at term 0.
    eloqstore::CreateBranchRequest create_req;
    create_req.SetTableId(test_tbl_id);
    create_req.branch_name = "feature";
    store->ExecSync(&create_req);
    REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_feature_0"));
    REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature"));

    // Simulate the branch having been written to at higher terms (e.g. after a
    // failover).  Write placeholder manifests for terms 1–3 and advance
    // CURRENT_TERM.feature to "3".  DeleteBranchFiles reads CURRENT_TERM to
    // discover max_term, then unlinks manifests 0..max_term; it never reads the
    // manifest contents, so placeholder content is fine.
    for (int t = 1; t <= 3; ++t)
    {
        std::ofstream mf(table_path /
                         ("manifest_feature_" + std::to_string(t)));
        mf << "placeholder";
    }
    {
        std::ofstream ct(table_path / "CURRENT_TERM.feature",
                         std::ios::out | std::ios::trunc);
        ct << "3";
    }

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "feature";
    store->ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    // ALL manifests (terms 0–3) and CURRENT_TERM must be gone.
    for (int t = 0; t <= 3; ++t)
    {
        REQUIRE(!fs::exists(
            table_path / ("manifest_feature_" + std::to_string(t))));
    }
    REQUIRE(!fs::exists(table_path / "CURRENT_TERM.feature"));

    store->Stop();
}

TEST_CASE("branch files persist after restart", "[branch][persist]")
{
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);

        verify.Upsert(0, 100);

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.branch_name = "feature1";
        store->ExecSync(&req);

        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }

    {
        // Restart without cleaning up to verify files persist across restarts.
        eloqstore::EloqStore fresh_store(default_opts);
        eloqstore::KvError err = fresh_store.Start(eloqstore::MainBranchName, 0);
        REQUIRE(err == eloqstore::KvError::NoError);

        fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
        REQUIRE(fs::exists(table_path / "manifest_feature1_0"));
        REQUIRE(fs::exists(table_path / "CURRENT_TERM.feature1"));

        fresh_store.Stop();
    }
}

TEST_CASE("branch data isolation: bidirectional fork", "[branch][isolation]")
{
    // Phase 1: open on main, write DS1 (keys 0-99), create branch "feature1".
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);

        verify.Upsert(0, 100);  // DS1: keys [0, 100)

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("feature1");
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }

    // Phase 2: open on feature1, verify DS1 inherited, write DS2 (keys 100-199).
    {
        eloqstore::EloqStore feature1_store(default_opts);
        eloqstore::KvError err = feature1_store.Start("feature1", 0);
        REQUIRE(err == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &feature1_store, false);
        verify.SetAutoClean(false);

        // DS1 must be visible on feature1 (inherited from main at fork point).
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(50) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);

        // DS2 not yet written on feature1.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NotFound);

        verify.Upsert(100, 200);  // DS2: keys [100, 200)

        // DS2 now visible on feature1.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);

        feature1_store.Stop();
    }

    // Phase 3: open on main, verify DS1 still present and DS2 NOT visible,
    //          then write DS3 (keys 200-299).
    {
        eloqstore::EloqStore main_store(default_opts);
        eloqstore::KvError err = main_store.Start(eloqstore::MainBranchName, 0);
        REQUIRE(err == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &main_store, false);
        verify.SetAutoClean(false);

        // DS1 still on main.
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);

        // DS2 written on feature1 must NOT be visible on main.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NotFound);

        verify.Upsert(200, 300);  // DS3: keys [200, 300)

        // DS3 visible on main.
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NoError);

        main_store.Stop();
    }

    // Phase 4: open on feature1 again, verify DS1+DS2 present and DS3 NOT
    //          visible (main's writes after the fork must not leak into feature1).
    {
        eloqstore::EloqStore feature1_store(default_opts);
        eloqstore::KvError err = feature1_store.Start("feature1", 0);
        REQUIRE(err == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &feature1_store, false);
        verify.SetAutoClean(false);

        // DS1 still visible on feature1.
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);

        // DS2 still visible on feature1.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);

        // DS3 written on main after the fork must NOT be visible on feature1.
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NotFound);

        feature1_store.Stop();
    }

    CleanupStore(default_opts);
}

TEST_CASE("chained fork: fork from feature branch", "[branch][isolation]")
{
    // Phase 1: main → write DS1, create feature1.
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);

        verify.Upsert(0, 100);  // DS1: keys [0, 100)

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("feature1");
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }

    // Phase 2: feature1 → verify DS1 inherited, write DS2, create sub1.
    {
        eloqstore::EloqStore f1_store(default_opts);
        REQUIRE(f1_store.Start("feature1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f1_store, false);
        verify.SetAutoClean(false);

        // DS1 inherited from main.
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);

        verify.Upsert(100, 200);  // DS2: keys [100, 200)

        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);

        // Fork sub1 from feature1 (captures DS1 + DS2).
        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("sub1");
        f1_store.ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        f1_store.Stop();
    }

    // Phase 3: sub1 → DS1+DS2 both inherited, write DS3.
    {
        eloqstore::EloqStore sub1_store(default_opts);
        REQUIRE(sub1_store.Start("sub1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &sub1_store, false);
        verify.SetAutoClean(false);

        // DS1 (from main) and DS2 (from feature1) must both be visible.
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);

        verify.Upsert(200, 300);  // DS3: keys [200, 300)

        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NoError);

        sub1_store.Stop();
    }

    // Phase 4: feature1 (restart) → DS1+DS2 still visible, DS3 must NOT leak.
    {
        eloqstore::EloqStore f1_store(default_opts);
        REQUIRE(f1_store.Start("feature1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f1_store, false);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);
        // DS3 is sub1-only — must not be visible on feature1.
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NotFound);

        f1_store.Stop();
    }

    CleanupStore(default_opts);
}

TEST_CASE("sibling branches are isolated from each other", "[branch][isolation]")
{
    // Phase 1: main → write DS1, fork both feature1 and feature2.
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);

        verify.Upsert(0, 100);  // DS1: keys [0, 100)

        eloqstore::CreateBranchRequest req1;
        req1.SetTableId(test_tbl_id);
        req1.SetArgs("feature1");
        store->ExecSync(&req1);
        REQUIRE(req1.Error() == eloqstore::KvError::NoError);

        eloqstore::CreateBranchRequest req2;
        req2.SetTableId(test_tbl_id);
        req2.SetArgs("feature2");
        store->ExecSync(&req2);
        REQUIRE(req2.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }

    // Phase 2: feature1 → verify DS1, write DS2.
    {
        eloqstore::EloqStore f1_store(default_opts);
        REQUIRE(f1_store.Start("feature1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f1_store, false);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);

        verify.Upsert(100, 200);  // DS2: keys [100, 200)

        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);

        f1_store.Stop();
    }

    // Phase 3: feature2 → DS1 visible, DS2 (feature1-only) NOT visible,
    //           write DS3.
    {
        eloqstore::EloqStore f2_store(default_opts);
        REQUIRE(f2_store.Start("feature2", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f2_store, false);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        // DS2 written on feature1 must not bleed into feature2.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NotFound);

        verify.Upsert(200, 300);  // DS3: keys [200, 300)

        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NoError);

        f2_store.Stop();
    }

    // Phase 4: feature1 (restart) → DS1+DS2 visible, DS3 (feature2-only)
    //          must NOT be visible.
    {
        eloqstore::EloqStore f1_store(default_opts);
        REQUIRE(f1_store.Start("feature1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f1_store, false);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);
        // DS3 is feature2-only — must not be visible on feature1.
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NotFound);

        f1_store.Stop();
    }

    CleanupStore(default_opts);
}

TEST_CASE("sequential forks capture correct snapshot", "[branch][isolation]")
{
    // Phase 1: main → write DS1, fork featureA (snapshot: DS1 only).
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);

        verify.Upsert(0, 100);  // DS1: keys [0, 100)

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("featurea");
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }

    // Phase 2: main (restart) → write DS2, fork featureB (snapshot: DS1+DS2).
    {
        eloqstore::EloqStore main_store(default_opts);
        REQUIRE(main_store.Start(eloqstore::MainBranchName, 0) ==
                eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &main_store, false);
        verify.SetAutoClean(false);

        verify.Upsert(100, 200);  // DS2: keys [100, 200)

        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("featureb");
        main_store.ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        main_store.Stop();
    }

    // Phase 3: featureA → only DS1 visible (forked before DS2 was written).
    {
        eloqstore::EloqStore fa_store(default_opts);
        REQUIRE(fa_store.Start("featurea", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &fa_store, false);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        // DS2 written to main after featureA's fork must not be visible.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NotFound);

        fa_store.Stop();
    }

    // Phase 4: featureB → DS1+DS2 visible (forked after DS2 was written).
    {
        eloqstore::EloqStore fb_store(default_opts);
        REQUIRE(fb_store.Start("featureb", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &fb_store, false);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);

        fb_store.Stop();
    }

    CleanupStore(default_opts);
}

TEST_CASE(
    "sibling branches forked from same parent at different Raft terms inherit "
    "correct snapshots and are isolated",
    "[branch][cloud]")
{
    // Phase 1: clean slate — InitStore wipes local + cloud, starts at term=0,
    // then we stop immediately so we can restart at explicit terms.
    eloqstore::EloqStore *store = InitStore(cloud_options);
    store->Stop();

    // Phase 2: main at term=1 — write DS1 (keys [0,100)), fork "feature1".
    // feature1's snapshot contains only DS1.
    {
        REQUIRE(store->Start(eloqstore::MainBranchName, 1) ==
                eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(0, 100);  // DS1

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("feature1");
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        store->Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 3: main at term=3 — write DS2 (keys [100,200)), fork "feature2".
    // feature2's snapshot contains DS1+DS2.
    {
        REQUIRE(store->Start(eloqstore::MainBranchName, 3) ==
                eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(100, 200);  // DS2

        eloqstore::CreateBranchRequest req;
        req.SetTableId(test_tbl_id);
        req.SetArgs("feature2");
        store->ExecSync(&req);
        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        store->Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 4: main at term=5 — write DS3 (keys [200,300)).
    // DS3 is written after both forks; it must NOT appear in either branch.
    {
        REQUIRE(store->Start(eloqstore::MainBranchName, 5) ==
                eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(200, 300);  // DS3

        store->Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 5: feature1 — verify snapshot (DS1 only), then write DS4
    // (keys [300,400)).
    {
        eloqstore::EloqStore f1_store(cloud_options);
        REQUIRE(f1_store.Start("feature1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f1_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);

        // DS1 must be visible.
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        // DS2 written after this branch's fork must not be visible.
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NotFound);
        // DS3 written after both forks must not be visible.
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NotFound);

        // Write DS4 — branch-local data.
        verify.Upsert(300, 400);  // DS4

        f1_store.Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 6: feature2 — verify snapshot (DS1+DS2), then write DS5
    // (keys [400,500)).
    {
        eloqstore::EloqStore f2_store(cloud_options);
        REQUIRE(f2_store.Start("feature2", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f2_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);

        // DS1+DS2 must be visible.
        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);
        // DS3 written after both forks must not be visible.
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NotFound);
        // DS4 written on feature1 must not bleed into feature2.
        REQUIRE(verify.CheckKey(300) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(399) == eloqstore::KvError::NotFound);

        // Write DS5 — branch-local data.
        verify.Upsert(400, 500);  // DS5

        f2_store.Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 7: restart feature1 — verify DS1+DS4 visible; DS2, DS3, DS5 absent.
    {
        eloqstore::EloqStore f1_r_store(cloud_options);
        REQUIRE(f1_r_store.Start("feature1", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f1_r_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(300) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(399) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(400) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(499) == eloqstore::KvError::NotFound);

        f1_r_store.Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 8: restart feature2 — verify DS1+DS2+DS5 visible; DS3, DS4 absent.
    {
        eloqstore::EloqStore f2_r_store(cloud_options);
        REQUIRE(f2_r_store.Start("feature2", 0) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &f2_r_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);

        REQUIRE(verify.CheckKey(0) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(99) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(100) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(199) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(400) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(499) == eloqstore::KvError::NoError);
        REQUIRE(verify.CheckKey(200) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(299) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(300) == eloqstore::KvError::NotFound);
        REQUIRE(verify.CheckKey(399) == eloqstore::KvError::NotFound);

        f2_r_store.Stop();
    }

    CleanupStore(cloud_options);
}
