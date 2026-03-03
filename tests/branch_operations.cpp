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
