#include <glog/logging.h>

#include <algorithm>
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

using ::CleanupLocalStore;
using ::CleanupStore;
using ::InitStore;
using ::ListCloudFiles;
using test_util::MapVerifier;

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
    // CURRENT_TERM is NOT created at branch creation time; it is created
    // lazily when a store starts with the branch as its active branch.

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

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "feature1";
    store->ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    REQUIRE(!fs::exists(table_path / "manifest_feature1_0"));

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

TEST_CASE("global create branch - creates manifest on single partition",
          "[branch][global]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("feature1");
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / ("manifest_" + req.ResultBranch() + "_0")));

    store->Stop();
}

TEST_CASE("global create branch - creates manifests on all partitions",
          "[branch][global]")
{
    // Test both local and cloud mode with many partitions
    auto test_impl = [](const eloqstore::KvOptions &opts,
                        const char *mode_name,
                        int num_partitions)
    {
        INFO("Testing mode: " << mode_name << " with " << num_partitions
                              << " partitions");
        eloqstore::EloqStore *store = InitStore(opts);

        // Write to multiple partitions to verify scalability.
        // Each partition gets minimal data to keep test fast.
        std::vector<eloqstore::TableIdent> partitions;

        for (int p = 0; p < num_partitions; ++p)
        {
            eloqstore::TableIdent tbl_id = {"t0", static_cast<uint32_t>(p)};
            partitions.push_back(tbl_id);

            MapVerifier verify(tbl_id, store, false);
            verify.SetAutoClean(false);
            verify.SetAutoValidate(false);
            verify.Upsert(0, 5);  // 5 keys per partition (minimal)
        }

        // GlobalCreateBranch must fan out to all partitions.
        eloqstore::GlobalCreateBranchRequest req;
        req.SetArgs("feature1");
        store->ExecSync(&req);

        REQUIRE(req.Error() == eloqstore::KvError::NoError);

        // Verify manifest files exist for all partitions.
        // In cloud mode, verify a representative sample to keep test fast.
        std::vector<int> partitions_to_verify;
        if (opts.cloud_store_path.empty())
        {
            // Local mode: verify all
            for (int p = 0; p < num_partitions; ++p)
                partitions_to_verify.push_back(p);
        }
        else
        {
            // Cloud mode: verify sample (first 5 + last 5)
            for (int p = 0; p < std::min(5, num_partitions); ++p)
                partitions_to_verify.push_back(p);
            for (int p = std::max(0, num_partitions - 5); p < num_partitions;
                 ++p)
            {
                if (std::find(partitions_to_verify.begin(),
                              partitions_to_verify.end(),
                              p) == partitions_to_verify.end())
                {
                    partitions_to_verify.push_back(p);
                }
            }
        }

        for (int p : partitions_to_verify)
        {
            const auto &tbl_id = partitions[p];
            if (opts.cloud_store_path.empty())
            {
                // Local mode: check filesystem
                fs::path table_path = fs::path(test_path) / tbl_id.ToString();
                REQUIRE(fs::exists(table_path /
                                   ("manifest_" + req.ResultBranch() + "_0")));
            }
            else
            {
                // Cloud mode: verify manifest objects exist in cloud storage
                std::string tbl_prefix = std::string(opts.cloud_store_path) +
                                         "/" + tbl_id.ToString();
                std::vector<std::string> cloud_files =
                    ListCloudFiles(opts, tbl_prefix);

                bool found_manifest = false;
                for (const auto &f : cloud_files)
                {
                    if (f.find("manifest_" + req.ResultBranch() + "_0") !=
                        std::string::npos)
                        found_manifest = true;
                }
                INFO("Partition " << tbl_id.ToString()
                                  << " cloud files checked");
                REQUIRE(found_manifest);
            }
        }

        store->Stop();
        CleanupStore(opts);
    };

    SECTION("local mode - 100 partitions")
    {
        test_impl(default_opts, "local", 100);
    }

    SECTION("cloud mode - 20 partitions")
    {
        // Create custom cloud options with higher fd_limit to support 20
        // partitions. Cloud mode requires more file descriptors and takes
        // longer due to network I/O, so we test with 20 partitions (10x the
        // original test) instead of 100.
        eloqstore::KvOptions cloud_opts_high_fd = cloud_options;
        cloud_opts_high_fd.fd_limit = 100 + eloqstore::num_reserved_fd;
        test_impl(cloud_opts_high_fd, "cloud", 20);
    }
}

TEST_CASE("global create branch - invalid branch name returns InvalidArgs",
          "[branch][global]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);

    verify.Upsert(0, 100);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("bad_name");  // underscore not allowed
    store->ExecSync(&req);

    REQUIRE(req.Error() == eloqstore::KvError::InvalidArgs);

    store->Stop();
}

TEST_CASE("global create branch - no-op on empty store", "[branch][global]")
{
    // InitStore cleans up the store directory and starts fresh with no data.
    // There are no partition subdirectories, so the handler returns NoError
    // immediately without fanning out any sub-requests.
    eloqstore::EloqStore *store = InitStore(default_opts);

    eloqstore::GlobalCreateBranchRequest req;
    req.SetArgs("feature1");
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
    fs::path store_root = fs::path(test_path);
    REQUIRE(fs::exists(table_path / "manifest_feature_0"));

    // Simulate the branch having been written to at higher terms (e.g. after a
    // failover).  Write placeholder manifests for terms 1–3 and create
    // CURRENT_TERM_feature_0 with value "3" (branch creation does not write
    // the term file; it is normally created at store startup via
    // BootstrapUpsertTermFile, so we create it manually here).
    // DeleteBranchFiles reads CURRENT_TERM to discover max_term, then unlinks
    // manifests 0..max_term; it never reads the manifest contents, so
    // placeholder content is fine.
    for (int t = 1; t <= 3; ++t)
    {
        std::ofstream mf(table_path /
                         ("manifest_feature_" + std::to_string(t)));
        mf << "placeholder";
    }
    {
        std::ofstream ct(store_root / "CURRENT_TERM_feature_0",
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
        REQUIRE(!fs::exists(table_path /
                            ("manifest_feature_" + std::to_string(t))));
    }
    REQUIRE(!fs::exists(store_root / "CURRENT_TERM_feature_0"));

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
        eloqstore::KvError err =
            fresh_store.Start(eloqstore::MainBranchName, 0);
        REQUIRE(err == eloqstore::KvError::NoError);

        fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
        REQUIRE(fs::exists(table_path / "manifest_feature1_0"));

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

    // Phase 2: open on feature1, verify DS1 inherited, write DS2 (keys
    // 100-199).
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
    //          visible (main's writes after the fork must not leak into
    //          feature1).
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

TEST_CASE("sibling branches are isolated from each other",
          "[branch][isolation]")
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
    "sibling branches forked from same parent at different terms inherit "
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

// ---------------------------------------------------------------------------
// G1: Start on deleted branch and on a never-created branch both fail the
//     same way on first I/O (lazy failure — KvError::NotFound).
// ---------------------------------------------------------------------------
TEST_CASE(
    "start on deleted branch and nonexistent branch give same first-IO error",
    "[branch]")
{
    // Sub-case A: branch that existed but was deleted.
    eloqstore::KvError deleted_branch_err;
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);
        verify.Upsert(
            0, 100);  // write key 0 so it would be readable if manifest exists

        eloqstore::CreateBranchRequest create_req;
        create_req.SetTableId(test_tbl_id);
        create_req.branch_name = "deletedone";
        store->ExecSync(&create_req);
        REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

        eloqstore::DeleteBranchRequest delete_req;
        delete_req.SetTableId(test_tbl_id);
        delete_req.branch_name = "deletedone";
        store->ExecSync(&delete_req);
        REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

        store->Stop();

        // Start on the now-deleted branch — Start itself returns NoError
        // (lazy manifest resolution).
        eloqstore::EloqStore branch_store(default_opts);
        REQUIRE(branch_store.Start("deletedone", 0) ==
                eloqstore::KvError::NoError);

        MapVerifier bverify(test_tbl_id, &branch_store, false);
        // Key 0 was written on main before the fork, but the manifest is gone,
        // so the first read surfaces NotFound.
        deleted_branch_err = bverify.CheckKey(0);

        branch_store.Stop();
    }

    // Sub-case B: branch that was never created.
    eloqstore::KvError nonexistent_branch_err;
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);
        verify.Upsert(0, 100);
        store->Stop();

        eloqstore::EloqStore branch_store(default_opts);
        REQUIRE(branch_store.Start("neverexists", 0) ==
                eloqstore::KvError::NoError);

        MapVerifier bverify(test_tbl_id, &branch_store, false);
        nonexistent_branch_err = bverify.CheckKey(0);

        branch_store.Stop();
    }

    // Both cases must surface the same error on first I/O.
    REQUIRE(deleted_branch_err == nonexistent_branch_err);
    REQUIRE(deleted_branch_err == eloqstore::KvError::NotFound);

    CleanupStore(default_opts);
}

// ---------------------------------------------------------------------------
// G3: DeleteBranchRequest normalizes mixed-case branch names the same way
//     CreateBranchRequest does, so "FeatureX" deletes the "featurex" manifest.
// ---------------------------------------------------------------------------
TEST_CASE("delete branch with mixed-case name is normalized and succeeds",
          "[branch]")
{
    eloqstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetAutoClean(false);
    verify.Upsert(0, 100);

    // Create with lowercase name (required by create validation).
    eloqstore::CreateBranchRequest create_req;
    create_req.SetTableId(test_tbl_id);
    create_req.branch_name = "featurex";
    store->ExecSync(&create_req);
    REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_featurex_0"));

    // Delete with mixed-case name — must normalize to "featurex" and succeed.
    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "FeatureX";
    store->ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    REQUIRE(!fs::exists(table_path / "manifest_featurex_0"));

    store->Stop();
}

// ---------------------------------------------------------------------------
// G5: Deleting the branch the store was Start()-ed with must be rejected.
// ---------------------------------------------------------------------------
TEST_CASE("delete currently active branch is rejected", "[branch]")
{
    // Set up: create "activebr" from main, then switch to it.
    {
        eloqstore::EloqStore *store = InitStore(default_opts);
        MapVerifier verify(test_tbl_id, store, false);
        verify.SetAutoClean(false);
        verify.Upsert(0, 100);

        eloqstore::CreateBranchRequest create_req;
        create_req.SetTableId(test_tbl_id);
        create_req.branch_name = "activebr";
        store->ExecSync(&create_req);
        REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }

    // Start the store on "activebr" and attempt to delete it.
    eloqstore::EloqStore branch_store(default_opts);
    REQUIRE(branch_store.Start("activebr", 0) == eloqstore::KvError::NoError);

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "activebr";
    branch_store.ExecSync(&delete_req);

    // Must be rejected with InvalidArgs — cannot delete the active branch.
    REQUIRE(delete_req.Error() == eloqstore::KvError::InvalidArgs);

    // Branch manifest must still exist.
    fs::path table_path = fs::path(test_path) / test_tbl_id.ToString();
    REQUIRE(fs::exists(table_path / "manifest_activebr_0"));

    branch_store.Stop();

    CleanupStore(default_opts);
}

// ---------------------------------------------------------------------------
// G4: Cloud-mode delete removes all objects from object storage.
// ---------------------------------------------------------------------------
TEST_CASE("delete branch in cloud mode removes all cloud objects",
          "[branch][cloud]")
{
    // Phase 1: clean slate, write data on main, fork "cloudfeature".
    eloqstore::EloqStore *store = InitStore(cloud_options);

    {
        MapVerifier verify(test_tbl_id, store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(0, 50);

        eloqstore::CreateBranchRequest create_req;
        create_req.SetTableId(test_tbl_id);
        create_req.branch_name = "cloudfeature";
        store->ExecSync(&create_req);
        REQUIRE(create_req.Error() == eloqstore::KvError::NoError);
    }

    store->Stop();
    CleanupLocalStore(cloud_options);

    // Phase 2: write data to cloudfeature@term=1.
    // 50 keys × 40KB = 2MB → ~2 data_*_cloudfeature_1 files on cloud.
    {
        eloqstore::EloqStore br_store(cloud_options);
        REQUIRE(br_store.Start("cloudfeature", 1) ==
                eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &br_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(50, 100);

        br_store.Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 3: back on main@term=2, delete the cloudfeature branch.
    REQUIRE(store->Start(eloqstore::MainBranchName, 2) ==
            eloqstore::KvError::NoError);

    eloqstore::DeleteBranchRequest delete_req;
    delete_req.SetTableId(test_tbl_id);
    delete_req.branch_name = "cloudfeature";
    store->ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    store->Stop();
    CleanupLocalStore(cloud_options);

    // Phase 4: verify no "cloudfeature" objects remain in cloud (manifests,
    // CURRENT_TERM, and data files all deleted).
    // Note: CURRENT_TERM is at store root, manifests/data under table prefix.
    // Search both prefixes for any remaining "cloudfeature" objects.
    std::string tbl_prefix = std::string(cloud_options.cloud_store_path) + "/" +
                             test_tbl_id.ToString();
    std::vector<std::string> cloud_files =
        ListCloudFiles(cloud_options, tbl_prefix);
    for (const auto &f : cloud_files)
    {
        INFO("Unexpected cloud object still present: " << f);
        REQUIRE(f.find("cloudfeature") == std::string::npos);
    }
    // Also check store-root for any lingering CURRENT_TERM files.
    std::vector<std::string> root_files =
        ListCloudFiles(cloud_options, cloud_options.cloud_store_path);
    for (const auto &f : root_files)
    {
        INFO("Unexpected CURRENT_TERM object still present: " << f);
        REQUIRE(f.find("CURRENT_TERM_cloudfeature_") == std::string::npos);
    }

    CleanupStore(cloud_options);
}

// ---------------------------------------------------------------------------
// G6: End-to-end delete across real terms (cloud mode).
//     Writes real data on the branch across term=1 and term=3, then deletes.
//     Verifies that manifest_branchname_0, _1, _3 and CURRENT_TERM_<branch>_0
//     are all gone from cloud storage.
// ---------------------------------------------------------------------------
TEST_CASE(
    "delete branch removes all term manifests end-to-end across real terms",
    "[branch][cloud]")
{
    // Phase 1: clean slate.
    eloqstore::EloqStore *store = InitStore(cloud_options);
    store->Stop();

    // Phase 2: main@term=1 — write DS1, fork "multitemp".
    // Creates manifest_multitemp_0 on cloud.
    {
        REQUIRE(store->Start(eloqstore::MainBranchName, 1) ==
                eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(0, 50);  // DS1

        eloqstore::CreateBranchRequest create_req;
        create_req.SetTableId(test_tbl_id);
        create_req.branch_name = "multitemp";
        store->ExecSync(&create_req);
        REQUIRE(create_req.Error() == eloqstore::KvError::NoError);

        store->Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 3: multitemp@term=1 — write on the branch.
    // Creates manifest_multitemp_1 on cloud; CURRENT_TERM_multitemp_0 = 1.
    {
        eloqstore::EloqStore br_store(cloud_options);
        REQUIRE(br_store.Start("multitemp", 1) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &br_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(50, 100);

        br_store.Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 4: multitemp@term=3 — write more on the branch.
    // Creates manifest_multitemp_3 on cloud; CURRENT_TERM_multitemp_0 = 3.
    {
        eloqstore::EloqStore br_store(cloud_options);
        REQUIRE(br_store.Start("multitemp", 3) == eloqstore::KvError::NoError);

        MapVerifier verify(test_tbl_id, &br_store);
        verify.SetValueSize(40960);
        verify.SetAutoClean(false);
        verify.SetAutoValidate(false);
        verify.Upsert(100, 150);

        br_store.Stop();
        CleanupLocalStore(cloud_options);
    }

    // Phase 5: main@term=5 — delete the branch.
    {
        REQUIRE(store->Start(eloqstore::MainBranchName, 5) ==
                eloqstore::KvError::NoError);

        eloqstore::DeleteBranchRequest delete_req;
        delete_req.SetTableId(test_tbl_id);
        delete_req.branch_name = "multitemp";
        store->ExecSync(&delete_req);
        REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

        store->Stop();
    }
    CleanupLocalStore(cloud_options);

    // Phase 6: verify all "multitemp" objects are gone from cloud storage.
    std::string tbl_prefix = std::string(cloud_options.cloud_store_path) + "/" +
                             test_tbl_id.ToString();
    std::vector<std::string> cloud_files =
        ListCloudFiles(cloud_options, tbl_prefix);
    for (const auto &f : cloud_files)
    {
        INFO("Unexpected cloud object still present: " << f);
        REQUIRE(f.find("multitemp") == std::string::npos);
    }
    // Also check store-root for any lingering CURRENT_TERM files.
    std::vector<std::string> root_files =
        ListCloudFiles(cloud_options, cloud_options.cloud_store_path);
    for (const auto &f : root_files)
    {
        INFO("Unexpected CURRENT_TERM object still present: " << f);
        REQUIRE(f.find("CURRENT_TERM_multitemp_") == std::string::npos);
    }

    CleanupStore(cloud_options);
}
