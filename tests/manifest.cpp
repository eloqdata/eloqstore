#include <algorithm>
#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>

#include "kv_options.h"
#include "test_utils.h"
#include "tests/common.h"
#include "utils.h"

using namespace test_util;
namespace fs = std::filesystem;

TEST_CASE("simple manifest recovery", "[manifest]")
{
    eloqstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    verifier.NewMapping();
    verifier.NewMapping();
    verifier.UpdateMapping();
    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();

    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();
}

TEST_CASE("medium manifest recovery", "[manifest]")
{
    eloqstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    for (int i = 0; i < 100; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();
    }
    verifier.Verify();

    verifier.Snapshot();
    verifier.Verify();

    for (int i = 0; i < 10; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();

        verifier.Verify();
    }
}

TEST_CASE("detect manifest corruption", "[manifest]")
{
    // TODO:
}

TEST_CASE("create archives", "[archive][slow]")
{
    eloqstore::KvOptions options{
        .num_retained_archives = 1,
        .archive_interval_secs = 1,
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };
    eloqstore::EloqStore *store = InitStore(options);

    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(10000);

    for (int i = 0; i < 10; i++)
    {
        tester.WriteRnd(0, 1000, 50, 80);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    tester.Validate();
}

TEST_CASE("rollback to archive", "[archive][slow]")
{
    // 简单的rollback测试：写数据->归档->写更多数据->回滚->验证->恢复->验证
    eloqstore::KvOptions options{
        .num_retained_archives = 10,
        .archive_interval_secs = 1,  // 1秒归档间隔
        .file_amplify_factor = 4,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };

    eloqstore::EloqStore *store = InitStore(options);
    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(1000);

    // 写入初始数据
    tester.Upsert(0, 50);  // 键值范围 0-49
    tester.Validate();

    // 等待归档
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // 写入更多数据
    tester.Upsert(50, 100);  // 键值范围 50-99
    tester.Validate();

    // 停止store
    store->Stop();

    // 找到归档文件
    std::string manifest_path = std::string(test_path) + "/manifest";
    std::string archive_file;

    for (const auto &entry : fs::directory_iterator(std::string(test_path)))
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            if (filename.find("manifest_") == 0)
            {
                archive_file = entry.path().string();
                break;
            }
        }
    }

    REQUIRE(!archive_file.empty());

    // 备份当前manifest
    std::string backup_manifest = manifest_path + "_backup";
    fs::copy_file(manifest_path, backup_manifest);

    // 回滚：用归档文件替换当前manifest
    fs::copy_file(
        archive_file, manifest_path, fs::copy_options::overwrite_existing);

    // 重新启动store验证回滚结果
    store->Start();
    MapVerifier rollback_tester(test_tbl_id, store, false);
    rollback_tester.SetValueSize(1000);
    rollback_tester.Validate();

    // 验证只有初始数据（0-49）
    auto rollback_data = rollback_tester.DataSet();
    REQUIRE(rollback_data.size() == 50);  // 应该只有50个键

    // 停止store
    store->Stop();

    // 恢复：用备份的manifest恢复
    fs::copy_file(
        backup_manifest, manifest_path, fs::copy_options::overwrite_existing);
    fs::remove(backup_manifest);

    // 重新启动store验证恢复结果
    store->Start();
    MapVerifier final_tester(test_tbl_id, store, false);
    final_tester.SetValueSize(1000);
    final_tester.Validate();

    // 验证有完整数据（0-99）
    auto final_data = final_tester.DataSet();
    REQUIRE(final_data.size() == 100);  // 应该有100个键
}
