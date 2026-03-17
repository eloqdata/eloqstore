#include <algorithm>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "kv_options.h"
#include "test_utils.h"
#include "utils.h"

using namespace test_util;
namespace chrono = std::chrono;

TEST_CASE("cloud start with different term", "[cloud][term]")
{
    eloqstore::EloqStore *store = InitStore(cloud_options);
    store->Stop();

    // start with term 1
    store->Start(1);
    MapVerifier tester(test_tbl_id, store);
    tester.SetValueSize(40960);
    tester.SetStore(store);
    tester.Upsert(0, 100);
    tester.Validate();

    REQUIRE(tester.CheckKey(30) == eloqstore::KvError::NoError);
    REQUIRE(tester.CheckKey(200) == eloqstore::KvError::NotFound);

    store->Stop();
    CleanupLocalStore(cloud_options);

    // start with term 5, can read data written by term 1
    store->Start(5);
    tester.Validate();
    REQUIRE(tester.CheckKey(30) == eloqstore::KvError::NoError);
    REQUIRE(tester.CheckKey(200) == eloqstore::KvError::NotFound);

    tester.Upsert(100, 200);
    tester.Validate();

    store->Stop();
    CleanupLocalStore(cloud_options);

    // start with term 3, should be expired, because term 3 is less than
    // term 5
    store->Start(3);
    REQUIRE(tester.CheckKey(30) == eloqstore::KvError::ExpiredTerm);

    store->Stop();
    CleanupLocalStore(cloud_options);

    // start with term 1', should only read data written by term 1
    store->Start(1);
    REQUIRE(tester.CheckKey(50) == eloqstore::KvError::NoError);
    REQUIRE(tester.CheckKey(200) == eloqstore::KvError::NotFound);

    MapVerifier tester2(test_tbl_id, store);
    tester2.SetValueSize(40960);
    tester2.SetStore(store);
    tester2.SetAutoValidate(false);

    tester2.Upsert(400, 500);
    tester2.SetAutoClean(false);

    store->Stop();
    CleanupLocalStore(cloud_options);

    // start with term 7, can read data written by term 1 and term 5,
    // can't read data written by term 1'
    store->Start(7);
    tester.Validate();
    REQUIRE(tester.CheckKey(450) == eloqstore::KvError::NotFound);

    tester.Clean();
    tester.SetAutoClean(false);

    store->Stop();

    CleanupStore(cloud_options);
}

TEST_CASE("cloud delete current term after truncate", "[cloud][term][gc]")
{
    using namespace std::chrono_literals;

    CleanupStore(cloud_options);

    const eloqstore::TableIdent tbl_id{"cloud_term_cleanup", 0};

    eloqstore::EloqStore writer_store(cloud_options);
    REQUIRE(writer_store.Start(1) == eloqstore::KvError::NoError);

    eloqstore::BatchWriteRequest write_req;
    write_req.SetArgs(tbl_id, {});
    write_req.AddWrite("k1", "v1", 1, eloqstore::WriteOp::Upsert);
    writer_store.ExecSync(&write_req);
    REQUIRE(write_req.Error() == eloqstore::KvError::NoError);

    writer_store.Stop();
    CleanupLocalStore(cloud_options);

    eloqstore::EloqStore store(cloud_options);
    REQUIRE(store.Start(1) == eloqstore::KvError::NoError);

    eloqstore::ReadRequest read_existing_req;
    read_existing_req.SetArgs(tbl_id, "k1");
    store.ExecSync(&read_existing_req);
    REQUIRE(read_existing_req.Error() == eloqstore::KvError::NoError);

    eloqstore::TruncateRequest truncate_req;
    truncate_req.SetArgs(tbl_id, std::string_view{});
    store.ExecSync(&truncate_req);
    REQUIRE(truncate_req.Error() == eloqstore::KvError::NoError);

    std::vector<std::string> cloud_files;
    bool found_current_term_only = false;
    for (int i = 0; i < 40; ++i)
    {
        cloud_files = ListCloudFiles(
            cloud_options, cloud_options.cloud_store_path, tbl_id.ToString());
        if (cloud_files.size() == 1 &&
            cloud_files[0] == eloqstore::CurrentTermFileName)
        {
            found_current_term_only = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    REQUIRE(found_current_term_only);

    eloqstore::DeleteCurrentTermRequest delete_req;
    delete_req.SetTableId(tbl_id);
    delete_req.SetTerm(2);
    store.ExecSync(&delete_req);
    REQUIRE(delete_req.Error() == eloqstore::KvError::NoError);

    cloud_files = ListCloudFiles(
        cloud_options, cloud_options.cloud_store_path, tbl_id.ToString());
    REQUIRE(cloud_files.empty());

    eloqstore::ReadRequest read_req;
    read_req.SetArgs(tbl_id, "k1");
    store.ExecSync(&read_req);
    REQUIRE(read_req.Error() == eloqstore::KvError::NotFound);

    cloud_files = ListCloudFiles(
        cloud_options, cloud_options.cloud_store_path, tbl_id.ToString());
    REQUIRE(cloud_files.empty());

    store.Stop();
    CleanupStore(cloud_options);
}
