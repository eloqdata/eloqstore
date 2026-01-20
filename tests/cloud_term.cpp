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
