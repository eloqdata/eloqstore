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
    eloqstore::KvOptions options = cloud_options;
    options.allow_reuse_local_caches = true;

    eloqstore::EloqStore *store = InitStore(options);
    store->Stop();

    // start with term 1
    REQUIRE(store->Start(1) == eloqstore::KvError::NoError);
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
    REQUIRE(store->Start(5) == eloqstore::KvError::NoError);
    tester.Validate();
    REQUIRE(tester.CheckKey(30) == eloqstore::KvError::NoError);
    REQUIRE(tester.CheckKey(200) == eloqstore::KvError::NotFound);

    tester.Upsert(100, 200);
    tester.Validate();

    store->Stop();
    CleanupLocalStore(cloud_options);

    // start with term 3, should be expired, because term 3 is less than
    // term 5
    REQUIRE(store->Start(3) == eloqstore::KvError::ExpiredTerm);

    store->Stop();
    CleanupLocalStore(cloud_options);

    // start with term 7 in the same partition group, can read data written by
    // term 1 and term 5.
    REQUIRE(store->Start(7) == eloqstore::KvError::NoError);
    tester.Validate();

    tester.Clean();
    tester.SetAutoClean(false);

    store->Stop();

    CleanupStore(options);
}
