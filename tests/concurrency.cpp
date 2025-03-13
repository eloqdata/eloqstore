#include <glog/logging.h>

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "common.h"
#include "test_utils.h"

TEST_CASE("concurrent tasks with memory store", "[concurrency]")
{
    InitMemStore();
    ConcurrencyTester tester(memstore.get(), "t1", 1, 16, 32, 20);
    tester.Init();
    tester.Run(5);
}
