#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

void InitMemStore()
{
    if (memstore)
    {
        return;
    }

    kvstore::KvOptions opts;
    memstore = std::make_unique<kvstore::EloqStore>(opts);
    memstore->Start();
}
