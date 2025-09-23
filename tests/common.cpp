#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts)
{
    static std::unique_ptr<eloqstore::EloqStore> eloq_store = nullptr;

    CleanupStore(opts);

    if (!eloq_store)
    {
        eloq_store = std::make_unique<eloqstore::EloqStore>(opts);
    }

    if (!eloq_store->IsStopped())
    {
        eloq_store->Stop();
    }
    eloqstore::KvError err = eloq_store->Start();
    CHECK(err == eloqstore::KvError::NoError);
    return eloq_store.get();
}
