#pragma once
#ifdef ELOQ_MODULE_ENABLED
#include <bthread/eloq_module.h>

#include <vector>

#include "storage/shard.h"

namespace eloqstore
{

class EloqStoreModule : public eloq::EloqModule
{
public:
    EloqStoreModule() = default;
    explicit EloqStoreModule(std::vector<std::unique_ptr<Shard>> *shards)
        : shards_(shards)
    {
    }
    ~EloqStoreModule() = default;

    void ExtThdStart(int thd_id) override;
    void ExtThdEnd(int thd_id) override;
    void Process(int thd_id) override;
    bool HasTask(int thd_id) const override;

    std::vector<std::unique_ptr<Shard>> *shards_;
};

}  // namespace eloqstore
#endif