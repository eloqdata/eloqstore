#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "db_stress_test_base.h"

namespace StressTest
{

class BatchedOpsStressTest : public StressTest
{
public:
    BatchedOpsStressTest(const std::string &table_name);
    virtual ~BatchedOpsStressTest() = default;

    void TestPut(uint32_t partition_id,
                 std::vector<int64_t> &rand_keys) override;

    void TestDelete(uint32_t partition_id,
                    std::vector<int64_t> &rand_keys) override;
    
    void TestMixedOps(uint32_t partition_id,
                std::vector<int64_t> &rand_keys) override;
    void TestGet(uint32_t reader_id, int64_t rand_key) override;

    void TestScan(uint32_t reader_id, int64_t rand_key) override;

    void VerifyDb() override;
};

}  // namespace StressTest