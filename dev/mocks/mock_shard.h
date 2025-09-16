#pragma once

#include "types.h"

namespace eloqstore::test {

// Forward declaration to avoid including shard.h
class Shard;

/**
 * Mock Shard for testing - minimal stub implementation
 */
class MockShard {
public:
    explicit MockShard(uint16_t shard_id = 0)
        : shard_id_(shard_id) {}

    uint16_t GetShardId() const { return shard_id_; }

    // Test control methods
    void SimulateLoad(double load_factor) {
        load_factor_ = load_factor;
    }

    double GetLoadFactor() const { return load_factor_; }

private:
    uint16_t shard_id_;
    double load_factor_ = 0.0;
};

} // namespace eloqstore::test