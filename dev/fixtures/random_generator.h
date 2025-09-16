#pragma once

#include <random>
#include <string>
#include <vector>
#include <cstdint>

namespace eloqstore {
namespace test {

enum class Distribution {
    UNIFORM,
    NORMAL,
    ZIPFIAN,
    SEQUENTIAL,
    HOTSPOT
};

class RandomGenerator {
public:
    explicit RandomGenerator(uint64_t seed = 0);

    // Set seed (0 = use timestamp)
    void SetSeed(uint64_t seed);
    uint64_t GetSeed() const { return seed_; }

    // Integer generation
    int64_t GetInt(int64_t min, int64_t max);
    uint64_t GetUInt(uint64_t min, uint64_t max);

    // Float generation
    double GetDouble(double min = 0.0, double max = 1.0);

    // Boolean with probability
    bool GetBool(double probability = 0.5);

    // String generation
    std::string GetString(size_t min_len, size_t max_len);
    std::string GetAlphaNumeric(size_t length);
    std::string GetHex(size_t length);

    // Key generation with patterns
    std::string GetKey(size_t min_len, size_t max_len, Distribution dist = Distribution::UNIFORM);
    std::string GetSequentialKey(size_t length);
    std::string GetHotspotKey(size_t length, double hotspot_probability = 0.9);

    // Value generation
    std::string GetValue(size_t min_len, size_t max_len);
    std::vector<uint8_t> GetBytes(size_t length);

    // Operation selection
    enum class Operation {
        READ,
        WRITE,
        SCAN,
        DELETE
    };
    Operation GetRandomOperation(const std::vector<double>& weights = {0.4, 0.4, 0.15, 0.05});

    // Zipfian distribution for skewed access patterns
    uint64_t GetZipfian(uint64_t max, double theta = 0.99);

private:
    uint64_t seed_;
    std::mt19937_64 rng_;
    uint64_t sequence_counter_ = 0;

    // Cached distributions
    std::uniform_int_distribution<int64_t> int_dist_;
    std::uniform_real_distribution<double> real_dist_;
    std::bernoulli_distribution bool_dist_;

    // Zipfian cache
    struct ZipfianCache {
        uint64_t max = 0;
        double theta = 0;
        std::vector<double> probabilities;
    };
    ZipfianCache zipfian_cache_;

    void InitializeZipfian(uint64_t max, double theta);
};

// Global random generator instance
RandomGenerator& GetRandomGenerator();

} // namespace test
} // namespace eloqstore