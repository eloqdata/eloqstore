#include "random_generator.h"
#include <chrono>
#include <algorithm>
#include <cmath>
#include <numeric>

namespace eloqstore {
namespace test {

RandomGenerator::RandomGenerator(uint64_t seed) {
    SetSeed(seed);
}

void RandomGenerator::SetSeed(uint64_t seed) {
    if (seed == 0) {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        seed_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    } else {
        seed_ = seed;
    }
    rng_.seed(seed_);
    sequence_counter_ = 0;
}

int64_t RandomGenerator::GetInt(int64_t min, int64_t max) {
    std::uniform_int_distribution<int64_t> dist(min, max);
    return dist(rng_);
}

uint64_t RandomGenerator::GetUInt(uint64_t min, uint64_t max) {
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(rng_);
}

double RandomGenerator::GetDouble(double min, double max) {
    std::uniform_real_distribution<double> dist(min, max);
    return dist(rng_);
}

bool RandomGenerator::GetBool(double probability) {
    std::bernoulli_distribution dist(probability);
    return dist(rng_);
}

std::string RandomGenerator::GetString(size_t min_len, size_t max_len) {
    size_t length = GetUInt(min_len, max_len);
    std::string result;
    result.reserve(length);

    static const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);
    for (size_t i = 0; i < length; ++i) {
        result += charset[dist(rng_)];
    }

    return result;
}

std::string RandomGenerator::GetAlphaNumeric(size_t length) {
    std::string result;
    result.reserve(length);

    static const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);
    for (size_t i = 0; i < length; ++i) {
        result += charset[dist(rng_)];
    }

    return result;
}

std::string RandomGenerator::GetHex(size_t length) {
    std::string result;
    result.reserve(length);

    static const char hexchars[] = "0123456789ABCDEF";
    std::uniform_int_distribution<size_t> dist(0, 15);

    for (size_t i = 0; i < length; ++i) {
        result += hexchars[dist(rng_)];
    }

    return result;
}

std::string RandomGenerator::GetKey(size_t min_len, size_t max_len, Distribution dist) {
    switch (dist) {
        case Distribution::SEQUENTIAL:
            return GetSequentialKey(GetUInt(min_len, max_len));
        case Distribution::HOTSPOT:
            return GetHotspotKey(GetUInt(min_len, max_len));
        case Distribution::ZIPFIAN: {
            uint64_t key_num = GetZipfian(1000000);  // 1M key range
            std::string key = "key_" + std::to_string(key_num);
            // Pad to min length if needed
            while (key.length() < min_len) {
                key = "0" + key;
            }
            return key;
        }
        case Distribution::NORMAL:
        case Distribution::UNIFORM:
        default:
            return "key_" + GetAlphaNumeric(GetUInt(min_len - 4, max_len - 4));
    }
}

std::string RandomGenerator::GetSequentialKey(size_t length) {
    std::string key = std::to_string(sequence_counter_++);
    // Pad with zeros to reach desired length
    while (key.length() < length) {
        key = "0" + key;
    }
    return key.substr(0, length);
}

std::string RandomGenerator::GetHotspotKey(size_t length, double hotspot_probability) {
    std::string key;
    if (GetBool(hotspot_probability)) {
        // Return a key from a small set (hotspot)
        uint64_t hotspot_key = GetUInt(0, 9);  // 10 hotspot keys
        key = "HOT_" + std::to_string(hotspot_key);
    } else {
        // Return a random key
        key = "COLD_" + GetAlphaNumeric(length - 5);
    }

    // Ensure correct length
    while (key.length() < length) {
        key += "0";
    }
    return key.substr(0, length);
}

std::string RandomGenerator::GetValue(size_t min_len, size_t max_len) {
    size_t length = GetUInt(min_len, max_len);
    std::string result;
    result.reserve(length);

    // Generate more realistic value data
    static const std::string words[] = {
        "data", "value", "content", "information", "record",
        "entry", "item", "element", "object", "entity"
    };

    while (result.length() < length) {
        if (!result.empty()) result += " ";
        result += words[GetUInt(0, 9)];
        result += "_" + std::to_string(GetUInt(0, 999999));
    }

    return result.substr(0, length);
}

std::vector<uint8_t> RandomGenerator::GetBytes(size_t length) {
    std::vector<uint8_t> result(length);
    std::uniform_int_distribution<int> dist(0, 255);
    for (size_t i = 0; i < length; ++i) {
        result[i] = static_cast<uint8_t>(dist(rng_));
    }
    return result;
}

RandomGenerator::Operation RandomGenerator::GetRandomOperation(const std::vector<double>& weights) {
    // Normalize weights
    double total = std::accumulate(weights.begin(), weights.end(), 0.0);
    double rand = GetDouble(0, total);

    double cumulative = 0;
    if (rand < (cumulative += weights[0])) return Operation::READ;
    if (rand < (cumulative += weights[1])) return Operation::WRITE;
    if (rand < (cumulative += weights[2])) return Operation::SCAN;
    return Operation::DELETE;
}

uint64_t RandomGenerator::GetZipfian(uint64_t max, double theta) {
    // Initialize cache if needed
    if (zipfian_cache_.max != max || zipfian_cache_.theta != theta) {
        InitializeZipfian(max, theta);
    }

    double rand = GetDouble(0, 1);
    for (uint64_t i = 0; i < max; ++i) {
        if (rand <= zipfian_cache_.probabilities[i]) {
            return i;
        }
    }
    return max - 1;
}

void RandomGenerator::InitializeZipfian(uint64_t max, double theta) {
    zipfian_cache_.max = max;
    zipfian_cache_.theta = theta;
    zipfian_cache_.probabilities.clear();
    zipfian_cache_.probabilities.reserve(max);

    // Calculate normalization constant
    double c = 0;
    for (uint64_t i = 1; i <= max; ++i) {
        c += 1.0 / std::pow(i, theta);
    }

    // Calculate cumulative probabilities
    double sum = 0;
    for (uint64_t i = 1; i <= max; ++i) {
        sum += (1.0 / std::pow(i, theta)) / c;
        zipfian_cache_.probabilities.push_back(sum);
    }
}

RandomGenerator& GetRandomGenerator() {
    static RandomGenerator generator;
    return generator;
}

} // namespace test
} // namespace eloqstore