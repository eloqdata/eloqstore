#include "data_generator.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <set>
#include <memory>

namespace eloqstore::test {

// DataGenerator implementation
DataGenerator::DataGenerator(uint32_t seed) : rng_(seed) {
    if (seed == 0) {
        std::random_device rd;
        rng_.seed(rd());
    }
}

std::string DataGenerator::GenerateKey(size_t length) {
    return RandomString(length, [this] { return RandomAlphaNumeric(); });
}

std::string DataGenerator::GenerateSequentialKey(uint64_t sequence) {
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(16) << sequence;
    return ss.str();
}

std::string DataGenerator::GenerateRandomKey(size_t min_length, size_t max_length) {
    std::uniform_int_distribution<size_t> length_dist(min_length, max_length);
    return GenerateKey(length_dist(rng_));
}

std::string DataGenerator::GeneratePrefixedKey(const std::string& prefix, uint64_t id) {
    return prefix + "_" + std::to_string(id);
}

std::vector<std::string> DataGenerator::GenerateKeyRange(uint64_t start, uint64_t count) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        keys.push_back(GenerateSequentialKey(start + i));
    }
    return keys;
}

std::vector<std::string> DataGenerator::GenerateRandomKeys(size_t count, size_t key_length) {
    std::vector<std::string> keys;
    keys.reserve(count);
    std::set<std::string> unique_keys; // Ensure uniqueness

    while (unique_keys.size() < count) {
        unique_keys.insert(GenerateKey(key_length));
    }

    keys.assign(unique_keys.begin(), unique_keys.end());
    return keys;
}

std::string DataGenerator::GenerateValue(size_t length) {
    return RandomString(length, [this] { return RandomPrintableChar(); });
}

std::string DataGenerator::GenerateRandomValue(size_t min_length, size_t max_length) {
    std::uniform_int_distribution<size_t> length_dist(min_length, max_length);
    return GenerateValue(length_dist(rng_));
}

std::string DataGenerator::GenerateCompressibleValue(size_t length, double compression_ratio) {
    // Generate value with repeated patterns for compressibility
    size_t pattern_length = static_cast<size_t>(length * compression_ratio);
    std::string pattern = GenerateValue(std::max(size_t(1), pattern_length));

    std::string result;
    result.reserve(length);
    while (result.size() < length) {
        result += pattern;
    }
    result.resize(length);
    return result;
}

std::string DataGenerator::GeneratePatternedValue(const std::string& pattern, size_t repeats) {
    std::string result;
    result.reserve(pattern.size() * repeats);
    for (size_t i = 0; i < repeats; ++i) {
        result += pattern;
    }
    return result;
}

std::vector<std::string> DataGenerator::GenerateValues(size_t count, size_t value_length) {
    std::vector<std::string> values;
    values.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        values.push_back(GenerateValue(value_length));
    }
    return values;
}

std::map<std::string, std::string> DataGenerator::GenerateKeyValuePairs(
    size_t count, size_t key_length, size_t value_length) {
    std::map<std::string, std::string> kvs;
    auto keys = GenerateRandomKeys(count, key_length);
    for (const auto& key : keys) {
        kvs[key] = GenerateValue(value_length);
    }
    return kvs;
}

std::map<std::string, std::string> DataGenerator::GenerateSequentialData(
    uint64_t start, uint64_t count, size_t value_length) {
    std::map<std::string, std::string> kvs;
    for (uint64_t i = 0; i < count; ++i) {
        kvs[GenerateSequentialKey(start + i)] = GenerateValue(value_length);
    }
    return kvs;
}

std::map<std::string, std::string> DataGenerator::GenerateSkewedData(
    size_t count, double skew_factor) {
    // Use Zipfian distribution for skewed access
    std::map<std::string, std::string> kvs;
    ZipfianGenerator zipf(count, skew_factor);

    for (size_t i = 0; i < count; ++i) {
        size_t key_id = zipf.Next(rng_);
        std::string key = GenerateSequentialKey(key_id);
        kvs[key] = GenerateValue(100);
    }
    return kvs;
}

std::vector<std::string> DataGenerator::GenerateSortedKeys(size_t count, size_t key_length) {
    auto keys = GenerateRandomKeys(count, key_length);
    std::sort(keys.begin(), keys.end());
    return keys;
}

std::vector<std::string> DataGenerator::GenerateReverseSortedKeys(size_t count, size_t key_length) {
    auto keys = GenerateSortedKeys(count, key_length);
    std::reverse(keys.begin(), keys.end());
    return keys;
}

std::vector<std::string> DataGenerator::GenerateClusteredKeys(
    size_t clusters, size_t keys_per_cluster) {
    std::vector<std::string> keys;
    for (size_t c = 0; c < clusters; ++c) {
        std::string cluster_prefix = "cluster_" + std::to_string(c);
        for (size_t k = 0; k < keys_per_cluster; ++k) {
            keys.push_back(GeneratePrefixedKey(cluster_prefix, k));
        }
    }
    return keys;
}

std::string DataGenerator::GenerateBinaryString(size_t length) {
    std::string result;
    result.reserve(length);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    for (size_t i = 0; i < length; ++i) {
        result += static_cast<char>(byte_dist(rng_));
    }
    return result;
}

std::string DataGenerator::GenerateUTF8String(size_t char_count) {
    std::string result;
    std::uniform_int_distribution<int> type_dist(0, 2);

    for (size_t i = 0; i < char_count; ++i) {
        int type = type_dist(rng_);
        if (type == 0) {
            // ASCII
            result += RandomPrintableChar();
        } else if (type == 1) {
            // 2-byte UTF-8
            std::uniform_int_distribution<int> code_dist(0x80, 0x7FF);
            int code = code_dist(rng_);
            result += static_cast<char>(0xC0 | (code >> 6));
            result += static_cast<char>(0x80 | (code & 0x3F));
        } else {
            // 3-byte UTF-8
            std::uniform_int_distribution<int> code_dist(0x800, 0xFFFF);
            int code = code_dist(rng_);
            result += static_cast<char>(0xE0 | (code >> 12));
            result += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
            result += static_cast<char>(0x80 | (code & 0x3F));
        }
    }
    return result;
}

std::string DataGenerator::GenerateSpecialCharacters() {
    const std::string special = "!@#$%^&*()_+-=[]{}|;:'\",.<>?/\\`~\n\t\r";
    return RandomString(20, [&special, this]() {
        std::uniform_int_distribution<size_t> dist(0, special.size() - 1);
        return special[dist(rng_)];
    });
}

std::string DataGenerator::GenerateMaxLengthKey(size_t max_allowed) {
    return std::string(max_allowed, 'K');
}

std::string DataGenerator::GenerateMaxLengthValue(size_t max_allowed) {
    return std::string(max_allowed, 'V');
}

std::vector<DataGenerator::Operation> DataGenerator::GenerateWorkload(
    const WorkloadConfig& config, size_t op_count) {
    std::vector<Operation> operations;
    operations.reserve(op_count);

    // Generate key pool
    auto keys = GenerateRandomKeys(config.key_count, 16);

    // Distribution for operation types
    std::discrete_distribution<> op_dist({
        config.read_ratio,
        config.write_ratio,
        config.scan_ratio,
        config.delete_ratio
    });

    // Key selection distribution based on workload type
    std::unique_ptr<std::uniform_int_distribution<size_t>> uniform_dist;
    std::unique_ptr<ZipfianGenerator> zipf_gen;

    if (config.type == WorkloadType::ZIPFIAN) {
        zipf_gen = std::make_unique<ZipfianGenerator>(config.key_count, config.zipf_alpha);
    } else {
        uniform_dist = std::make_unique<std::uniform_int_distribution<size_t>>(0, config.key_count - 1);
    }

    for (size_t i = 0; i < op_count; ++i) {
        Operation op;
        op.type = static_cast<Operation::Type>(op_dist(rng_));

        // Select key based on workload type
        size_t key_index;
        switch (config.type) {
            case WorkloadType::SEQUENTIAL:
                key_index = i % config.key_count;
                break;
            case WorkloadType::ZIPFIAN:
                key_index = zipf_gen->Next(rng_);
                break;
            case WorkloadType::HOTSPOT:
                // 90% of ops go to hotspot keys
                if (std::uniform_real_distribution<>(0, 1)(rng_) < 0.9) {
                    key_index = std::uniform_int_distribution<size_t>(0, config.key_count / config.hotspot_fraction)(rng_);
                } else {
                    key_index = (*uniform_dist)(rng_);
                }
                break;
            default: // UNIFORM or RANDOM
                key_index = (*uniform_dist)(rng_);
                break;
        }

        op.key = keys[key_index];

        switch (op.type) {
            case Operation::WRITE:
                op.value = GenerateValue(config.value_size);
                break;
            case Operation::SCAN:
                {
                    size_t end_index = std::min(key_index + 10, config.key_count - 1);
                    op.end_key = keys[end_index];
                }
                break;
            default:
                break;
        }

        operations.push_back(op);
    }

    return operations;
}

void DataGenerator::Reset(uint32_t seed) {
    rng_.seed(seed);
    sequential_counter_ = 0;
}

char DataGenerator::RandomChar() {
    std::uniform_int_distribution<int> dist(0, 255);
    return static_cast<char>(dist(rng_));
}

char DataGenerator::RandomPrintableChar() {
    std::uniform_int_distribution<int> dist(32, 126);
    return static_cast<char>(dist(rng_));
}

char DataGenerator::RandomAlphaNumeric() {
    const std::string alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<size_t> dist(0, alphanum.size() - 1);
    return alphanum[dist(rng_)];
}

std::string DataGenerator::RandomString(size_t length, std::function<char()> char_generator) {
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += char_generator();
    }
    return result;
}

// ZipfianGenerator implementation
DataGenerator::ZipfianGenerator::ZipfianGenerator(size_t n, double alpha)
    : n_(n), alpha_(alpha) {
    // Compute normalization constant
    zeta_n_ = 0;
    for (size_t i = 1; i <= n; ++i) {
        zeta_n_ += 1.0 / std::pow(i, alpha);
    }
    eta_ = (1 - std::pow(2.0 / n_, 1 - alpha)) / (1 - zeta_n_ / 2.0 / zeta_n_);
}

size_t DataGenerator::ZipfianGenerator::Next(std::mt19937& rng) {
    std::uniform_real_distribution<> dist(0, 1);
    double u = dist(rng);
    double uz = u * zeta_n_;

    if (uz < 1.0) return 0;
    if (uz < 1.0 + std::pow(0.5, alpha_)) return 1;

    return static_cast<size_t>((n_ + 1) * std::pow(eta_ * u - eta_ + 1, 1.0 / (1.0 - alpha_)));
}

// TestDataSet implementation
TestDataSet::TestDataSet(const std::string& name) : name_(name) {}

void TestDataSet::Save(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    // Save name
    size_t name_len = name_.size();
    file.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    file.write(name_.data(), name_len);

    // Save data
    size_t data_size = data_.size();
    file.write(reinterpret_cast<const char*>(&data_size), sizeof(data_size));

    for (const auto& [key, value] : data_) {
        size_t key_len = key.size();
        size_t val_len = value.size();
        file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        file.write(key.data(), key_len);
        file.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
        file.write(value.data(), val_len);
    }
}

void TestDataSet::Load(const std::string& path) {
    std::ifstream file(path, std::ios::binary);

    // Load name
    size_t name_len;
    file.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    name_.resize(name_len);
    file.read(name_.data(), name_len);

    // Load data
    size_t data_size;
    file.read(reinterpret_cast<char*>(&data_size), sizeof(data_size));

    data_.clear();
    for (size_t i = 0; i < data_size; ++i) {
        size_t key_len, val_len;
        std::string key, value;

        file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        key.resize(key_len);
        file.read(key.data(), key_len);

        file.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
        value.resize(val_len);
        file.read(value.data(), val_len);

        data_[key] = value;
    }
}

void TestDataSet::AddKeyValuePair(const std::string& key, const std::string& value) {
    data_[key] = value;
}

void TestDataSet::AddKeyValuePairs(const std::map<std::string, std::string>& kvs) {
    data_.insert(kvs.begin(), kvs.end());
}

std::vector<std::string> TestDataSet::GetKeys() const {
    std::vector<std::string> keys;
    keys.reserve(data_.size());
    for (const auto& [key, _] : data_) {
        keys.push_back(key);
    }
    return keys;
}

std::vector<std::string> TestDataSet::GetValues() const {
    std::vector<std::string> values;
    values.reserve(data_.size());
    for (const auto& [_, value] : data_) {
        values.push_back(value);
    }
    return values;
}

size_t TestDataSet::TotalKeyBytes() const {
    size_t total = 0;
    for (const auto& [key, _] : data_) {
        total += key.size();
    }
    return total;
}

size_t TestDataSet::TotalValueBytes() const {
    size_t total = 0;
    for (const auto& [_, value] : data_) {
        total += value.size();
    }
    return total;
}

double TestDataSet::AverageKeyLength() const {
    return data_.empty() ? 0.0 : static_cast<double>(TotalKeyBytes()) / data_.size();
}

double TestDataSet::AverageValueLength() const {
    return data_.empty() ? 0.0 : static_cast<double>(TotalValueBytes()) / data_.size();
}

// TestPatterns implementation
std::vector<std::string> TestPatterns::GetBoundaryKeys() {
    return {
        "",                    // Empty
        "a",                   // Single char
        std::string(255, 'k'), // Max single byte length
        std::string(256, 'k'), // Overflow single byte
        std::string(65535, 'k'), // Max two byte length
        std::string(65536, 'k'), // Overflow two byte
        "\x00",                // Null byte
        "\xFF\xFF\xFF\xFF",    // All bits set
        "key\x00key",          // Embedded null
    };
}

std::vector<std::string> TestPatterns::GetBoundaryValues() {
    return {
        "",                    // Empty
        "v",                   // Single char
        std::string(4096, 'v'), // Page size
        std::string(4095, 'v'), // Just under page
        std::string(4097, 'v'), // Just over page
        std::string(1048576, 'v'), // 1MB
        "\x00\x00\x00\x00",    // Null bytes
        std::string(10000, '\n'), // Newlines
    };
}

std::vector<std::string> TestPatterns::GetUnicodeTestStrings() {
    return {
        "Hello, world",          // ASCII baseline
        "Test123",               // Mixed alphanumeric
        "Special chars: !@#$%",  // Special characters
        "Tab\tand\nnewline",     // Control characters
        "Path/To/File",          // Path-like string
        "key=value&param=test",  // Query-like string
        std::string("\x00\x01\x02", 3), // Binary data
    };
}

std::vector<int64_t> TestPatterns::GetBoundaryIntegers() {
    return {
        0,
        1,
        -1,
        INT8_MAX,
        INT8_MIN,
        INT16_MAX,
        INT16_MIN,
        INT32_MAX,
        INT32_MIN,
        INT64_MAX,
        INT64_MIN,
    };
}

std::vector<size_t> TestPatterns::GetPowerOfTwoSizes() {
    std::vector<size_t> sizes;
    for (size_t i = 0; i <= 20; ++i) {
        sizes.push_back(1ULL << i);
    }
    return sizes;
}

} // namespace eloqstore::test