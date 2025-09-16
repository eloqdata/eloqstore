#pragma once

#include <string>
#include <vector>
#include <random>
#include <functional>
#include <memory>
#include <map>

namespace eloqstore::test {

/**
 * Test data generator for various test scenarios
 */
class DataGenerator {
public:
    DataGenerator(uint32_t seed = 0);

    // Key generators
    std::string GenerateKey(size_t length);
    std::string GenerateSequentialKey(uint64_t sequence);
    std::string GenerateRandomKey(size_t min_length, size_t max_length);
    std::string GeneratePrefixedKey(const std::string& prefix, uint64_t id);
    std::vector<std::string> GenerateKeyRange(uint64_t start, uint64_t count);
    std::vector<std::string> GenerateRandomKeys(size_t count, size_t key_length);

    // Value generators
    std::string GenerateValue(size_t length);
    std::string GenerateRandomValue(size_t min_length, size_t max_length);
    std::string GenerateCompressibleValue(size_t length, double compression_ratio);
    std::string GeneratePatternedValue(const std::string& pattern, size_t repeats);
    std::vector<std::string> GenerateValues(size_t count, size_t value_length);

    // Key-value pair generators
    std::map<std::string, std::string> GenerateKeyValuePairs(size_t count,
                                                             size_t key_length,
                                                             size_t value_length);
    std::map<std::string, std::string> GenerateSequentialData(uint64_t start,
                                                              uint64_t count,
                                                              size_t value_length);
    std::map<std::string, std::string> GenerateSkewedData(size_t count,
                                                          double skew_factor);

    // Special data patterns
    std::vector<std::string> GenerateSortedKeys(size_t count, size_t key_length);
    std::vector<std::string> GenerateReverseSortedKeys(size_t count, size_t key_length);
    std::vector<std::string> GenerateClusteredKeys(size_t clusters, size_t keys_per_cluster);

    // Edge case data
    std::string GenerateBinaryString(size_t length);
    std::string GenerateUTF8String(size_t char_count);
    std::string GenerateSpecialCharacters();
    std::string GenerateEmptyString() { return ""; }
    std::string GenerateMaxLengthKey(size_t max_allowed);
    std::string GenerateMaxLengthValue(size_t max_allowed);

    // Workload generators
    enum class WorkloadType {
        UNIFORM,
        ZIPFIAN,
        SEQUENTIAL,
        RANDOM,
        HOTSPOT
    };

    struct WorkloadConfig {
        WorkloadType type = WorkloadType::UNIFORM;
        size_t key_count = 1000;
        size_t value_size = 100;
        double read_ratio = 0.5;
        double write_ratio = 0.3;
        double scan_ratio = 0.2;
        double delete_ratio = 0.0;
        double zipf_alpha = 0.99;  // For Zipfian distribution
        size_t hotspot_fraction = 10; // 1/10 keys are hot
    };

    struct Operation {
        enum Type { READ, WRITE, SCAN, DELETE } type;
        std::string key;
        std::string value;  // For writes
        std::string end_key; // For scans
    };

    std::vector<Operation> GenerateWorkload(const WorkloadConfig& config, size_t op_count);

    // Reset random generator
    void Reset(uint32_t seed);

private:
    std::mt19937 rng_;
    uint64_t sequential_counter_ = 0;

    char RandomChar();
    char RandomPrintableChar();
    char RandomAlphaNumeric();
    std::string RandomString(size_t length, std::function<char()> char_generator);

    // Zipfian distribution for skewed access patterns
    class ZipfianGenerator {
    public:
        ZipfianGenerator(size_t n, double alpha);
        size_t Next(std::mt19937& rng);

    private:
        size_t n_;
        double alpha_;
        double zeta_n_;
        double eta_;
    };
};

/**
 * Test data set manager for consistent test data
 */
class TestDataSet {
public:
    explicit TestDataSet(const std::string& name);

    // Load/save test data sets
    void Save(const std::string& path) const;
    void Load(const std::string& path);

    // Add data to set
    void AddKeyValuePair(const std::string& key, const std::string& value);
    void AddKeyValuePairs(const std::map<std::string, std::string>& kvs);

    // Access data
    const std::map<std::string, std::string>& GetData() const { return data_; }
    std::vector<std::string> GetKeys() const;
    std::vector<std::string> GetValues() const;

    // Data manipulation
    void Shuffle();
    void Sort();
    void Reverse();
    TestDataSet Sample(size_t count) const;
    TestDataSet Filter(std::function<bool(const std::string&, const std::string&)> predicate) const;

    // Statistics
    size_t Size() const { return data_.size(); }
    size_t TotalKeyBytes() const;
    size_t TotalValueBytes() const;
    double AverageKeyLength() const;
    double AverageValueLength() const;

private:
    std::string name_;
    std::map<std::string, std::string> data_;
};

/**
 * Predefined test data patterns
 */
class TestPatterns {
public:
    // Common test patterns
    static std::vector<std::string> GetBoundaryKeys();
    static std::vector<std::string> GetBoundaryValues();
    static std::vector<std::string> GetUnicodeTestStrings();
    static std::vector<std::string> GetSQLInjectionStrings();
    static std::vector<std::string> GetPathTraversalStrings();

    // Numeric patterns
    static std::vector<int64_t> GetBoundaryIntegers();
    static std::vector<double> GetBoundaryFloats();

    // Size patterns
    static std::vector<size_t> GetPowerOfTwoSizes();
    static std::vector<size_t> GetFibonacciSizes(size_t max);
    static std::vector<size_t> GetPrimeSizes(size_t max);
};

} // namespace eloqstore::test