#pragma once

#include <string>
#include <cstdint>
#include <vector>
#include <optional>

namespace eloqstore {
namespace test {

struct TestConfig {
    // General settings
    bool verbose = false;
    std::string output_dir = "/mnt/ramdisk/test_results";
    std::string temp_dir = "/mnt/ramdisk/test_temp";

    // Stress test settings
    uint64_t seed = 0;  // 0 means use timestamp
    uint64_t iterations = 1000;
    uint64_t duration_seconds = 0;  // 0 means use iterations
    size_t min_key_size = 8;
    size_t max_key_size = 256;
    size_t min_value_size = 16;
    size_t max_value_size = 8192;
    int thread_count = 4;
    int ops_per_thread = 100;

    // Performance settings
    int warmup_iterations = 100;
    int measurement_iterations = 1000;
    std::vector<double> report_percentiles = {50.0, 95.0, 99.0, 99.9};

    // Fault injection settings
    double io_error_rate = 0.0;
    double memory_fail_rate = 0.0;
    double crash_probability = 0.0;

    // Load configuration from various sources
    static TestConfig LoadConfig(int argc = 0, char* argv[] = nullptr);

    // Load from specific sources
    bool LoadFromFile(const std::string& path);
    bool LoadFromEnvironment();
    bool LoadFromCommandLine(int argc, char* argv[]);

    // Get effective seed (handles 0 = timestamp case)
    uint64_t GetEffectiveSeed() const;

    // Check if should use duration-based testing
    bool IsTimeBased() const { return duration_seconds > 0; }

    // Singleton access
    static TestConfig& GetInstance();

private:
    bool ParseCommandLineArg(const std::string& arg);
    std::optional<std::string> GetEnvVar(const std::string& name) const;
};

} // namespace test
} // namespace eloqstore