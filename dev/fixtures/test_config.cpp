#include "test_config.h"
#include <fstream>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <algorithm>
#include <iostream>

namespace eloqstore {
namespace test {

TestConfig& TestConfig::GetInstance() {
    static TestConfig instance;
    return instance;
}

TestConfig TestConfig::LoadConfig(int argc, char* argv[]) {
    TestConfig config;

    // Load in priority order: file -> env -> command line
    config.LoadFromFile("dev/test_config.ini");
    config.LoadFromEnvironment();
    if (argc > 0 && argv != nullptr) {
        config.LoadFromCommandLine(argc, argv);
    }

    return config;
}

uint64_t TestConfig::GetEffectiveSeed() const {
    if (seed == 0) {
        // Use current timestamp as seed
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }
    return seed;
}

bool TestConfig::LoadFromFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        // Try alternative path
        file.open("test_config.ini");
        if (!file.is_open()) {
            return false;
        }
    }

    std::string line;
    std::string current_section;

    while (std::getline(file, line)) {
        // Remove comments
        size_t comment_pos = line.find('#');
        if (comment_pos != std::string::npos) {
            line = line.substr(0, comment_pos);
        }

        // Trim whitespace
        line.erase(0, line.find_first_not_of(" \t\r\n"));
        line.erase(line.find_last_not_of(" \t\r\n") + 1);

        if (line.empty()) continue;

        // Check for section
        if (line.front() == '[' && line.back() == ']') {
            current_section = line.substr(1, line.size() - 2);
            continue;
        }

        // Parse key = value
        size_t eq_pos = line.find('=');
        if (eq_pos == std::string::npos) continue;

        std::string key = line.substr(0, eq_pos);
        std::string value = line.substr(eq_pos + 1);

        // Trim key and value
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);

        // Apply setting based on section
        if (current_section == "general") {
            if (key == "verbose") verbose = (value == "true" || value == "1");
            else if (key == "output_dir") output_dir = value;
            else if (key == "temp_dir") temp_dir = value;
        }
        else if (current_section == "stress") {
            if (key == "seed") seed = std::stoull(value);
            else if (key == "iterations") iterations = std::stoull(value);
            else if (key == "duration_seconds") duration_seconds = std::stoull(value);
            else if (key == "min_key_size") min_key_size = std::stoul(value);
            else if (key == "max_key_size") max_key_size = std::stoul(value);
            else if (key == "min_value_size") min_value_size = std::stoul(value);
            else if (key == "max_value_size") max_value_size = std::stoul(value);
            else if (key == "thread_count") thread_count = std::stoi(value);
            else if (key == "ops_per_thread") ops_per_thread = std::stoi(value);
        }
        else if (current_section == "performance") {
            if (key == "warmup_iterations") warmup_iterations = std::stoi(value);
            else if (key == "measurement_iterations") measurement_iterations = std::stoi(value);
            else if (key == "report_percentiles") {
                // Parse comma-separated percentiles
                report_percentiles.clear();
                std::stringstream ss(value);
                std::string item;
                while (std::getline(ss, item, ',')) {
                    report_percentiles.push_back(std::stod(item));
                }
            }
        }
        else if (current_section == "fault_injection") {
            if (key == "io_error_rate") io_error_rate = std::stod(value);
            else if (key == "memory_fail_rate") memory_fail_rate = std::stod(value);
            else if (key == "crash_probability") crash_probability = std::stod(value);
        }
    }

    return true;
}

bool TestConfig::LoadFromEnvironment() {
    auto get_env = [](const char* name) -> std::optional<std::string> {
        const char* val = std::getenv(name);
        return val ? std::optional<std::string>(val) : std::nullopt;
    };

    if (auto val = get_env("ELOQ_TEST_SEED")) {
        seed = std::stoull(*val);
    }
    if (auto val = get_env("ELOQ_TEST_ITERATIONS")) {
        iterations = std::stoull(*val);
    }
    if (auto val = get_env("ELOQ_TEST_DURATION")) {
        duration_seconds = std::stoull(*val);
    }
    if (auto val = get_env("ELOQ_TEST_THREADS")) {
        thread_count = std::stoi(*val);
    }
    if (auto val = get_env("ELOQ_TEST_VERBOSE")) {
        verbose = (*val == "1" || *val == "true");
    }

    return true;
}

bool TestConfig::LoadFromCommandLine(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        ParseCommandLineArg(arg);
    }
    return true;
}

bool TestConfig::ParseCommandLineArg(const std::string& arg) {
    if (arg.substr(0, 7) == "--seed=") {
        seed = std::stoull(arg.substr(7));
        return true;
    }
    if (arg.substr(0, 13) == "--iterations=") {
        iterations = std::stoull(arg.substr(13));
        return true;
    }
    if (arg.substr(0, 11) == "--duration=") {
        duration_seconds = std::stoull(arg.substr(11));
        return true;
    }
    if (arg.substr(0, 10) == "--threads=") {
        thread_count = std::stoi(arg.substr(10));
        return true;
    }
    if (arg == "--verbose" || arg == "-v") {
        verbose = true;
        return true;
    }
    return false;
}

std::optional<std::string> TestConfig::GetEnvVar(const std::string& name) const {
    const char* val = std::getenv(name.c_str());
    return val ? std::optional<std::string>(val) : std::nullopt;
}

} // namespace test
} // namespace eloqstore