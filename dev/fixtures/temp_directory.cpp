#include "temp_directory.h"
#include <random>
#include <sstream>
#include <chrono>
#include <glog/logging.h>

namespace eloqstore::test {

TempDirectory::TempDirectory(const std::string& prefix) {
    path_ = std::filesystem::temp_directory_path() / generateUniqueName(prefix);
    std::filesystem::create_directories(path_);
    LOG(INFO) << "Created temporary directory: " << path_;
}

TempDirectory::~TempDirectory() {
    if (should_cleanup_ && std::filesystem::exists(path_)) {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
        if (ec) {
            LOG(ERROR) << "Failed to remove temporary directory " << path_
                      << ": " << ec.message();
        } else {
            LOG(INFO) << "Removed temporary directory: " << path_;
        }
    }
}

TempDirectory::TempDirectory(TempDirectory&& other) noexcept
    : path_(std::move(other.path_)), should_cleanup_(other.should_cleanup_) {
    other.should_cleanup_ = false;
}

TempDirectory& TempDirectory::operator=(TempDirectory&& other) noexcept {
    if (this != &other) {
        if (should_cleanup_ && std::filesystem::exists(path_)) {
            std::filesystem::remove_all(path_);
        }
        path_ = std::move(other.path_);
        should_cleanup_ = other.should_cleanup_;
        other.should_cleanup_ = false;
    }
    return *this;
}

std::filesystem::path TempDirectory::createSubdir(const std::string& name) {
    auto subdir = path_ / name;
    std::filesystem::create_directories(subdir);
    return subdir;
}

void TempDirectory::clean() {
    if (std::filesystem::exists(path_)) {
        for (const auto& entry : std::filesystem::directory_iterator(path_)) {
            std::filesystem::remove_all(entry.path());
        }
    }
}

std::string TempDirectory::generateUniqueName(const std::string& prefix) {
    std::stringstream ss;
    ss << prefix << "_";

    // Add timestamp
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    ss << millis << "_";

    // Add random component
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(10000, 99999);
    ss << dis(gen);

    return ss.str();
}

} // namespace eloqstore::test