#pragma once

#include <filesystem>
#include <string>

namespace eloqstore::test {

/**
 * RAII wrapper for temporary directory creation and cleanup
 */
class TempDirectory {
public:
    explicit TempDirectory(const std::string& prefix = "test");
    ~TempDirectory();

    // Disable copy
    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    // Enable move
    TempDirectory(TempDirectory&& other) noexcept;
    TempDirectory& operator=(TempDirectory&& other) noexcept;

    const std::filesystem::path& path() const { return path_; }
    std::string pathString() const { return path_.string(); }

    // Create subdirectory
    std::filesystem::path createSubdir(const std::string& name);

    // Clean all contents but keep directory
    void clean();

private:
    std::filesystem::path path_;
    bool should_cleanup_ = true;

    static std::string generateUniqueName(const std::string& prefix);
};

} // namespace eloqstore::test