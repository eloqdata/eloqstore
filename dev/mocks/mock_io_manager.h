#pragma once

#include <map>
#include <vector>
#include <span>
#include "async_io_manager.h"
#include "page.h"
#include "types.h"

namespace eloqstore::test {

/**
 * Mock AsyncIoManager for testing without real I/O
 */
class MockAsyncIoManager : public AsyncIoManager {
public:
    explicit MockAsyncIoManager(const KvOptions* opts = nullptr)
        : AsyncIoManager(opts) {
        enable_failures_ = false;
        failure_rate_ = 0.0;
    }

    // Implement pure virtual methods
    KvError Init(Shard* shard) override {
        return KvError::NoError;
    }

    void Submit() override {}
    void PollComplete() override {}

    std::pair<Page, KvError> ReadPage(const TableIdent& tbl_id,
                                      FilePageId fp_id,
                                      Page page) override {
        read_count_++;
        if (ShouldFail()) {
            return std::make_pair(std::move(page), KvError::IoFail);
        }
        // Return page as-is for mock
        return std::make_pair(std::move(page), KvError::NoError);
    }

    KvError ReadPages(const TableIdent& tbl_id,
                     std::span<FilePageId> page_ids,
                     std::vector<Page>& pages) override {
        read_count_ += page_ids.size();
        if (ShouldFail()) {
            return KvError::IoFail;
        }
        // Pages are already allocated, just return success
        return KvError::NoError;
    }

    KvError WritePage(const TableIdent& tbl_id,
                     VarPage page,
                     FilePageId file_page_id) override {
        write_count_++;
        if (ShouldFail()) {
            return KvError::IoFail;
        }
        // Store page data in memory (simplified - just track the write)
        written_pages_[file_page_id] = std::vector<char>();
        return KvError::NoError;
    }

    KvError WritePages(const TableIdent& tbl_id,
                      std::span<VarPage> pages,
                      FilePageId first_fp_id) override {
        write_count_ += pages.size();
        if (ShouldFail()) {
            return KvError::IoFail;
        }
        // Store all pages (simplified - just track the writes)
        FilePageId fp_id = first_fp_id;
        for (size_t i = 0; i < pages.size(); ++i) {
            written_pages_[fp_id + i] = std::vector<char>();
        }
        return KvError::NoError;
    }

    KvError SyncData(const TableIdent& tbl_id) override {
        sync_count_++;
        if (ShouldFail()) {
            return KvError::IoFail;
        }
        return KvError::NoError;
    }

    KvError AbortWrite(const TableIdent& tbl_id) override {
        abort_count_++;
        return KvError::NoError;
    }

    KvError AppendManifest(const TableIdent& tbl_id,
                          std::string_view log,
                          uint64_t manifest_size) override {
        manifest_writes_++;
        if (ShouldFail()) {
            return KvError::IoFail;
        }
        manifest_data_.append(log);
        return KvError::NoError;
    }

    KvError SwitchManifest(const TableIdent& tbl_id,
                          std::string_view snapshot) override {
        manifest_switches_++;
        if (ShouldFail()) {
            return KvError::IoFail;
        }
        manifest_data_ = snapshot;
        return KvError::NoError;
    }

    // Test control methods
    void SetFailureRate(double rate) { failure_rate_ = rate; }
    void EnableFailures(bool enable) { enable_failures_ = enable; }
    void ResetCounters() {
        read_count_ = 0;
        write_count_ = 0;
        sync_count_ = 0;
        abort_count_ = 0;
        manifest_writes_ = 0;
        manifest_switches_ = 0;
    }

    // Test inspection methods
    uint64_t GetReadCount() const { return read_count_; }
    uint64_t GetWriteCount() const { return write_count_; }
    uint64_t GetSyncCount() const { return sync_count_; }
    uint64_t GetAbortCount() const { return abort_count_; }
    uint64_t GetManifestWrites() const { return manifest_writes_; }
    uint64_t GetManifestSwitches() const { return manifest_switches_; }
    const std::string& GetManifestData() const { return manifest_data_; }

private:
    bool ShouldFail() {
        if (!enable_failures_) return false;
        return (rand() / static_cast<double>(RAND_MAX)) < failure_rate_;
    }

    // Mock state
    std::map<FilePageId, std::vector<char>> written_pages_;
    std::string manifest_data_;

    // Failure injection
    bool enable_failures_;
    double failure_rate_;

    // Statistics
    uint64_t read_count_ = 0;
    uint64_t write_count_ = 0;
    uint64_t sync_count_ = 0;
    uint64_t abort_count_ = 0;
    uint64_t manifest_writes_ = 0;
    uint64_t manifest_switches_ = 0;
};

} // namespace eloqstore::test