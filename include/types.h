#pragma once

#include <boost/functional/hash.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>  // NOLINT(build/include_order)
#include <vector>

#include "external/span.hpp"

namespace eloqstore
{
enum class StoreMode
{
    Local = 0,
    StandbyMaster,
    StandbyReplica,
    Cloud
};

using PageId = uint32_t;
constexpr PageId MaxPageId = UINT32_MAX;

using FilePageId = uint64_t;
constexpr FilePageId MaxFilePageId = UINT64_MAX;

using FileId = uint64_t;
static constexpr FileId MaxFileId = UINT64_MAX;
using PartitonGroupId = uint32_t;

constexpr char FileNameSeparator = '_';
static constexpr char FileNameData[] = "data";
static constexpr char FileNameManifest[] = "manifest";
static constexpr char CurrentTermFileName[] = "CURRENT_TERM";
static constexpr char TmpSuffix[] = ".tmp";
constexpr size_t kDefaultScanPrefetchPageCount = 6;

// Branch name constants
static constexpr char MainBranchName[] = "main";

// BranchFileRange: tracks file_id range per branch
// Used in BranchFileMapping to find which branch a file_id belongs to
struct BranchFileRange
{
    std::string branch_name;  // branch identifier (e.g., "main", "feature")
    uint64_t term{};          // term when this file_id range was allocated
    FileId max_file_id{};     // highest file_id allocated in this branch

    // For sorting by max_file_id (required for binary search)
    bool operator<(const BranchFileRange &other) const
    {
        return max_file_id < other.max_file_id;
    }

    bool operator<(FileId fid) const
    {
        return max_file_id < fid;
    }
};

// BranchFileMapping: sorted vector of branch ranges
// Sorted by max_file_id for efficient binary search lookup
// Use std::lower_bound to find branch given file_id
using BranchFileMapping = std::vector<BranchFileRange>;

// BranchManifestMetadata: branch-specific manifest metadata
// Stored in manifest to identify branch and track file ranges
struct BranchManifestMetadata
{
    std::string branch_name;  // unique branch identifier (e.g., "main",
                              // "feature-a3f7b2c1")
    uint64_t term{};          // current term for this branch
    BranchFileMapping
        file_ranges;  // per-branch file ranges (sorted by max_file_id)
};

// RetainedFileKey: identifies a data file uniquely by (file_id, branch, term)
// Used in GC to correctly distinguish files with the same FileId from
// different branches (which can happen when sibling branches fork from the
// same parent at the same time and allocate overlapping FileId ranges).
struct RetainedFileKey
{
    FileId file_id{};
    std::string branch_name;
    uint64_t term{};

    bool operator==(const RetainedFileKey &other) const
    {
        return file_id == other.file_id && branch_name == other.branch_name &&
               term == other.term;
    }

    bool operator!=(const RetainedFileKey &other) const
    {
        return !(*this == other);
    }
};

namespace fs = std::filesystem;

struct TableIdent
{
    static constexpr char separator = '.';
    friend bool operator==(const TableIdent &lhs, const TableIdent &rhs)
    {
        return lhs.tbl_name_ == rhs.tbl_name_ &&
               lhs.partition_id_ == rhs.partition_id_;
    }
    friend std::ostream &operator<<(std::ostream &os, const TableIdent &point);

    TableIdent() = default;
    TableIdent(std::string tbl_name, uint32_t id)
        : tbl_name_(std::move(tbl_name)), partition_id_(id) {};
    std::string ToString() const;
    static TableIdent FromString(const std::string &str);
    size_t StorePathIndex(size_t num_paths,
                          tcb::span<const uint32_t> store_path_lut = {}) const;
    fs::path StorePath(tcb::span<const std::string> store_paths,
                       tcb::span<const uint32_t> store_path_lut = {}) const;
    uint16_t ShardIndex(uint16_t num_shards) const;
    bool IsValid() const;

    std::string tbl_name_;
    uint32_t partition_id_{};
};

std::ostream &operator<<(std::ostream &out, const TableIdent &tid);

struct FileKey
{
    bool operator==(const FileKey &other) const
    {
        return tbl_id_ == other.tbl_id_ && filename_ == other.filename_;
    }
    TableIdent tbl_id_;
    std::string filename_;
};

struct KvEntry
{
    bool operator==(const KvEntry &other) const
    {
        return key_ == other.key_ && value_ == other.value_ &&
               timestamp_ == other.timestamp_ && expire_ts_ == other.expire_ts_;
    }
    std::string key_;
    std::string value_;
    uint64_t timestamp_;
    uint64_t expire_ts_;
};

enum class WriteOp : uint8_t
{
    Upsert = 0,
    Delete
};

struct WriteDataEntry
{
    WriteDataEntry() = default;
    WriteDataEntry(std::string key,
                   std::string val,
                   uint64_t ts,
                   WriteOp op,
                   uint64_t expire_ts = 0);
    bool operator<(const WriteDataEntry &other) const;

    std::string key_;
    std::string val_;
    uint64_t timestamp_;
    WriteOp op_;
    uint64_t expire_ts_{0};  // 0 means never expire.
};

}  // namespace eloqstore

template <>
struct std::hash<eloqstore::TableIdent>
{
    std::size_t operator()(const eloqstore::TableIdent &tbl_ident) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, tbl_ident.tbl_name_);
        boost::hash_combine(seed, tbl_ident.partition_id_);
        return seed;
    }
};

template <>
struct std::hash<eloqstore::FileKey>
{
    std::size_t operator()(const eloqstore::FileKey &file_key) const
    {
        size_t seed = std::hash<eloqstore::TableIdent>()(file_key.tbl_id_);
        boost::hash_combine(seed, file_key.filename_);
        return seed;
    }
};

template <>
struct std::hash<eloqstore::RetainedFileKey>
{
    std::size_t operator()(const eloqstore::RetainedFileKey &key) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, key.file_id);
        boost::hash_combine(seed, key.branch_name);
        boost::hash_combine(seed, key.term);
        return seed;
    }
};
