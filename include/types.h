#pragma once

#include <boost/functional/hash.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <ostream>
#include <string>
#include <utility>  // NOLINT(build/include_order)

#include "external/span.hpp"

namespace eloqstore
{
using PageId = uint32_t;
constexpr PageId MaxPageId = UINT32_MAX;

using FilePageId = uint64_t;
constexpr FilePageId MaxFilePageId = UINT64_MAX;

using FileId = uint64_t;
static constexpr FileId MaxFileId = UINT64_MAX;

constexpr char FileNameSeparator = '_';
static constexpr char FileNameData[] = "data";
static constexpr char FileNameManifest[] = "manifest";
static constexpr char CurrentTermFileName[] = "CURRENT_TERM";
static constexpr char TmpSuffix[] = ".tmp";
constexpr size_t kDefaultScanPrefetchPageCount = 6;

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
    uint8_t DiskIndex(uint8_t num_disks) const;
    fs::path StorePath(tcb::span<const std::string> disks) const;
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
