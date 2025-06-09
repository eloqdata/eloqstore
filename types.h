#pragma once

#include <boost/functional/hash.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <ostream>
#include <span>
#include <string>

namespace kvstore
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
static constexpr char TmpSuffix[] = ".tmp";

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
    fs::path StorePath(std::span<const std::string> disks) const;
    size_t Hash() const;
    bool IsValid() const;

    std::string tbl_name_;
    uint32_t partition_id_;
};

inline std::string TableIdent::ToString() const
{
    return tbl_name_ + separator + std::to_string(partition_id_);
}

inline TableIdent TableIdent::FromString(const std::string &str)
{
    size_t p = str.find_last_of(separator);
    if (p == std::string::npos)
    {
        return {};
    }

    try
    {
        uint32_t id = std::stoul(str.data() + p + 1);
        return {str.substr(0, p), id};
    }
    catch (...)
    {
        return {};
    }
}

inline uint8_t TableIdent::DiskIndex(uint8_t num_disks) const
{
    assert(num_disks > 0);
    return partition_id_ % num_disks;
}

inline fs::path TableIdent::StorePath(std::span<const std::string> disks) const
{
    fs::path partition_path = disks[DiskIndex(disks.size())];
    partition_path.append(ToString());
    return partition_path;
}

inline bool TableIdent::IsValid() const
{
    return !tbl_name_.empty();
}

inline std::ostream &operator<<(std::ostream &out, const TableIdent &tid)
{
    out << tid.tbl_name_ << TableIdent::separator << tid.partition_id_;
    return out;
}

struct FileKey
{
    bool operator==(const FileKey &other) const
    {
        return tbl_id_ == other.tbl_id_ && filename_ == other.filename_;
    }
    TableIdent tbl_id_;
    std::string filename_;
};

using KvEntry = std::tuple<std::string, std::string, uint64_t>;

enum class WriteOp : uint8_t
{
    Upsert = 0,
    Delete
};

struct WriteDataEntry
{
    std::string key_;
    std::string val_;
    uint64_t timestamp_;
    WriteOp op_;
};

}  // namespace kvstore

template <>
struct std::hash<kvstore::TableIdent>
{
    std::size_t operator()(const kvstore::TableIdent &tbl_ident) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, tbl_ident.tbl_name_);
        boost::hash_combine(seed, tbl_ident.partition_id_);
        return seed;
    }
};

template <>
struct std::hash<kvstore::FileKey>
{
    std::size_t operator()(const kvstore::FileKey &file_key) const
    {
        size_t seed = std::hash<kvstore::TableIdent>()(file_key.tbl_id_);
        boost::hash_combine(seed, file_key.filename_);
        return seed;
    }
};