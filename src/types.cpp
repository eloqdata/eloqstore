#include "types.h"

namespace eloqstore
{
std::ostream &operator<<(std::ostream &out, const TableIdent &tid)
{
    out << tid.tbl_name_ << TableIdent::separator << tid.partition_id_;
    return out;
}

std::string TableIdent::ToString() const
{
    return tbl_name_ + separator + std::to_string(partition_id_);
}

TableIdent TableIdent::FromString(const std::string &str)
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

size_t TableIdent::StorePathIndex(
    size_t num_paths, tcb::span<const uint32_t> store_path_lut) const
{
    assert(num_paths > 0);
    assert(!store_path_lut.empty());
    size_t slot = partition_id_ % store_path_lut.size();
    size_t store_path_idx = store_path_lut[slot];
    assert(store_path_idx < num_paths);
    return store_path_idx;
}

fs::path TableIdent::StorePath(tcb::span<const std::string> store_paths,
                               tcb::span<const uint32_t> store_path_lut) const
{
    fs::path partition_path =
        store_paths[StorePathIndex(store_paths.size(), store_path_lut)];
    partition_path.append(ToString());
    return partition_path;
}

uint16_t TableIdent::ShardIndex(uint16_t num_shards) const
{
    assert(num_shards > 0);
    return partition_id_ % num_shards;
}

bool TableIdent::IsValid() const
{
    return !tbl_name_.empty();
}

WriteDataEntry::WriteDataEntry(std::string key,
                               std::string val,
                               uint64_t ts,
                               WriteOp op,
                               uint64_t expire_ts)
    : key_(std::move(key)),
      val_(std::move(val)),
      timestamp_(ts),
      op_(op),
      expire_ts_(expire_ts)
{
}

bool WriteDataEntry::operator<(const WriteDataEntry &other) const
{
    // TODO: use comparator defined in KvOptions ?
    return key_ < other.key_;
}
}  // namespace eloqstore
