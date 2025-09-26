#include "types.h"

namespace eloqstore
{
std::ostream &operator<<(std::ostream &out, const TablePartitionIdent &tid)
{
    out << tid.tbl_name_ << TablePartitionIdent::separator << tid.partition_id_;
    return out;
}

std::string TablePartitionIdent::ToString() const
{
    return tbl_name_ + separator + std::to_string(partition_id_);
}

TablePartitionIdent TablePartitionIdent::FromString(const std::string &str)
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

uint8_t TablePartitionIdent::DiskIndex(uint8_t num_disks) const
{
    assert(num_disks > 0);
    return partition_id_ % num_disks;
}

fs::path TablePartitionIdent::StorePath(tcb::span<const std::string> disks) const
{
    fs::path partition_path = disks[DiskIndex(disks.size())];
    partition_path.append(ToString());
    return partition_path;
}

uint16_t TablePartitionIdent::ShardIndex(uint16_t num_shards) const
{
    assert(num_shards > 0);
    return partition_id_ % num_shards;
}

bool TablePartitionIdent::IsValid() const
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