#include "storage/root_meta.h"

#include <algorithm>
#include <cassert>
#include <string>
#include <string_view>

#include "coding.h"
#include "storage/page_mapper.h"

namespace eloqstore
{

ManifestBuilder::ManifestBuilder()
{
    buff_.resize(header_bytes);
}

void ManifestBuilder::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    buff_.AppendVarint32(page_id);
    buff_.AppendVarint64(MappingSnapshot::EncodeFilePageId(file_page_id));
}

void ManifestBuilder::DeleteMapping(PageId page_id)
{
    buff_.AppendVarint32(page_id);
    buff_.AppendVarint64(MappingSnapshot::InvalidValue);
}

std::string_view ManifestBuilder::Snapshot(PageId root_id,
                                           PageId ttl_root,
                                           const MappingSnapshot *mapping,
                                           FilePageId max_fp_id,
                                           std::string_view dict_bytes)
{
    Reset();
    buff_.reserve(4 + 8 * (mapping->mapping_tbl_.size() + 1));
    buff_.AppendVarint64(max_fp_id);
    buff_.AppendVarint32(dict_bytes.size());
    buff_.append(dict_bytes.data(), dict_bytes.size());
    mapping->Serialize(buff_);
    return Finalize(root_id, ttl_root);
}

void ManifestBuilder::Reset()
{
    buff_.resize(header_bytes);
}

bool ManifestBuilder::Empty() const
{
    return buff_.size() <= header_bytes;
}

uint32_t ManifestBuilder::CurrentSize() const
{
    return buff_.size();
}

size_t ManifestBuilder::DirectIoSize() const
{
    return buff_.padded_size();
}

std::string_view ManifestBuilder::Finalize(PageId new_root, PageId ttl_root)
{
    EncodeFixed32(buff_.data() + offset_root, new_root);
    EncodeFixed32(buff_.data() + offset_ttl_root, ttl_root);

    const uint32_t payload_len = buff_.size() - header_bytes;
    EncodeFixed32(buff_.data() + offset_len, payload_len);

    const std::string_view content(buff_.data() + checksum_bytes,
                                   buff_.size() - checksum_bytes);
    EncodeFixed64(buff_.data(), CalcChecksum(content));
    return buff_.View();
}

bool ManifestBuilder::ValidateChecksum(std::string_view record)
{
    if (record.size() < header_bytes)
    {
        return false;
    }
    const uint64_t stored = DecodeFixed64(record.data());
    const std::string_view content(record.data() + checksum_bytes,
                                   record.size() - checksum_bytes);
    return stored == CalcChecksum(content);
}

uint64_t ManifestBuilder::CalcChecksum(std::string_view content)
{
    if (content.empty())
    {
        return 0;
    }

    uint64_t agg_checksum = 0;
    static constexpr size_t kCheckSumBatchSize = 1024 * 1024;
    static constexpr uint64_t kChecksumMixer = 0x9e3779b97f4a7c15ULL;
    bool can_yield = shard != nullptr;
    for (size_t off = 0; off < content.size(); off += kCheckSumBatchSize)
    {
        const size_t batch_size =
            std::min<size_t>(kCheckSumBatchSize, content.size() - off);
        const uint64_t checksum = XXH3_64bits(content.data() + off, batch_size);
        agg_checksum = std::rotl(agg_checksum, 1) ^ checksum;
        agg_checksum *= kChecksumMixer;
        if (__builtin_expect(can_yield, 1))
        {
            ThdTask()->YieldToNextRound();
        }
    }
    return agg_checksum;
}

void RootMeta::Pin()
{
    ref_cnt_++;
}

void RootMeta::Unpin()
{
    assert(ref_cnt_ > 0);
    ref_cnt_--;
}

}  // namespace eloqstore
