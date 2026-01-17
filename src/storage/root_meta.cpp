#include "storage/root_meta.h"

#include <algorithm>
#include <cassert>
#include <string>
#include <string_view>

#include "coding.h"
#include "storage/page_mapper.h"
#include "types.h"

namespace eloqstore
{

ManifestBuilder::ManifestBuilder()
{
    Reset();
}

void ManifestBuilder::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    if (!resized_for_mapping_bytes_len_)
    {
        // When changing mapping with ManifestBuilder, mapping info will be
        // appended to buf_ directly, so we should pre-allocate space for
        // mapping_bytes_len and update them later.
        CHECK(buff_.size() == header_bytes);
        buff_.resize(buff_.size() + 4);
        resized_for_mapping_bytes_len_ = true;
    }

    buff_.AppendVarint32(page_id);
    buff_.AppendVarint64(MappingSnapshot::EncodeFilePageId(file_page_id));
}

void ManifestBuilder::DeleteMapping(PageId page_id)
{
    if (!resized_for_mapping_bytes_len_)
    {
        CHECK(buff_.size() == header_bytes);
        buff_.resize(buff_.size() + 4);
        resized_for_mapping_bytes_len_ = true;
    }

    buff_.AppendVarint32(page_id);
    buff_.AppendVarint64(MappingSnapshot::InvalidValue);
}

void ManifestBuilder::AppendFileIdTermMapping(
    std::string_view file_term_mapping)
{
    CHECK(resized_for_mapping_bytes_len_ || buff_.size() == header_bytes);
    if (!resized_for_mapping_bytes_len_)
    {
        buff_.resize(buff_.size() + 4);
        resized_for_mapping_bytes_len_ = true;
    }
    // update the mapping_len(4B)
    uint32_t mapping_len =
        static_cast<uint32_t>(buff_.size() - header_bytes - 4);
    EncodeFixed32(buff_.data() + header_bytes, mapping_len);
    // append the serialized file_term_mapping
    buff_.append(file_term_mapping);
}

std::string_view ManifestBuilder::Snapshot(PageId root_id,
                                           PageId ttl_root,
                                           const MappingSnapshot *mapping,
                                           FilePageId max_fp_id,
                                           std::string_view dict_bytes,
                                           std::string_view file_term_mapping)
{
    // For snapshot, the structure is:
    // Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) |
    // MaxFpId(8B) | DictLen(4B) | dict_bytes(bytes) | mapping_len(4B) |
    // mapping_tbl(varint64...) | file_term_mapping_len(4B) |
    // file_term_mapping(varint64...)
    Reset();
    buff_.reserve(4 + 8 * (mapping->mapping_tbl_.size() + 1) + 4 +
                  file_term_mapping.size());
    buff_.AppendVarint64(max_fp_id);
    buff_.AppendVarint32(dict_bytes.size());
    buff_.append(dict_bytes.data(), dict_bytes.size());
    // mapping_bytes_len(4B)
    size_t mapping_bytes_len_offset = buff_.size();
    buff_.resize(buff_.size() + 4);
    // mapping_tbl
    mapping->Serialize(buff_);
    // update the mapping_bytes_len
    uint32_t mapping_bytes_len =
        static_cast<uint32_t>(buff_.size() - mapping_bytes_len_offset - 4);
    EncodeFixed32(buff_.data() + mapping_bytes_len_offset, mapping_bytes_len);
    // file_term_mapping
    buff_.append(file_term_mapping);
    return Finalize(root_id, ttl_root);
}

void ManifestBuilder::Reset()
{
    buff_.resize(header_bytes);
    resized_for_mapping_bytes_len_ = false;
}

bool ManifestBuilder::Empty() const
{
    return buff_.size() <= header_bytes;
}

uint32_t ManifestBuilder::CurrentSize() const
{
    return buff_.size();
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
    buff_.AlignTo(page_align);
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
