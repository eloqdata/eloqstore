#include "root_meta.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>

#include "coding.h"
#include "page_mapper.h"

namespace eloqstore
{

ManifestBuilder::ManifestBuilder()
{
    buff_.resize(header_bytes);
}

void ManifestBuilder::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    PutVarint32(&buff_, page_id);
    PutVarint64(&buff_, MappingSnapshot::EncodeFilePageId(file_page_id));
}

void ManifestBuilder::DeleteMapping(PageId page_id)
{
    PutVarint32(&buff_, page_id);
    PutVarint64(&buff_, MappingSnapshot::InvalidValue);
}

std::string_view ManifestBuilder::Snapshot(PageId root_id,
                                           PageId ttl_root,
                                           const MappingSnapshot *mapping,
                                           FilePageId max_fp_id,
                                           std::string_view dict_bytes)
{
    Reset();
    buff_.reserve(4 + 8 * (mapping->mapping_tbl_.size() + 1));
    PutVarint64(&buff_, max_fp_id);
    PutVarint32(&buff_, dict_bytes.size());
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

std::string_view ManifestBuilder::Finalize(PageId new_root, PageId ttl_root)
{
    EncodeFixed32(buff_.data() + offset_root, new_root);
    EncodeFixed32(buff_.data() + offset_ttl_root, ttl_root);

    uint32_t len = buff_.size() - header_bytes;
    EncodeFixed32(buff_.data() + offset_len, len);

    SetChecksum(buff_);
    return buff_;
}

std::string_view ManifestBuilder::BuffView() const
{
    return buff_;
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
