#include "replayer.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "async_io_manager.h"
#include "coding.h"
#include "eloq_store.h"
#include "error.h"
#include "kv_options.h"
#include "storage/index_page_manager.h"
#include "storage/page.h"
#include "storage/root_meta.h"

namespace eloqstore
{

Replayer::Replayer(const KvOptions *opts) : opts_(opts)
{
    log_buf_.resize(ManifestBuilder::header_bytes);
}

KvError Replayer::Replay(ManifestFile *file)
{
    root_ = MaxPageId;
    ttl_root_ = MaxPageId;
    mapping_tbl_.clear();
    mapping_tbl_.reserve(opts_->init_page_count);
    segment_mapping_tbl_.clear();
    file_size_ = 0;
    max_fp_id_ = MaxFilePageId;
    max_segment_fp_id_ = 0;
    dict_bytes_.clear();

    KvError err = ParseNextRecord(file);
    CHECK_KV_ERR(err);
    assert(!payload_.empty());
    DeserializeSnapshot(payload_);
    bool corrupted_log_found = false;

    while (true)
    {
        err = ParseNextRecord(file);
        if (err != KvError::NoError)
        {
            if (err == KvError::EndOfFile)
            {
                break;
            }
            if (err == KvError::Corrupted)
            {
                LOG(ERROR) << "Ignoring the corrupted log, continuing.";
                corrupted_log_found = true;
                continue;
            }
            return err;
        }
        if (corrupted_log_found)
        {
            LOG(ERROR) << "Found corruption log between normal log";
            return KvError::Corrupted;
        }
        ReplayLog();
    }
    if (corrupted_log_found_)
    {
        file_size_ = file_size_before_corrupted_log_;
    }
    return KvError::NoError;
}

KvError Replayer::ParseNextRecord(ManifestFile *file)
{
    constexpr uint16_t header_len = ManifestBuilder::header_bytes;
    log_buf_.resize(header_len);
    KvError err = file->Read(log_buf_.data(), header_len);
    if (err != KvError::NoError)
    {
        return err;
    }

    const uint32_t payload_len =
        DecodeFixed32(log_buf_.data() + ManifestBuilder::offset_len);
    log_buf_.resize(static_cast<size_t>(header_len) + payload_len);
    err = file->Read(log_buf_.data() + header_len, payload_len);
    CHECK_KV_ERR(err);

    std::string_view content(log_buf_.data(),
                             static_cast<size_t>(header_len) + payload_len);
    if (!ManifestBuilder::ValidateChecksum(content))
    {
        LOG(ERROR) << "Manifest file corrupted, checksum mismatch.";
        LOG(ERROR) << "Corruption found at offset " << file_size_;
        if (!corrupted_log_found_)
        {
            file_size_before_corrupted_log_ = file_size_;
        }
        corrupted_log_found_ = true;
        // Advance file_size_ and skip padding to position at next record
        const size_t record_bytes = header_len + payload_len;
        file_size_ += record_bytes;
        const size_t alignment = page_align;
        const size_t remainder = record_bytes & (alignment - 1);
        if (remainder > 0)
        {
            const size_t padding = alignment - remainder;
            (void) file->SkipPadding(padding);
            file_size_ += padding;
        }
        return KvError::Corrupted;
    }
    content = content.substr(checksum_bytes);

    const size_t record_bytes = header_len + payload_len;
    const size_t alignment = page_align;
    const size_t remainder = record_bytes & (alignment - 1);
    if (remainder > 0)
    {
        const size_t padding = alignment - remainder;
        err = file->SkipPadding(padding);
        if (err != KvError::NoError)
        {
            // The last record is truncated (padding missing). Discard it so
            // the caller sees the state up to the previous record.
            LOG(WARNING) << "Manifest is truncated. Ignore the missed padding";
            file_size_ += record_bytes + padding;
            return KvError::EndOfFile;
        }
        file_size_ += record_bytes + padding;
    }
    else
    {
        file_size_ += record_bytes;
    }

    root_ = DecodeFixed32(content.data());
    content = content.substr(sizeof(PageId));
    ttl_root_ = DecodeFixed32(content.data());
    content = content.substr(sizeof(PageId));
    payload_ = content.substr(sizeof(uint32_t), payload_len);

    return KvError::NoError;
}

void Replayer::DeserializeSnapshot(std::string_view snapshot)
{
    [[maybe_unused]] bool ok = GetVarint64(&snapshot, &max_fp_id_);
    assert(ok);

    uint32_t dict_len = 0;
    ok = GetVarint32(&snapshot, &dict_len);
    assert(ok);
    if (dict_len > 0)
    {
        assert(snapshot.size() >= dict_len);
        dict_bytes_.assign(snapshot.data(), snapshot.data() + dict_len);
        snapshot = snapshot.substr(dict_len);
    }
    else
    {
        dict_bytes_.clear();
    }

    // Read page_mapping_len (Fixed32, 4 bytes) - it's before mapping_tbl
    CHECK(snapshot.size() >= 4)
        << "DeserializeSnapshot failed, insufficient data for "
           "page_mapping_len, expect >= 4, got "
        << snapshot.size();
    const uint32_t mapping_len = DecodeFixed32(snapshot.data());

    CHECK(mapping_len < snapshot.size() - 4)
        << "DeserializeSnapshot failed, page_mapping_len " << mapping_len
        << " exceeds available data " << snapshot.size() - 4;
    std::string_view mapping_view = snapshot.substr(4, mapping_len);

    mapping_tbl_.reserve(opts_->init_page_count);
    while (!mapping_view.empty())
    {
        uint64_t value;
        ok = GetVarint64(&mapping_view, &value);
        assert(ok);
        mapping_tbl_.PushBack(value);
    }

    // Deserialize BranchManifestMetadata section (Fixed32 length prefix).
    std::string_view remaining = snapshot.substr(4 + mapping_len);
    if (remaining.size() >= 4)
    {
        uint32_t branch_meta_len = DecodeFixed32(remaining.data());
        remaining = remaining.substr(4);
        if (branch_meta_len > 0 && remaining.size() >= branch_meta_len)
        {
            std::string_view branch_view = remaining.substr(0, branch_meta_len);
            if (!DeserializeBranchManifestMetadata(branch_view,
                                                   branch_metadata_))
            {
                LOG(FATAL) << "Failed to deserialize BranchManifestMetadata "
                              "from snapshot.";
            }
            remaining = remaining.substr(branch_meta_len);
        }
    }

    // Deserialize segment mapping section (if present).
    // Format: max_segment_fp_id(varint64) | seg_mapping_bytes_len(Fixed32) |
    //         seg_mapping_tbl(varint64...)
    if (remaining.size() >= 4)
    {
        [[maybe_unused]] bool seg_ok =
            GetVarint64(&remaining, &max_segment_fp_id_);
        assert(seg_ok);
        CHECK(remaining.size() >= 4)
            << "DeserializeSnapshot failed, insufficient data for "
               "segment_mapping_len, expect >= 4, got "
            << remaining.size();
        uint32_t seg_mapping_len = DecodeFixed32(remaining.data());
        std::string_view seg_view = remaining.substr(4, seg_mapping_len);
        segment_mapping_tbl_.clear();
        while (!seg_view.empty())
        {
            uint64_t value;
            seg_ok = GetVarint64(&seg_view, &value);
            assert(seg_ok);
            segment_mapping_tbl_.PushBack(value);
        }
    }
}

void Replayer::ReplayLog()
{
    assert(payload_.size() > 4);
    uint32_t mapping_len = DecodeFixed32(payload_.data());
    std::string_view mapping_view = payload_.substr(4, mapping_len);

    while (!mapping_view.empty())
    {
        PageId page_id;
        [[maybe_unused]] bool ok = GetVarint32(&mapping_view, &page_id);
        assert(ok);
        while (page_id >= mapping_tbl_.size())
        {
            mapping_tbl_.PushBack(MappingSnapshot::InvalidValue);
        }
        uint64_t value;
        ok = GetVarint64(&mapping_view, &value);
        assert(ok);
        mapping_tbl_.Set(page_id, value);
        if (MappingSnapshot::IsFilePageId(value))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(value);
            max_fp_id_ = std::max(max_fp_id_, fp_id + 1);
        }
    }

    // Deserialize BranchManifestMetadata section (Fixed32 length prefix).
    std::string_view remaining = payload_.substr(4 + mapping_len);
    if (remaining.size() >= 4)
    {
        uint32_t branch_meta_len = DecodeFixed32(remaining.data());
        remaining = remaining.substr(4);
        if (branch_meta_len > 0 && remaining.size() >= branch_meta_len)
        {
            std::string_view branch_view = remaining.substr(0, branch_meta_len);
            if (!DeserializeBranchManifestMetadata(branch_view,
                                                   branch_metadata_))
            {
                LOG(FATAL)
                    << "Failed to deserialize BranchManifestMetadata from log.";
            }
            remaining = remaining.substr(branch_meta_len);
        }
    }

    // Deserialize segment mapping deltas (if present).
    if (remaining.size() >= 4)
    {
        uint32_t seg_delta_len = DecodeFixed32(remaining.data());
        std::string_view seg_view = remaining.substr(4, seg_delta_len);
        while (!seg_view.empty())
        {
            PageId seg_page_id;
            [[maybe_unused]] bool seg_ok = GetVarint32(&seg_view, &seg_page_id);
            assert(seg_ok);
            while (seg_page_id >= segment_mapping_tbl_.size())
            {
                segment_mapping_tbl_.PushBack(MappingSnapshot::InvalidValue);
            }
            uint64_t value;
            seg_ok = GetVarint64(&seg_view, &value);
            assert(seg_ok);
            segment_mapping_tbl_.Set(seg_page_id, value);
            if (MappingSnapshot::IsFilePageId(value))
            {
                FilePageId fp_id = MappingSnapshot::DecodeId(value);
                max_segment_fp_id_ = std::max(max_segment_fp_id_, fp_id + 1);
            }
        }
    }
}

std::unique_ptr<PageMapper> Replayer::GetMapper(IndexPageManager *idx_mgr,
                                                const TableIdent *tbl_ident,
                                                uint64_t expect_term)
{
    auto mapping = MappingSnapshot::Ref(new MappingSnapshot(
        idx_mgr,
        tbl_ident,
        MappingSnapshot::MappingTbl(std::move(mapping_tbl_))));
    auto mapper = std::make_unique<PageMapper>(std::move(mapping));
    auto &m_table = mapper->GetMapping()->mapping_tbl_;

    std::vector<FilePageId> using_fp_ids;
    std::unordered_set<FilePageId> using_fp_ids_set;
    const size_t table_size = m_table.size();
    if (opts_->data_append_mode)
    {
        using_fp_ids.reserve(table_size);
    }
    else
    {
        using_fp_ids_set.reserve(table_size);
    }

    for (PageId page_id = 0; page_id < table_size; page_id++)
    {
        // Get all free page ids.
        uint64_t val = m_table.Get(page_id);
        if (!MappingSnapshot::IsFilePageId(val))
        {
            mapper->FreePage(page_id);
            continue;
        }

        // For constructing file page id allocator.
        FilePageId fp_id = MappingSnapshot::DecodeId(val);
        if (opts_->data_append_mode)
        {
            using_fp_ids.emplace_back(fp_id);
        }
        else
        {
            using_fp_ids_set.insert(fp_id);
        }
    }

    if (opts_->data_append_mode)
    {
        // In cloud mode, when manifest term differs from process term, bump
        // the allocator to the next file boundary to avoid cross-term
        // collisions.
        uint64_t manifest_term = branch_metadata_.term;
        if (DeriveStoreMode(*opts_) != StoreMode::Local && expect_term != 0 &&
            manifest_term != expect_term)
        {
            FileId next_file_id =
                (max_fp_id_ >> opts_->pages_per_file_shift) + 1;
            max_fp_id_ = next_file_id << opts_->pages_per_file_shift;
        }

        if (using_fp_ids.empty())
        {
            FileId min_file_id = max_fp_id_ >> opts_->pages_per_file_shift;
            mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
                opts_, min_file_id, max_fp_id_, 0);
        }
        else
        {
            std::sort(using_fp_ids.begin(), using_fp_ids.end());
            FileId min_file_id =
                using_fp_ids.front() >> opts_->pages_per_file_shift;
            uint32_t empty_file_cnt = 0;
            for (FileId cur_file_id = min_file_id;
                 FilePageId fp_id : using_fp_ids)
            {
                FileId file_id = fp_id >> opts_->pages_per_file_shift;
                assert(file_id >= cur_file_id);
                if (file_id > cur_file_id + 1)
                {
                    empty_file_cnt += file_id - cur_file_id - 1;
                }
                cur_file_id = file_id;
            }

            assert(using_fp_ids.back() < max_fp_id_);
            mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
                opts_, min_file_id, max_fp_id_, empty_file_cnt);
        }
    }
    else
    {
        // In non-append mode, only give back as free the pages that belong to
        // the CURRENT branch (branch_metadata_.branch_name).  Pages that live
        // in a parent-branch file (tracked in branch_metadata_.file_ranges with
        // a different branch_name) must never be recycled by this branch;
        // writing to them would silently corrupt the parent's live data.
        //
        // When file_ranges is empty (legacy manifests or the very first main
        // manifest) there is no parent-file information, so we fall back to
        // the original behaviour and allow all unused pages.
        const BranchFileMapping &ranges = branch_metadata_.file_ranges;
        const std::string &active_branch = branch_metadata_.branch_name;
        std::vector<uint32_t> free_ids;
        free_ids.reserve(mapper->free_page_cnt_);
        for (FilePageId i = 0; i < max_fp_id_; i++)
        {
            if (using_fp_ids_set.contains(i))
            {
                continue;
            }
            // Skip pages belonging to a different branch's file range.
            if (!ranges.empty())
            {
                FileId fid = i >> opts_->pages_per_file_shift;
                if (!FileIdInBranch(ranges, DataFileKey(fid), active_branch))
                {
                    continue;
                }
            }
            free_ids.push_back(i);
        }
        mapper->file_page_allocator_ = std::make_unique<PooledFilePages>(
            opts_, max_fp_id_, std::move(free_ids));
    }

    return mapper;
}
std::unique_ptr<PageMapper> Replayer::GetSegmentMapper(
    IndexPageManager *idx_mgr,
    const TableIdent *tbl_ident,
    uint64_t expect_term)
{
    if (segment_mapping_tbl_.size() == 0 && max_segment_fp_id_ == 0)
    {
        return nullptr;
    }

    auto mapping = MappingSnapshot::Ref(new MappingSnapshot(
        idx_mgr,
        tbl_ident,
        MappingSnapshot::MappingTbl(std::move(segment_mapping_tbl_))));
    auto mapper = std::make_unique<PageMapper>(std::move(mapping));
    auto &m_table = mapper->GetMapping()->mapping_tbl_;

    std::vector<FilePageId> using_fp_ids;
    using_fp_ids.reserve(m_table.size());

    for (PageId page_id = 0; page_id < m_table.size(); page_id++)
    {
        uint64_t val = m_table.Get(page_id);
        if (!MappingSnapshot::IsFilePageId(val))
        {
            mapper->FreePage(page_id);
            continue;
        }
        FilePageId fp_id = MappingSnapshot::DecodeId(val);
        using_fp_ids.emplace_back(fp_id);
    }

    // Segment files are always append-only.
    const uint8_t shift = opts_->segments_per_file_shift;

    // In cloud mode, bump allocator to next file boundary when the manifest
    // term differs from the current process term, same as GetMapper().
    const bool cloud_mode = !opts_->cloud_store_path.empty();
    if (cloud_mode && expect_term != 0)
    {
        uint64_t manifest_term = branch_metadata_.term;
        if (manifest_term != expect_term)
        {
            FileId next_file_id = (max_segment_fp_id_ >> shift) + 1;
            max_segment_fp_id_ = next_file_id << shift;
        }
    }
    if (using_fp_ids.empty())
    {
        FileId min_file_id = max_segment_fp_id_ >> shift;
        mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
            shift, min_file_id, max_segment_fp_id_, 0);
    }
    else
    {
        std::sort(using_fp_ids.begin(), using_fp_ids.end());
        FileId min_file_id = using_fp_ids.front() >> shift;
        uint32_t empty_file_cnt = 0;
        for (FileId cur_file_id = min_file_id; FilePageId fp_id : using_fp_ids)
        {
            FileId file_id = fp_id >> shift;
            assert(file_id >= cur_file_id);
            if (file_id > cur_file_id + 1)
            {
                empty_file_cnt += file_id - cur_file_id - 1;
            }
            cur_file_id = file_id;
        }
        assert(using_fp_ids.back() < max_segment_fp_id_);
        mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
            shift, min_file_id, max_segment_fp_id_, empty_file_cnt);
    }

    return mapper;
}

}  // namespace eloqstore
