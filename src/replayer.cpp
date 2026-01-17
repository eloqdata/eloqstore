#include "replayer.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "async_io_manager.h"
#include "coding.h"
#include "error.h"
#include "kv_options.h"
#include "storage/index_page_manager.h"
#include "storage/page.h"
#include "storage/root_meta.h"

namespace eloqstore
{

Replayer::Replayer(const KvOptions *opts)
    : opts_(opts), file_id_term_mapping_(std::make_shared<FileIdTermMapping>())
{
    log_buf_.resize(ManifestBuilder::header_bytes);
}

KvError Replayer::Replay(ManifestFile *file)
{
    root_ = MaxPageId;
    ttl_root_ = MaxPageId;
    mapping_tbl_.resize(0);
    mapping_tbl_.reserve(opts_->init_page_count);
    file_size_ = 0;
    max_fp_id_ = MaxFilePageId;
    dict_bytes_.clear();

    KvError err = ParseNextRecord(file);
    CHECK_KV_ERR(err);
    assert(!payload_.empty());
    DeserializeSnapshot(payload_);

    while (true)
    {
        err = ParseNextRecord(file);
        if (err != KvError::NoError)
        {
            if (err == KvError::EndOfFile)
            {
                break;
            }
            return err;
        }
        ReplayLog();
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
        return KvError::Corrupted;
    }
    content = content.substr(checksum_bytes);

    root_ = DecodeFixed32(content.data());
    content = content.substr(sizeof(PageId));
    ttl_root_ = DecodeFixed32(content.data());
    content = content.substr(sizeof(PageId));
    payload_ = content.substr(sizeof(uint32_t), payload_len);
    const size_t record_bytes = header_len + payload_len;
    file_size_ += record_bytes;
    const size_t alignment = page_align;
    const size_t remainder = record_bytes & (alignment - 1);
    if (remainder > 0)
    {
        const size_t padding = alignment - remainder;
        err = file->SkipPadding(padding);
        CHECK(err == KvError::NoError) << "Manifest is corrupted";
        file_size_ += padding;
    }

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

    // Read mapping_len (Fixed32, 4 bytes) - it's before mapping_tbl
    CHECK(snapshot.size() >= 4)
        << "DeserializeSnapshot failed, insufficient data for mapping_len, "
           "expect >= 4, got "
        << snapshot.size();
    const uint32_t mapping_len = DecodeFixed32(snapshot.data());

    CHECK(mapping_len < snapshot.size() - 4)
        << "DeserializeSnapshot failed, mapping_len " << mapping_len
        << " exceeds available data " << snapshot.size() - 4;
    std::string_view mapping_view = snapshot.substr(4, mapping_len);

    mapping_tbl_.reserve(opts_->init_page_count);
    while (!mapping_view.empty())
    {
        uint64_t value;
        ok = GetVarint64(&mapping_view, &value);
        assert(ok);
        mapping_tbl_.push_back(value);
    }

    // Deserialize FileIdTermMapping section
    std::string_view file_term_mapping_view = snapshot.substr(4 + mapping_len);
    CHECK(file_term_mapping_view.size() >= 4)
        << "DeserializeSnapshot failed, insufficient data for "
           "file_term_mapping, expect >= 4, got "
        << file_term_mapping_view.size();
    if (!DeserializeFileIdTermMapping(file_term_mapping_view,
                                      *file_id_term_mapping_))
    {
        LOG(FATAL) << "Failed to deserialize FileIdTermMapping from snapshot.";
    }
}

void Replayer::ReplayLog()
{
    assert(payload_.size() > 4);
    uint32_t mapping_len = DecodeFixed32(payload_.data());
    std::string_view mapping_view = payload_.substr(4, mapping_len);
    std::string_view file_term_mapping_view = payload_.substr(4 + mapping_len);

    while (!mapping_view.empty())
    {
        PageId page_id;
        [[maybe_unused]] bool ok = GetVarint32(&mapping_view, &page_id);
        assert(ok);
        while (page_id >= mapping_tbl_.size())
        {
            mapping_tbl_.emplace_back(MappingSnapshot::InvalidValue);
        }
        uint64_t value;
        ok = GetVarint64(&mapping_view, &value);
        assert(ok);
        mapping_tbl_[page_id] = value;
        if (MappingSnapshot::IsFilePageId(value))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(value);
            max_fp_id_ = std::max(max_fp_id_, fp_id + 1);
        }
    }

    // Deserialize FileIdTermMapping section
    if (!DeserializeFileIdTermMapping(file_term_mapping_view,
                                      *file_id_term_mapping_))
    {
        LOG(FATAL) << "Failed to deserialize FileIdTermMapping from snapshot.";
    }
}

std::unique_ptr<PageMapper> Replayer::GetMapper(IndexPageManager *idx_mgr,
                                                const TableIdent *tbl_ident,
                                                uint64_t expect_term)
{
    auto mapping = std::make_shared<MappingSnapshot>(
        idx_mgr,
        tbl_ident,
        MappingSnapshot::MappingTbl(std::move(mapping_tbl_)));
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
        uint64_t manifest_term = 0;
        auto it = file_id_term_mapping_->find(IouringMgr::LruFD::kManifest);
        if (it != file_id_term_mapping_->end())
        {
            manifest_term = it->second;
        }
        const bool cloud_mode = !opts_->cloud_store_path.empty();
        if (cloud_mode && manifest_term != expect_term)
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
            uint32_t hole_cnt = 0;
            for (FileId cur_file_id = min_file_id;
                 FilePageId fp_id : using_fp_ids)
            {
                FileId file_id = fp_id >> opts_->pages_per_file_shift;
                assert(file_id >= cur_file_id);
                if (file_id > cur_file_id + 1)
                {
                    hole_cnt += file_id - cur_file_id - 1;
                }
                cur_file_id = file_id;
            }

            assert(using_fp_ids.back() < max_fp_id_);
            mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
                opts_, min_file_id, max_fp_id_, hole_cnt);
        }
    }
    else
    {
        std::vector<uint32_t> free_ids;
        free_ids.reserve(mapper->free_page_cnt_);
        for (FilePageId i = 0; i < max_fp_id_; i++)
        {
            if (!using_fp_ids_set.contains(i))
            {
                free_ids.push_back(i);
            }
        }
        mapper->file_page_allocator_ = std::make_unique<PooledFilePages>(
            opts_, max_fp_id_, std::move(free_ids));
    }

    return mapper;
}
}  // namespace eloqstore
