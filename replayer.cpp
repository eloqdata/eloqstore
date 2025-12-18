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
#include "index_page_manager.h"
#include "kv_options.h"
#include "root_meta.h"

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
    // Read header
    KvError err = file->Read(log_buf_.data(), ManifestBuilder::header_bytes);
    CHECK_KV_ERR(err);

    // Read payload
    const uint32_t len =
        DecodeFixed32(log_buf_.data() + ManifestBuilder::offset_len);
    log_buf_.resize(ManifestBuilder::header_bytes + len);
    err = file->Read(log_buf_.data() + ManifestBuilder::header_bytes, len);
    CHECK_KV_ERR(err);

    std::string_view content = log_buf_;
    // Verify checksum
    if (!ValidateChecksum(content))
    {
        LOG(ERROR) << "Manifest file corrupted, checksum mismatch.";
        return KvError::Corrupted;
    }
    content = content.substr(checksum_bytes);

    root_ = DecodeFixed32(content.data());
    content = content.substr(sizeof(PageId));
    ttl_root_ = DecodeFixed32(content.data());
    content = content.substr(sizeof(PageId));
    payload_ = content.substr(sizeof(uint32_t));  // Skip payload length
    file_size_ += ManifestBuilder::header_bytes + len;
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

    mapping_tbl_.reserve(opts_->init_page_count);
    while (!snapshot.empty())
    {
        uint64_t value;
        ok = GetVarint64(&snapshot, &value);
        assert(ok);
        mapping_tbl_.push_back(value);
    }
}

void Replayer::ReplayLog()
{
    while (!payload_.empty())
    {
        PageId page_id;
        [[maybe_unused]] bool ok = GetVarint32(&payload_, &page_id);
        assert(ok);
        while (page_id >= mapping_tbl_.size())
        {
            mapping_tbl_.emplace_back(MappingSnapshot::InvalidValue);
        }
        uint64_t value;
        ok = GetVarint64(&payload_, &value);
        assert(ok);
        mapping_tbl_[page_id] = value;
        if (MappingSnapshot::IsFilePageId(value))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(value);
            max_fp_id_ = std::max(max_fp_id_, fp_id + 1);
        }
    }
}

std::unique_ptr<PageMapper> Replayer::GetMapper(IndexPageManager *idx_mgr,
                                                const TableIdent *tbl_ident)
{
    auto mapping = std::make_shared<MappingSnapshot>(
        idx_mgr, tbl_ident, std::move(mapping_tbl_), idx_mgr->MapperArena());
    auto mapper = std::make_unique<PageMapper>(std::move(mapping));
    auto &m_table = mapper->GetMapping()->mapping_tbl_;

    std::vector<FilePageId> using_fp_ids;
    std::unordered_set<FilePageId> using_fp_ids_set;
    if (opts_->data_append_mode)
    {
        using_fp_ids.reserve(m_table.size());
    }
    else
    {
        using_fp_ids_set.reserve(m_table.size());
    }

    for (PageId page_id = 0; page_id < m_table.size(); page_id++)
    {
        // Get all free page ids.
        uint64_t val = m_table[page_id];
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
