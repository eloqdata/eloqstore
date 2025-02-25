#include "replayer.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "async_io_manager.h"
#include "coding.h"
#include "crc32.h"
#include "error.h"
#include "kv_options.h"
#include "root_meta.h"

namespace kvstore
{

Replayer::Replayer()
{
    log_buf_.resize(ManifestBuilder::header_bytes);
}

KvError Replayer::Replay(ManifestFilePtr log, const KvOptions *opts)
{
    root_ = UINT32_MAX;
    mapper_ = std::make_unique<PageMapper>();
    file_size_ = 0;

    KvError err = NextRecord(log.get());
    CHECK_KV_ERR(err);
    assert(!mapping_.empty());
    mapper_->Deserialize(mapping_);

    while (true)
    {
        err = NextRecord(log.get());
        if (err != KvError::NoError)
        {
            if (err == KvError::EndOfFile)
            {
                break;
            }
            return err;
        }

        err = ReplayMapping(mapping_);
        CHECK_KV_ERR(err);
    }

    return KvError::NoError;
}

KvError Replayer::NextRecord(ManifestFile *log)
{
    size_t nread = log->Read(log_buf_.data(), ManifestBuilder::header_bytes);
    if (nread < ManifestBuilder::header_bytes)
    {
        return nread < 0 ? KvError::IoFail : KvError::EndOfFile;
    }

    root_ = DecodeFixed32(log_buf_.data() + ManifestBuilder::offset_root);

    const uint32_t len =
        DecodeFixed32(log_buf_.data() + ManifestBuilder::offset_len);
    log_buf_.resize(ManifestBuilder::header_bytes + len);
    nread = log->Read(log_buf_.data() + ManifestBuilder::header_bytes, len);
    if (nread < len)
    {
        return nread < 0 ? KvError::IoFail : KvError::EndOfFile;
    }
    mapping_ = {log_buf_.data() + ManifestBuilder::header_bytes,
                log_buf_.size() - ManifestBuilder::header_bytes};

    uint32_t crc_stored = DecodeFixed32(log_buf_.data());
    crc_stored = crc32::Unmask(crc_stored);
    uint32_t crc = crc32::Value(log_buf_.data() + ManifestBuilder::crc_bytes,
                                log_buf_.size() - ManifestBuilder::crc_bytes);
    if (crc != crc_stored)
    {
        return KvError::Corrupted;
    }

    file_size_ += (ManifestBuilder::header_bytes + len);
    return KvError::NoError;
}

KvError Replayer::ReplayMapping(std::string_view bat)
{
    while (!bat.empty())
    {
        uint32_t page_id, file_page;
        GetVarint32(&bat, &page_id);
        GetVarint32(&bat, &file_page);
        if (file_page == UINT32_MAX)
        {
            // Delete mapping
            mapper_->FreeFilePage(mapper_->GetMapping()->ToFilePage(page_id));
            mapper_->FreePage(page_id);
        }
        else
        {
            // Insert/Update mapping
            std::vector<uint64_t> &mapping = mapper_->Mapping();
            assert(page_id <= mapping.size());
            if (page_id == mapping.size())
            {
                // Insert new mapping. Allocate a new logical page id
                mapping.emplace_back(UINT32_MAX);
            }
            else if (mapper_->DequeFreePage(page_id))
            {
                // Insert new mapping. Reuse freed logical page id
            }
            else
            {
                // Update existing mapping. Free old physical page id
                uint32_t old_fp = mapper_->GetMapping()->ToFilePage(page_id);
                mapper_->FreeFilePage(old_fp);
            }
            // Allocate new physical page id
            if (!mapper_->DelFreeFilePage(file_page))
            {
                uint32_t fp = mapper_->ExpandFilePage();
                assert(fp == file_page);
            }
            mapper_->UpdateMapping(page_id, file_page);
        }
    }
    return KvError::NoError;
}

std::unique_ptr<PageMapper> Replayer::Mapper(IndexPageManager *idx_mgr,
                                             const TableIdent *tbl_ident)
{
    mapper_->GetMapping()->idx_mgr_ = idx_mgr;
    mapper_->GetMapping()->tbl_ident_ = tbl_ident;
    return std::move(mapper_);
}
}  // namespace kvstore