#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "error.h"
#include "page_mapper.h"
#include "shard.h"

namespace eloqstore
{
ScanIterator::ScanIterator(const TableIdent &tbl_id)
    : tbl_id_(tbl_id), iter_(nullptr, Options())
{
}

KvError ScanIterator::Seek(std::string_view key, bool ttl)
{
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id_);
    CHECK_KV_ERR(err);
    PageId root_id = ttl ? meta->ttl_root_id_ : meta->root_id_;
    if (root_id == MaxPageId)
    {
        return KvError::EndOfFile;
    }
    compression_ = meta->compression_.get();
    mapping_ = meta->mapper_->GetMappingSnapshot();

    PageId page_id;
    err =
        shard->IndexManager()->SeekIndex(mapping_.get(), root_id, key, page_id);
    CHECK_KV_ERR(err);
    assert(page_id != MaxPageId);
    FilePageId file_page = mapping_->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id_, page_id, file_page);
    CHECK_KV_ERR(err_load);

    data_page_ = std::move(page);
    iter_.Reset(&data_page_, Options()->data_page_size);

    if (!iter_.Seek(key))
    {
        err = Next();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError ScanIterator::Next()
{
    if (!iter_.HasNext())
    {
        PageId page_id = data_page_.NextPageId();
        if (page_id == MaxPageId)
        {
            return KvError::EndOfFile;
        }
        FilePageId file_page = mapping_->ToFilePage(page_id);
        assert(file_page != MaxFilePageId);
        auto [page, err] = LoadDataPage(tbl_id_, page_id, file_page);
        CHECK_KV_ERR(err);

        data_page_ = std::move(page);
        iter_.Reset(&data_page_, Options()->data_page_size);
        assert(iter_.HasNext());
    }
    iter_.Next();
    return KvError::NoError;
}

std::string_view ScanIterator::Key() const
{
    return iter_.Key();
}

std::pair<std::string_view, KvError> ScanIterator::ResolveValue(
    std::string &storage)
{
    return eloqstore::ResolveValue(
        tbl_id_, mapping_.get(), iter_, storage, compression_);
}

uint64_t ScanIterator::ExpireTs() const
{
    return iter_.ExpireTs();
}

uint64_t ScanIterator::Timestamp() const
{
    return iter_.Timestamp();
}

bool ScanIterator::HasNext() const
{
    return iter_.HasNext() || data_page_.NextPageId() != MaxPageId;
}

MappingSnapshot *ScanIterator::Mapping() const
{
    return mapping_.get();
}

KvError ScanTask::Scan()
{
    const TableIdent &tbl_id = req_->TableId();
    auto req = static_cast<ScanRequest *>(req_);
    assert(req->page_entries_ > 0 && req->page_size_ > 0);
    req->num_entries_ = 0;
    req->has_remaining_ = false;
    size_t result_size = 0;

    ScanIterator iter(tbl_id);
    KvError err = iter.Seek(req->BeginKey());
    if (err != KvError::NoError)
    {
        return err == KvError::EndOfFile ? KvError::NoError : err;
    }

    if (!req->begin_inclusive_ &&
        Comp()->Compare(iter.Key(), req->BeginKey()) == 0)
    {
        err = iter.Next();
        if (err != KvError::NoError)
        {
            return err == KvError::EndOfFile ? KvError::NoError : err;
        }
    }

    std::string value_storage;
    while (req->EndKey().empty() ||
           Comp()->Compare(iter.Key(), req->EndKey()) < 0)
    {
        // Check entries number limit.
        if (req->num_entries_ == req->page_entries_)
        {
            req->has_remaining_ = true;
            break;
        }

        // Fetch value
        auto [value, fetch_err] = iter.ResolveValue(value_storage);
        err = fetch_err;
        assert(err != KvError::EndOfFile);
        CHECK_KV_ERR(err);

        // Check result size limit.
        const size_t entry_size = iter.Key().size() + value.size() +
                                  sizeof(iter.Timestamp()) +
                                  sizeof(iter.ExpireTs());
        if (result_size > 0 && result_size + entry_size > req->page_size_)
        {
            req->has_remaining_ = true;
            break;
        }
        result_size += entry_size;

        KvEntry &entry = req->num_entries_ < req->entries_.size()
                             ? req->entries_[req->num_entries_]
                             : req->entries_.emplace_back();
        req->num_entries_++;
        entry.key_.assign(iter.Key());
        entry.value_ = value_storage.empty() ? value : std::move(value_storage);
        entry.timestamp_ = iter.Timestamp();
        entry.expire_ts_ = iter.ExpireTs();

        err = iter.Next();
        if (err != KvError::NoError)
        {
            return err == KvError::EndOfFile ? KvError::NoError : err;
        }
    }
    return KvError::NoError;
}
}  // namespace eloqstore
