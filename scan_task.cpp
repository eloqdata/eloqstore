#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>

#include "error.h"
#include "index_page_manager.h"
#include "page_mapper.h"
#include "read_task.h"
#include "storage_manager.h"

namespace kvstore
{
ScanTask::ScanTask(IndexPageManager *idx_manager)
    : page_mgr_(idx_manager), iter_(nullptr, idx_manager->GetComparator())
{
}

void ScanTask::Yield()
{
}

void ScanTask::Resume()
{
}

void ScanTask::Rollback()
{
}

KvError ScanTask::ScanVec(const TableIdent &tbl_ident,
                          std::string_view begin_key,
                          std::string_view end_key,
                          std::vector<Tuple> &tuples)
{
    tbl_ident_ = tbl_ident;
    auto [root, mapper] = page_mgr_->FindRoot(tbl_ident);
    if (root == nullptr)
    {
        return KvError::NotFound;
    }
    mapping_ = mapper->GetMappingSnapshot();

    uint32_t page_id =
        SeekIndex(page_mgr_, mapping_.get(), tbl_ident, root, begin_key);
    uint32_t file_page = mapping_->ToFilePage(page_id);
    storage_manager->Read(
        data_page_.PagePtr(), kv_options.data_page_size, tbl_ident, file_page);
    if (kv_error_ != KvError::NoError)
    {
        return kv_error_;
    }
    data_page_.SetPageId(page_id);

    iter_.Reset(&data_page_);
    iter_.Seek(begin_key);
    if (iter_.Key().empty())
    {
        if (Next() != KvError::NoError)
        {
            return kv_error_;
        }
    }
    while (end_key.empty() || iter_.Key() < end_key)
    {
        tuples.emplace_back(iter_.Key(), iter_.Value(), iter_.Timestamp());
        if (Next() != KvError::NoError)
        {
            return kv_error_;
        }
    }
    return kv_error_;
}

KvError ScanTask::NextPage()
{
    uint32_t page_id = data_page_.NextPageId();
    if (page_id == UINT32_MAX)
    {
        // EndOfFile will just break the scan process
        return KvError::EndOfFile;
    }
    uint32_t file_page = mapping_->ToFilePage(page_id);
    storage_manager->Read(
        data_page_.PagePtr(), kv_options.data_page_size, tbl_ident_, file_page);
    data_page_.SetPageId(page_id);
    if (kv_error_ != KvError::NoError)
    {
        return kv_error_;
    }
    iter_.Reset(&data_page_);
    return KvError::NoError;
}

KvError ScanTask::Next()
{
    while (!iter_.HasNext())
    {
        KvError err = NextPage();
        if (err != KvError::NoError)
        {
            return err;
        }
    }
    iter_.Next();
    assert(!iter_.Key().empty());
    return KvError::NoError;
}

KvError ScanTask::Scan(const TableIdent &tbl_ident,
                       std::string_view begin_key,
                       std::string_view end_key)
{
    end_key_ = end_key;
    tbl_ident_ = tbl_ident;
    auto [root, mapper] = page_mgr_->FindRoot(tbl_ident);
    if (root == nullptr)
    {
        return KvError::NotFound;
    }
    mapping_ = mapper->GetMappingSnapshot();

    uint32_t page_id =
        SeekIndex(page_mgr_, mapping_.get(), tbl_ident, root, begin_key);
    uint32_t file_page = mapping_->ToFilePage(page_id);
    storage_manager->Read(
        data_page_.PagePtr(), kv_options.data_page_size, tbl_ident, file_page);
    if (kv_error_ != KvError::NoError)
    {
        return kv_error_;
    }
    data_page_.SetPageId(page_id);

    iter_.Reset(&data_page_);
    iter_.Seek(begin_key);
    if (iter_.Key().empty())
    {
        Next();
    }
    return kv_error_;
}

bool ScanTask::Valid() const
{
    return !iter_.Key().empty() && (end_key_.empty() || iter_.Key() < end_key_);
}

std::string_view ScanTask::Key() const
{
    return iter_.Key();
}

std::string_view ScanTask::Value() const
{
    return iter_.Value();
}

uint64_t ScanTask::Timestamp() const
{
    return iter_.Timestamp();
}

}  // namespace kvstore