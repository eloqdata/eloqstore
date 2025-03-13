#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>

#include "error.h"
#include "index_page_manager.h"
#include "page_mapper.h"

namespace kvstore
{
ScanTask::ScanTask() : iter_(nullptr, Options())
{
}

KvError ScanTask::Scan(const TableIdent &tbl_id,
                       std::string_view begin_key,
                       std::string_view end_key,
                       std::vector<KvEntry> &entries)
{
    entries.clear();

    auto [meta, err] = index_mgr->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (meta->root_page_ == nullptr)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();

    uint32_t page_id;
    err = index_mgr->SeekIndex(
        mapping.get(), tbl_id, meta->root_page_, begin_key, page_id);
    CHECK_KV_ERR(err);
    uint32_t file_page = mapping->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id, page_id, file_page);
    CHECK_KV_ERR(err_load);
    data_page_ = std::move(page);

    iter_.Reset(&data_page_, Options()->data_page_size);
    iter_.Seek(begin_key);
    if (iter_.Key().empty() && (err = Next(mapping.get())) != KvError::NoError)
    {
        return err == KvError::EndOfFile ? KvError::NoError : err;
    }
    while (end_key.empty() || iter_.Key() < end_key)
    {
        entries.emplace_back(iter_.Key(), iter_.Value(), iter_.Timestamp());
        if ((err = Next(mapping.get())) != KvError::NoError)
        {
            break;
        }
    }
    return err == KvError::EndOfFile ? KvError::NoError : err;
}

KvError ScanTask::Next(MappingSnapshot *m)
{
    if (!iter_.HasNext())
    {
        uint32_t page_id = data_page_.NextPageId();
        data_page_.Clear();
        if (page_id == UINT32_MAX)
        {
            // EndOfFile will just break the scan process
            return KvError::EndOfFile;
        }
        uint32_t file_page = m->ToFilePage(page_id);
        auto [page, err] = LoadDataPage(*m->tbl_ident_, page_id, file_page);
        CHECK_KV_ERR(err);
        data_page_ = std::move(page);

        iter_.Reset(&data_page_, Options()->data_page_size);
    }
    bool ok = iter_.Next();
    assert(ok && !iter_.Key().empty());
    return KvError::NoError;
}

}  // namespace kvstore