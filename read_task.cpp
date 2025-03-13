#include "read_task.h"

#include "error.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"

namespace kvstore
{

KvError ReadTask::Read(const TableIdent &tbl_ident,
                       std::string_view search_key,
                       std::string &value,
                       uint64_t &timestamp)
{
    auto [meta, err] = index_mgr->FindRoot(tbl_ident);
    CHECK_KV_ERR(err);
    if (meta->root_page_ == nullptr)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();

    uint32_t page_id;
    err = index_mgr->SeekIndex(
        mapping.get(), tbl_ident, meta->root_page_, search_key, page_id);
    CHECK_KV_ERR(err);
    uint32_t file_page = mapping->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_ident, page_id, file_page);
    CHECK_KV_ERR(err_load);

    DataPageIter data_iter{&page, Options()};
    data_iter.Seek(search_key);
    std::string_view seek_key = data_iter.Key();
    if (!seek_key.empty() && seek_key == search_key)
    {
        value = data_iter.Value();
        timestamp = data_iter.Timestamp();
        return KvError::NoError;
    }
    else
    {
        return KvError::NotFound;
    }
}

}  // namespace kvstore