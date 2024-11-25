#include "read_task.h"

#include "global_variables.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "storage_manager.h"

namespace kvstore
{
ReadTask::ReadTask(IndexPageManager *idx_manager)
    : idx_page_manager_(idx_manager)
{
}

void ReadTask::Yield()
{
}

void ReadTask::Resume()
{
}

void ReadTask::Rollback()
{
}

void ReadTask::Reset(IndexPageManager *idx_page_manager)
{
}

KvError ReadTask::Read(const TableIdent &tbl_ident,
                       std::string_view search_key,
                       std::string_view &value,
                       uint64_t &timestamp)
{
    auto [root, mapper] = idx_page_manager_->FindRoot(tbl_ident);
    if (root == nullptr)
    {
        return KvError::NotFound;
    }
    page_mapping_ = mapper->GetMappingSnapshot();

    uint32_t data_page_id = SeekIndex(tbl_ident, root, search_key);
    uint32_t file_page = page_mapping_->ToFilePage(data_page_id);
    storage_manager->Read(
        data_page_.PagePtr(), kv_options.data_page_size, tbl_ident, file_page);
    data_page_.SetPageId(data_page_id);

    if (kv_error_ != KvError::NoError)
    {
        return kv_error_;
    }

    page_mapping_ = nullptr;

    DataPageIter data_iter{&data_page_, idx_page_manager_->GetComparator()};
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

uint32_t ReadTask::SeekIndex(const TableIdent &tbl_ident,
                             MemIndexPage *node,
                             std::string_view key)
{
    return ::kvstore::SeekIndex(
        idx_page_manager_, page_mapping_.get(), tbl_ident, node, key);
}

uint32_t SeekIndex(IndexPageManager *idx_page_mgr,
                   MappingSnapshot *mapping,
                   const TableIdent &tbl_ident,
                   MemIndexPage *node,
                   std::string_view key)
{
    IndexPageIter idx_it{node, idx_page_mgr->GetComparator()};
    idx_it.Seek(key);
    uint32_t page_id = idx_it.PageId();
    if (node->IsPointingToLeaf() || page_id == UINT32_MAX)
    {
        // Updates the cache replacement list.
        idx_page_mgr->EnqueuIndexPage(node);
        return page_id;
    }
    else
    {
        MemIndexPage *child = idx_page_mgr->FindPage(mapping, page_id);
        return SeekIndex(idx_page_mgr, mapping, tbl_ident, child, key);
    }
}
}  // namespace kvstore