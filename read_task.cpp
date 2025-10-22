#include "read_task.h"

#include <string>
#include <utility>

#include "error.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "shard.h"

namespace eloqstore
{

KvError ReadTask::Read(const TableIdent &tbl_id,
                       std::string_view search_key,
                       std::string &value,
                       uint64_t &timestamp,
                       uint64_t &expire_ts)
{
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (meta->root_id_ == MaxPageId)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();

    PageId page_id;
    err = shard->IndexManager()->SeekIndex(
        mapping.get(), meta->root_id_, search_key, page_id);
    CHECK_KV_ERR(err);
    FilePageId file_page = mapping->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id, page_id, file_page);
    CHECK_KV_ERR(err_load);

    DataPageIter iter{&page, Options()};
    bool found = iter.Seek(search_key);
    if (!found || Comp()->Compare(iter.Key(), search_key) != 0)
    {
        return KvError::NotFound;
    }

    std::string value_storage;
    auto [val_view, fetch_err] = ResolveValue(
        tbl_id, mapping.get(), iter, value_storage, meta->compression_.get());
    CHECK_KV_ERR(fetch_err);
    value = value_storage.empty() ? val_view : std::move(value_storage);
    timestamp = iter.Timestamp();
    expire_ts = iter.ExpireTs();
    return KvError::NoError;
}

KvError ReadTask::Floor(const TableIdent &tbl_id,
                        std::string_view search_key,
                        std::string &floor_key,
                        std::string &value,
                        uint64_t &timestamp,
                        uint64_t &expire_ts)
{
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (meta->root_id_ == MaxPageId)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();

    PageId page_id;
    err = shard->IndexManager()->SeekIndex(
        mapping.get(), meta->root_id_, search_key, page_id);
    CHECK_KV_ERR(err);
    FilePageId file_page = mapping->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id, page_id, file_page);
    CHECK_KV_ERR(err_load);

    DataPageIter iter{&page, Options()};
    if (!iter.SeekFloor(search_key))
    {
        PageId page_id = page.PrevPageId();
        if (page_id == MaxPageId)
        {
            return KvError::NotFound;
        }
        FilePageId file_page = mapping->ToFilePage(page_id);
        auto [prev_page, err] = LoadDataPage(tbl_id, page_id, file_page);
        CHECK_KV_ERR(err);
        page = std::move(prev_page);
        iter.Reset(&page, Options()->data_page_size);
        bool found = iter.SeekFloor(search_key);
        CHECK(found);
    }
    floor_key = iter.Key();
    std::string value_storage;
    auto [val_view, fetch_err] = ResolveValue(
        tbl_id, mapping.get(), iter, value_storage, meta->compression_.get());
    CHECK_KV_ERR(fetch_err);
    value = value_storage.empty() ? val_view : std::move(value_storage);
    timestamp = iter.Timestamp();
    expire_ts = iter.ExpireTs();
    return KvError::NoError;
}
}  // namespace eloqstore
