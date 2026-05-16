#include "tasks/read_task.h"

#include <string>
#include <utility>

#include "error.h"
#include "io_string_buffer.h"
#include "storage/mem_index_page.h"
#include "storage/page_mapper.h"
#include "storage/root_meta.h"
#include "storage/shard.h"

namespace eloqstore
{
namespace
{
// Common path shared by all three Read() overloads: descend through the index
// tree to the leaf data page that should contain @p search_key, load it, and
// position @p iter at the matching entry. Returns KvError::NotFound when the
// partition is empty or the key isn't in the page; any other non-NoError
// status is forwarded from the underlying I/O. On success, @p handler is
// invoked with the data-page iterator and surrounding context; its return
// value is propagated back to the caller.
//
// @p page outlives @p iter, and both live in the caller's stack frame because
// DataPageIter holds a non-owning DataPage* and cannot be moved after
// construction. The handler is invoked while @p mapping_ref /
// @p seg_mapping_ref are still alive, so it may pass the raw snapshot
// pointers to async helpers like GetLargeValue (which may yield during
// segment allocation).
template <typename Handler>
KvError LocateAndProcess(const TableIdent &tbl_id,
                         std::string_view search_key,
                         uint64_t &timestamp,
                         uint64_t &expire_ts,
                         Handler &&handler)
{
    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    RootMeta *meta = root_handle.Get();
    if (meta->root_id_ == MaxPageId)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();
    MappingSnapshot::Ref seg_mapping_ref{nullptr};
    if (meta->segment_mapper_ != nullptr)
    {
        seg_mapping_ref = meta->segment_mapper_->GetMappingSnapshot();
    }

    PageId page_id;
    err = shard->IndexManager()->SeekIndex(
        mapping.Get(), meta->root_id_, search_key, page_id);
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

    KvError fetch_err =
        handler(meta, mapping.Get(), seg_mapping_ref.Get(), iter);
    if (fetch_err != KvError::NoError)
    {
        return fetch_err;
    }
    timestamp = iter.Timestamp();
    expire_ts = iter.ExpireTs();
    return KvError::NoError;
}
}  // namespace

KvError ReadTask::Read(const TableIdent &tbl_id,
                       std::string_view search_key,
                       std::string &value,
                       uint64_t &timestamp,
                       uint64_t &expire_ts,
                       IoStringBuffer *large_value,
                       bool extract_metadata)
{
    return LocateAndProcess(
        tbl_id,
        search_key,
        timestamp,
        expire_ts,
        [&](RootMeta *meta,
            MappingSnapshot *mapping,
            MappingSnapshot *seg_mapping,
            DataPageIter &iter) -> KvError
        {
            KvError err = ResolveValueOrMetadata(tbl_id,
                                                 mapping,
                                                 iter,
                                                 value,
                                                 meta->compression_.get(),
                                                 extract_metadata);
            if (err != KvError::NoError)
            {
                return err;
            }
            if (iter.IsLargeValue() && large_value != nullptr)
            {
                auto yield_fn = [this]() { Yield(); };
                return GetLargeValue(tbl_id,
                                     seg_mapping,
                                     iter.Value(),
                                     *large_value,
                                     IoMgr()->GetGlobalRegisteredMemory(),
                                     IoMgr()->GlobalRegMemIndexBase(),
                                     yield_fn);
            }
            return KvError::NoError;
        });
}

KvError ReadTask::Read(const TableIdent &tbl_id,
                       std::string_view search_key,
                       std::string &value,
                       uint64_t &timestamp,
                       uint64_t &expire_ts,
                       char *large_value,
                       size_t large_value_size)
{
    return LocateAndProcess(
        tbl_id,
        search_key,
        timestamp,
        expire_ts,
        [&](RootMeta *meta,
            MappingSnapshot *mapping,
            MappingSnapshot *seg_mapping,
            DataPageIter &iter) -> KvError
        {
            KvError err = ResolveValueOrMetadata(
                tbl_id, mapping, iter, value, meta->compression_.get());
            if (err != KvError::NoError)
            {
                return err;
            }
            if (iter.IsLargeValue() && large_value != nullptr)
            {
                return GetLargeValueContiguous(tbl_id,
                                               seg_mapping,
                                               iter.Value(),
                                               large_value,
                                               large_value_size,
                                               IoMgr());
            }
            return KvError::NoError;
        });
}

KvError ReadTask::Read(const TableIdent &tbl_id,
                       std::string_view search_key,
                       uint64_t &timestamp,
                       uint64_t &expire_ts,
                       char *large_value,
                       size_t large_value_size)
{
    return LocateAndProcess(
        tbl_id,
        search_key,
        timestamp,
        expire_ts,
        [&](RootMeta * /*meta*/,
            MappingSnapshot * /*mapping*/,
            MappingSnapshot *seg_mapping,
            DataPageIter &iter) -> KvError
        {
            if (!iter.IsLargeValue() || large_value == nullptr)
            {
                // Caller asserted a large value with a non-null sink; refuse
                // when either contract bit is violated.
                return KvError::InvalidArgs;
            }
            return GetLargeValueContiguous(tbl_id,
                                           seg_mapping,
                                           iter.Value(),
                                           large_value,
                                           large_value_size,
                                           IoMgr());
        });
}

KvError ReadTask::Floor(const TableIdent &tbl_id,
                        std::string_view search_key,
                        std::string &floor_key,
                        std::string &value,
                        uint64_t &timestamp,
                        uint64_t &expire_ts,
                        IoStringBuffer *large_value)
{
    auto [root_handle, err] = shard->IndexManager()->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    RootMeta *meta = root_handle.Get();
    if (meta->root_id_ == MaxPageId)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();
    MappingSnapshot::Ref seg_mapping_ref{nullptr};
    if (meta->segment_mapper_ != nullptr)
    {
        seg_mapping_ref = meta->segment_mapper_->GetMappingSnapshot();
    }

    PageId page_id;
    err = shard->IndexManager()->SeekIndex(
        mapping.Get(), meta->root_id_, search_key, page_id);
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
    KvError fetch_err = ResolveValueOrMetadata(
        tbl_id, mapping.Get(), iter, value, meta->compression_.get());
    CHECK_KV_ERR(fetch_err);
    if (iter.IsLargeValue() && large_value != nullptr)
    {
        auto yield_fn = [this]() { Yield(); };
        KvError large_err = GetLargeValue(tbl_id,
                                          seg_mapping_ref.Get(),
                                          iter.Value(),
                                          *large_value,
                                          IoMgr()->GetGlobalRegisteredMemory(),
                                          IoMgr()->GlobalRegMemIndexBase(),
                                          yield_fn);
        CHECK_KV_ERR(large_err);
    }
    timestamp = iter.Timestamp();
    expire_ts = iter.ExpireTs();
    return KvError::NoError;
}
}  // namespace eloqstore
