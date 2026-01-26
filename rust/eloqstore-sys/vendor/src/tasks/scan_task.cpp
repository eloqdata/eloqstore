#include "tasks/scan_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "storage/page_mapper.h"
#include "storage/shard.h"

namespace eloqstore
{
ScanIterator::ScanIterator(const TableIdent &tbl_id,
                           std::vector<DataPage> &prefetched_pages,
                           std::vector<Page> &read_pages,
                           size_t prefetch_pages)
    : tbl_id_(tbl_id),
      prefetch_page_num_(prefetch_pages),
      mapping_(),
      iter_(nullptr, Options()),
      prefetched_pages_(&prefetched_pages),
      read_pages_(&read_pages)
{
    prefetched_pages_->reserve(prefetch_page_num_);
    read_pages_->reserve(prefetch_page_num_);
}

ScanIterator::~ScanIterator()
{
    ClearIndexStack();
}

KvError ScanIterator::Seek(std::string_view key, bool ttl)
{
    ResetPrefetchState();
    ClearIndexStack();
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id_);
    CHECK_KV_ERR(err);
    root_id_ = ttl ? meta->ttl_root_id_ : meta->root_id_;
    if (root_id_ == MaxPageId)
    {
        return KvError::EndOfFile;
    }
    compression_ = meta->compression_.get();
    mapping_ = meta->mapper_->GetMappingSnapshot();

    err = BuildIndexStack(key);
    CHECK_KV_ERR(err);

    err = PrefetchFromStack();
    CHECK_KV_ERR(err);

    err = ConsumePrefetchedPage();
    CHECK_KV_ERR(err);

    if (!iter_.Seek(key))
    {
        err = Next();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

void ScanIterator::ResetPrefetchState()
{
    prefetched_offset_ = 0;
    prefetched_count_ = 0;
    prefetched_pages_->clear();
    read_pages_->clear();
}

void ScanIterator::ClearIndexStack()
{
    while (!index_stack_.empty())
    {
        IndexFrame &frame = index_stack_.back();
        if (frame.page != nullptr)
        {
            frame.page->Unpin();
        }
        index_stack_.pop_back();
    }
}

KvError ScanIterator::BuildIndexStack(std::string_view key)
{
    PageId page_id = root_id_;
    while (true)
    {
        auto [node, err] =
            shard->IndexManager()->FindPage(mapping_.get(), page_id);
        if (err != KvError::NoError)
        {
            ClearIndexStack();
            return err;
        }
        node->Pin();
        IndexPageIter idx_it{node, Options()};
        idx_it.Seek(key);
        index_stack_.push_back(IndexFrame{node, idx_it});
        if (idx_it.GetPageId() == MaxPageId)
        {
            ClearIndexStack();
            return KvError::EndOfFile;
        }
        PageId child_id = idx_it.GetPageId();
        if (node->IsPointingToLeaf())
        {
            return KvError::NoError;
        }
        page_id = child_id;
    }
}

KvError ScanIterator::AdvanceToNextLeaf()
{
    while (!index_stack_.empty())
    {
        auto &frame = index_stack_.back();
        if (frame.iter.HasNext() && frame.iter.Next())
        {
            PageId child_id = frame.iter.GetPageId();
            if (frame.page->IsPointingToLeaf())
            {
                return KvError::NoError;
            }

            PageId descend_id = child_id;
            while (true)
            {
                auto [node, err] =
                    shard->IndexManager()->FindPage(mapping_.get(), descend_id);
                if (err != KvError::NoError)
                {
                    ClearIndexStack();
                    return err;
                }
                node->Pin();
                IndexPageIter it{node, Options()};
                bool ok = it.Next();
                CHECK(ok);
                index_stack_.push_back(IndexFrame{node, it});
                PageId next_child = it.GetPageId();
                if (node->IsPointingToLeaf())
                {
                    return KvError::NoError;
                }
                descend_id = next_child;
            }
        }
        frame.page->Unpin();
        index_stack_.pop_back();
    }
    return KvError::EndOfFile;
}

KvError ScanIterator::PrefetchFromStack()
{
    ResetPrefetchState();
    if (index_stack_.empty())
    {
        return KvError::EndOfFile;
    }

    size_t leaf_cnt = 0;
    prefetched_file_page_ids_.fill(MaxFilePageId);
    prefetched_leaf_ids_.fill(MaxPageId);

    PageId leaf_id = index_stack_.back().iter.GetPageId();
    while (leaf_cnt < prefetch_page_num_ && leaf_id != MaxPageId)
    {
        prefetched_leaf_ids_[leaf_cnt] = leaf_id;
        FilePageId file_id = mapping_->ToFilePage(leaf_id);
        assert(file_id != MaxFilePageId);
        prefetched_file_page_ids_[leaf_cnt] = file_id;
        ++leaf_cnt;

        bool reached_limit = leaf_cnt >= prefetch_page_num_;
        if (reached_limit)
        {
            KvError err = AdvanceToNextLeaf();
            if (err != KvError::NoError && err != KvError::EndOfFile)
            {
                return err;
            }
            break;
        }

        KvError err = AdvanceToNextLeaf();
        if (err == KvError::EndOfFile)
        {
            break;
        }
        CHECK_KV_ERR(err);
        if (!index_stack_.empty())
        {
            leaf_id = index_stack_.back().iter.GetPageId();
        }
        else
        {
            leaf_id = MaxPageId;
        }
    }

    KvError err = IoMgr()->ReadPages(
        tbl_id_,
        std::span<FilePageId>(prefetched_file_page_ids_.data(), leaf_cnt),
        *read_pages_);
    CHECK_KV_ERR(err);
    assert(read_pages_->size() == leaf_cnt);

    for (size_t i = 0; i < read_pages_->size(); ++i)
    {
        prefetched_pages_->emplace_back(prefetched_leaf_ids_[i],
                                        std::move((*read_pages_)[i]));
    }
    prefetched_count_ = prefetched_pages_->size();
    prefetched_offset_ = 0;
    return prefetched_count_ == 0 ? KvError::EndOfFile : KvError::NoError;
}

KvError ScanIterator::ConsumePrefetchedPage()
{
    if (prefetched_offset_ >= prefetched_count_)
    {
        return KvError::EndOfFile;
    }
    data_page_ = std::move((*prefetched_pages_)[prefetched_offset_++]);
    iter_.Reset(&data_page_, Options()->data_page_size);
    assert(iter_.HasNext());
    return KvError::NoError;
}

KvError ScanIterator::LoadNextDataPage()
{
    if (prefetched_offset_ >= prefetched_count_)
    {
        KvError err = PrefetchFromStack();
        if (err != KvError::NoError)
        {
            return err;
        }
    }
    return ConsumePrefetchedPage();
}

KvError ScanIterator::Next()
{
    if (!iter_.HasNext())
    {
        KvError err = LoadNextDataPage();
        if (err != KvError::NoError)
        {
            return err;
        }
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

    ScanIterator iter(
        tbl_id, prefetched_pages_, read_pages_, req->PrefetchPageNum());
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
