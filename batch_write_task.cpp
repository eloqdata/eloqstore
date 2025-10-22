#include "batch_write_task.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <memory>

#include "coding.h"
#include "compression.h"
#include "shard.h"
#include "task.h"
#include "utils.h"
#include "write_tree_stack.h"

namespace eloqstore
{
BatchWriteTask::BatchWriteTask()
    : idx_page_builder_(Options()), data_page_builder_(Options())
{
    overflow_ptrs_.reserve(Options()->overflow_pointers * 4);

    if (Options()->data_append_mode)
    {
        batch_pages_.reserve(Options()->max_write_batch_pages);
    }
}

KvError BatchWriteTask::SeekStack(std::string_view search_key)
{
    const Comparator *cmp = shard->IndexManager()->GetComparator();

    auto entry_contains = [](std::string_view start,
                             std::string_view end,
                             std::string_view search_key,
                             const Comparator *cmp)
    {
        return (start.empty() || cmp->Compare(search_key, start) >= 0) &&
               (end.empty() || cmp->Compare(search_key, end) < 0);
    };

    // The bottom index entry (i.e., the tree root) ranges from negative
    // infinity to positive infinity.
    while (stack_.size() > 1)
    {
        IndexPageIter &idx_iter = stack_.back()->idx_page_iter_;

        if (idx_iter.HasNext())
        {
            idx_iter.Next();
            std::string_view idx_entry_start = idx_iter.Key();
            std::string idx_entry_end = RightBound(false);

            if (entry_contains(idx_entry_start, idx_entry_end, search_key, cmp))
            {
                break;
            }
            else
            {
                auto [_, err] = Pop();
                CHECK_KV_ERR(err);
            }
        }
        else
        {
            auto [_, err] = Pop();
            CHECK_KV_ERR(err);
        }
    }
    return KvError::NoError;
}

std::pair<PageId, KvError> BatchWriteTask::Seek(std::string_view key)
{
    if (stack_.back()->idx_page_ == nullptr)
    {
        stack_.back()->is_leaf_index_ = true;
        return {MaxPageId, KvError::NoError};
    }

    while (true)
    {
        IndexStackEntry *idx_entry = stack_.back().get();
        IndexPageIter &idx_iter = idx_entry->idx_page_iter_;
        idx_iter.Seek(key);
        PageId page_id = idx_iter.GetPageId();
        assert(page_id != MaxPageId);
        if (idx_entry->idx_page_->IsPointingToLeaf())
        {
            break;
        }
        assert(!stack_.back()->is_leaf_index_);
        auto [node, err] = shard->IndexManager()->FindPage(
            cow_meta_.mapper_->GetMapping(), page_id);
        if (err != KvError::NoError)
        {
            return {MaxPageId, err};
        }
        node->Pin();
        stack_.emplace_back(std::make_unique<IndexStackEntry>(node, Options()));
    }
    stack_.back()->is_leaf_index_ = true;
    return {stack_.back()->idx_page_iter_.GetPageId(), KvError::NoError};
}

inline DataPage *BatchWriteTask::TripleElement(uint8_t idx)
{
    return leaf_triple_[idx].IsEmpty() ? nullptr : &leaf_triple_[idx];
}

KvError BatchWriteTask::LoadTripleElement(uint8_t idx, PageId page_id)
{
    if (TripleElement(idx))
    {
        return KvError::NoError;
    }
    assert(page_id != MaxPageId);
    auto [page, err] = LoadDataPage(page_id);
    CHECK_KV_ERR(err);
    leaf_triple_[idx] = std::move(page);
    return KvError::NoError;
}

KvError BatchWriteTask::ShiftLeafLink()
{
    if (TripleElement(0))
    {
        KvError err = WritePage(std::move(leaf_triple_[0]));
        CHECK_KV_ERR(err);
    }
    leaf_triple_[0] = std::move(leaf_triple_[1]);
    return KvError::NoError;
}

void BatchWriteTask::LeafLinkUpdate(DataPage &&page)
{
    page.SetNextPageId(applying_page_.NextPageId());
    page.SetPrevPageId(applying_page_.PrevPageId());
    leaf_triple_[1] = std::move(page);
}

KvError BatchWriteTask::LeafLinkInsert(DataPage &&page)
{
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(page);
    DataPage &new_elem = leaf_triple_[1];
    DataPage *prev_page = TripleElement(0);
    if (prev_page == nullptr)
    {
        // Add first element into empty link list
        assert(stack_.back()->idx_page_iter_.GetPageId() == MaxPageId);
        new_elem.SetNextPageId(MaxPageId);
        new_elem.SetPrevPageId(MaxPageId);
        return KvError::NoError;
    }
    assert(stack_.back()->idx_page_iter_.GetPageId() == MaxPageId ||
           prev_page->NextPageId() == applying_page_.NextPageId());
    if (prev_page->NextPageId() != MaxPageId)
    {
        KvError err = LoadTripleElement(2, prev_page->NextPageId());
        CHECK_KV_ERR(err);
        TripleElement(2)->SetPrevPageId(new_elem.GetPageId());
    }
    new_elem.SetPrevPageId(prev_page->GetPageId());
    new_elem.SetNextPageId(prev_page->NextPageId());
    prev_page->SetNextPageId(new_elem.GetPageId());
    return KvError::NoError;
}

KvError BatchWriteTask::LeafLinkDelete()
{
    if (applying_page_.PrevPageId() != MaxPageId)
    {
        KvError err = LoadTripleElement(0, applying_page_.PrevPageId());
        CHECK_KV_ERR(err);
        TripleElement(0)->SetNextPageId(applying_page_.NextPageId());
    }
    if (applying_page_.NextPageId() != MaxPageId)
    {
        assert(!TripleElement(2));
        KvError err = LoadTripleElement(2, applying_page_.NextPageId());
        CHECK_KV_ERR(err);
        TripleElement(2)->SetPrevPageId(applying_page_.PrevPageId());
    }
    return KvError::NoError;
}

bool BatchWriteTask::SetBatch(std::span<WriteDataEntry> entries)
{
#ifndef NDEBUG
    const Comparator *cmp = Comp();
    if (entries.size() > 1)
    {
        // Ensure the input batch keys are unique and ordered.
        for (uint64_t i = 1; i < entries.size(); i++)
        {
            if (cmp->Compare(entries[i - 1].key_, entries[i].key_) >= 0)
            {
                return false;
            }
        }
    }

    // Limit max key length to half of page size.
    uint16_t max_key_len = Options()->data_page_size >> 1;
    for (WriteDataEntry &ent : entries)
    {
        if (ent.key_.empty() || ent.key_.size() > max_key_len)
        {
            return false;
        }
    }
#endif

    data_batch_ = entries;
    return true;
}

void BatchWriteTask::Abort()
{
    WriteTask::Abort();

    // Unpins all index pages in the stack.
    while (!stack_.empty())
    {
        IndexStackEntry *stack_entry = stack_.back().get();
        if (stack_entry->idx_page_)
        {
            stack_entry->idx_page_->Unpin();
        }
        stack_.pop_back();
    }

    for (DataPage &page : leaf_triple_)
    {
        page.Clear();
    }
    ttl_batch_.clear();
}

KvError BatchWriteTask::Apply()
{
    KvError err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    cow_meta_.compression_->SampleAndBuildDictionaryIfNeeded(data_batch_);
    CHECK_KV_ERR(err);
    err = ApplyBatch(cow_meta_.root_id_, true);
    CHECK_KV_ERR(err);
    err = ApplyTTLBatch();
    CHECK_KV_ERR(err);
    err = UpdateMeta();
    CHECK_KV_ERR(err);
    TriggerTTL();
    return KvError::NoError;
}

KvError BatchWriteTask::ApplyTTLBatch()
{
    if (!ttl_batch_.empty())
    {
        std::sort(ttl_batch_.begin(), ttl_batch_.end());
        SetBatch(ttl_batch_);
        KvError err = ApplyBatch(cow_meta_.ttl_root_id_, false);
        ttl_batch_.clear();
        return err;
    }
    else
    {
        return KvError::NoError;
    }
}

KvError BatchWriteTask::ApplyBatch(PageId &root_id, bool update_ttl)
{
    do_update_ttl_ = update_ttl;
    assert(!update_ttl || ttl_batch_.empty());

    if (root_id != MaxPageId)
    {
        auto [root_page, err] = shard->IndexManager()->FindPage(
            cow_meta_.old_mapping_.get(), root_id);
        CHECK_KV_ERR(err);
        stack_.emplace_back(
            std::make_unique<IndexStackEntry>(root_page, Options()));
        root_page->Pin();
    }
    else
    {
        stack_.emplace_back(
            std::make_unique<IndexStackEntry>(nullptr, Options()));
    }

    KvError err;
    size_t cidx = 0;
    const uint64_t now_ms = utils::UnixTs<chrono::milliseconds>();
    while (cidx < data_batch_.size())
    {
        std::string_view batch_start_key = {data_batch_[cidx].key_.data(),
                                            data_batch_[cidx].key_.size()};
        if (stack_.size() > 1)
        {
            err = SeekStack(batch_start_key);
            CHECK_KV_ERR(err);
        }
        auto [page_id, err] = Seek(batch_start_key);
        CHECK_KV_ERR(err);
        if (page_id != MaxPageId)
        {
            err = LoadApplyingPage(page_id);
            CHECK_KV_ERR(err);
        }
        err = ApplyOnePage(cidx, now_ms);
        CHECK_KV_ERR(err);
    }
    // Flush all dirty leaf data pages in leaf_triple_ .
    assert(TripleElement(2) == nullptr);
    err = ShiftLeafLink();
    CHECK_KV_ERR(err);
    err = ShiftLeafLink();
    CHECK_KV_ERR(err);

    assert(!stack_.empty());
    MemIndexPage *new_root = nullptr;
    while (!stack_.empty())
    {
        auto [new_page, err] = Pop();
        CHECK_KV_ERR(err);
        new_root = new_page;
    }
    root_id = new_root == nullptr ? MaxPageId : new_root->GetPageId();

    return KvError::NoError;
}

KvError BatchWriteTask::LoadApplyingPage(PageId page_id)
{
    assert(page_id != MaxPageId);
    // Now we are going to fetch a data page before execute ApplyOnePage.
    // But this page may already exists at leaf_triple_[1], because it may be
    // loaded by previous ApplyOnePage for linking purpose.
    if (TripleElement(1) && TripleElement(1)->GetPageId() == page_id)
    {
        // Fast path: leaf_triple_[1] is exactly the page we want. Just move it
        // to avoid a disk access.
        applying_page_.Clear();
        applying_page_ = std::move(*TripleElement(1));
    }
    else
    {
        auto [page, err] = LoadDataPage(page_id);
        CHECK_KV_ERR(err);
        applying_page_ = std::move(page);
    }
    assert(TypeOfPage(applying_page_.PagePtr()) == PageType::Data);

    if (TripleElement(1))
    {
        assert(TripleElement(1)->GetPageId() != applying_page_.GetPageId());
        KvError err = ShiftLeafLink();
        CHECK_KV_ERR(err);
    }
    if (TripleElement(0) &&
        TripleElement(0)->GetPageId() != applying_page_.PrevPageId())
    {
        // leaf_triple_[0] is not the previously adjacent page of the
        // applying page.
        KvError err = ShiftLeafLink();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError BatchWriteTask::ApplyOnePage(size_t &cidx, uint64_t now_ms)
{
    assert(!stack_.empty());
    KvError err;
    DataPage *base_page = nullptr;
    std::string_view page_left_bound{};
    std::string page_right_key;
    std::string_view page_right_bound{};

    if (stack_.back()->idx_page_iter_.GetPageId() != MaxPageId)
    {
        assert(stack_.back()->idx_page_iter_.GetPageId() ==
               applying_page_.GetPageId());
        base_page = &applying_page_;
        page_left_bound = LeftBound(true);
        page_right_key = RightBound(true);
        page_right_bound = {page_right_key.data(), page_right_key.size()};
    }

    const Comparator *cmp = shard->IndexManager()->GetComparator();
    compression::DictCompression *compression = cow_meta_.compression_.get();
    DataPageIter base_page_iter{base_page, Options()};
    bool is_base_iter_valid = false;
    AdvanceDataPageIter(base_page_iter, is_base_iter_valid);

    data_page_builder_.Reset();

    assert(cidx < data_batch_.size());
    std::string_view change_key = {data_batch_[cidx].key_.data(),
                                   data_batch_[cidx].key_.size()};
    assert(cmp->Compare(page_left_bound, change_key) <= 0);
    assert(page_right_bound.empty() ||
           cmp->Compare(page_left_bound, page_right_bound) < 0);

    auto change_it = data_batch_.begin() + cidx;
    auto change_end_it = std::lower_bound(
        change_it,
        data_batch_.end(),
        page_right_bound,
        [&](const WriteDataEntry &change_item, std::string_view key)
        {
            if (key.empty())
            {
                // An empty-string right bound represents positive infinity.
                return true;
            }

            std::string_view ckey{change_item.key_.data(),
                                  change_item.key_.size()};
            return cmp->Compare(ckey, key) < 0;
        });

    std::string prev_key;
    std::string_view page_key = stack_.back()->idx_page_iter_.Key();
    std::string curr_page_key{page_key.data(), page_key.size()};

    PageId page_id = MaxPageId;
    if (base_page != nullptr)
    {
        page_id = base_page->GetPageId();
        assert(page_key <= base_page_iter.Key());
    }

    std::string compression_scratch;
    compression::CompressionType compression_type =
        compression::CompressionType::None;

    auto add_to_page = utils::MakeYCombinator(
        [&](auto &&self,
            std::string_view key,
            std::string_view val,
            bool is_ptr,
            uint64_t ts,
            uint64_t expire_ts) -> KvError
        {
            if (expire_ts != 0 && expire_ts <= now_ms)
            {
                // Skip expired key-value.
                if (is_ptr)
                {
                    err = DelOverflowValue(val);
                    CHECK_KV_ERR(err);
                }
                UpdateTTL(expire_ts, key, WriteOp::Delete);
                return KvError::NoError;
            }

            bool success = data_page_builder_.Add(
                key, val, is_ptr, ts, expire_ts, compression_type);
            if (!success)
            {
                if (!is_ptr && DataPageBuilder::IsOverflowKV(
                                   key, val.size(), ts, expire_ts, Options()))
                {
                    // The key-value pair is too large to fit in a single
                    // data page. Split it into multiple overflow pages.
                    KvError err = WriteOverflowValue(val);
                    CHECK_KV_ERR(err);
                    return self(key, overflow_ptrs_, true, ts, expire_ts);
                }

                // Finishes the current page.
                KvError err = FinishDataPage(std::move(curr_page_key), page_id);
                CHECK_KV_ERR(err);
                // Starts a new page.
                curr_page_key = cmp->FindShortestSeparator(
                    {prev_key.data(), prev_key.size()}, key);
                assert(!prev_key.empty() && prev_key < curr_page_key);
                data_page_builder_.Reset();
                success = data_page_builder_.Add(
                    key, val, is_ptr, ts, expire_ts, compression_type);
                assert(success);
                page_id = MaxPageId;
            }
            assert(curr_page_key <= key);
            prev_key = key;
            return KvError::NoError;
        });

    while (is_base_iter_valid && change_it != change_end_it)
    {
        std::string_view base_key = base_page_iter.Key();
        bool add_change_key = false;
        std::string_view base_val = base_page_iter.Value();
        uint64_t base_ts = base_page_iter.Timestamp();
        bool is_overflow_ptr = false;

        change_key = {change_it->key_.data(), change_it->key_.size()};
        std::string_view change_val = {change_it->val_.data(),
                                       change_it->val_.size()};
        uint64_t change_ts = change_it->timestamp_;

        enum struct AdvanceType
        {
            PageIter,
            Changes,
            Both
        };

        std::string_view new_key;
        std::string_view new_val;
        uint64_t new_ts;
        uint64_t expire_ts;
        AdvanceType adv_type;

        int cmp_ret = cmp->Compare(base_key, change_key);
        if (cmp_ret < 0)
        {
            // no change
            new_key = base_key;
            new_val = base_val;
            new_ts = base_ts;
            is_overflow_ptr = base_page_iter.IsOverflow();
            expire_ts = base_page_iter.ExpireTs();
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (change_ts >= base_ts)
            {
                if (base_page_iter.IsOverflow())
                {
                    err = DelOverflowValue(base_val);
                    CHECK_KV_ERR(err);
                }
                const uint32_t base_expire = base_page_iter.ExpireTs();
                expire_ts = change_it->expire_ts_;
                if (change_it->op_ == WriteOp::Delete)
                {
                    /* DELETE */
                    new_key = std::string_view{};
                    UpdateTTL(base_expire, base_key, WriteOp::Delete);
                    assert(expire_ts == 0 || expire_ts == base_expire);
                }
                else if (expire_ts != 0 && expire_ts <= now_ms)
                {
                    /* DELETE (expired update) */
                    new_key = std::string_view{};
                    UpdateTTL(base_expire, base_key, WriteOp::Delete);
                }
                else
                {
                    /* UPDATE */
                    new_key = change_key;
                    new_val = change_val;
                    new_ts = change_ts;
                    add_change_key = true;
                    // update expire timestamp only if it is changed.
                    if (base_expire != expire_ts)
                    {
                        UpdateTTL(base_expire, base_key, WriteOp::Delete);
                        UpdateTTL(expire_ts, new_key, WriteOp::Upsert);
                    }
                }
            }
            else
            {
                // no change, this update/delete is ignored.
                new_key = base_key;
                new_val = base_val;
                new_ts = base_ts;
                is_overflow_ptr = base_page_iter.IsOverflow();
                expire_ts = base_page_iter.ExpireTs();
            }
        }
        else
        {
            adv_type = AdvanceType::Changes;
            if (change_it->op_ == WriteOp::Delete)
            {
                // deleting key not exists.
                new_key = std::string_view{};
            }
            else
            {
                /* INSERT */
                new_key = change_key;
                new_val = change_val;
                new_ts = change_ts;
                add_change_key = true;
                expire_ts = change_it->expire_ts_;
                UpdateTTL(expire_ts, new_key, WriteOp::Upsert);
            }
        }

        if (!new_key.empty())
        {
            if (add_change_key)
            {
                compression::PreparedValue prepared = compression::Prepare(
                    new_val, compression, compression_scratch);
                new_val = prepared.data;
                compression_type = prepared.compression_kind;
                assert(uint8_t(compression_type) <= 3);
            }
            else
            {
                compression_type = base_page_iter.CompressionType();
            }
            err = add_to_page(
                new_key, new_val, is_overflow_ptr, new_ts, expire_ts);
            CHECK_KV_ERR(err);
        }

        switch (adv_type)
        {
        case AdvanceType::PageIter:
            AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
            break;
        case AdvanceType::Changes:
            ++change_it;
            break;
        default:
            AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
            ++change_it;
            break;
        }
    }

    while (is_base_iter_valid)
    {
        // no change
        std::string_view key = base_page_iter.Key();
        std::string_view val = base_page_iter.Value();
        bool overflow = base_page_iter.IsOverflow();
        uint64_t ts = base_page_iter.Timestamp();
        uint64_t expire_ts = base_page_iter.ExpireTs();
        compression_type = base_page_iter.CompressionType();
        err = add_to_page(key, val, overflow, ts, expire_ts);
        CHECK_KV_ERR(err);
        AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
    }

    while (change_it != change_end_it)
    {
        if (change_it->op_ == WriteOp::Upsert)
        {
            /* INSERT */
            std::string_view key{change_it->key_.data(),
                                 change_it->key_.size()};
            std::string_view val{change_it->val_.data(),
                                 change_it->val_.size()};
            uint64_t ts = change_it->timestamp_;
            uint64_t expire_ts = change_it->expire_ts_;
            UpdateTTL(expire_ts, key, WriteOp::Upsert);

            compression::PreparedValue prepared =
                compression::Prepare(val, compression, compression_scratch);
            val = prepared.data;
            compression_type = prepared.compression_kind;
            err = add_to_page(key, val, false, ts, expire_ts);
            CHECK_KV_ERR(err);
        }
        else
        {
            // deleting key not exists.
        }
        ++change_it;
    }

    if (data_page_builder_.IsEmpty())
    {
        if (base_page)
        {
            err = LeafLinkDelete();
            CHECK_KV_ERR(err);
            FreePage(applying_page_.GetPageId());
            assert(stack_.back()->changes_.empty() ||
                   stack_.back()->changes_.back().key_ < curr_page_key);
            stack_.back()->changes_.emplace_back(
                std::move(curr_page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        err = FinishDataPage(std::move(curr_page_key), page_id);
        CHECK_KV_ERR(err);
    }
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(leaf_triple_[2]);

    cidx = cidx + std::distance(data_batch_.begin() + cidx, change_end_it);
    return KvError::NoError;
}

std::pair<MemIndexPage *, KvError> BatchWriteTask::Pop()
{
    if (stack_.empty())
    {
        return {nullptr, KvError::NoError};
    }

    IndexStackEntry *stack_entry = stack_.back().get();
    // There is no change at this level.
    if (stack_entry->changes_.empty())
    {
        MemIndexPage *page = stack_entry->idx_page_;
        if (page != nullptr)
        {
            page->Unpin();
        }
        stack_.pop_back();
        return {page, KvError::NoError};
    }

    idx_page_builder_.Reset();

    const Comparator *cmp = shard->IndexManager()->GetComparator();
    std::vector<IndexOp> &changes = stack_entry->changes_;
    MemIndexPage *stack_page = stack_entry->idx_page_;
    // If the change op contains no index page pointer, this is the lowest level
    // index page.
    bool is_leaf_index = stack_entry->is_leaf_index_;
    IndexPageIter base_page_iter{stack_page, Options()};
    bool is_base_iter_valid = false;
    AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);

    idx_page_builder_.Reset();

    // Merges index entries in the page with the change vector.
    auto cit = changes.begin();

    KvError err;
    // We keep the previous built page in the pipeline before flushing it to
    // storage. This is to redistribute between last two pages in case the last
    // page is sparse.
    DirtyIndexPage prev_page;
    std::string_view page_key =
        stack_.size() == 1 ? std::string_view{}
                           : stack_[stack_.size() - 2]->idx_page_iter_.Key();
    std::string curr_page_key{page_key};
    if (stack_page != nullptr)
    {
        prev_page.page_id_ = stack_page->GetPageId();
    }

    auto add_to_page = [&](std::string_view new_key,
                           uint32_t new_page_id) -> KvError
    {
        bool success =
            idx_page_builder_.Add(new_key, new_page_id, is_leaf_index);
        if (!success)
        {
            err = FinishIndexPage(prev_page, std::move(curr_page_key));
            CHECK_KV_ERR(err);
            curr_page_key = new_key;
            idx_page_builder_.Reset();
            // The first index entry is the leftmost pointer w/o the key.
            idx_page_builder_.Add(
                std::string_view{}, new_page_id, is_leaf_index);
        }
        return KvError::NoError;
    };

    while (is_base_iter_valid && cit != changes.end())
    {
        std::string_view base_key = base_page_iter.Key();
        uint32_t base_page_id = base_page_iter.GetPageId();
        std::string_view change_key{cit->key_.data(), cit->key_.size()};
        uint32_t change_page = cit->page_id_;
        int cmp_ret = cmp->Compare(base_key, change_key);

        enum struct AdvanceType
        {
            PageIter,
            Changes,
            Both
        };

        std::string_view new_key;
        uint32_t new_page_id;
        AdvanceType adv_type;

        if (cmp_ret < 0)
        {
            new_key = base_key;
            new_page_id = base_page_id;
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (cit->op_ == WriteOp::Delete)
            {
                new_key = std::string_view{};
                new_page_id = MaxPageId;
            }
            else
            {
                new_key = change_key;
                new_page_id = change_page;
            }
        }
        else
        {
            // base_key > change_key
            assert(cit->op_ == WriteOp::Upsert);
            adv_type = AdvanceType::Changes;
            new_key = change_key;
            new_page_id = change_page;
        }

        // The first inserted entry is the leftmost pointer whose key is empty.
        if (!new_key.empty() || new_page_id != MaxPageId)
        {
            KvError err = add_to_page(new_key, new_page_id);
            if (err != KvError::NoError)
            {
                return {nullptr, err};
            }
        }

        switch (adv_type)
        {
        case AdvanceType::PageIter:
            AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
            break;
        case AdvanceType::Changes:
            cit++;
            break;
        default:
            AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
            cit++;
            break;
        }
    }

    while (is_base_iter_valid)
    {
        std::string_view new_key = base_page_iter.Key();
        uint32_t new_page_id = base_page_iter.GetPageId();
        KvError err = add_to_page(new_key, new_page_id);
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
    }

    while (cit != changes.end())
    {
        if (cit->op_ != WriteOp::Delete)
        {
            std::string_view new_key{cit->key_.data(), cit->key_.size()};
            uint32_t new_page = cit->page_id_;
            KvError err = add_to_page(new_key, new_page);
            if (err != KvError::NoError)
            {
                return {nullptr, err};
            }
        }
        ++cit;
    }

    MemIndexPage *new_root = nullptr;
    if (idx_page_builder_.IsEmpty())
    {
        FreePage(stack_.back()->idx_page_->GetPageId());
        if (stack_.size() > 1)
        {
            IndexStackEntry *parent = stack_[stack_.size() - 2].get();
            std::string_view page_key = parent->idx_page_iter_.Key();
            parent->changes_.emplace_back(
                std::string(page_key), prev_page.page_id_, WriteOp::Delete);
        }
    }
    else
    {
        bool splited = prev_page.page_ != nullptr;
        err = FinishIndexPage(prev_page, std::move(curr_page_key));
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        err = FlushIndexPage(prev_page.page_,
                             std::move(prev_page.key_),
                             prev_page.page_id_,
                             splited);
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        if (!splited)
        {
            new_root = prev_page.page_;
        }
        prev_page.page_ = nullptr;
    }

    if (stack_page != nullptr)
    {
        stack_page->Unpin();
    }
    stack_.pop_back();
    return {new_root, KvError::NoError};
}

KvError BatchWriteTask::FinishIndexPage(DirtyIndexPage &prev,
                                        std::string cur_page_key)
{
    assert(!idx_page_builder_.IsEmpty());
    const uint16_t cur_page_len = idx_page_builder_.CurrentSizeEstimate();
    std::string_view page_view = idx_page_builder_.Finish();
    if (prev.page_ != nullptr)
    {
        // Redistributing only if the current page length is smaller than
        // one quarter of data_page_size and the previous page length is
        // bigger than three quarter of data_page_size.
        const uint16_t one_quarter = Options()->data_page_size >> 2;
        const uint16_t three_quarter = Options()->data_page_size - one_quarter;
        if (cur_page_len < one_quarter && prev.page_ != nullptr &&
            prev.page_->RestartNum() > 1 &&
            prev.page_->ContentLength() > three_quarter)
        {
            page_view = Redistribute(prev.page_, page_view, cur_page_key);
        }

        KvError err = FlushIndexPage(
            prev.page_, std::move(prev.key_), prev.page_id_, true);
        CHECK_KV_ERR(err);
        prev.page_ = nullptr;
        prev.page_id_ = MaxPageId;
    }
    MemIndexPage *cur_page = shard->IndexManager()->AllocIndexPage();
    if (cur_page == nullptr)
    {
        return KvError::OutOfMem;
    }
    memcpy(cur_page->PagePtr(), page_view.data(), page_view.size());
    prev.page_ = cur_page;
    prev.key_ = std::move(cur_page_key);
    return KvError::NoError;
}

BatchWriteTask::DirtyIndexPage::~DirtyIndexPage()
{
    if (page_ != nullptr)
    {
        assert(page_->IsDetached());
        shard->IndexManager()->FreeIndexPage(page_);
        page_ = nullptr;
    }
}

KvError BatchWriteTask::FlushIndexPage(MemIndexPage *idx_page,
                                       std::string idx_page_key,
                                       PageId page_id,
                                       bool split)
{
    // Flushes the built index page.
    idx_page->SetPageId(page_id);
    KvError err = WritePage(idx_page);
    CHECK_KV_ERR(err);

    // The parent node needs to be updated only when the index page splits and
    // the current page is either a newly generated page or the root page.
    if (split && (page_id == MaxPageId || stack_.size() == 1))
    {
        assert(stack_.size() >= 1);
        if (stack_.size() == 1)
        {
            stack_.emplace(
                stack_.begin(),
                std::make_unique<IndexStackEntry>(nullptr, Options()));
        }

        IndexStackEntry *parent_entry = stack_[stack_.size() - 2].get();
        parent_entry->changes_.emplace_back(
            std::move(idx_page_key), idx_page->GetPageId(), WriteOp::Upsert);
    }
    return KvError::NoError;
}

KvError BatchWriteTask::FinishDataPage(std::string page_key, PageId page_id)
{
    const uint16_t cur_page_len = data_page_builder_.CurrentSizeEstimate();
    std::string_view page_view = data_page_builder_.Finish();
    if (page_id == MaxPageId)
    {
        page_id = cow_meta_.mapper_->GetPage();
        DataPage new_data_page(false);
        new_data_page.SetPageId(page_id);
        // Redistributing only if the current page length is smaller than one
        // quarter of data_page_size and the previous page length is bigger than
        // three quarter of data_page_size.
        const uint16_t one_quarter = Options()->data_page_size >> 2;
        const uint16_t three_quarter = Options()->data_page_size - one_quarter;
        DataPage *prev_page = TripleElement(0);
        if (cur_page_len < one_quarter && prev_page != nullptr &&
            prev_page->RestartNum() > 1 &&
            prev_page->ContentLength() > three_quarter)
        {
            // This page is too small, redistribute it with the previous page.
            Page page = Redistribute(*prev_page, page_view);
            new_data_page.SetPage(std::move(page));
            DataPageIter iter(&new_data_page, Options());
            assert(iter.HasNext());
            iter.Next();
            page_key.assign(iter.Key());
        }
        else
        {
            new_data_page.SetPage(Page(true));
            memcpy(new_data_page.PagePtr(), page_view.data(), page_view.size());
        }

        // This is a new data page that does not exist in the tree and has a new
        // page Id.
        KvError err = LeafLinkInsert(std::move(new_data_page));
        CHECK_KV_ERR(err);
        // This is a new page that does not exist in the parent index page.
        // Elevates to the parent index page.
        assert(stack_.back()->changes_.empty() ||
               stack_.back()->changes_.back().key_ < page_key);
        stack_.back()->changes_.emplace_back(
            std::move(page_key), page_id, WriteOp::Upsert);
    }
    else
    {
        DataPage new_page(page_id);
        memcpy(new_page.PagePtr(), page_view.data(), page_view.size());
        // This is an existing data page with updated content.
        LeafLinkUpdate(std::move(new_page));
    }
    return ShiftLeafLink();
}

Page BatchWriteTask::Redistribute(DataPage &prev_page,
                                  std::string_view cur_page)
{
    const uint16_t prev_page_len = prev_page.ContentLength();
    const uint16_t cur_page_len =
        DecodeFixed16(cur_page.data() + DataPage::page_size_offset);
    assert(prev_page_len > cur_page_len);
    const uint16_t average_len = (prev_page_len + cur_page_len) >> 1;
    assert(prev_page_len >= average_len);

    PageRegionIter iter({prev_page.PagePtr(), prev_page_len});
    uint16_t preserve_region_cnt = 0;
    // Number of bytes occupied by header and tailer (number of regions).
    uint16_t preserve_len = DataPageBuilder::HeaderSize() + sizeof(uint16_t);
    while (preserve_len < average_len)
    {
        // Skip preserved regions for previous page.
        assert(iter.Valid());
        std::string_view region = iter.Region();
        // Space for region data and region offset
        preserve_len += region.size() + sizeof(uint16_t);
        preserve_region_cnt++;
        iter.Next();
    }
    assert(preserve_len <= prev_page_len);
    Page new_page(true);
    if (preserve_len == prev_page_len)
    {
        // Corner case: can not move any region from the previous page.
        std::memcpy(new_page.Ptr(), cur_page.data(), cur_page.size());
        return new_page;
    }

    // Merge last several regions of previous page with current page.
    FastPageBuilder builder(Options());
    builder.Reset(new_page.Ptr(), PageType::Data);
    for (; iter.Valid(); iter.Next())
    {
        std::string_view region = iter.Region();
        builder.AddRegion(region);
    }
    std::string_view content = cur_page.substr(0, cur_page_len);
    for (iter.Reset(content); iter.Valid(); iter.Next())
    {
        std::string_view region = iter.Region();
        builder.AddRegion(region);
    }
    builder.Finish();

    // Update previous page in-place.
    iter.Reset({prev_page.PagePtr(), prev_page_len});
    builder.Reset(prev_page.PagePtr(), PageType::Data);
    for (uint16_t i = 0; i < preserve_region_cnt; i++)
    {
        // Copy preserved regions.
        std::string_view region = iter.Region();
        builder.AddRegion(region);
        iter.Next();
    }
    builder.Finish();

#ifndef NDEBUG
    assert(prev_page.RestartNum() == preserve_region_cnt);
    assert(prev_page.ContentLength() == preserve_len);
    const uint16_t new_page_len =
        DecodeFixed16(new_page.Ptr() + DataPage::page_size_offset);
    assert(prev_page_len + cur_page_len == preserve_len + new_page_len);
#endif
    return new_page;
}

std::string_view BatchWriteTask::Redistribute(MemIndexPage *prev_page,
                                              std::string_view cur_page,
                                              std::string &cur_page_key)
{
    const uint16_t prev_page_len = prev_page->ContentLength();
    const uint16_t cur_page_len =
        DecodeFixed16(cur_page.data() + DataPage::page_size_offset);
    cur_page = cur_page.substr(0, cur_page_len);
    assert(prev_page_len > cur_page_len);
    const uint16_t average_len = (prev_page_len + cur_page_len) >> 1;
    assert(prev_page_len >= average_len);

    PageRegionIter region_iter({prev_page->PagePtr(), prev_page_len});
    uint16_t preserve_region_cnt = 0;
    // Number of bytes occupied by header and tailer (number of regions).
    uint16_t preserve_len = IndexPageBuilder::HeaderSize() + sizeof(uint16_t);
    while (preserve_len < average_len)
    {
        // Skip preserved regions for previous page.
        assert(region_iter.Valid());
        std::string_view region = region_iter.Region();
        // Space for region data and region offset
        preserve_len += region.size() + sizeof(uint16_t);
        preserve_region_cnt++;
        region_iter.Next();
    }
    assert(preserve_len <= prev_page_len);
    if (preserve_len == prev_page_len)
    {
        // Corner case: can not move any region from the previous page.
        return cur_page;
    }

    // Merge last several regions of previous page with current page.
    bool is_leaf_idx = prev_page->IsPointingToLeaf();
    IndexPageBuilder idx_builder(Options());
    IndexPageIter iter_prev(prev_page, Options());
    iter_prev.SeekToRestart(preserve_region_cnt);
    iter_prev.Next();
    std::string new_page_key(iter_prev.Key());
    idx_builder.Add({}, iter_prev.GetPageId(), is_leaf_idx);
    while (iter_prev.Next())
    {
        idx_builder.Add(iter_prev.Key(), iter_prev.GetPageId(), is_leaf_idx);
    }
    IndexPageIter iter_cur(cur_page, Options());
    iter_cur.Next();
    assert(iter_cur.Key().empty());
    idx_builder.Add(cur_page_key, iter_cur.GetPageId(), is_leaf_idx);
    while (iter_cur.Next())
    {
        idx_builder.Add(iter_cur.Key(), iter_cur.GetPageId(), is_leaf_idx);
    }
    std::string_view new_page = idx_builder.Finish();
    idx_page_builder_.Swap(idx_builder);
    cur_page_key = std::move(new_page_key);

    // Update previous page in-place.
    FastPageBuilder builder(Options());
    builder.Reset(prev_page->PagePtr(), TypeOfPage(prev_page->PagePtr()));
    region_iter.Reset({prev_page->PagePtr(), prev_page_len});
    for (uint16_t i = 0; i < preserve_region_cnt; i++)
    {
        // Copy preserved regions.
        std::string_view region = region_iter.Region();
        builder.AddRegion(region);
        region_iter.Next();
    }
    builder.Finish();

#ifndef NDEBUG
    assert(prev_page->RestartNum() == preserve_region_cnt);
    assert(prev_page->ContentLength() == preserve_len);
#endif
    return new_page;
}

std::string_view BatchWriteTask::LeftBound(bool is_data_page)
{
    size_t level = is_data_page ? 0 : 1;
    auto stack_it = stack_.crbegin() + level;
    while (stack_it != stack_.crend())
    {
        IndexPageIter &idx_iter = (*stack_it)->idx_page_iter_;
        std::string_view idx_key = idx_iter.Key();
        if (!idx_key.empty())
        {
            return idx_key;
        }
        ++stack_it;
    }

    // An empty string for left bound means negative infinity.
    return std::string_view{};
}

std::string BatchWriteTask::RightBound(bool is_data_page)
{
    size_t level = is_data_page ? 0 : 1;
    auto stack_it = stack_.crbegin() + level;
    while (stack_it != stack_.crend())
    {
        IndexPageIter &idx_iter = (*stack_it)->idx_page_iter_;
        std::string next_key = idx_iter.PeekNextKey();
        if (!next_key.empty())
        {
            return next_key;
        }
        ++stack_it;
    }

    // An empty string for right bound means positive infinity.
    return std::string{};
}

KvError BatchWriteTask::DeleteTree(PageId page_id, bool update_prev)
{
    auto [idx_page, err] = shard->IndexManager()->FindPage(
        cow_meta_.mapper_->GetMapping(), page_id);
    CHECK_KV_ERR(err);
    idx_page->Pin();
    IndexPageIter iter(idx_page, Options());

    if (idx_page->IsPointingToLeaf())
    {
        while (iter.Next())
        {
            err = DeleteDataPage(iter.GetPageId(), update_prev);
            update_prev = false;
            if (err != KvError::NoError)
            {
                idx_page->Unpin();
                return err;
            }
        }
        idx_page->Unpin();
        FreePage(page_id);
        return KvError::NoError;
    }

    while (iter.Next())
    {
        KvError err = DeleteTree(iter.GetPageId(), update_prev);
        update_prev = false;
        if (err != KvError::NoError)
        {
            idx_page->Unpin();
            return err;
        }
    }
    idx_page->Unpin();
    FreePage(page_id);
    return KvError::NoError;
}

KvError BatchWriteTask::DeleteDataPage(PageId page_id, bool update_prev)
{
    auto [page, err] = LoadDataPage(page_id);
    CHECK_KV_ERR(err);
    DataPageIter iter(&page, Options());
    while (iter.Next())
    {
        if (iter.IsOverflow())
        {
            err = DelOverflowValue(iter.Value());
            CHECK_KV_ERR(err);
        }
        uint64_t expire_ts = iter.ExpireTs();
        UpdateTTL(expire_ts, iter.Key(), WriteOp::Delete);
    }
    FreePage(page_id);

    if (!update_prev || page.PrevPageId() == MaxPageId)
    {
        return KvError::NoError;
    }
    // This is the first truncated data page, and the previous data page will
    // become the new tail data page.
    auto [prev_page, err_prev] = LoadDataPage(page.PrevPageId());
    if (err_prev != KvError::NoError)
    {
        return err_prev;
    }
    prev_page.SetNextPageId(MaxPageId);
    return WritePage(std::move(prev_page));
}

KvError BatchWriteTask::WriteOverflowValue(std::string_view value)
{
    const KvOptions *opts = Options();
    const uint16_t page_cap = OverflowPage::Capacity(opts, false);
    const uint16_t end_page_cap = OverflowPage::Capacity(opts, true);
    overflow_ptrs_.clear();
    uint32_t end_page_id = MaxPageId;
    std::string_view page_val;
    KvError err;
    std::array<uint32_t, max_overflow_pointers> buf;
    std::span<uint32_t> pointers;

    while (!value.empty())
    {
        // Calculates how many page ids are needed for the next group.
        // Round upward and up to KvOptions::overflow_pointers.
        size_t next_group_size = (value.size() + page_cap - 1) / page_cap;
        next_group_size =
            std::min(next_group_size, size_t(opts->overflow_pointers));
        // Allocate pointers to the next group.
        for (uint8_t i = 0; i < next_group_size; i++)
        {
            buf[i] = cow_meta_.mapper_->GetPage();
        }
        pointers = {buf.data(), next_group_size};
        if (end_page_id == MaxPageId)
        {
            // Store head pointers of this link list in overflow_ptrs_.
            for (uint32_t pg_id : pointers)
            {
                PutFixed32(&overflow_ptrs_, pg_id);
            }
        }
        else
        {
            // Write end page of the previous group that contains pointers to
            // the next group.
            err =
                WritePage(OverflowPage(end_page_id, opts, page_val, pointers));
            CHECK_KV_ERR(err);
        }

        // Write the next overflow pages group.
        uint8_t i = 0;
        for (uint32_t pg_id : pointers)
        {
            i++;
            if (i == opts->overflow_pointers && value.size() > page_cap)
            {
                // The end page of this group can't hold the remaining value.
                // So we need at least one more group.
                end_page_id = pg_id;
                // The end page of a group has a smaller capacity.
                uint16_t page_val_size =
                    std::min(size_t(end_page_cap), value.size());
                page_val = value.substr(0, page_val_size);
                value = value.substr(page_val_size);
                break;
            }
            uint16_t page_val_size = std::min(size_t(page_cap), value.size());
            page_val = value.substr(0, page_val_size);
            value = value.substr(page_val_size);
            err = WritePage(OverflowPage(pg_id, opts, page_val));
            CHECK_KV_ERR(err);
        }
        assert(i == pointers.size());
    }
    return KvError::NoError;
}

KvError BatchWriteTask::DelOverflowValue(std::string_view encoded_ptrs)
{
    std::array<PageId, max_overflow_pointers> pointers;
    uint8_t n_ptrs = DecodeOverflowPointers(encoded_ptrs, pointers);
    for (uint8_t i = 0; i < n_ptrs;)
    {
        PageId page_id = pointers[i];
        if (i == (Options()->overflow_pointers - 1))
        {
            auto [page, err] = LoadOverflowPage(page_id);
            CHECK_KV_ERR(err);
            encoded_ptrs = page.GetEncodedPointers(Options());
            n_ptrs = DecodeOverflowPointers(encoded_ptrs, pointers);
            i = 0;
        }
        else
        {
            i++;
        }
        FreePage(page_id);
    }
    return KvError::NoError;
}

void BatchWriteTask::AdvanceDataPageIter(DataPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

void BatchWriteTask::AdvanceIndexPageIter(IndexPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

std::pair<MemIndexPage *, KvError> BatchWriteTask::TruncateIndexPage(
    PageId page_id, std::string_view trunc_pos)
{
    auto [idx_page, err] = shard->IndexManager()->FindPage(
        cow_meta_.mapper_->GetMapping(), page_id);
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }

    const bool is_leaf_idx = idx_page->IsPointingToLeaf();
    IndexPageBuilder builder(Options());

    auto truncate_sub_node = [&](std::string_view sub_node_key,
                                 PageId sub_node_id) -> KvError
    {
        // truncate sub-node
        std::pair<bool, KvError> ret;
        if (is_leaf_idx)
        {
            ret = TruncateDataPage(sub_node_id, trunc_pos);
        }
        else
        {
            ret = TruncateIndexPage(sub_node_id, trunc_pos);
        }
        CHECK_KV_ERR(ret.second);
        if (ret.first)
        {
            // This sub-node is partially truncated
            builder.Add(sub_node_key, sub_node_id, is_leaf_idx);
        }
        return KvError::NoError;
    };

    auto delete_sub_node = [this, is_leaf_idx](std::string_view sub_node_key,
                                               PageId sub_node_id,
                                               bool is_trunc_begin) -> KvError
    {
        // delete whole sub-node
        if (is_leaf_idx)
        {
            return DeleteDataPage(sub_node_id, is_trunc_begin);
        }
        else
        {
            return DeleteTree(sub_node_id, is_trunc_begin);
        }
    };

    IndexPageIter iter(idx_page, Options());
    CHECK(iter.Next());
    std::string sub_node_key = {};
    PageId sub_node_id = iter.GetPageId();

    // Depth first search recursively
    bool cut_point_found = false;
    idx_page->Pin();
    while (iter.Next())
    {
        std::string_view next_node_key = iter.Key();
        PageId next_node_id = iter.GetPageId();

        if (cut_point_found)
        {
            // Delete all sub-nodes bigger than cutting point.
            err = delete_sub_node(sub_node_key, sub_node_id, false);
            if (err != KvError::NoError)
            {
                idx_page->Unpin();
                return {nullptr, err};
            }
        }
        else if (Comp()->Compare(trunc_pos, next_node_key) >= 0)
        {
            // Preserve this sub-node smaller than cutting point.
            builder.Add(sub_node_key, sub_node_id, is_leaf_idx);
        }
        else
        {
            // Mark cutting point has been found to reduce string comparison.
            cut_point_found = true;
            if (Comp()->Compare(trunc_pos, sub_node_key) > 0)
            {
                // Truncate the biggest sub-node smaller than trunc_pos.
                err = truncate_sub_node(sub_node_key, sub_node_id);
            }
            else
            {
                // Delete sub-node equal to trunc_pos.
                assert(Comp()->Compare(trunc_pos, sub_node_key) == 0);
                err = delete_sub_node(sub_node_key, sub_node_id, true);
            }
            if (err != KvError::NoError)
            {
                idx_page->Unpin();
                return {nullptr, err};
            }
        }

        sub_node_key = next_node_key;
        sub_node_id = next_node_id;
    }
    if (cut_point_found)
    {
        err = delete_sub_node(sub_node_key, sub_node_id, false);
    }
    else if (Comp()->Compare(trunc_pos, sub_node_key) > 0)
    {
        err = truncate_sub_node(sub_node_key, sub_node_id);
    }
    else
    {
        err = delete_sub_node(sub_node_key, sub_node_id, true);
    }
    idx_page->Unpin();
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }

    if (builder.IsEmpty())
    {
        // This index page is wholly deleted
        return {nullptr, KvError::NoError};
    }
    // This index page is partially truncated
    MemIndexPage *new_page = shard->IndexManager()->AllocIndexPage();
    if (new_page == nullptr)
    {
        return {nullptr, KvError::OutOfMem};
    }
    std::string_view page_view = builder.Finish();
    memcpy(new_page->PagePtr(), page_view.data(), page_view.size());
    new_page->SetPageId(page_id);
    err = WritePage(new_page);
    if (err != KvError::NoError)
    {
        shard->IndexManager()->FreeIndexPage(new_page);
        return {nullptr, err};
    }
    return {new_page, KvError::NoError};
}

KvError BatchWriteTask::Truncate(std::string_view trunc_pos)
{
    KvError err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);
    if (cow_meta_.root_id_ == MaxPageId)
    {
        return KvError::NoError;
    }

    if (trunc_pos.empty())
    {
        do_update_ttl_ = false;
        err = DeleteTree(cow_meta_.root_id_, false);
        CHECK_KV_ERR(err);
        cow_meta_.root_id_ = MaxPageId;
        if (cow_meta_.ttl_root_id_ != MaxPageId)
        {
            err = DeleteTree(cow_meta_.ttl_root_id_, false);
            CHECK_KV_ERR(err);
            cow_meta_.ttl_root_id_ = MaxPageId;
        }
        return UpdateMeta();
    }

    do_update_ttl_ = true;
    auto [new_root, error] = TruncateIndexPage(cow_meta_.root_id_, trunc_pos);
    CHECK_KV_ERR(error);
    cow_meta_.root_id_ =
        new_root == nullptr ? MaxPageId : new_root->GetPageId();
    err = ApplyTTLBatch();
    CHECK_KV_ERR(err);
    return UpdateMeta();
}

std::pair<bool, KvError> BatchWriteTask::TruncateDataPage(
    PageId page_id, std::string_view trunc_pos)
{
    auto [page, err] = LoadDataPage(page_id);
    if (err != KvError::NoError)
    {
        return {true, err};
    }

    const Comparator *cmp = shard->IndexManager()->GetComparator();
    DataPageIter iter{&page, Options()};
    data_page_builder_.Reset();
    while (iter.Next() && cmp->Compare(iter.Key(), trunc_pos) < 0)
    {
        data_page_builder_.Add(iter.Key(),
                               iter.Value(),
                               iter.IsOverflow(),
                               iter.Timestamp(),
                               iter.ExpireTs(),
                               iter.CompressionType());
    }
    while (iter.Next())
    {
        if (iter.IsOverflow())
        {
            err = DelOverflowValue(iter.Value());
            if (err != KvError::NoError)
            {
                return {true, err};
            }
        }
        uint64_t expire_ts = iter.ExpireTs();
        UpdateTTL(expire_ts, iter.Key(), WriteOp::Delete);
    }

    if (data_page_builder_.IsEmpty())
    {
        FreePage(page.GetPageId());

        uint32_t prev_page_id = page.PrevPageId();
        if (prev_page_id == MaxPageId)
        {
            return {false, KvError::NoError};
        }
        // The previous data page will become the new tail data page.
        // We don't need to update the previous page id of the next data page.
        auto [prev_page, err] = LoadDataPage(prev_page_id);
        if (err != KvError::NoError)
        {
            return {false, err};
        }
        prev_page.SetNextPageId(MaxPageId);
        err = WritePage(std::move(prev_page));
        return {false, err};
    }
    else
    {
        // This currently updated data page will become the new tail data page.
        DataPage new_page(page_id);
        std::string_view page_view = data_page_builder_.Finish();
        memcpy(new_page.PagePtr(), page_view.data(), page_view.size());
        new_page.SetNextPageId(MaxPageId);
        new_page.SetPrevPageId(page.PrevPageId());
        err = WritePage(std::move(new_page));
        return {true, err};
    }
}

KvError BatchWriteTask::CleanExpiredKeys()
{
    // Scan from leftmost to get all expired keys.
    ScanIterator iter(tbl_ident_);
    KvError err = iter.Seek({}, true);
    if (err != KvError::NoError)
    {
        if (err == KvError::EndOfFile)
        {
            // TTL tree is empty.
            return KvError::NoError;
        }
        else
        {
            LOG(ERROR) << "failed to clean expired keys: " << ErrorString(err);
            return err;
        }
    }

    std::vector<WriteDataEntry> data_batch, ttl_batch;
    data_batch.reserve(128);
    ttl_batch.reserve(128);
    const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
    uint64_t next_expire_ts = 0;
    do
    {
        std::string_view ttl_key = iter.Key();
        uint64_t expire_ts = BigEndianToNative(DecodeFixed64(ttl_key.data()));
        if (expire_ts > now_ts)
        {
            next_expire_ts = expire_ts;
            break;
        }
        ttl_batch.emplace_back(std::string(ttl_key), "", 0, WriteOp::Delete);
        std::string key(ttl_key.substr(8));
        data_batch.emplace_back(
            std::move(key), "", now_ts, WriteOp::Delete, expire_ts);
    } while (iter.Next() == KvError::NoError);

    if (ttl_batch.empty())
    {
        return KvError::NoError;
    }

    err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);
    assert(cow_meta_.next_expire_ts_ != 0 &&
           cow_meta_.next_expire_ts_ <= now_ts);

    std::sort(data_batch.begin(), data_batch.end());
    SetBatch(data_batch);
    err = ApplyBatch(cow_meta_.root_id_, false);
    CHECK_KV_ERR(err);

    assert(std::is_sorted(ttl_batch.begin(), ttl_batch.end()));
    SetBatch(ttl_batch);
    err = ApplyBatch(cow_meta_.ttl_root_id_, false);
    CHECK_KV_ERR(err);
    cow_meta_.next_expire_ts_ = next_expire_ts;
    return UpdateMeta();
}

void BatchWriteTask::UpdateTTL(uint64_t expire_ts,
                               std::string_view key,
                               WriteOp op)
{
    if (do_update_ttl_ && expire_ts != 0)
    {
        std::string ttl_key;
        ttl_key.resize(sizeof(expire_ts) + key.size());
        EncodeFixed64(ttl_key.data(), ToBigEndian(expire_ts));
        std::memcpy(ttl_key.data() + sizeof(expire_ts), key.data(), key.size());
        ttl_batch_.emplace_back(std::move(ttl_key), "", 0, op, 0);
        if (op == WriteOp::Upsert)
        {
            uint64_t next_ts = cow_meta_.next_expire_ts_;
            cow_meta_.next_expire_ts_ =
                next_ts == 0 ? expire_ts : std::min(next_ts, expire_ts);
        }
    }
}

}  // namespace eloqstore