#include "write_task.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include "async_io_manager.h"
#include "data_page.h"
#include "error.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "table_ident.h"
#include "write_op.h"

namespace kvstore
{
WriteTask::WriteTask(const TableIdent &tid)
    : tbl_ident_(tid),
      idx_page_builder_(Options()),
      data_page_builder_(Options())
{
}

const TableIdent &WriteTask::TableId() const
{
    return tbl_ident_;
}

void WriteTask::Reset(const TableIdent &tbl_id)
{
    tbl_ident_ = tbl_id;
    stack_.clear();
}

void WriteTask::Abort()
{
    // Wait all asynchronous IO finished.
    // PollComplete will access KvTask* stored in io_uring_cqe->user_data
    WaitAsynIo();

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

    if (cow_meta_.old_mapping_ != nullptr)
    {
        // Cancel all free file page operations.
        cow_meta_.old_mapping_->AbortFreeFilePage();
        cow_meta_.old_mapping_ = nullptr;
    }
    cow_meta_.mapper_ = nullptr;

    if (cow_meta_.root_ == nullptr)
    {
        // MakeCowRoot will create a empty RootMeta if partition not found
        index_mgr->CleanStubRoot(tbl_ident_);
    }
}

KvError WriteTask::DeleteTree(uint32_t page_id)
{
    auto [idx_page, err] =
        index_mgr->FindPage(cow_meta_.mapper_->GetMapping(), page_id);
    CHECK_KV_ERR(err);
    IndexPageIter iter(idx_page, Options());

    if (idx_page->IsPointingToLeaf())
    {
        while (iter.Next())
        {
            FreePage(iter.PageId());
        }
        return KvError::NoError;
    }

    idx_page->Pin();
    while (iter.Next())
    {
        KvError err = DeleteTree(iter.PageId());
        if (err != KvError::NoError)
        {
            idx_page->Unpin();
            return err;
        }
    }
    idx_page->Unpin();
    return KvError::NoError;
}

std::string_view WriteTask::LeftBound(bool is_data_page)
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

std::string WriteTask::RightBound(bool is_data_page)
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

void WriteTask::AdvanceDataPageIter(DataPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

void WriteTask::AdvanceIndexPageIter(IndexPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

KvError WriteTask::WritePage(DataPage &&page)
{
    auto [_, fp_id] = AllocatePage(page.PageId());
    return IoMgr()->WritePage(tbl_ident_, std::move(page), fp_id);
}

KvError WriteTask::WritePage(MemIndexPage *page)
{
    auto [page_id, file_page_id] = AllocatePage(page->PageId());
    page->SetPageId(page_id);
    page->SetFilePageId(file_page_id);
    return IoMgr()->WritePage(tbl_ident_, page, file_page_id);
}

void WriteTask::WritePageCallback(VarPage page)
{
    if (page.index() == 0)
    {
        MemIndexPage *idx_page = std::get<MemIndexPage *>(page);
        index_mgr->FinishIo(cow_meta_.mapper_->GetMapping(), idx_page);
    }
}

KvError BatchWriteTask::SeekStack(std::string_view search_key)
{
    const Comparator *cmp = index_mgr->GetComparator();

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

std::pair<uint32_t, KvError> WriteTask::Seek(std::string_view key)
{
    if (stack_.back()->idx_page_ == nullptr)
    {
        stack_.back()->is_leaf_index_ = true;
        return {UINT32_MAX, KvError::NoError};
    }

    while (true)
    {
        IndexStackEntry *idx_entry = stack_.back().get();
        IndexPageIter &idx_iter = idx_entry->idx_page_iter_;
        idx_iter.Seek(key);
        uint32_t page_id = idx_iter.PageId();
        assert(page_id != UINT32_MAX);
        if (idx_entry->idx_page_->IsPointingToLeaf())
        {
            break;
        }
        assert(!stack_.back()->is_leaf_index_);
        auto [node, err] =
            index_mgr->FindPage(cow_meta_.mapper_->GetMapping(), page_id);
        if (err != KvError::NoError)
        {
            return {UINT32_MAX, err};
        }
        node->Pin();
        stack_.emplace_back(std::make_unique<IndexStackEntry>(node, Options()));
    }
    stack_.back()->is_leaf_index_ = true;
    return {stack_.back()->idx_page_iter_.PageId(), KvError::NoError};
}

std::pair<uint32_t, uint32_t> WriteTask::AllocatePage(uint32_t page_id)
{
    if (page_id != UINT32_MAX)
    {
        if (uint32_t old_fp_id = ToFilePage(page_id); old_fp_id != UINT32_MAX)
        {
            // The page is mapped to a new file page. The old file page will be
            // recycled. However, the old file page shall only be recycled when
            // the old mapping snapshot is destructed, i.e., no one is using the
            // old mapping.
            cow_meta_.old_mapping_->AddFreeFilePage(old_fp_id);
        }
    }

    page_id = (page_id != UINT32_MAX) ? page_id : cow_meta_.mapper_->GetPage();
    uint32_t file_page_id = cow_meta_.mapper_->GetFilePage();
    cow_meta_.mapper_->UpdateMapping(page_id, file_page_id);
    wal_builder_.UpdateMapping(page_id, file_page_id);
    return {page_id, file_page_id};
}

void WriteTask::FreePage(uint32_t page_id)
{
    uint32_t file_page = ToFilePage(page_id);
    cow_meta_.old_mapping_->AddFreeFilePage(file_page);
    cow_meta_.mapper_->FreePage(page_id);
    wal_builder_.UpdateMapping(page_id, UINT32_MAX);
}

uint32_t WriteTask::ToFilePage(uint32_t page_id)
{
    return cow_meta_.mapper_->GetMapping()->ToFilePage(page_id);
}

inline DataPage *BatchWriteTask::TripleElement(uint8_t idx)
{
    return leaf_triple_[idx].IsEmpty() ? nullptr : &leaf_triple_[idx];
}

KvError BatchWriteTask::LoadTripleElement(uint8_t idx, uint32_t page_id)
{
    if (TripleElement(idx))
    {
        return KvError::NoError;
    }
    assert(page_id != UINT32_MAX);
    auto [page, err] = LoadDataPage(tbl_ident_, page_id, ToFilePage(page_id));
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

KvError BatchWriteTask::LeafLinkUpdate(DataPage &&page)
{
    page.SetNextPageId(applying_page_.NextPageId());
    page.SetPrevPageId(applying_page_.PrevPageId());
    leaf_triple_[1] = std::move(page);
    return ShiftLeafLink();
}

KvError BatchWriteTask::LeafLinkInsert(DataPage &&page)
{
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(page);
    DataPage &new_elem = leaf_triple_[1];
    if (TripleElement(0) == nullptr)
    {
        // Add first element into empty link list
        assert(stack_.back()->idx_page_iter_.PageId() == UINT32_MAX);
        new_elem.SetNextPageId(UINT32_MAX);
        new_elem.SetPrevPageId(UINT32_MAX);
        return ShiftLeafLink();
    }
    DataPage *prev_page = TripleElement(0);
    assert(stack_.back()->idx_page_iter_.PageId() == UINT32_MAX ||
           prev_page->NextPageId() == applying_page_.NextPageId());
    if (prev_page->NextPageId() != UINT32_MAX)
    {
        KvError err = LoadTripleElement(2, prev_page->NextPageId());
        CHECK_KV_ERR(err);
        TripleElement(2)->SetPrevPageId(new_elem.PageId());
    }
    new_elem.SetPrevPageId(prev_page->PageId());
    new_elem.SetNextPageId(prev_page->NextPageId());
    prev_page->SetNextPageId(new_elem.PageId());
    return ShiftLeafLink();
}

KvError BatchWriteTask::LeafLinkDelete()
{
    if (applying_page_.PrevPageId() != UINT32_MAX)
    {
        KvError err = LoadTripleElement(0, applying_page_.PrevPageId());
        CHECK_KV_ERR(err);
        TripleElement(0)->SetNextPageId(applying_page_.NextPageId());
    }
    if (applying_page_.NextPageId() != UINT32_MAX)
    {
        assert(!TripleElement(2));
        KvError err = LoadTripleElement(2, applying_page_.NextPageId());
        CHECK_KV_ERR(err);
        TripleElement(2)->SetPrevPageId(applying_page_.PrevPageId());
    }
    return KvError::NoError;
}

KvError WriteTask::FlushManifest(const MemIndexPage *root)
{
    uint32_t root_pg_id = root ? root->PageId() : UINT32_MAX;
    std::string_view blob = wal_builder_.Finalize(root_pg_id);

    if (cow_meta_.manifest_size_ > 0 &&
        cow_meta_.manifest_size_ + blob.size() <= Options()->manifest_limit)
    {
        KvError err =
            IoMgr()->AppendManifest(tbl_ident_, blob, cow_meta_.manifest_size_);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ += blob.size();
    }
    else
    {
        blob = wal_builder_.Snapshot(root_pg_id, *cow_meta_.mapper_);
        KvError err = IoMgr()->SwitchManifest(tbl_ident_, blob);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ = blob.size();
    }
    return KvError::NoError;
}

KvError WriteTask::UpdateMeta(MemIndexPage *root)
{
    cow_meta_.old_mapping_ = nullptr;
    KvError err = IoMgr()->FlushData(tbl_ident_);
    CHECK_KV_ERR(err);
    if (!wal_builder_.Empty())
    {
        err = FlushManifest(root);
        CHECK_KV_ERR(err);
    }
    index_mgr->UpdateRoot(tbl_ident_,
                          root,
                          std::move(cow_meta_.mapper_),
                          cow_meta_.manifest_size_);
    return KvError::NoError;
}

bool BatchWriteTask::SetBatch(std::vector<WriteDataEntry> &&entries)
{
#ifndef NDEBUG
    const Comparator *cmp = Comp();
    if (entries.size() > 1)
    {
        // Ensure the input batch keys are unique and ordered
        for (uint64_t i = 1; i < entries.size(); i++)
        {
            if (cmp->Compare(entries[i - 1].key_, entries[i].key_) >= 0)
            {
                return false;
            }
        }
    }
#endif
    batch_ = std::move(entries);
    return true;
}

void BatchWriteTask::Abort()
{
    WriteTask::Abort();

    for (DataPage &page : leaf_triple_)
    {
        page.Clear();
    }
    batch_.clear();
}

KvError BatchWriteTask::Apply()
{
    KvError err = index_mgr->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);

    wal_builder_.Reset();

    stack_.emplace_back(
        std::make_unique<IndexStackEntry>(cow_meta_.root_, Options()));
    if (cow_meta_.root_ != nullptr)
    {
        cow_meta_.root_->Pin();
    }

    size_t cidx = 0;
    while (cidx < batch_.size())
    {
        std::string_view batch_start_key = {batch_[cidx].key_.data(),
                                            batch_[cidx].key_.size()};
        if (stack_.size() > 1)
        {
            err = SeekStack(batch_start_key);
            CHECK_KV_ERR(err);
        }
        auto [page_id, err] = Seek(batch_start_key);
        CHECK_KV_ERR(err);
        if (page_id != UINT32_MAX)
        {
            err = LoadApplyingPage(page_id);
            CHECK_KV_ERR(err);
        }
        err = ApplyOnePage(cidx);
        CHECK_KV_ERR(err);
    }
    // Flush all dirty leaf data pages in leaf_triple_ .
    assert(TripleElement(2) == nullptr);
    err = ShiftLeafLink();
    CHECK_KV_ERR(err);
    err = ShiftLeafLink();
    CHECK_KV_ERR(err);
    batch_.clear();

    assert(!stack_.empty());
    MemIndexPage *new_root = nullptr;
    while (!stack_.empty())
    {
        auto [new_page, err] = Pop();
        CHECK_KV_ERR(err);
        new_root = new_page;
    }

    return UpdateMeta(new_root);
}

KvError BatchWriteTask::LoadApplyingPage(uint32_t page_id)
{
    assert(page_id != UINT32_MAX);
    // Now we are going to fetch a data page before execute ApplyOnePage.
    // But this page may already exists at leaf_triple_[1], because it may be
    // loaded by previous ApplyOnePage for linking purpose.
    if (TripleElement(1) && TripleElement(1)->PageId() == page_id)
    {
        // Fast path: leaf_triple_[1] is exactly the page we want. Just move it
        // to avoid a disk access.
        applying_page_.Clear();
        applying_page_ = std::move(*TripleElement(1));
    }
    else
    {
        auto [page, err] =
            LoadDataPage(tbl_ident_, page_id, ToFilePage(page_id));
        CHECK_KV_ERR(err);
        applying_page_ = std::move(page);
    }
    assert(TypeOfPage(applying_page_.PagePtr()) == PageType::Data);

    if (TripleElement(1))
    {
        assert(TripleElement(1)->PageId() != applying_page_.PageId());
        KvError err = ShiftLeafLink();
        CHECK_KV_ERR(err);
    }
    if (TripleElement(0) &&
        TripleElement(0)->PageId() != applying_page_.PrevPageId())
    {
        // leaf_triple_[0] is not the previously adjacent page of the
        // applying page.
        KvError err = ShiftLeafLink();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError BatchWriteTask::ApplyOnePage(size_t &cidx)
{
    assert(!stack_.empty());

    DataPage *base_page = nullptr;
    std::string_view page_left_bound{};
    std::string page_right_key;
    std::string_view page_right_bound{};

    if (stack_.back()->idx_page_iter_.PageId() != UINT32_MAX)
    {
        assert(stack_.back()->idx_page_iter_.PageId() ==
               applying_page_.PageId());
        base_page = &applying_page_;
        page_left_bound = LeftBound(true);
        page_right_key = RightBound(true);
        page_right_bound = {page_right_key.data(), page_right_key.size()};
    }

    const Comparator *cmp = index_mgr->GetComparator();
    DataPageIter base_page_iter{base_page, Options()};
    bool is_base_iter_valid = false;
    AdvanceDataPageIter(base_page_iter, is_base_iter_valid);

    data_page_builder_.Reset();

    assert(cidx < batch_.size());
    std::string_view change_key = {batch_[cidx].key_.data(),
                                   batch_[cidx].key_.size()};
    assert(cmp->Compare(page_left_bound, change_key) <= 0);
    assert(page_right_bound.empty() ||
           cmp->Compare(page_left_bound, page_right_bound) < 0);

    auto change_it = batch_.begin() + cidx;
    auto change_end_it = std::lower_bound(
        change_it,
        batch_.end(),
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

    uint32_t page_id = UINT32_MAX;
    if (base_page != nullptr)
    {
        page_id = base_page->PageId();
        assert(page_key <= base_page_iter.Key());
    }

    auto add_to_page =
        [&](std::string_view key, std::string_view val, uint64_t ts)
    {
        bool success = data_page_builder_.Add(key, val, ts);
        if (!success)
        {
            // Finishes the current page.
            std::string_view page_view = data_page_builder_.Finish();
            KvError err =
                FinishDataPage(page_view, std::move(curr_page_key), page_id);
            CHECK_KV_ERR(err);
            // Starts a new page.
            curr_page_key = cmp->FindShortestSeparator(
                {prev_key.data(), prev_key.size()}, key);
            assert(!prev_key.empty() && prev_key < curr_page_key);
            data_page_builder_.Reset();
            success = data_page_builder_.Add(key, val, ts);
            // Doesn't support a single key-value pair spanning more than one
            // page for now.
            assert(success);
            page_id = UINT32_MAX;
        }
        assert(curr_page_key <= key);
        prev_key = key;
        return KvError::NoError;
    };

    while (is_base_iter_valid && change_it != change_end_it)
    {
        std::string_view base_key = base_page_iter.Key();
        std::string_view base_val = base_page_iter.Value();
        uint64_t base_ts = base_page_iter.Timestamp();

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
        AdvanceType adv_type;

        int cmp_ret = cmp->Compare(base_key, change_key);
        if (cmp_ret < 0)
        {
            new_key = base_key;
            new_val = base_val;
            new_ts = base_ts;
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (change_ts > base_ts)
            {
                if (change_it->op_ == WriteOp::Delete)
                {
                    new_key = std::string_view{};
                }
                else
                {
                    new_key = change_key;
                    new_val = change_val;
                    new_ts = change_ts;
                }
            }
            else
            {
                new_key = base_key;
                new_val = base_val;
                new_ts = base_ts;
            }
        }
        else
        {
            adv_type = AdvanceType::Changes;
            if (change_it->op_ == WriteOp::Delete)
            {
                new_key = std::string_view{};
            }
            else
            {
                new_key = change_key;
                new_val = change_val;
                new_ts = change_ts;
            }
        }

        if (!new_key.empty())
        {
            KvError err = add_to_page(new_key, new_val, new_ts);
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
        std::string_view new_key = base_page_iter.Key();
        std::string_view new_val = base_page_iter.Value();
        uint64_t new_ts = base_page_iter.Timestamp();
        KvError err = add_to_page(new_key, new_val, new_ts);
        CHECK_KV_ERR(err);
        AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
    }

    while (change_it != change_end_it)
    {
        if (change_it->op_ != WriteOp::Delete)
        {
            std::string_view new_key{change_it->key_.data(),
                                     change_it->key_.size()};
            std::string_view new_val{change_it->val_.data(),
                                     change_it->val_.size()};
            uint64_t new_ts = change_it->timestamp_;
            KvError err = add_to_page(new_key, new_val, new_ts);
            CHECK_KV_ERR(err);
        }
        ++change_it;
    }

    if (data_page_builder_.IsEmpty())
    {
        if (base_page)
        {
            KvError err = LeafLinkDelete();
            CHECK_KV_ERR(err);
            FreePage(applying_page_.PageId());
            assert(stack_.back()->changes_.empty() ||
                   stack_.back()->changes_.back().key_ < curr_page_key);
            stack_.back()->changes_.emplace_back(
                std::move(curr_page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        std::string_view page_view = data_page_builder_.Finish();
        KvError err =
            FinishDataPage(page_view, std::move(curr_page_key), page_id);
        CHECK_KV_ERR(err);
    }
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(leaf_triple_[2]);

    cidx = cidx + std::distance(batch_.begin() + cidx, change_end_it);
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

    const Comparator *cmp = index_mgr->GetComparator();
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

    // We keep the previous built page in the pipeline before flushing it to
    // storage. This is to redistribute between last two pages in case the last
    // page is sparse.
    MemIndexPage *prev_page = nullptr;
    MemIndexPage *curr_page = nullptr;
    std::string prev_page_key{};
    std::string_view page_key =
        stack_.size() == 1 ? std::string_view{}
                           : stack_[stack_.size() - 2]->idx_page_iter_.Key();
    std::string curr_page_key{page_key};

    uint32_t page_id = UINT32_MAX;
    if (stack_page != nullptr)
    {
        page_id = stack_page->PageId();
    }

    auto add_to_page = [&](std::string_view new_key,
                           uint32_t new_page_id) -> KvError
    {
        bool success =
            idx_page_builder_.Add(new_key, new_page_id, is_leaf_index);
        if (!success)
        {
            curr_page = index_mgr->AllocIndexPage();
            if (curr_page == nullptr)
            {
                return KvError::OutOfMem;
            }
            // The page is full.
            std::string_view page_view = idx_page_builder_.Finish();
            memcpy(curr_page->PagePtr(), page_view.data(), page_view.size());

            if (prev_page != nullptr)
            {
                // The update results in an index page split, because there is
                // at least one new index page to come. Flushes the previously
                // built index page and elevates it to the parent in the stack.
                KvError err = FinishIndexPage(
                    prev_page, std::move(prev_page_key), page_id, true);
                if (err != KvError::NoError)
                {
                    index_mgr->FreeIndexPage(prev_page);
                    prev_page = nullptr;
                    return err;
                }

                // The first split index page shares the same page Id with the
                // original one. The following index pages will have new page
                // Id's.
                page_id = UINT32_MAX;
            }

            prev_page = curr_page;
            prev_page_key = std::move(curr_page_key);
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
        uint32_t base_page_id = base_page_iter.PageId();
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
                new_page_id = UINT32_MAX;
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
        if (!new_key.empty() || new_page_id != UINT32_MAX)
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
        uint32_t new_page_id = base_page_iter.PageId();
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

    bool elevate;
    if (prev_page != nullptr)
    {
        KvError err =
            FinishIndexPage(prev_page, std::move(prev_page_key), page_id, true);
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        page_id = UINT32_MAX;
        // The index page is split into two or more pages, all of which are
        // elevated.
        elevate = true;
    }
    else
    {
        // The update does not yield a page split. The new page has the same
        // page Id as the original one. It is mapped to a new file page and is
        // not elevated to its parent page.
        elevate = false;
    }

    if (idx_page_builder_.IsEmpty())
    {
        FreePage(stack_.back()->idx_page_->PageId());
        if (stack_.size() > 1)
        {
            IndexStackEntry *parent = stack_[stack_.size() - 2].get();
            std::string_view page_key = parent->idx_page_iter_.Key();
            parent->changes_.emplace_back(
                std::string(page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        curr_page = index_mgr->AllocIndexPage();
        if (curr_page == nullptr)
        {
            return {nullptr, KvError::OutOfMem};
        }
        std::string_view page_view = idx_page_builder_.Finish();
        memcpy(curr_page->PagePtr(), page_view.data(), page_view.size());
        KvError err = FinishIndexPage(
            curr_page, std::move(curr_page_key), page_id, elevate);
        if (err != KvError::NoError)
        {
            index_mgr->FreeIndexPage(curr_page);
            return {nullptr, err};
        }
    }

    if (stack_page != nullptr)
    {
        stack_page->Unpin();
    }
    stack_.pop_back();
    return {curr_page, KvError::NoError};
}

KvError BatchWriteTask::FinishIndexPage(MemIndexPage *idx_page,
                                        std::string idx_page_key,
                                        uint32_t page_id,
                                        bool elevate)
{
    // Flushes the built index page.
    idx_page->SetPageId(page_id);
    KvError err = WritePage(idx_page);
    CHECK_KV_ERR(err);

    // The index page is linked to the parent.
    if (elevate)
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
            std::move(idx_page_key), idx_page->PageId(), WriteOp::Upsert);
    }
    return KvError::NoError;
}

KvError BatchWriteTask::FinishDataPage(std::string_view page_view,
                                       std::string page_key,
                                       uint32_t page_id)
{
    uint32_t new_page_id =
        page_id == UINT32_MAX ? cow_meta_.mapper_->GetPage() : page_id;
    DataPage new_page(new_page_id, Options()->data_page_size);
    memcpy(new_page.PagePtr(), page_view.data(), page_view.size());

    if (page_id == UINT32_MAX)
    {
        // This is a new data page that does not exist in the tree and has a new
        // page Id.
        KvError err = LeafLinkInsert(std::move(new_page));
        CHECK_KV_ERR(err);

        // This is a new page that does not exist in the parent index page.
        // Elevates to the parent index page.
        assert(stack_.back()->changes_.empty() ||
               stack_.back()->changes_.back().key_ < page_key);
        stack_.back()->changes_.emplace_back(
            std::move(page_key), new_page_id, WriteOp::Upsert);
    }
    else
    {
        // This is an existing data page with updated content.
        KvError err = LeafLinkUpdate(std::move(new_page));
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

std::pair<MemIndexPage *, KvError> TruncateTask::TruncateIndexPage(
    uint32_t page_id, std::string_view trunc_pos)
{
    auto [idx_page, err] =
        index_mgr->FindPage(cow_meta_.mapper_->GetMapping(), page_id);
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }

    const bool is_leaf_idx = idx_page->IsPointingToLeaf();
    IndexPageBuilder builder(Options());

    auto truncate_sub_node = [&](std::string_view sub_node_key,
                                 uint32_t sub_node_id) -> KvError
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
                                               uint32_t sub_node_id) -> KvError
    {
        // delete whole sub-node
        if (is_leaf_idx)
        {
            FreePage(sub_node_id);
            return KvError::NoError;
        }
        else
        {
            return DeleteTree(sub_node_id);
        }
    };

    IndexPageIter iter(idx_page, Options());
    CHECK(iter.Next());
    std::string sub_node_key = {};
    uint32_t sub_node_id = iter.PageId();

    // Depth first search recursively
    bool cut_point_found = false;
    idx_page->Pin();
    while (iter.Next())
    {
        std::string_view next_node_key = iter.Key();
        uint32_t next_node_id = iter.PageId();

        if (cut_point_found)
        {
            // Delete all sub-nodes bigger than cutting point.
            err = delete_sub_node(sub_node_key, sub_node_id);
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
                err = delete_sub_node(sub_node_key, sub_node_id);
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
        err = delete_sub_node(sub_node_key, sub_node_id);
    }
    else if (Comp()->Compare(trunc_pos, sub_node_key) > 0)
    {
        err = truncate_sub_node(sub_node_key, sub_node_id);
    }
    else
    {
        err = delete_sub_node(sub_node_key, sub_node_id);
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
    MemIndexPage *new_page = index_mgr->AllocIndexPage();
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
        index_mgr->FreeIndexPage(new_page);
        return {nullptr, err};
    }
    return {new_page, KvError::NoError};
}

KvError TruncateTask::Truncate(std::string_view trunc_pos)
{
    KvError err = index_mgr->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);
    if (cow_meta_.root_ == nullptr)
    {
        return KvError::NotFound;
    }
    wal_builder_.Reset();

    MemIndexPage *new_root = nullptr;
    if (trunc_pos.empty())
    {
        DeleteTree(cow_meta_.root_->PageId());
    }
    else
    {
        auto ret = TruncateIndexPage(cow_meta_.root_->PageId(), trunc_pos);
        CHECK_KV_ERR(ret.second);
        new_root = ret.first;
    }
    return UpdateMeta(new_root);
}

std::pair<bool, KvError> TruncateTask::TruncateDataPage(
    uint32_t page_id, std::string_view trunc_pos)
{
    auto [page, err] = LoadDataPage(tbl_ident_, page_id, ToFilePage(page_id));
    if (err != KvError::NoError)
    {
        return {true, err};
    }

    const Comparator *cmp = index_mgr->GetComparator();
    DataPageIter page_iter{&page, Options()};
    data_page_builder_.Reset();
    while (page_iter.Next() && cmp->Compare(page_iter.Key(), trunc_pos) < 0)
    {
        bool ok = data_page_builder_.Add(
            page_iter.Key(), page_iter.Value(), page_iter.Timestamp());
        assert(ok);
    }

    if (data_page_builder_.IsEmpty())
    {
        FreePage(page.PageId());

        uint32_t prev_page_id = page.PrevPageId();
        if (prev_page_id == UINT32_MAX)
        {
            return {false, KvError::NoError};
        }
        // The previous data page will become the new tail data page.
        // We don't need to update the previous page id of the next data page.
        auto [prev_page, err] =
            LoadDataPage(tbl_ident_, prev_page_id, ToFilePage(prev_page_id));
        if (err != KvError::NoError)
        {
            return {false, err};
        }
        prev_page.SetNextPageId(UINT32_MAX);
        err = WritePage(std::move(prev_page));
        return {false, err};
    }
    else
    {
        // This currently updated data page will become the new tail data page.
        DataPage new_page(page_id, Options()->data_page_size);
        std::string_view page_view = data_page_builder_.Finish();
        memcpy(new_page.PagePtr(), page_view.data(), page_view.size());
        new_page.SetNextPageId(UINT32_MAX);
        new_page.SetPrevPageId(page.PrevPageId());
        err = WritePage(std::move(new_page));
        return {true, err};
    }
}

}  // namespace kvstore