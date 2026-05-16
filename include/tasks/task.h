#pragma once

#include <boost/context/continuation.hpp>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include "comparator.h"
#include "compression.h"
#include "error.h"
#include "storage/data_page.h"
#include "types.h"

namespace eloqstore
{
class KvRequest;
class KvTask;
class IndexPageManager;
class AsyncIoManager;
class IoStringBuffer;
struct KvOptions;
class TaskManager;
class PagesPool;
class MappingSnapshot;
class Shard;
class EloqStore;

inline EloqStore *eloq_store;
inline thread_local Shard *shard;
KvTask *ThdTask();
AsyncIoManager *IoMgr();
const KvOptions *Options();
const Comparator *Comp();

enum class TaskStatus : uint8_t
{
    Idle = 0,
    Ongoing,
    Blocked,
    BlockedIO,
    Finished
};

enum struct TaskType
{
    Read = 0,
    Scan,
    EvictFile,
    Prewarm,
    ListObject,
    ListStandbyPartition,
    BatchWrite,
    Reopen,
    BackgroundWrite
};

std::pair<Page, KvError> LoadPage(const TableIdent &tbl_id,
                                  FilePageId file_page_id);
std::pair<DataPage, KvError> LoadDataPage(const TableIdent &tbl_id,
                                          PageId page_id,
                                          FilePageId file_page_id);
std::pair<OverflowPage, KvError> LoadOverflowPage(const TableIdent &tbl_id,
                                                  PageId page_id,
                                                  FilePageId file_page_id);

/**
 * @brief Get overflow value.
 * @param tbl_id The table partition identifier.
 * @param mapping The mapping snapshot of this table partition.
 * @param encoded_ptrs The encoded overflow pointers.
 * @param[out] value Output string to receive the assembled overflow value.
 */
KvError GetOverflowValue(const TableIdent &tbl_id,
                         const MappingSnapshot *mapping,
                         std::string_view encoded_ptrs,
                         std::string &value);

class GlobalRegisteredMemory;

/**
 * @brief Read a large value stored in segment files into an IoStringBuffer.
 *
 * Segments are allocated from @p global_mem and appended to @p large_value
 * one per K. The metadata trailer (if any) is parsed but not surfaced here -
 * the caller should call DecodeLargeValueHeader / DecodeLargeValueContent
 * separately if metadata is needed.
 *
 * @param tbl_id The table partition identifier.
 * @param seg_mapping The segment mapping snapshot.
 * @param encoded_content The encoded large value content from the data page.
 * @param large_value Output IoStringBuffer to receive the segments.
 * @param global_mem The global registered memory for segment allocation.
 * @param reg_mem_index_base The io_uring buffer index base for global memory.
 * @param yield Yield function for coroutine context when allocating segments.
 */
KvError GetLargeValue(const TableIdent &tbl_id,
                      const MappingSnapshot *seg_mapping,
                      std::string_view encoded_content,
                      IoStringBuffer &large_value,
                      GlobalRegisteredMemory *global_mem,
                      uint16_t reg_mem_index_base,
                      std::function<void()> yield);

/**
 * @brief Read a large value stored in segment files into contiguous pinned
 * memory (KV Cache mode).
 *
 * @param tbl_id The table partition identifier.
 * @param seg_mapping The segment mapping snapshot.
 * @param encoded_content The encoded large value content from the data page.
 * @param dst Pinned memory base where the value is written. Must lie within
 *   a single chunk registered with this shard's io_uring; resolved to a
 *   buffer index via IouringMgr::BufIndexForAddress once and reused for all
 *   segments.
 * @param dst_size Capacity at @p dst. Must satisfy
 *   `(K - 1) * segment_size + 4096 <= dst_size <= K * segment_size`. The final
 *   segment is read with `dst_size - (K - 1) * segment_size` bytes.
 * @param io_mgr The IO manager that owns the io_uring and the buf-index
 *   resolution for the pinned chunks.
 */
KvError GetLargeValueContiguous(const TableIdent &tbl_id,
                                const MappingSnapshot *seg_mapping,
                                std::string_view encoded_content,
                                char *dst,
                                size_t dst_size,
                                AsyncIoManager *io_mgr);

/**
 * @brief Resolve the iterator's entry into @p value as either an inline value
 * or the metadata trailer.
 *
 * - Non-large entry: writes the decoded inline / overflow / compressed bytes.
 * - Large entry: writes the metadata blob (empty if the entry carries no
 *   metadata trailer). The segment bytes are not touched -- the caller invokes
 *   GetLargeValue or GetLargeValueContiguous separately when they need the
 *   value's payload.
 *
 * Asserting `mapping` and `compression` are used directly here (for overflow
 * resolution and compression respectively); no other plumbing is hidden in
 * delegation.
 */
KvError ResolveValueOrMetadata(const TableIdent &tbl_id,
                               MappingSnapshot *mapping,
                               DataPageIter &iter,
                               std::string &value,
                               const compression::DictCompression *compression,
                               bool extract_metadata = true);
/**
 * @brief Decode overflow pointers.
 * @param encoded The encoded overflow pointers.
 * @param pointers The buffer to store the decoded overflow pointers.
 * @return The number of decoded overflow pointers.
 */
uint8_t DecodeOverflowPointers(
    std::string_view encoded,
    std::span<PageId, max_overflow_pointers> pointers);
uint8_t DecodeOverflowPointers(
    std::string_view encoded,
    std::span<FilePageId, max_overflow_pointers> pointers,
    const MappingSnapshot *mapping);

using boost::context::continuation;
class KvTask
{
public:
    virtual ~KvTask() = default;
    virtual TaskType Type() const = 0;
    virtual void Abort() {};
    bool ReadOnly() const
    {
        return Type() < TaskType::BatchWrite;
    }
    void Yield();
    void YieldToLowPQ();
    /**
     * @brief Re-schedules the task to run. Note: the resumed task does not run
     * in place.
     *
     */
    void Resume();

    int WaitIoResult();
    void WaitIo();
    void FinishIo();

    uint32_t inflight_io_{0};
    int io_res_{0};
    uint32_t io_flags_{0};
    bool needs_auto_reopen_{false};

    TaskStatus status_{TaskStatus::Idle};
    KvRequest *req_{nullptr};
    continuation coro_;
    KvTask *next_{nullptr};
};

class WaitingZone
{
public:
    WaitingZone() = default;
    void Wait(KvTask *task);
    void WakeOne();
    void WakeN(size_t n);
    void WakeAll();
    bool Empty() const;

private:
    void PushBack(KvTask *task);
    KvTask *PopFront();

    KvTask *head_{nullptr}, *tail_{nullptr};
};

class WaitingSeat
{
public:
    void Wait(KvTask *task);
    void Wake();

private:
    KvTask *task_{nullptr};
};

class Mutex
{
public:
    void Lock();
    void Unlock();

private:
    bool locked_{false};
    WaitingZone waiting_;
};
}  // namespace eloqstore
