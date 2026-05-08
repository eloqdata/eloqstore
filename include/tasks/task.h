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
 * @brief Read a large value stored in segment files.
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
 * @brief Resolve the value at the iterator's current position.
 *
 * Writes the decoded value bytes directly into @p value. If the entry is a
 * large value, @p large_value is populated instead and @p value is left
 * unchanged.
 */
KvError ResolveValue(const TableIdent &tbl_id,
                     MappingSnapshot *mapping,
                     DataPageIter &iter,
                     std::string &value,
                     const compression::DictCompression *compression,
                     MappingSnapshot *seg_mapping = nullptr,
                     IoStringBuffer *large_value = nullptr,
                     GlobalRegisteredMemory *global_mem = nullptr,
                     uint16_t reg_mem_index_base = 0,
                     std::function<void()> yield = nullptr);
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
