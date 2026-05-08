#pragma once

#include <utility>
#include <vector>

#include "tasks/write_task.h"

namespace eloqstore
{
class MovingCachedPages;

class BackgroundWrite : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::BackgroundWrite;
    }

    /**
     * @brief Run compaction on data files and segment files in a single pass:
     * one `MakeCowRoot`, up to two rewrite passes (data pages, then segments),
     * one `UpdateMeta` flushing a single manifest record, and one
     * `TriggerFileGC`.
     *
     * This method is unconditional -- callers are responsible for deciding
     * that compaction is warranted. The pending-compact dispatch path is
     * already gated by `WriteTask::CompactIfNeeded`; `CreateArchive` runs its
     * own per-mapper amplification check before invoking.
     *
     * Per-file under-utilization is re-evaluated inside each rewrite pass, so
     * files above their amplification threshold are skipped cheaply.
     */
    KvError Compact();

    KvError CreateArchive(std::string_view tag = {});

    KvError RunLocalFileGc();

    KvError CreateBranch(std::string_view branch_name);

    KvError DeleteBranch(std::string_view branch_name);

private:
    void HeapSortFpIdsWithYield(
        std::vector<std::pair<FilePageId, PageId>> &fp_ids);

    /**
     * @brief Rewrite loop for under-utilized data files. Operates on
     * `cow_meta_.mapper_`; does NOT call UpdateMeta or TriggerFileGC -- the
     * caller (`Compact`) batches the flush across both passes.
     */
    KvError DoCompactDataFile(MovingCachedPages &moving_cached);

    /**
     * @brief Rewrite loop for under-utilized segment files. Operates on
     * `cow_meta_.segment_mapper_`; does NOT call UpdateMeta or TriggerFileGC
     * for the same reason as DoCompactDataFile.
     */
    KvError DoCompactSegmentFile();
};
}  // namespace eloqstore
