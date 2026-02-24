#pragma once

#include <utility>
#include <vector>

#include "tasks/write_task.h"

namespace eloqstore
{
class BackgroundWrite : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::BackgroundWrite;
    }
    /**
     * @brief Compact data files with a low utilization rate. Copy all pages
     * referenced by the latest mapping and append them to the latest data file.
     */
    KvError CompactDataFile();

    KvError CreateArchive(uint64_t provided_ts = 0);

    KvError RunLocalFileGc();

private:
    void HeapSortFpIdsWithYield(
        std::vector<std::pair<FilePageId, PageId>> &fp_ids);
};
}  // namespace eloqstore
