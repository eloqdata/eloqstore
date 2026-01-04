#pragma once

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

    KvError CreateArchive();
};
}  // namespace eloqstore