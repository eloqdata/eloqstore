#pragma once

#include <filesystem>
#include <thread>
#include <unordered_set>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "types.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift);

class FileGarbageCollector
{
public:
    explicit FileGarbageCollector(const KvOptions *opts) : options_(opts) {};
    ~FileGarbageCollector();
    void Start(uint16_t n_workers);
    void Stop();
    bool AddTask(TableIdent tbl_id,
                 uint64_t ts,
                 FileId max_file_id,
                 std::unordered_set<FileId> retained_files);

    static KvError Execute(const KvOptions *opts,
                           const std::filesystem::path &dir_path,
                           uint64_t mapping_ts,
                           FileId max_file_id,
                           std::unordered_set<FileId> retained_files);

private:
    void GCRoutine();

    struct GcTask
    {
        GcTask() = default;
        GcTask(TableIdent tbl_id,
               uint64_t ts,
               FileId max_file_id,
               std::unordered_set<FileId> retained_files);
        bool IsStopSignal() const;

        TableIdent tbl_id_;
        uint64_t mapping_ts_{0};
        FileId max_file_id_{0};
        std::unordered_set<FileId> retained_files_;
    };

    const KvOptions *options_;
    moodycamel::BlockingConcurrentQueue<GcTask> tasks_;
    std::vector<std::thread> workers_;
};

}  // namespace eloqstore