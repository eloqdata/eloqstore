#pragma once

#include <filesystem>
#include <thread>
#include <unordered_set>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "object_store.h"
#include "types.h"
// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift);
class ObjectStore;
class FileGarbageCollector
{
public:
    struct GcTask
    {
        GcTask() = default;
        GcTask(TableIdent tbl_id,
               uint64_t ts,
               FileId max_file_id,
               std::unordered_set<FileId> retained_files)
            : tbl_id_(std::move(tbl_id)),
              mapping_ts_(ts),
              max_file_id_(max_file_id),
              retained_files_(std::move(retained_files))
        {
        }
        bool IsStopSignal() const
        {
            return !tbl_id_.IsValid();
        }

        TableIdent tbl_id_;
        uint64_t mapping_ts_{0};
        FileId max_file_id_{0};
        std::unordered_set<FileId> retained_files_;
    };

    explicit FileGarbageCollector(const KvOptions *opts) : options_(opts){};
    virtual ~FileGarbageCollector() = default;
    void Start(uint16_t n_workers);
    void Stop();
    bool AddTask(TableIdent tbl_id,
                 uint64_t ts,
                 FileId max_file_id,
                 std::unordered_set<FileId> retained_files);

    virtual KvError Execute(const GcTask &task) = 0;

protected:
    const KvOptions *options_;

    virtual void WorkerRoutine() = 0;
    virtual const char *GetCollectorName() const = 0;

    std::vector<std::thread> workers_;
    moodycamel::BlockingConcurrentQueue<GcTask> tasks_;
};

class LocalFileGarbageCollector : public FileGarbageCollector
{
public:
    explicit LocalFileGarbageCollector(const KvOptions *options)
        : FileGarbageCollector(options)
    {
    }
    ~LocalFileGarbageCollector() override;

    KvError Execute(const GcTask &task) override;

private:
    void WorkerRoutine() override;
    const char *GetCollectorName() const override
    {
        return "local file garbage collector";
    }
};

// use forward declaration to get eloqstore and then use the object_store
class ObjectStore;
class CloudFileGarbageCollector : public FileGarbageCollector
{
public:
    CloudFileGarbageCollector(const KvOptions *options,
                              ObjectStore *object_store)
        : FileGarbageCollector(options), object_store_(object_store)
    {
    }
    ~CloudFileGarbageCollector() override;

    KvError Execute(const GcTask &task) override;

private:
    void WorkerRoutine() override;
    const char *GetCollectorName() const override
    {
        return "cloud file garbage collector";
    }
    KvError ExecuteCloudGC(const GcTask &task);
    KvError ProcessManifestFiles(
        const std::string &table_path,
        uint64_t mapping_ts,
        std::unordered_set<FileId> &all_retained_files);

    ObjectStore *object_store_;
};

}  // namespace eloqstore