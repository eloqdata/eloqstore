#pragma once
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "task.h"

namespace utils
{
struct CloudObjectInfo;
}  // namespace utils

namespace eloqstore
{
class EloqStore;
class CloudStoreMgr;

struct PrewarmFile
{
    TableIdent tbl_id;
    FileId file_id;
    size_t file_size;
    bool is_manifest;
    std::string mod_time;
};

class PrewarmTask : public KvTask
{
public:
    explicit PrewarmTask(CloudStoreMgr *io_mgr);

    TaskType Type() const override
    {
        return TaskType::Prewarm;
    }

    void Run();
    void Shutdown();

    bool HasPending() const;

private:
    friend class CloudStoreMgr;
    friend class PrewarmService;

    bool PopNext(PrewarmFile &file);
    void Clear();

    CloudStoreMgr *io_mgr_;
    std::vector<PrewarmFile> pending_;
    size_t next_index_{0};
    bool stop_{true};
    bool shutting_down_{false};
};

class PrewarmService
{
public:
    explicit PrewarmService(EloqStore *store);
    ~PrewarmService();

    void Start();
    void Stop();

private:
    friend class EloqStore;
    friend class CloudStoreMgr;
    void PrewarmCloudCache();
    bool ListCloudObjects(const std::string &remote_path,
                          std::vector<utils::CloudObjectInfo> &details);
    bool ExtractPartition(const std::string &path,
                          TableIdent &tbl_id,
                          std::string &filename);

    EloqStore *store_;
    std::thread thread_;
};
}  // namespace eloqstore
