#pragma once

#include <atomic>
#include <cstdint>
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
class PrewarmRequest;
enum class RequestType : uint8_t;

class PrewarmTask : public KvTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Prewarm;
    }

    KvError Prewarm(const PrewarmRequest &request);
};

class PrewarmService
{
public:
    explicit PrewarmService(EloqStore *store);
    ~PrewarmService();

    void Start();
    void Stop();
    void Cancel();
    bool IsCancelled() const;

private:
    void ThreadMain();
    void PrewarmCloudCache();
    bool ListCloudObjects(const std::string &remote_path,
                          std::vector<utils::CloudObjectInfo> &details);
    bool ExtractPartition(const std::string &path,
                          TableIdent &tbl_id,
                          std::string &filename);

    EloqStore *store_;
    std::thread thread_;
    std::atomic<bool> cancelled_{true};
};
}  // namespace eloqstore
