#include "prewarm_task.h"

#include <glog/logging.h>

#include <utility>

#include "async_io_manager.h"
#include "eloq_store.h"
#include "error.h"
#include "shard.h"

namespace eloqstore
{
KvError PrewarmTask::Prewarm(const PrewarmRequest &request)
{
    auto *cloud_mgr = dynamic_cast<CloudStoreMgr *>(shard->IoManager());
    if (cloud_mgr == nullptr)
    {
        return KvError::NoError;
    }

    const TableIdent &tbl_id = request.TableId();
    auto open_file = [&](FileId file_id, bool is_manifest) -> KvError
    {
        KvError err = cloud_mgr->EnsureCached(tbl_id, file_id);
        if (err == KvError::NoError)
        {
            return KvError::NoError;
        }

        if (err == KvError::NotFound)
        {
            // File missing in cloud is not fatal; skip silently.
            LOG(WARNING) << "Prewarm skip missing file "
                         << (is_manifest ? "manifest" : "data file")
                         << " for table " << tbl_id;
            return KvError::NoError;
        }

        if (err == KvError::OutOfSpace || err == KvError::OpenFileLimit)
        {
            LOG(WARNING) << "Prewarm stop for " << tbl_id << ": cannot cache "
                         << (is_manifest ? "manifest" : "data file")
                         << " due to " << ErrorString(err);
            return err;
        }

        if (err == KvError::TryAgain)
        {
            LOG(WARNING) << "Prewarm retryable failure for " << tbl_id << ": "
                         << ErrorString(err);
            return err;
        }

        LOG(WARNING) << "Prewarm failed for " << tbl_id << ": "
                     << ErrorString(err);
        return err;
    };

    if (request.ShouldPrewarmManifest())
    {
        KvError err = open_file(CloudStoreMgr::ManifestFileId(), true);
        if (err != KvError::NoError)
        {
            return err;
        }
    }

    for (FileId file_id : request.DataFiles())
    {
        KvError err = open_file(file_id, false);
        if (err != KvError::NoError)
        {
            return err;
        }
    }

    return KvError::NoError;
}
}  // namespace eloqstore
