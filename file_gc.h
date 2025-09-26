#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "object_store.h"
#include "types.h"
// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift);
class ObjectStore;
class FileGarbageCollector
{
public:
    FileGarbageCollector() = default;
    ~FileGarbageCollector() = default;

    // Local mode method (direct execution)
    KvError ExecuteLocalGC(const TablePartitionIdent &tbl_id,
                           const std::unordered_set<FileId> &retained_files,
                           class IouringMgr *io_mgr);

    // Cloud mode method (coroutine-based)
    KvError ExecuteCloudGC(const TablePartitionIdent &tbl_id,
                           const std::unordered_set<FileId> &retained_files,
                           class CloudStoreMgr *cloud_mgr);

private:
    // Local mode implementation
    KvError ListLocalFiles(const TablePartitionIdent &tbl_id,
                           std::vector<std::string> &local_files,
                           class IouringMgr *io_mgr);

    KvError DeleteUnreferencedLocalFiles(
        const TablePartitionIdent &tbl_id,
        const std::vector<std::string> &data_files,
        const std::unordered_set<FileId> &retained_files,
        FileId least_not_archived_file_id,
        class IouringMgr *io_mgr);

    KvError GetOrUpdateArchivedMaxFileId(
        const TablePartitionIdent &tbl_id,
        const std::vector<std::string> &archive_files,
        const std::vector<uint64_t> &archive_timestamps,
        FileId &archived_max_file_id,
        class IouringMgr *io_mgr);
    // Cloud mode implementation
    KvError ListCloudFiles(const TablePartitionIdent &tbl_id,
                           std::vector<std::string> &cloud_files,
                           class CloudStoreMgr *cloud_mgr);
    void ClassifyFiles(const std::vector<std::string> &files,
                       std::vector<std::string> &archive_files,
                       std::vector<uint64_t> &archive_timestamps,
                       std::vector<std::string> &data_files);
    KvError DownloadArchiveFile(const TablePartitionIdent &tbl_id,
                                const std::string &archive_file,
                                std::string &content,
                                class CloudStoreMgr *cloud_mgr,
                                const KvOptions *options);

    FileId ParseArchiveForMaxFileId(const std::string &archive_content,
                                    class IouringMgr *io_mgr);
    KvError DeleteUnreferencedCloudFiles(
        const TablePartitionIdent &tbl_id,
        const std::vector<std::string> &data_files,
        const std::unordered_set<FileId> &retained_files,
        FileId least_not_archived_file_id,
        class CloudStoreMgr *cloud_mgr);
};

}  // namespace eloqstore