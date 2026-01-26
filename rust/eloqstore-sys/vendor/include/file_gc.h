#pragma once
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "error.h"
#include "kv_options.h"
#include "storage/object_store.h"
#include "storage/page_mapper.h"
#include "types.h"
// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE

namespace eloqstore
{
void GetRetainedFiles(absl::flat_hash_set<FileId> &result,
                      const MappingSnapshot::MappingTbl &tbl,
                      uint8_t pages_per_file_shift);

class ObjectStore;
class IouringMgr;
class CloudStoreMgr;

namespace FileGarbageCollector
{
// Local mode method (direct execution)
KvError ExecuteLocalGC(const TableIdent &tbl_id,
                       const absl::flat_hash_set<FileId> &retained_files,
                       IouringMgr *io_mgr);

// Cloud mode method (coroutine-based)
KvError ExecuteCloudGC(const TableIdent &tbl_id,
                       const absl::flat_hash_set<FileId> &retained_files,
                       CloudStoreMgr *cloud_mgr);

// Local mode implementation
KvError ListLocalFiles(const TableIdent &tbl_id,
                       std::vector<std::string> &local_files,
                       IouringMgr *io_mgr);

KvError DeleteUnreferencedLocalFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const absl::flat_hash_set<FileId> &retained_files,
    FileId least_not_archived_file_id,
    IouringMgr *io_mgr);

KvError GetOrUpdateArchivedMaxFileId(
    const TableIdent &tbl_id,
    const std::vector<std::string> &archive_files,
    const std::vector<uint64_t> &archive_timestamps,
    FileId &archived_max_file_id,
    IouringMgr *io_mgr);

// Cloud mode implementation
KvError ListCloudFiles(const TableIdent &tbl_id,
                       std::vector<std::string> &cloud_files,
                       CloudStoreMgr *cloud_mgr);

void ClassifyFiles(const std::vector<std::string> &files,
                   std::vector<std::string> &archive_files,
                   std::vector<uint64_t> &archive_timestamps,
                   std::vector<std::string> &data_files,
                   std::vector<uint64_t> &manifest_terms);

KvError DownloadArchiveFile(const TableIdent &tbl_id,
                            const std::string &archive_file,
                            std::string &content,
                            CloudStoreMgr *cloud_mgr,
                            const KvOptions *options);

FileId ParseArchiveForMaxFileId(const std::string &archive_filename,
                                std::string_view archive_content);

KvError DeleteUnreferencedCloudFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const std::vector<uint64_t> &manifest_terms,
    const absl::flat_hash_set<FileId> &retained_files,
    FileId least_not_archived_file_id,
    CloudStoreMgr *cloud_mgr);
}  // namespace FileGarbageCollector

}  // namespace eloqstore
