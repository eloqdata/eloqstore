#pragma once
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "async_io_manager.h"
#include "error.h"
#include "kv_options.h"
#include "storage/object_store.h"
#include "storage/page_mapper.h"
#include "types.h"
// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE

namespace eloqstore
{
using RetainedFiles = absl::flat_hash_set<RetainedFileKey>;

class ObjectStore;
class IouringMgr;
class CloudStoreMgr;

// Walk a MappingTbl and accumulate every file the table references into
// `file_keys`. The (branch, term) for each file_id is resolved against
// `file_ranges`. `is_segment` selects which axis of the BranchFileMapping
// entries is consulted (max_file_id_ vs max_segment_file_id_) and which
// TypedFileId encoding (DataFileKey vs SegmentFileKey) is used; it must
// match the kind of MappingTbl passed in.
void GetRetainedFiles(absl::flat_hash_set<RetainedFileKey> &file_keys,
                      const MappingSnapshot::MappingTbl &tbl,
                      const BranchFileMapping &file_ranges,
                      uint8_t per_file_shift,
                      bool is_segment);

namespace FileGarbageCollector
{
// Local mode method (direct execution)
KvError ExecuteLocalGC(const TableIdent &tbl_id,
                       RetainedFiles &retained_files,
                       RetainedFiles &retained_segment_files,
                       IouringMgr *io_mgr);

// Cloud mode method (coroutine-based)
KvError ExecuteCloudGC(const TableIdent &tbl_id,
                       RetainedFiles &retained_files,
                       RetainedFiles &retained_segment_files,
                       CloudStoreMgr *cloud_mgr);

// Local mode implementation
KvError ListLocalFiles(const TableIdent &tbl_id,
                       std::vector<std::string> &local_files,
                       IouringMgr *io_mgr);

KvError DeleteUnreferencedLocalFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const absl::flat_hash_set<RetainedFileKey> &retained_files,
    const absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    IouringMgr *io_mgr);

// Delete unreferenced segment files on local disk. A segment file is eligible
// for deletion only when its file_id >= least_not_archived_segment_file_id and
// it is not referenced by the current mapping (retained_segment_files). The
// archive floor protects segments referenced by archive snapshots from being
// collected before the archive itself is deleted.
KvError DeleteUnreferencedLocalSegmentFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &segment_files,
    const RetainedFiles &retained_segment_files,
    FileId least_not_archived_segment_file_id,
    IouringMgr *io_mgr);

KvError GetOrUpdateArchivedMaxFileId(
    const TableIdent &tbl_id,
    const std::vector<std::string> &archive_files,
    const std::vector<uint64_t> &archive_timestamps,
    FileId &least_not_archived_file_id,
    FileId &least_not_archived_segment_file_id,
    IouringMgr *io_mgr);

// Cloud mode implementation
KvError ListCloudFiles(const TableIdent &tbl_id,
                       std::vector<std::string> &cloud_files,
                       CloudStoreMgr *cloud_mgr);

void ClassifyFiles(const std::vector<std::string> &files,
                   std::vector<std::string> &archive_files,
                   std::vector<std::string> &archive_tags,
                   std::vector<std::string> &archive_branch_names,
                   std::vector<std::string> &data_files,
                   std::vector<uint64_t> &manifest_terms,
                   std::vector<std::string> &manifest_branch_names,
                   std::vector<std::string> *segment_files = nullptr);

KvError ReadCloudFile(const TableIdent &tbl_id,
                      const std::string &cloud_file,
                      DirectIoBuffer &content,
                      CloudStoreMgr *cloud_mgr);

KvError DownloadArchiveFile(const TableIdent &tbl_id,
                            const std::string &archive_file,
                            std::string &content,
                            CloudStoreMgr *cloud_mgr,
                            const KvOptions *options);

ArchivedMaxFileIds ParseArchiveForMaxFileId(std::string_view archive_content);

KvError DeleteUnreferencedCloudFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const std::vector<uint64_t> &manifest_terms,
    const std::vector<std::string> &manifest_branch_names,
    const absl::flat_hash_set<RetainedFileKey> &retained_files,
    const absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    CloudStoreMgr *cloud_mgr);

KvError DeleteOldArchives(const TableIdent &tbl_id,
                          const std::vector<std::string> &archive_files,
                          const std::vector<std::string> &archive_tags,
                          const std::vector<std::string> &archive_branch_names,
                          uint32_t num_retained_archives,
                          IouringMgr *io_mgr);

// Augment retained_files by reading every on-disk manifest (both regular and
// archive) and collecting all file IDs they reference.  Also builds
// max_file_id_per_branch_term: for each (branch, term) key derived from the
// BranchManifestMetadata.file_ranges stored in each manifest, records the
// highest known allocated file ID.  This is used by GC rule 2: any data file
// whose file_id exceeds the max for its (branch, term) is in-flight and must
// not be deleted.
KvError AugmentRetainedFilesFromBranchManifests(
    const TableIdent &tbl_id,
    const std::vector<std::string> &manifest_branch_names,
    const std::vector<uint64_t> &manifest_terms,
    const std::vector<std::string> &archive_files,
    const std::vector<std::string> &archive_branch_names,
    absl::flat_hash_set<RetainedFileKey> &retained_files,
    absl::flat_hash_set<RetainedFileKey> &retained_segment_files,
    absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    uint8_t pages_per_file_shift,
    uint8_t segments_per_file_shift,
    IouringMgr *io_mgr);

// Delete unreferenced segment files in the cloud. A segment file is eligible
// for deletion only when its file_id >= least_not_archived_segment_file_id and
// it is not referenced by the current mapping (retained_segment_files). The
// per-file term check also guards against deleting files from a newer term.
KvError DeleteUnreferencedCloudSegmentFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &segment_files,
    const RetainedFiles &retained_segment_files,
    FileId least_not_archived_segment_file_id,
    CloudStoreMgr *cloud_mgr);
}  // namespace FileGarbageCollector

}  // namespace eloqstore
