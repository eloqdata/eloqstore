#pragma once
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
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
using RetainedFiles = absl::flat_hash_set<RetainedFileKey>;

class ObjectStore;
class IouringMgr;
class CloudStoreMgr;

void GetRetainedFiles(absl::flat_hash_set<RetainedFileKey> &file_keys,
                      const MappingSnapshot::MappingTbl &tbl,
                      const BranchFileMapping &file_ranges,
                      uint8_t pages_per_file_shift);

namespace FileGarbageCollector
{
// Local mode method (direct execution)
KvError ExecuteLocalGC(const TableIdent &tbl_id,
                       RetainedFiles &retained_files,
                       IouringMgr *io_mgr);

// Cloud mode method (coroutine-based)
KvError ExecuteCloudGC(const TableIdent &tbl_id,
                       RetainedFiles &retained_files,
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
                   std::vector<std::string> &manifest_branch_names);

KvError ReadCloudFile(const TableIdent &tbl_id,
                      const std::string &cloud_file,
                      DirectIoBuffer &content,
                      CloudStoreMgr *cloud_mgr);

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
    absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    uint8_t pages_per_file_shift,
    IouringMgr *io_mgr);

}  // namespace FileGarbageCollector

}  // namespace eloqstore
