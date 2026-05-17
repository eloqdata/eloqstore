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
// In-flight write guard state per branch. At most one writer instance may
// operate on a branch, and only on that branch's newest term. The guard
// records, per branch, that newest term plus the highest committed data /
// segment file ids. A file at (branch, newest_term_) with id beyond max is
// an in-flight write and must be preserved; files at any other term for
// the same branch are by definition already complete.
struct BranchGuard
{
    uint64_t newest_term_{0};
    FileId max_data_file_id_{0};
    FileId max_segment_file_id_{0};
};
using BranchGuardMap = absl::flat_hash_map<std::string, BranchGuard>;

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

// In-flight write guard: for each on-disk file, look up its branch in
// `branch_guards`. If the branch is present and the file's term is >= the
// branch's newest_term_, any file_id beyond max_data_file_id_ is preserved
// as an in-flight write. The `>=` comparison (not `==`) is required because
// our manifest snapshot may lag a concurrent primary that has bumped its
// term but not yet flushed a manifest at the new term. Files for branches
// absent from the map, or at terms strictly below the branch's newest, are
// not protected.
KvError DeleteUnreferencedLocalFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const absl::flat_hash_set<RetainedFileKey> &retained_files,
    const BranchGuardMap &branch_guards,
    IouringMgr *io_mgr);

// Delete unreferenced segment files on local disk. A segment file is eligible
// for deletion only when it is not referenced by `retained_segment_files` and
// it is not protected by the in-flight write guard. Callers must populate
// `retained_segment_files` from both regular and archive manifests (see
// AugmentRetainedFilesFromBranchManifests) so archive-referenced segments
// are protected by inclusion in the retained set. The in-flight guard
// mirrors DeleteUnreferencedLocalFiles, using max_segment_file_id_.
KvError DeleteUnreferencedLocalSegmentFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &segment_files,
    const RetainedFiles &retained_segment_files,
    const BranchGuardMap &branch_guards,
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

// In-flight write guard rules match DeleteUnreferencedLocalFiles.
KvError DeleteUnreferencedCloudFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const std::vector<uint64_t> &manifest_terms,
    const std::vector<std::string> &manifest_branch_names,
    const absl::flat_hash_set<RetainedFileKey> &retained_files,
    const BranchGuardMap &branch_guards,
    std::vector<std::string> &deleted_filenames,
    CloudStoreMgr *cloud_mgr);

KvError DeleteOldArchives(const TableIdent &tbl_id,
                          const std::vector<std::string> &archive_files,
                          const std::vector<std::string> &archive_tags,
                          const std::vector<std::string> &archive_branch_names,
                          uint32_t num_retained_archives,
                          IouringMgr *io_mgr);

// Augment retained_files by reading every on-disk manifest (both regular and
// archive) and collecting all file IDs they reference. Also folds every
// BranchFileRange seen into `branch_guards` using the rule "for each branch,
// keep the newest term and the highest file ids observed at that term".
// Callers must pre-seed `branch_guards` with an entry for their own
// (active_branch, process_term) so in-flight files protected even before
// the first manifest at this term is flushed.
KvError AugmentRetainedFilesFromBranchManifests(
    const TableIdent &tbl_id,
    const std::vector<std::string> &manifest_branch_names,
    const std::vector<uint64_t> &manifest_terms,
    const std::vector<std::string> &archive_files,
    const std::vector<std::string> &archive_branch_names,
    absl::flat_hash_set<RetainedFileKey> &retained_files,
    absl::flat_hash_set<RetainedFileKey> &retained_segment_files,
    BranchGuardMap &branch_guards,
    uint8_t pages_per_file_shift,
    uint8_t segments_per_file_shift,
    IouringMgr *io_mgr);

// Delete unreferenced segment files in the cloud. Same eligibility rule as
// DeleteUnreferencedLocalSegmentFiles: not in `retained_segment_files` and
// not protected by the in-flight write guard (see that function for
// guard semantics). The per-file term check also guards against deleting
// files from a newer term.
KvError DeleteUnreferencedCloudSegmentFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &segment_files,
    const RetainedFiles &retained_segment_files,
    const BranchGuardMap &branch_guards,
    CloudStoreMgr *cloud_mgr);
}  // namespace FileGarbageCollector

}  // namespace eloqstore
