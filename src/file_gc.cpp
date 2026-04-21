#include "file_gc.h"

#include <jsoncpp/json/json.h>

#include <algorithm>
#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "async_io_manager.h"
#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "kv_options.h"
#include "replayer.h"
#include "storage/mem_index_page.h"
#include "storage/object_store.h"
#include "storage/shard.h"
#include "tasks/task.h"
#include "utils.h"
namespace eloqstore
{
void GetRetainedFiles(absl::flat_hash_set<RetainedFileKey> &result,
                      const MappingSnapshot::MappingTbl &tbl,
                      const BranchFileMapping &file_ranges,
                      uint8_t pages_per_file_shift)
{
    const size_t tbl_size = tbl.size();
    for (PageId page_id = 0; page_id < tbl_size; ++page_id)
    {
        uint64_t val = tbl.Get(page_id);
        if (MappingSnapshot::IsFilePageId(val))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(val);
            FileId file_id = fp_id >> pages_per_file_shift;

            // Look up the branch and term for this file_id
            std::string branch_name;
            uint64_t term = 0;
            if (GetBranchNameAndTerm(file_ranges, file_id, branch_name, term))
            {
                result.emplace(
                    RetainedFileKey{file_id, std::move(branch_name), term});
            }
            else
            {
                // File ID not in any known branch range; skip it
                DLOG(WARNING) << "GetRetainedFiles: file_id " << file_id
                              << " not found in any branch range";
            }
        }
        else if (MappingSnapshot::IsSwizzlingPointer(val))
        {
            MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
            FilePageId fp_id = idx_page->GetFilePageId();
            FileId file_id = fp_id >> pages_per_file_shift;

            // Look up the branch and term for this file_id
            std::string branch_name;
            uint64_t term = 0;
            if (GetBranchNameAndTerm(file_ranges, file_id, branch_name, term))
            {
                result.emplace(
                    RetainedFileKey{file_id, std::move(branch_name), term});
            }
            else
            {
                // File ID not in any known branch range; skip it
                DLOG(WARNING)
                    << "GetRetainedFiles: file_id " << file_id
                    << " not found in any branch range (swizzling pointer)";
            }
        }
        if ((page_id & 0xFF) == 0)
        {
            ThdTask()->YieldToLowPQ();
        }
    }
}

namespace FileGarbageCollector
{

namespace
{
bool ParseCloudCleanupFilename(std::string_view filename,
                               FileId &file_id,
                               uint64_t &term)
{
    auto [type, suffix] = ParseFileName(filename);
    if (type == FileNameData)
    {
        std::string_view branch_name;
        return ParseDataFileSuffix(suffix, file_id, branch_name, term);
    }
    if (type == FileNameManifest)
    {
        std::string_view branch_name;
        std::optional<std::string> tag;
        if (!ParseManifestFileSuffix(suffix, branch_name, term, tag) ||
            tag.has_value())
        {
            return false;
        }
        file_id = IouringMgr::LruFD::kManifest;
        return true;
    }
    return false;
}

void CollectLocalCleanupTargets(
    const TableIdent &tbl_id,
    const std::vector<std::string> &deleted_filenames,
    CloudStoreMgr *cloud_mgr,
    std::vector<std::string> &targets)
{
    targets.clear();
    targets.reserve(deleted_filenames.size());
    for (const std::string &filename : deleted_filenames)
    {
        FileId file_id = 0;
        uint64_t term = 0;
        if (!ParseCloudCleanupFilename(filename, file_id, term))
        {
            continue;
        }

        IouringMgr::LruFD::Ref fd_ref = cloud_mgr->GetOpenedFD(tbl_id, file_id);
        if (fd_ref != nullptr && fd_ref.Get()->term_ == term)
        {
            if (fd_ref.Get()->ref_count_ > 1)
            {
                targets.push_back(filename);
                continue;
            }

            KvError close_err = cloud_mgr->CloseFile(std::move(fd_ref));
            if (close_err != KvError::NoError)
            {
                LOG(WARNING)
                    << "Failed to close idle file before local GC "
                    << "cleanup, table=" << tbl_id << " file=" << filename
                    << " err=" << ErrorString(close_err);
                continue;
            }
        }

        targets.push_back(filename);
    }
}
}  // namespace

KvError ExecuteLocalGC(const TableIdent &tbl_id,
                       RetainedFiles &retained_files,
                       IouringMgr *io_mgr)
{
    DLOG(INFO) << "ExecuteLocalGC: starting for table " << tbl_id.tbl_name_
               << ", partition " << tbl_id.partition_id_
               << ", retained_files count=" << retained_files.size();

    // 1. list all files in local directory.
    std::vector<std::string> local_files;
    KvError err = ListLocalFiles(tbl_id, local_files, io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteLocalGC: ListLocalFiles failed, error="
                   << static_cast<int>(err);
        return err;
    }

    // 2. classify files.
    std::vector<std::string> archive_files;
    std::vector<std::string> archive_tags;
    std::vector<std::string> archive_branch_names;
    std::vector<std::string> data_files;
    std::vector<uint64_t> manifest_terms;
    std::vector<std::string> manifest_branch_names;
    ClassifyFiles(local_files,
                  archive_files,
                  archive_tags,
                  archive_branch_names,
                  data_files,
                  manifest_terms,
                  manifest_branch_names);

    // No need to check term expired for local mode.

    // 2a. augment retained_files from all branch manifests (regular + archive)
    // on disk; also build max_file_id_per_branch_term map.
    absl::flat_hash_map<std::string, FileId> max_file_id_per_branch_term;
    err = AugmentRetainedFilesFromBranchManifests(
        tbl_id,
        manifest_branch_names,
        manifest_terms,
        archive_files,
        archive_branch_names,
        retained_files,
        max_file_id_per_branch_term,
        io_mgr->options_->pages_per_file_shift,
        io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteLocalGC: AugmentRetainedFilesFromBranchManifests "
                      "failed, error="
                   << static_cast<int>(err) << "; aborting GC cycle";
        return err;
    }

    // 3. delete unreferenced data files.
    err = DeleteUnreferencedLocalFiles(tbl_id,
                                       data_files,
                                       retained_files,
                                       max_file_id_per_branch_term,
                                       io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR)
            << "ExecuteLocalGC: DeleteUnreferencedLocalFiles failed, error="
            << static_cast<int>(err);
        return err;
    }

    // If the partition is now empty except for the current manifest, clean the
    // manifest as well so the local directory can be removed without waiting
    // for later root-meta eviction.
    local_files.clear();
    err = ListLocalFiles(tbl_id, local_files, io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteLocalGC: post-delete ListLocalFiles failed, "
                   << "error=" << static_cast<int>(err);
        return err;
    }

    archive_files.clear();
    archive_tags.clear();
    archive_branch_names.clear();
    data_files.clear();
    manifest_terms.clear();
    manifest_branch_names.clear();
    ClassifyFiles(local_files,
                  archive_files,
                  archive_tags,
                  archive_branch_names,
                  data_files,
                  manifest_terms,
                  manifest_branch_names);

    if (data_files.empty() && archive_files.empty())
    {
        const StoreMode mode = eloq_store->Mode();
        if (mode == StoreMode::Cloud)
        {
            auto *cloud_mgr = static_cast<CloudStoreMgr *>(io_mgr);
            err = cloud_mgr->CleanupLocalPartitionFiles(tbl_id);
            if (err != KvError::NoError)
            {
                LOG(ERROR)
                    << "ExecuteLocalGC: CleanupLocalPartitionFiles failed, "
                    << "error=" << static_cast<int>(err);
                return err;
            }
        }
        else
        {
            err = io_mgr->CleanManifest(tbl_id);
            if (err != KvError::NoError)
            {
                LOG(ERROR) << "ExecuteLocalGC: CleanManifest failed, error="
                           << static_cast<int>(err);
                return err;
            }

            CHECK(shard != nullptr);
            RootMetaMgr *root_meta_mgr =
                shard->IndexManager()->RootMetaManager();
            auto *entry = root_meta_mgr->Find(tbl_id);
            if (entry != nullptr)
            {
                RootMeta &meta = entry->meta_;
                if (meta.mapper_ != nullptr && meta.manifest_size_ != 0 &&
                    meta.root_id_ == MaxPageId &&
                    meta.ttl_root_id_ == MaxPageId &&
                    meta.mapper_->MappingCount() == 0)
                {
                    meta.manifest_size_ = 0;
                }
            }
        }
    }

    /*
    // 4. delete old archives beyond num_retained_archives per branch.
    // NOTE: this step is intentionally AFTER DeleteUnreferencedLocalFiles so
    // that ALL archives (including those about to be pruned) contribute their
    // file IDs to retained_files first.  Files exclusively referenced by pruned
    // archives become deletable only on the next GC cycle.
    err = DeleteOldArchives(tbl_id,
                            archive_files,
                            archive_tags,
                            archive_branch_names,
                            io_mgr->options_->num_retained_archives,
                            io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteLocalGC: DeleteOldArchives failed, error="
                   << static_cast<int>(err);
        return err;
    }
    */

    return KvError::NoError;
}

KvError ListLocalFiles(const TableIdent &tbl_id,
                       std::vector<std::string> &local_files,
                       IouringMgr *io_mgr)
{
    namespace fs = std::filesystem;

    fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path,
                                         io_mgr->options_->store_path_lut);

    for (auto &ent : fs::directory_iterator{dir_path})
    {
        const std::string name = ent.path().filename();
        if (boost::algorithm::ends_with(name, TmpSuffix))
        {
            // Skip temporary files.
            continue;
        }
        local_files.emplace_back(name);
    }

    return KvError::NoError;
}

// Helper functions for cloud GC optimization
KvError ListCloudFiles(const TableIdent &tbl_id,
                       std::vector<std::string> &cloud_files,
                       CloudStoreMgr *cloud_mgr)
{
    std::string table_path = tbl_id.ToString();
    KvTask *current_task = ThdTask();

    ObjectStore &object_store = cloud_mgr->GetObjectStore();
    cloud_files.clear();

    std::string continuation_token;
    do
    {
        // List files in batches (S3 ListObjectsV2 caps responses at 1000)
        ObjectStore::ListTask list_task(table_path);
        list_task.SetContinuationToken(continuation_token);
        list_task.SetKvTask(current_task);

        cloud_mgr->AcquireCloudSlot(current_task);
        cloud_mgr->GetObjectStore().SubmitTask(&list_task, shard);
        current_task->WaitIo();

        if (list_task.error_ != KvError::NoError)
        {
            LOG(ERROR) << "Failed to list cloud files for table " << table_path
                       << ", error: " << static_cast<int>(list_task.error_);
            return list_task.error_;
        }

        std::vector<std::string> batch_files;
        std::string next_token;
        if (!object_store.ParseListObjectsResponse(
                list_task.response_data_.view(),
                list_task.json_data_,
                &batch_files,
                nullptr,
                &next_token))
        {
            LOG(ERROR) << "Failed to parse cloud list response";
            return KvError::Corrupted;
        }

        cloud_files.insert(cloud_files.end(),
                           std::make_move_iterator(batch_files.begin()),
                           std::make_move_iterator(batch_files.end()));

        continuation_token = std::move(next_token);
    } while (!continuation_token.empty());

    return KvError::NoError;
}

void ClassifyFiles(const std::vector<std::string> &files,
                   std::vector<std::string> &archive_files,
                   std::vector<std::string> &archive_tags,
                   std::vector<std::string> &archive_branch_names,
                   std::vector<std::string> &data_files,
                   std::vector<uint64_t> &manifest_terms,
                   std::vector<std::string> &manifest_branch_names)
{
    archive_files.clear();
    archive_tags.clear();
    archive_branch_names.clear();
    data_files.clear();
    manifest_terms.clear();
    manifest_branch_names.clear();
    data_files.reserve(files.size());

    for (const std::string &file_name : files)
    {
        // Ignore temporary files.
        if (boost::algorithm::ends_with(file_name, TmpSuffix))
        {
            continue;
        }

        // Cloud list responses already strip the prefix, so file_name contains
        // just the basename we need to classify.
        auto ret = ParseFileName(file_name);

        if (ret.first == FileNameManifest)
        {
            // Only support term-aware archive format:
            // manifest_<term>_<tag> Legacy format manifest_<ts> is no longer
            // supported.
            std::string_view branch_name;
            uint64_t term = 0;
            std::optional<std::string> tag;
            if (!ParseManifestFileSuffix(ret.second, branch_name, term, tag))
            {
                continue;
            }
            // Only recognize term-aware archives (both term and tag must
            // be present).
            if (tag.has_value())
            {
                archive_files.push_back(file_name);
                archive_tags.push_back(std::move(*tag));
                archive_branch_names.emplace_back(branch_name);
            }
            else
            {
                manifest_terms.push_back(term);
                manifest_branch_names.emplace_back(branch_name);
            }
        }
        else if (ret.first == FileNameData)
        {
            // data file: data_<file_id>
            data_files.push_back(file_name);
        }
        // Ignore other types of files.
    }
}

KvError ReadCloudFile(const TableIdent &tbl_id,
                      const std::string &cloud_file,
                      DirectIoBuffer &content,
                      CloudStoreMgr *cloud_mgr)
{
    KvTask *current_task = ThdTask();

    // Download the file from cloud.
    ObjectStore::DownloadTask download_task(&tbl_id, cloud_file);

    // Set KvTask pointer and initialize inflight_io_
    download_task.SetKvTask(current_task);

    cloud_mgr->AcquireCloudSlot(current_task);
    cloud_mgr->GetObjectStore().SubmitTask(&download_task, shard);
    current_task->WaitIo();

    if (download_task.error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to download cloud file: " << cloud_file
                   << ", error: " << static_cast<int>(download_task.error_);
        return download_task.error_;
    }
    content = std::move(download_task.response_data_);

    DLOG(INFO) << "Successfully downloaded and read cloud file: " << cloud_file;
    return KvError::NoError;
}

// Helper: process one manifest file (regular or archive) — replay it,
// add all referenced file IDs to retained_files, and update
// max_file_id_per_branch_term from BranchManifestMetadata.file_ranges.
static KvError ProcessOneManifest(
    const std::string &filename,
    uint64_t term,
    DirectIoBuffer &buf,
    absl::flat_hash_set<RetainedFileKey> &retained_files,
    absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    uint8_t pages_per_file_shift)
{
    MemStoreMgr::Manifest manifest(buf.view());
    Replayer replayer(Options());
    replayer.branch_metadata_.term = term;

    KvError replay_err = replayer.Replay(&manifest);
    if (replay_err != KvError::NoError)
    {
        LOG(WARNING) << "ProcessOneManifest: failed to replay manifest "
                     << filename << " term " << term
                     << ", error=" << static_cast<int>(replay_err);
        return replay_err;
    }

    GetRetainedFiles(retained_files,
                     replayer.mapping_tbl_,
                     replayer.branch_metadata_.file_ranges,
                     pages_per_file_shift);

    // Update max_file_id_per_branch_term from all file_ranges in this manifest.
    for (const BranchFileRange &range : replayer.branch_metadata_.file_ranges)
    {
        std::string key = range.branch_name + "_" + std::to_string(range.term);
        auto it = max_file_id_per_branch_term.find(key);
        if (it == max_file_id_per_branch_term.end() ||
            range.max_file_id > it->second)
        {
            max_file_id_per_branch_term[key] = range.max_file_id;
        }
    }

    DLOG(INFO) << "ProcessOneManifest: processed " << filename
               << ", retained_files now size=" << retained_files.size();
    return KvError::NoError;
}

KvError AugmentRetainedFilesFromBranchManifests(
    const TableIdent &tbl_id,
    const std::vector<std::string> &manifest_branch_names,
    const std::vector<uint64_t> &manifest_terms,
    const std::vector<std::string> &archive_files,
    const std::vector<std::string> &archive_branch_names,
    absl::flat_hash_set<RetainedFileKey> &retained_files,
    absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    uint8_t pages_per_file_shift,
    IouringMgr *io_mgr)
{
    assert(manifest_branch_names.size() == manifest_terms.size());
    assert(archive_files.size() == archive_branch_names.size());

    bool is_cloud = eloq_store->Mode() == StoreMode::Cloud;
    CloudStoreMgr *cloud_mgr =
        is_cloud ? static_cast<CloudStoreMgr *>(io_mgr) : nullptr;

    // --- Process regular manifests ---
    for (size_t i = 0; i < manifest_branch_names.size(); ++i)
    {
        const std::string &branch = manifest_branch_names[i];
        uint64_t term = manifest_terms[i];
        std::string filename = BranchManifestFileName(branch, term);

        DirectIoBuffer buf;
        KvError err = KvError::NoError;

        if (is_cloud)
        {
            err = ReadCloudFile(tbl_id, filename, buf, cloud_mgr);
        }
        else
        {
            err = io_mgr->ReadFile(tbl_id, filename, buf);
        }

        if (err == KvError::NoError)
        {
            err = ProcessOneManifest(filename,
                                     term,
                                     buf,
                                     retained_files,
                                     max_file_id_per_branch_term,
                                     pages_per_file_shift);
            if (err != KvError::NoError)
            {
                return err;
            }
        }
        else if (err != KvError::NotFound)
        {
            LOG(WARNING) << "AugmentRetainedFilesFromBranchManifests: "
                            "failed to read "
                            "manifest "
                         << filename << " for branch " << branch << " term "
                         << term << ", error=" << static_cast<int>(err);
            return err;
        }
        else
        {
            LOG(ERROR) << "AugmentRetainedFilesFromBranchManifests: manifest "
                       << filename << " for branch " << branch << " term "
                       << term
                       << " not found during GC scan, skipping retained-file "
                          "augmentation for this manifest";
        }
    }

    // --- Process archive manifests ---
    for (size_t i = 0; i < archive_files.size(); ++i)
    {
        const std::string &filename = archive_files[i];
        // Extract term from archive filename.
        uint64_t term = ManifestTermFromFilename(filename);

        DirectIoBuffer buf;
        KvError err = KvError::NoError;

        if (is_cloud)
        {
            err = ReadCloudFile(tbl_id, filename, buf, cloud_mgr);
        }
        else
        {
            err = io_mgr->ReadFile(tbl_id, filename, buf);
        }

        if (err != KvError::NoError)
        {
            LOG(WARNING)
                << "AugmentRetainedFilesFromBranchManifests: failed to read "
                   "archive "
                << filename << " for branch " << archive_branch_names[i]
                << " term " << term << ", error=" << static_cast<int>(err);
            return err;
        }

        err = ProcessOneManifest(filename,
                                 term,
                                 buf,
                                 retained_files,
                                 max_file_id_per_branch_term,
                                 pages_per_file_shift);
        if (err != KvError::NoError)
        {
            return err;
        }
    }

    return KvError::NoError;
}

KvError DeleteOldArchives(const TableIdent &tbl_id,
                          const std::vector<std::string> &archive_files,
                          const std::vector<std::string> &archive_tags,
                          const std::vector<std::string> &archive_branch_names,
                          uint32_t num_retained_archives,
                          IouringMgr *io_mgr)
{
    assert(archive_files.size() == archive_tags.size());
    assert(archive_files.size() == archive_branch_names.size());

    if (num_retained_archives == 0 || archive_files.empty())
    {
        return KvError::NoError;
    }

    // Group archive indices by branch name.
    std::unordered_map<std::string, std::vector<size_t>> branch_indices;
    for (size_t i = 0; i < archive_files.size(); ++i)
    {
        branch_indices[archive_branch_names[i]].push_back(i);
    }

    // For each branch, sort by tag descending (lexicographic) and collect
    // excess archives.
    std::vector<std::string> to_delete;
    for (auto &[branch, indices] : branch_indices)
    {
        if (indices.size() <= num_retained_archives)
        {
            continue;
        }
        // Sort descending by tag (newest first, assuming lexicographic order).
        std::sort(indices.begin(),
                  indices.end(),
                  [&](size_t a, size_t b)
                  { return archive_tags[a] > archive_tags[b]; });
        // Keep the first num_retained_archives, delete the rest.
        for (size_t j = num_retained_archives; j < indices.size(); ++j)
        {
            to_delete.push_back(archive_files[indices[j]]);
        }
    }

    if (to_delete.empty())
    {
        return KvError::NoError;
    }

    if (!io_mgr->options_->cloud_store_path.empty())
    {
        // Cloud mode: batch delete via object store.
        CloudStoreMgr *cloud_mgr = static_cast<CloudStoreMgr *>(io_mgr);
        KvTask *current_task = ThdTask();

        std::vector<ObjectStore::DeleteTask> delete_tasks;
        delete_tasks.reserve(to_delete.size());

        for (const std::string &file_name : to_delete)
        {
            std::string remote_path = tbl_id.ToString() + "/" + file_name;
            delete_tasks.emplace_back(remote_path);
            ObjectStore::DeleteTask &task = delete_tasks.back();
            task.SetKvTask(current_task);
            cloud_mgr->AcquireCloudSlot(current_task);
            cloud_mgr->GetObjectStore().SubmitTask(&task, shard);
        }

        current_task->WaitIo();

        for (const auto &task : delete_tasks)
        {
            if (task.error_ != KvError::NoError)
            {
                LOG(ERROR) << "DeleteOldArchives: failed to delete archive "
                           << task.remote_path_ << ": "
                           << ErrorString(task.error_);
                return task.error_;
            }
        }
    }
    else
    {
        // Local mode: delete files from filesystem.
        namespace fs = std::filesystem;
        fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path,
                                             io_mgr->options_->store_path_lut);

        std::vector<std::string> full_paths;
        full_paths.reserve(to_delete.size());
        for (const std::string &file_name : to_delete)
        {
            full_paths.push_back((dir_path / file_name).string());
        }

        KvError delete_err = io_mgr->DeleteFiles(full_paths);
        if (delete_err != KvError::NoError)
        {
            LOG(ERROR) << "DeleteOldArchives: failed to delete archive files, "
                          "error: "
                       << static_cast<int>(delete_err);
            return delete_err;
        }
    }

    DLOG(INFO) << "DeleteOldArchives: deleted " << to_delete.size()
               << " old archive(s) for table " << tbl_id;
    return KvError::NoError;
}

KvError DeleteUnreferencedCloudFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const std::vector<uint64_t> &manifest_terms,
    const std::vector<std::string> &manifest_branch_names,
    const absl::flat_hash_set<RetainedFileKey> &retained_files,
    const absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    std::vector<std::string> &deleted_filenames,
    CloudStoreMgr *cloud_mgr)
{
    std::vector<std::string> files_to_delete;
    std::vector<std::string> basenames_to_delete;
    auto process_term = cloud_mgr->ProcessTerm();
    const std::string current_manifest =
        BranchManifestFileName(cloud_mgr->GetActiveBranch(), process_term);
    deleted_filenames.clear();

    for (const std::string &file_name : data_files)
    {
        auto ret = ParseFileName(file_name);
        if (ret.first != FileNameData)
        {
            continue;
        }

        FileId file_id = 0;
        std::string_view branch_name;
        uint64_t term = 0;
        if (!ParseDataFileSuffix(ret.second, file_id, branch_name, term))
        {
            LOG(ERROR) << "Failed to parse data file suffix: " << file_name
                       << ", skipping";
            continue;
        }

        if (term > process_term)
        {
            // Skip files with term greater than process term.
            continue;
        }

        if (retained_files.contains(
                RetainedFileKey{file_id, std::string(branch_name), term}))
        {
            DLOG(INFO) << "skip file " << file_name << " (in retained_files)";
            continue;
        }

        // Check max_file_id_per_branch_term to detect in-flight writes.
        std::string key = std::string(branch_name) + "_" + std::to_string(term);
        auto it = max_file_id_per_branch_term.find(key);
        if (it != max_file_id_per_branch_term.end() && file_id > it->second)
        {
            // file_id beyond known max → in-flight write, preserve.
            DLOG(INFO) << "skip file " << file_name << " (file_id=" << file_id
                       << " > max_known=" << it->second << ", in-flight)";
            continue;
        }

        // No map entry → deleted/orphaned branch; or file_id within known
        // range and not retained → safe to delete.
        std::string remote_path = tbl_id.ToString() + "/" + file_name;
        files_to_delete.push_back(remote_path);
        basenames_to_delete.push_back(file_name);
    }

    if (files_to_delete.size() == data_files.size())
    {
        // Every data file for this table is unreferenced and will be deleted.
        // The active branch's manifest is now empty, so delete it too rather
        // than leaving a stale manifest in cloud storage.  We use
        // GetActiveBranch() directly (instead of scanning by term alone)
        // because multiple branches may share the same term value.
        std::string_view active_branch = cloud_mgr->GetActiveBranch();
        bool found_current = false;
        for (size_t i = 0; i < manifest_terms.size(); ++i)
        {
            if (manifest_branch_names[i] == active_branch &&
                manifest_terms[i] == process_term)
            {
                found_current = true;
                break;
            }
        }
        if (!found_current)
        {
            LOG(WARNING)
                << "ExecuteCloudGC: no manifest found for active_branch="
                << active_branch << " process_term=" << process_term
                << " in tbl=" << tbl_id.ToString()
                << "; skipping current-manifest deletion";
        }
        else
        {
            files_to_delete.emplace_back(
                tbl_id.ToString() + "/" +
                BranchManifestFileName(active_branch, process_term));
            basenames_to_delete.emplace_back(
                BranchManifestFileName(active_branch, process_term));
        }
    }

    // Delete superseded manifest files: only manifests belonging to the same
    // branch as the current process_term manifest are version-chained and safe
    // to prune. Manifests for OTHER branches are managed by DeleteBranch and
    // must not be deleted here.
    {
        // Use the known active branch directly rather than scanning by term.
        std::string_view active_branch = cloud_mgr->GetActiveBranch();
        for (size_t i = 0; i < manifest_terms.size(); ++i)
        {
            if (manifest_terms[i] < process_term &&
                manifest_branch_names[i] == active_branch)
            {
                files_to_delete.emplace_back(
                    tbl_id.ToString() + "/" +
                    BranchManifestFileName(manifest_branch_names[i],
                                           manifest_terms[i]));
                basenames_to_delete.emplace_back(BranchManifestFileName(
                    manifest_branch_names[i], manifest_terms[i]));
            }
        }
    }

    if (files_to_delete.empty())
    {
        return KvError::NoError;
    }

    KvTask *current_task = ThdTask();

    // If we're deleting every file in the directory, issue a single purge
    // request for the table path.
    std::vector<ObjectStore::DeleteTask> delete_tasks;
    delete_tasks.reserve(files_to_delete.size());

    for (const std::string &remote_path : files_to_delete)
    {
        delete_tasks.emplace_back(remote_path);
        ObjectStore::DeleteTask &task = delete_tasks.back();
        task.SetKvTask(current_task);
        cloud_mgr->AcquireCloudSlot(current_task);
        cloud_mgr->GetObjectStore().SubmitTask(&task, shard);
    }

    current_task->WaitIo();

    KvError first_error = KvError::NoError;
    deleted_filenames.reserve(basenames_to_delete.size());
    for (size_t i = 0; i < delete_tasks.size(); ++i)
    {
        const auto &task = delete_tasks[i];
        const bool manifest_gone = basenames_to_delete[i] == current_manifest &&
                                   (task.error_ == KvError::NoError ||
                                    task.error_ == KvError::NotFound);
        if (manifest_gone)
        {
            RootMetaMgr *root_meta_mgr =
                shard->IndexManager()->RootMetaManager();
            auto *entry = root_meta_mgr->Find(tbl_id);
            if (entry != nullptr)
            {
                RootMeta &meta = entry->meta_;
                // Cloud GC only deletes the current manifest for an empty
                // partition. Keep the in-memory RootMeta consistent so the
                // next write rebuilds a fresh snapshot instead of appending to
                // a manifest that no longer exists.
                if (meta.mapper_ != nullptr && meta.manifest_size_ != 0 &&
                    meta.root_id_ == MaxPageId &&
                    meta.ttl_root_id_ == MaxPageId &&
                    meta.mapper_->MappingCount() == 0)
                {
                    meta.manifest_size_ = 0;
                }
            }
        }
        if (task.error_ != KvError::NoError && task.error_ != KvError::NotFound)
        {
            LOG(ERROR) << "Failed to delete file " << task.remote_path_ << ": "
                       << ErrorString(task.error_);
            if (first_error == KvError::NoError)
            {
                first_error = task.error_;
            }
            continue;
        }

        deleted_filenames.push_back(basenames_to_delete[i]);
    }

    return first_error;
}

KvError DeleteUnreferencedLocalFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const absl::flat_hash_set<RetainedFileKey> &retained_files,
    const absl::flat_hash_map<std::string, FileId> &max_file_id_per_branch_term,
    IouringMgr *io_mgr)
{
    namespace fs = std::filesystem;
    fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path,
                                         io_mgr->options_->store_path_lut);

    std::vector<std::string> files_to_delete;
    std::vector<FileId> file_ids_to_close;
    files_to_delete.reserve(data_files.size());
    file_ids_to_close.reserve(data_files.size());

    for (const std::string &file_name : data_files)
    {
        auto ret = ParseFileName(file_name);
        if (ret.first != FileNameData)
        {
            DLOG(INFO) << "ExecuteLocalGC: skipping non-data file: "
                       << file_name;
            continue;
        }

        FileId file_id = 0;
        std::string_view branch_name;
        uint64_t term = 0;
        if (!ParseDataFileSuffix(ret.second, file_id, branch_name, term))
        {
            continue;
        }

        if (retained_files.contains(
                RetainedFileKey{file_id, std::string(branch_name), term}))
        {
            DLOG(INFO) << "ExecuteLocalGC: keep file " << file_name
                       << " (in retained_files)";
            continue;
        }

        // Check max_file_id_per_branch_term to detect in-flight writes.
        std::string key = std::string(branch_name) + "_" + std::to_string(term);
        auto it = max_file_id_per_branch_term.find(key);
        if (it != max_file_id_per_branch_term.end() && file_id > it->second)
        {
            // file_id beyond known max → in-flight write, preserve.
            DLOG(INFO) << "ExecuteLocalGC: keep file " << file_name
                       << " (file_id=" << file_id
                       << " > max_known=" << it->second << ", in-flight)";
            continue;
        }

        // No map entry → deleted/orphaned branch; or file_id within known
        // range and not retained → safe to delete.
        fs::path file_path = dir_path / file_name;
        files_to_delete.push_back(file_path.string());
        file_ids_to_close.push_back(file_id);
        DLOG(INFO) << "ExecuteLocalGC: marking file for deletion: " << file_name
                   << " (file_id=" << file_id << ")";
    }

    DLOG(INFO) << "ExecuteLocalGC: total files to delete: "
               << files_to_delete.size();
    if (!files_to_delete.empty())
    {
        KvError close_err = io_mgr->CloseFiles(tbl_id, file_ids_to_close);
        if (close_err != KvError::NoError)
        {
            LOG(ERROR) << "ExecuteLocalGC: Failed to close files before "
                          "deletion, error: "
                       << static_cast<int>(close_err);
            return close_err;
        }
        // Delete files using batch operation
        KvError delete_err = io_mgr->DeleteFiles(files_to_delete);
        if (delete_err != KvError::NoError)
        {
            LOG(ERROR) << "ExecuteLocalGC: Failed to delete files, error: "
                       << static_cast<int>(delete_err);
            return delete_err;
        }
        DLOG(INFO) << "ExecuteLocalGC: Successfully deleted "
                   << files_to_delete.size() << " unreferenced files";
    }
    else
    {
        DLOG(INFO) << "ExecuteLocalGC: No files to delete";
    }

    return KvError::NoError;
}

KvError ExecuteCloudGC(const TableIdent &tbl_id,
                       RetainedFiles &retained_files,
                       CloudStoreMgr *cloud_mgr)
{
    // Check term file before proceeding
    uint64_t process_term = cloud_mgr->ProcessTerm();
    auto [term_file_term, etag, err] =
        cloud_mgr->ReadTermFile(cloud_mgr->GetActiveBranch());

    if (err == KvError::NotFound)
    {
        LOG(INFO) << "ExecuteCloudGC: term file not found for partition_group "
                  << cloud_mgr->PartitionGroupId();
        return KvError::NoError;
    }
    else if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteCloudGC: failed to read term file for "
                   << "partition_group " << cloud_mgr->PartitionGroupId()
                   << " : " << ErrorString(err);
        return err;
    }
    else
    {
        if (term_file_term != process_term)
        {
            LOG(WARNING) << "ExecuteCloudGC: term file term " << term_file_term
                         << " != process_term " << process_term << " for table "
                         << tbl_id << ", partition_group "
                         << cloud_mgr->PartitionGroupId() << ", skipping GC";
            return KvError::ExpiredTerm;
        }
    }

    // 1. list all files in cloud.
    std::vector<std::string> cloud_files;
    err = ListCloudFiles(tbl_id, cloud_files, cloud_mgr);
    DLOG(INFO) << "ListCloudFiles got " << cloud_files.size() << " files";
    if (err != KvError::NoError)
    {
        return err;
    }

    // 2. classify files.
    std::vector<std::string> archive_files;
    std::vector<std::string> archive_tags;
    std::vector<std::string> archive_branch_names;
    std::vector<std::string> data_files;
    std::vector<uint64_t> manifest_terms;
    std::vector<std::string> manifest_branch_names;
    ClassifyFiles(cloud_files,
                  archive_files,
                  archive_tags,
                  archive_branch_names,
                  data_files,
                  manifest_terms,
                  manifest_branch_names);

    // 3. check if term expired to avoid deleting invisible files.
    for (auto term : manifest_terms)
    {
        if (term > process_term)
        {
            return KvError::ExpiredTerm;
        }
    }

    // 3a. augment retained_files from all branch manifests (regular + archive)
    // in cloud; also build max_file_id_per_branch_term map.
    absl::flat_hash_map<std::string, FileId> max_file_id_per_branch_term;
    err = AugmentRetainedFilesFromBranchManifests(
        tbl_id,
        manifest_branch_names,
        manifest_terms,
        archive_files,
        archive_branch_names,
        retained_files,
        max_file_id_per_branch_term,
        cloud_mgr->options_->pages_per_file_shift,
        static_cast<IouringMgr *>(cloud_mgr));
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteCloudGC: AugmentRetainedFilesFromBranchManifests "
                      "failed, error="
                   << static_cast<int>(err) << "; aborting GC cycle";
        return err;
    }

    // 4. delete unreferenced data files.
    std::vector<std::string> deleted_filenames;
    err = DeleteUnreferencedCloudFiles(tbl_id,
                                       data_files,
                                       manifest_terms,
                                       manifest_branch_names,
                                       retained_files,
                                       max_file_id_per_branch_term,
                                       deleted_filenames,
                                       cloud_mgr);
    if (err != KvError::NoError)
    {
        return err;
    }

    if (!deleted_filenames.empty())
    {
        std::vector<std::string> local_cleanup_targets;
        CollectLocalCleanupTargets(
            tbl_id, deleted_filenames, cloud_mgr, local_cleanup_targets);
        if (!local_cleanup_targets.empty())
        {
            cloud_mgr->ScheduleLocalFileCleanup(tbl_id, local_cleanup_targets);
        }
    }

    /*
    // 5. delete old archives beyond num_retained_archives per branch.
    // NOTE: intentionally AFTER DeleteUnreferencedCloudFiles so all archives
    // contribute their file IDs to retained_files before any are pruned.
    err = DeleteOldArchives(tbl_id,
                            archive_files,
                            archive_tags,
                            archive_branch_names,
                            cloud_mgr->options_->num_retained_archives,
                            static_cast<IouringMgr *>(cloud_mgr));
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteCloudGC: DeleteOldArchives failed, error="
                   << static_cast<int>(err);
        return err;
    }
    */

    return KvError::NoError;
}

}  // namespace FileGarbageCollector

}  // namespace eloqstore
