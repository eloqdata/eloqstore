#include "file_gc.h"

#include <jsoncpp/json/json.h>

#include <boost/algorithm/string/predicate.hpp>
#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "async_io_manager.h"
#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "kv_options.h"
#include "replayer.h"
#include "storage/mem_index_page.h"
#include "storage/object_store.h"
#include "tasks/task.h"
#include "utils.h"
namespace eloqstore
{
void GetRetainedFiles(absl::flat_hash_set<FileId> &result,
                      const MappingSnapshot::MappingTbl &tbl,
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
            result.emplace(file_id);
        }
        else if (MappingSnapshot::IsSwizzlingPointer(val))
        {
            MemIndexPage *idx_page = reinterpret_cast<MemIndexPage *>(val);
            FilePageId fp_id = idx_page->GetFilePageId();

            result.emplace(fp_id >> pages_per_file_shift);
        }
        if ((page_id & 0xFF) == 0)
        {
            ThdTask()->YieldToLowPQ();
        }
    }
}

namespace FileGarbageCollector
{

KvError ExecuteLocalGC(const TableIdent &tbl_id,
                       const absl::flat_hash_set<FileId> &retained_files,
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
    std::vector<uint64_t> archive_timestamps;
    std::vector<std::string> archive_branch_names;
    std::vector<std::string> data_files;
    std::vector<uint64_t> manifest_terms;
    std::vector<std::string> manifest_branch_names;
    ClassifyFiles(local_files,
                  archive_files,
                  archive_timestamps,
                  archive_branch_names,
                  data_files,
                  manifest_terms,
                  manifest_branch_names);

    // No need to check term expired for local mode.

    // 2a. augment retained_files from all other branch manifests on disk.
    auto all_retained = retained_files;
    AugmentRetainedFilesFromBranchManifests(tbl_id,
                                            manifest_branch_names,
                                            manifest_terms,
                                            all_retained,
                                            io_mgr->options_->pages_per_file_shift,
                                            io_mgr);

    // 3. get archived max file id.
    FileId least_not_archived_file_id = 0;
    err = GetOrUpdateArchivedMaxFileId(tbl_id,
                                       archive_files,
                                       archive_timestamps,
                                       least_not_archived_file_id,
                                       io_mgr);

    if (err != KvError::NoError)
    {
        LOG(ERROR)
            << "ExecuteLocalGC: GetOrUpdateArchivedMaxFileId failed, error="
            << static_cast<int>(err);
        return err;
    }

    // 4. delete old archives beyond num_retained_archives per branch.
    err = DeleteOldArchives(tbl_id,
                            archive_files,
                            archive_timestamps,
                            archive_branch_names,
                            io_mgr->options_->num_retained_archives,
                            io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR)
            << "ExecuteLocalGC: DeleteOldArchives failed, error="
            << static_cast<int>(err);
        return err;
    }

    // 5. delete unreferenced data files.
    err = DeleteUnreferencedLocalFiles(
        tbl_id, data_files, all_retained, least_not_archived_file_id, io_mgr);
    if (err != KvError::NoError)
    {
        LOG(ERROR)
            << "ExecuteLocalGC: DeleteUnreferencedLocalFiles failed, error="
            << static_cast<int>(err);
        return err;
    }

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
                   std::vector<uint64_t> &archive_timestamps,
                   std::vector<std::string> &archive_branch_names,
                   std::vector<std::string> &data_files,
                   std::vector<uint64_t> &manifest_terms,
                   std::vector<std::string> &manifest_branch_names)
{
    archive_files.clear();
    archive_timestamps.clear();
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
            // manifest_<term>_<ts> Legacy format manifest_<ts> is no longer
            // supported.
            std::string_view branch_name;
            uint64_t term = 0;
            std::optional<uint64_t> timestamp;
            if (!ParseManifestFileSuffix(ret.second, branch_name, term, timestamp))
            {
                continue;
            }
            // Only recognize term-aware archives (both term and timestamp must
            // be present).
            if (timestamp.has_value())
            {
                archive_files.push_back(file_name);
                archive_timestamps.push_back(timestamp.value());
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

KvError DownloadArchiveFile(const TableIdent &tbl_id,
                            const std::string &archive_file,
                            DirectIoBuffer &content,
                            CloudStoreMgr *cloud_mgr,
                            const KvOptions *options)
{
    KvTask *current_task = ThdTask();

    // Download the archive file.
    ObjectStore::DownloadTask download_task(&tbl_id, archive_file);

    // Set KvTask pointer and initialize inflight_io_
    download_task.SetKvTask(current_task);

    cloud_mgr->AcquireCloudSlot(current_task);
    cloud_mgr->GetObjectStore().SubmitTask(&download_task, shard);
    current_task->WaitIo();

    if (download_task.error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to download archive file: " << archive_file
                   << ", error: " << static_cast<int>(download_task.error_);
        return download_task.error_;
    }

    fs::path local_path =
        tbl_id.StorePath(options->store_path, options->store_path_lut) /
        archive_file;

    uint64_t flags = O_WRONLY | O_CREAT | O_DIRECT | O_NOATIME | O_TRUNC;
    KvError write_err = cloud_mgr->WriteFile(
        tbl_id, archive_file, download_task.response_data_, flags);
    cloud_mgr->RecycleBuffer(std::move(download_task.response_data_));
    if (write_err != KvError::NoError)
    {
        LOG(ERROR) << "Failed to persist archive file: " << local_path
                   << ", error: " << static_cast<int>(write_err);
        return write_err;
    }

    KvError err =
        cloud_mgr->ReadArchiveFileAndDelete(tbl_id, archive_file, content);
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "Failed to read archive file: " << local_path
                   << ", error: " << static_cast<int>(err);
        return err;
    }

    LOG(INFO) << "Successfully downloaded and read archive file: "
              << archive_file;
    return KvError::NoError;
}

KvError AugmentRetainedFilesFromBranchManifests(
    const TableIdent &tbl_id,
    const std::vector<std::string> &manifest_branch_names,
    const std::vector<uint64_t> &manifest_terms,
    absl::flat_hash_set<FileId> &retained_files,
    uint8_t pages_per_file_shift,
    IouringMgr *io_mgr)
{
    assert(manifest_branch_names.size() == manifest_terms.size());

    for (size_t i = 0; i < manifest_branch_names.size(); ++i)
    {
        const std::string &branch = manifest_branch_names[i];
        uint64_t term = manifest_terms[i];
        std::string filename = BranchManifestFileName(branch, term);

        DirectIoBuffer buf;
        KvError err = KvError::NoError;

        if (!io_mgr->options_->cloud_store_path.empty())
        {
            // Cloud mode: download the manifest file from cloud.
            CloudStoreMgr *cloud_mgr = static_cast<CloudStoreMgr *>(io_mgr);
            err = DownloadArchiveFile(
                tbl_id, filename, buf, cloud_mgr, cloud_mgr->options_);
        }
        else
        {
            // Local mode: read directly from disk.
            err = io_mgr->ReadFile(tbl_id, filename, buf);
        }

        if (err != KvError::NoError)
        {
            LOG(WARNING)
                << "AugmentRetainedFilesFromBranchManifests: failed to read "
                   "manifest "
                << filename << " for branch " << branch << " term " << term
                << ", error=" << static_cast<int>(err) << "; skipping";
            continue;
        }

        MemStoreMgr::Manifest manifest(buf.view());
        Replayer replayer(Options());
        replayer.branch_metadata_.term = term;

        KvError replay_err = replayer.Replay(&manifest);
        if (replay_err != KvError::NoError)
        {
            LOG(WARNING)
                << "AugmentRetainedFilesFromBranchManifests: failed to replay "
                   "manifest "
                << filename << " for branch " << branch << " term " << term
                << ", error=" << static_cast<int>(replay_err) << "; skipping";
            continue;
        }

        GetRetainedFiles(retained_files, replayer.mapping_tbl_, pages_per_file_shift);

        DLOG(INFO) << "AugmentRetainedFilesFromBranchManifests: augmented from "
                   << "branch=" << branch << " term=" << term
                   << ", retained_files now size=" << retained_files.size();
    }

    return KvError::NoError;
}

FileId ParseArchiveForMaxFileId(const std::string &archive_filename,
                                std::string_view archive_content)
{
    MemStoreMgr::Manifest manifest(archive_content);
    Replayer replayer(Options());

    // Extract manifest term from archive filename if present.
    uint64_t manifest_term = ManifestTermFromFilename(archive_filename);
    if (manifest_term != 0)
    {
        // Set branch metadata term for GC replay
        replayer.branch_metadata_.term = manifest_term;
    }

    KvError err = replayer.Replay(&manifest);
    if (err != KvError::NoError)
    {
        if (err == KvError::Corrupted)
        {
            LOG(ERROR) << "Found corrupted archive content";
            return 0;  // Corrupted archive, ignore.
        }
        LOG(ERROR) << "Failed to replay archive: " << static_cast<int>(err);
        return 0;
    }

    // Find the maximum file ID from the mapping table.
    FileId max_file_id = 0;
    const uint8_t pages_per_file_shift = Options()->pages_per_file_shift;

    for (PageId page_id = 0; page_id < replayer.mapping_tbl_.size(); ++page_id)
    {
        uint64_t val = replayer.mapping_tbl_.Get(page_id);
        if (MappingSnapshot::IsFilePageId(val))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(val);
            FileId file_id = fp_id >> pages_per_file_shift;
            if (file_id > max_file_id)
            {
                max_file_id = file_id;
            }
        }
    }

    return max_file_id;
}

KvError GetOrUpdateArchivedMaxFileId(
    const TableIdent &tbl_id,
    const std::vector<std::string> &archive_files,
    const std::vector<uint64_t> &archive_timestamps,
    FileId &least_not_archived_file_id,
    IouringMgr *io_mgr)
{
    // 1. check cached max file id.
    auto &cached_max_ids = io_mgr->least_not_archived_file_ids_;
    auto it = cached_max_ids.find(tbl_id);
    if (it != cached_max_ids.end())
    {
        least_not_archived_file_id = it->second;
        return KvError::NoError;
    }

    // 2. find the latest archive file (timestamp <= mapping_ts).
    // mapping_ts is the current timestamp, ensure only completed archive files
    // are processed.
    std::string latest_archive;
    uint64_t latest_ts = 0;
    for (size_t i = 0; i < archive_files.size(); ++i)
    {
        uint64_t ts = archive_timestamps[i];
        if (ts > latest_ts)
        {
            latest_ts = ts;
            latest_archive = archive_files[i];
        }
    }

    if (latest_archive.empty())
    {
        // No available archive file, use default value.
        assert(least_not_archived_file_id == 0);
        cached_max_ids[tbl_id] = least_not_archived_file_id;
        return KvError::NoError;
    }

    // 3. read archive file based on mode (cloud or local).
    DirectIoBuffer archive_content;
    KvError read_err = KvError::NoError;

    if (!io_mgr->options_->cloud_store_path.empty())
    {
        // Cloud mode: download the archive file
        CloudStoreMgr *cloud_mgr = static_cast<CloudStoreMgr *>(io_mgr);
        read_err = DownloadArchiveFile(tbl_id,
                                       latest_archive,
                                       archive_content,
                                       cloud_mgr,
                                       cloud_mgr->options_);
    }
    else
    {
        read_err = io_mgr->ReadFile(tbl_id, latest_archive, archive_content);
        if (read_err != KvError::NoError)
        {
            fs::path dir_path = tbl_id.StorePath(
                io_mgr->options_->store_path, io_mgr->options_->store_path_lut);
            fs::path archive_path = dir_path / latest_archive;
            LOG(ERROR) << "Failed to read archive file: " << archive_path;
        }
    }

    if (read_err != KvError::NoError)
    {
        return read_err;
    }

    // 4. parse the archive file to get the maximum file ID.
    least_not_archived_file_id =
        ParseArchiveForMaxFileId(latest_archive, archive_content.view()) + 1;

    // 5. cache the result.
    cached_max_ids[tbl_id] = least_not_archived_file_id;

    return KvError::NoError;
}

KvError DeleteOldArchives(
    const TableIdent &tbl_id,
    const std::vector<std::string> &archive_files,
    const std::vector<uint64_t> &archive_timestamps,
    const std::vector<std::string> &archive_branch_names,
    uint32_t num_retained_archives,
    IouringMgr *io_mgr)
{
    assert(archive_files.size() == archive_timestamps.size());
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

    // For each branch, sort by timestamp descending and collect excess archives.
    std::vector<std::string> to_delete;
    for (auto &[branch, indices] : branch_indices)
    {
        if (indices.size() <= num_retained_archives)
        {
            continue;
        }
        // Sort descending by timestamp (newest first).
        std::sort(indices.begin(),
                  indices.end(),
                  [&](size_t a, size_t b)
                  { return archive_timestamps[a] > archive_timestamps[b]; });
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
        fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path);

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
    const absl::flat_hash_set<FileId> &retained_files,
    FileId least_not_archived_file_id,
    CloudStoreMgr *cloud_mgr)
{
    std::vector<std::string> files_to_delete;
    auto process_term = cloud_mgr->ProcessTerm();

    for (const std::string &file_name : data_files)
    {
        auto ret = ParseFileName(file_name);
        if (ret.first != FileNameData)
        {
            continue;
        }

        FileId file_id = 0;
        std::string_view branch_name;
        [[maybe_unused]] uint64_t term = 0;
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

        // Only delete files that meet the following conditions:
        // 1. File ID >= least_not_archived_file_id (greater than the archived
        // max file ID)
        // 2. Not in retained_files (files not needed in the current version)
        if (file_id >= least_not_archived_file_id &&
            !retained_files.contains(file_id))
        {
            std::string remote_path = tbl_id.ToString() + "/" + file_name;
            files_to_delete.push_back(remote_path);
        }
        else
        {
            DLOG(INFO) << "skip file since file_id=" << file_id
                       << ", least_not_archived_file_id="
                       << least_not_archived_file_id;
        }
    }

    if (files_to_delete.size() == data_files.size())
    {
        // Find the branch name for the current process_term manifest.
        std::string_view current_manifest_branch = MainBranchName;
        for (size_t i = 0; i < manifest_terms.size(); ++i)
        {
            if (manifest_terms[i] == process_term)
            {
                current_manifest_branch = manifest_branch_names[i];
                break;
            }
        }
        files_to_delete.emplace_back(
            tbl_id.ToString() + "/" +
            BranchManifestFileName(current_manifest_branch, process_term));
    }

    // Delete superseded manifest files: only manifests belonging to the same
    // branch as the current process_term manifest are version-chained and safe
    // to prune. Manifests for OTHER branches are managed by DeleteBranch and
    // must not be deleted here.
    {
        // Identify the active branch (the one whose manifest carries process_term).
        std::string_view active_branch = MainBranchName;
        for (size_t i = 0; i < manifest_terms.size(); ++i)
        {
            if (manifest_terms[i] == process_term)
            {
                active_branch = manifest_branch_names[i];
                break;
            }
        }
        for (size_t i = 0; i < manifest_terms.size(); ++i)
        {
            if (manifest_terms[i] < process_term &&
                manifest_branch_names[i] == active_branch)
            {
                files_to_delete.emplace_back(
                    tbl_id.ToString() + "/" +
                    BranchManifestFileName(manifest_branch_names[i],
                                           manifest_terms[i]));
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

    for (const auto &task : delete_tasks)
    {
        if (task.error_ != KvError::NoError)
        {
            LOG(ERROR) << "Failed to delete file " << task.remote_path_ << ": "
                       << ErrorString(task.error_);
            return task.error_;
        }
    }

    return KvError::NoError;
}

KvError DeleteUnreferencedLocalFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const absl::flat_hash_set<FileId> &retained_files,
    FileId least_not_archived_file_id,
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
        [[maybe_unused]] uint64_t term = 0;
        if (!ParseDataFileSuffix(ret.second, file_id, branch_name, term))
        {
            continue;
        }

        // Only delete files that meet the following conditions:
        // 1. File ID >= least_not_archived_file_id (greater than or equal to
        // the archived max file ID)
        // 2. Not in retained_files (files not needed in the current version)
        if (file_id >= least_not_archived_file_id &&
            !retained_files.contains(file_id))
        {
            fs::path file_path = dir_path / file_name;
            files_to_delete.push_back(file_path.string());
            file_ids_to_close.push_back(file_id);
            DLOG(INFO) << "ExecuteLocalGC: marking file for deletion: "
                       << file_name << " (file_id=" << file_id << ")";
        }
        else
        {
            DLOG(INFO) << "ExecuteLocalGC: skip file " << file_name
                       << " since file_id=" << file_id
                       << ", least_not_archived_file_id="
                       << least_not_archived_file_id << ", in_retained="
                       << (retained_files.contains(file_id) ? "true" : "false");
        }
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
                       const absl::flat_hash_set<FileId> &retained_files,
                       CloudStoreMgr *cloud_mgr)
{
    // Check term file before proceeding
    uint64_t process_term = cloud_mgr->ProcessTerm();
    auto [term_file_term, etag, err] = cloud_mgr->ReadTermFile(tbl_id);

    if (err == KvError::NotFound)
    {
        // Legacy table - proceed with existing manifest term validation
        // (backward compatible behavior)
        LOG(INFO) << "ExecuteCloudGC: term file not found for table " << tbl_id;
        return KvError::NoError;
    }
    else if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteCloudGC: failed to read term file for table "
                   << tbl_id << " : " << ErrorString(err);
        return err;
    }
    else
    {
        // Term file exists - validate
        if (term_file_term != process_term)
        {
            LOG(WARNING) << "ExecuteCloudGC: term file term " << term_file_term
                         << " != process_term " << process_term << " for table "
                         << tbl_id << ", skipping GC";
            return KvError::ExpiredTerm;
        }
        // term_file_term == process_term, proceed with GC
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
    std::vector<uint64_t> archive_timestamps;
    std::vector<std::string> archive_branch_names;
    std::vector<std::string> data_files;
    std::vector<uint64_t> manifest_terms;
    std::vector<std::string> manifest_branch_names;
    ClassifyFiles(cloud_files,
                  archive_files,
                  archive_timestamps,
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

    // 3a. augment retained_files from all other branch manifests in cloud.
    auto all_retained = retained_files;
    AugmentRetainedFilesFromBranchManifests(
        tbl_id,
        manifest_branch_names,
        manifest_terms,
        all_retained,
        cloud_mgr->options_->pages_per_file_shift,
        static_cast<IouringMgr *>(cloud_mgr));

    // 4. get or update archived max file id.
    FileId least_not_archived_file_id = 0;
    err = GetOrUpdateArchivedMaxFileId(tbl_id,
                                       archive_files,
                                       archive_timestamps,
                                       least_not_archived_file_id,
                                       static_cast<IouringMgr *>(cloud_mgr));
    if (err != KvError::NoError)
    {
        return err;
    }

    // 4a. delete old archives beyond num_retained_archives per branch.
    err = DeleteOldArchives(tbl_id,
                            archive_files,
                            archive_timestamps,
                            archive_branch_names,
                            cloud_mgr->options_->num_retained_archives,
                            static_cast<IouringMgr *>(cloud_mgr));
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "ExecuteCloudGC: DeleteOldArchives failed, error="
                   << static_cast<int>(err);
        return err;
    }

    // 5. delete unreferenced data files.
    err = DeleteUnreferencedCloudFiles(tbl_id,
                                       data_files,
                                       manifest_terms,
                                       manifest_branch_names,
                                       all_retained,
                                       least_not_archived_file_id,
                                       cloud_mgr);
    if (err != KvError::NoError)
    {
        return err;
    }

    return KvError::NoError;
}

}  // namespace FileGarbageCollector

}  // namespace eloqstore
