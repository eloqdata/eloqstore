#include "file_gc.h"

#include <jsoncpp/json/json.h>

#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>
#include <unordered_set>
#include <vector>

#include "async_io_manager.h"
#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "kv_options.h"
#include "object_store.h"
#include "replayer.h"
#include "task.h"
#include "utils.h"

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift)
{
    for (uint64_t val : tbl)
    {
        if (MappingSnapshot::IsFilePageId(val))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(val);
            FileId file_id = fp_id >> pages_per_file_shift;
            result.emplace(file_id);
        }
    }
};

namespace FileGarbageCollector
{

KvError ExecuteLocalGC(const TableIdent &tbl_id,
                       const std::unordered_set<FileId> &retained_files,
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
    std::vector<std::string> data_files;
    ClassifyFiles(local_files, archive_files, archive_timestamps, data_files);

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

    // 4. delete unreferenced data files.
    err = DeleteUnreferencedLocalFiles(
        tbl_id, data_files, retained_files, least_not_archived_file_id, io_mgr);

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

    fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path);

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

    // List all files in cloud.
    ObjectStore::ListTask list_task(table_path);

    // Set KvTask pointer and initialize inflight_io_
    list_task.SetKvTask(current_task);

    cloud_mgr->GetObjectStore().GetHttpManager()->SubmitRequest(&list_task);
    current_task->WaitIo();

    if (list_task.error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to list cloud files for table " << table_path
                   << ", error: " << static_cast<int>(list_task.error_);
        return list_task.error_;
    }

    // Parse the JSON response.
    try
    {
        if (!utils::ParseRCloneListObjectsResponse(list_task.response_data_,
                                                   &cloud_files))
        {
            LOG(ERROR) << "Failed to parse JSON response: "
                       << list_task.response_data_;
            return KvError::Corrupted;
        }
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "JSON parsing exception: " << e.what();
        return KvError::Corrupted;
    }

    return KvError::NoError;
}

void ClassifyFiles(const std::vector<std::string> &files,
                   std::vector<std::string> &archive_files,
                   std::vector<uint64_t> &archive_timestamps,
                   std::vector<std::string> &data_files)
{
    archive_files.clear();
    archive_timestamps.clear();
    data_files.clear();

    for (const std::string &file_name : files)
    {
        // Ignore temporary files.
        if (boost::algorithm::ends_with(file_name, TmpSuffix))
        {
            continue;
        }

        // rclone list command returns the file name directly, no need to
        // handle the path prefix.
        auto ret = ParseFileName(file_name);

        if (ret.first == FileNameManifest)
        {
            // archive file: manifest_<timestamp>
            // Only manifest files with timestamp are archive files.
            std::string_view ts_str = ret.second;
            if (!ts_str.empty())
            {
                uint64_t timestamp = std::stoull(std::string(ts_str));
                archive_files.push_back(file_name);
                archive_timestamps.push_back(timestamp);
            }
            // Manifest files without timestamp are current manifest, ignore.
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
                            std::string &content,
                            CloudStoreMgr *cloud_mgr,
                            const KvOptions *options)
{
    KvTask *current_task = ThdTask();

    // Download the archive file.
    ObjectStore::DownloadTask download_task(&tbl_id, archive_file);

    // Set KvTask pointer and initialize inflight_io_
    download_task.SetKvTask(current_task);

    cloud_mgr->GetObjectStore().GetHttpManager()->SubmitRequest(&download_task);
    current_task->WaitIo();

    if (download_task.error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to download archive file: " << archive_file
                   << ", error: " << static_cast<int>(download_task.error_);
        return download_task.error_;
    }

    fs::path local_path = tbl_id.StorePath(options->store_path) / archive_file;

    KvError err =
        cloud_mgr->ReadArchiveFileAndDelete(local_path.string(), content);
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

FileId ParseArchiveForMaxFileId(const std::string &archive_content)
{
    MemStoreMgr::Manifest manifest(archive_content);
    Replayer replayer(Options());

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

    for (uint64_t val : replayer.mapping_tbl_)
    {
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
    std::string archive_content;
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
        // Local mode: read the archive file directly
        fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path);
        fs::path archive_path = dir_path / latest_archive;

        read_err =
            io_mgr->ReadArchiveFile(archive_path.string(), archive_content);
        if (read_err != KvError::NoError)
        {
            LOG(ERROR) << "Failed to read archive file: " << archive_path;
        }
    }

    if (read_err != KvError::NoError)
    {
        return read_err;
    }

    // 4. parse the archive file to get the maximum file ID.
    least_not_archived_file_id = ParseArchiveForMaxFileId(archive_content) + 1;

    // 5. cache the result.
    cached_max_ids[tbl_id] = least_not_archived_file_id;

    return KvError::NoError;
}

KvError DeleteUnreferencedCloudFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    const std::unordered_set<FileId> &retained_files,
    FileId least_not_archived_file_id,
    CloudStoreMgr *cloud_mgr)
{
    std::vector<std::string> files_to_delete;

    for (const std::string &file_name : data_files)
    {
        auto ret = ParseFileName(file_name);
        if (ret.first != FileNameData)
        {
            continue;
        }

        FileId file_id = std::stoull(std::string(ret.second));

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

    if (files_to_delete.empty())
    {
        return KvError::NoError;
    }

    KvTask *current_task = ThdTask();
    auto *http_mgr = cloud_mgr->GetObjectStore().GetHttpManager();

    // If we're deleting every file in the directory, issue a single purge
    // request for the table path.
    if (files_to_delete.size() == data_files.size())
    {
        ObjectStore::DeleteTask delete_task(tbl_id.ToString(), true);
        delete_task.SetKvTask(current_task);
        http_mgr->SubmitRequest(&delete_task);
        current_task->WaitIo();

        if (delete_task.error_ != KvError::NoError)
        {
            LOG(ERROR) << "Failed to delete directory: "
                       << ErrorString(delete_task.error_);
            return delete_task.error_;
        }

        return KvError::NoError;
    }

    std::vector<ObjectStore::DeleteTask> delete_tasks;
    delete_tasks.reserve(files_to_delete.size());

    for (const std::string &remote_path : files_to_delete)
    {
        delete_tasks.emplace_back(remote_path, false);
        ObjectStore::DeleteTask &task = delete_tasks.back();
        task.SetKvTask(current_task);
        http_mgr->SubmitRequest(&task);
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
    const std::unordered_set<FileId> &retained_files,
    FileId least_not_archived_file_id,
    IouringMgr *io_mgr)
{
    namespace fs = std::filesystem;
    fs::path dir_path = tbl_id.StorePath(io_mgr->options_->store_path);

    std::vector<std::string> files_to_delete;

    for (const std::string &file_name : data_files)
    {
        auto ret = ParseFileName(file_name);
        if (ret.first != FileNameData)
        {
            DLOG(INFO) << "ExecuteLocalGC: skipping non-data file: "
                       << file_name;
            continue;
        }

        FileId file_id = std::stoull(std::string(ret.second));

        // Only delete files that meet the following conditions:
        // 1. File ID >= least_not_archived_file_id (greater than or equal to
        // the archived max file ID)
        // 2. Not in retained_files (files not needed in the current version)
        if (file_id >= least_not_archived_file_id &&
            !retained_files.contains(file_id))
        {
            fs::path file_path = dir_path / file_name;
            files_to_delete.push_back(file_path.string());
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
                       const std::unordered_set<FileId> &retained_files,
                       CloudStoreMgr *cloud_mgr)
{
    // 1. list all files in cloud.
    std::vector<std::string> cloud_files;
    KvError err = ListCloudFiles(tbl_id, cloud_files, cloud_mgr);
    DLOG(INFO) << "ListCloudFiles got " << cloud_files.size() << " files";
    if (err != KvError::NoError)
    {
        return err;
    }

    // 2. classify files.
    std::vector<std::string> archive_files;
    std::vector<uint64_t> archive_timestamps;
    std::vector<std::string> data_files;
    ClassifyFiles(cloud_files, archive_files, archive_timestamps, data_files);

    // 3. get or update archived max file id.
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

    // 4. delete unreferenced data files.
    err = DeleteUnreferencedCloudFiles(tbl_id,
                                       data_files,
                                       retained_files,
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
