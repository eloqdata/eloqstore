#include "file_gc.h"

#include <jsoncpp/json/json.h>

#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>
#include <fstream>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "error.h"
#include "replayer.h"

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
bool ReadFileContent(fs::path path, std::string &result)
{
    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
        return false;
    }
    size_t size = fs::file_size(path);
    result.resize(size);
    file.read(result.data(), size);
    return true;
}
void FileGarbageCollector::StartLocalThreadPool(uint16_t n_workers)
{
    assert(workers_.empty());
    workers_.reserve(n_workers);
    for (int i = 0; i < n_workers; i++)
    {
        workers_.emplace_back(&FileGarbageCollector::WorkerRoutine, this);
    }
    LOG(INFO) << "local file garbage collector started";
}

void FileGarbageCollector::Stop()
{
    if (workers_.empty())
    {
        return;
    }
    // Send stop signal to all workers.
    std::vector<GcTask> stop_tasks;
    stop_tasks.resize(workers_.size());
    tasks_.enqueue_bulk(stop_tasks.data(), stop_tasks.size());
    for (auto &w : workers_)
    {
        w.join();
    }
    workers_.clear();
    LOG(INFO) << "local file garbage collector stopped";
}

bool FileGarbageCollector::AddTask(TableIdent tbl_id,
                                   uint64_t ts,
                                   FileId max_file_id,
                                   std::unordered_set<FileId> retained_files)
{
    return tasks_.enqueue(
        GcTask(std::move(tbl_id), ts, max_file_id, std::move(retained_files)));
}

FileGarbageCollector::~FileGarbageCollector()
{
    Stop();
}

void FileGarbageCollector::WorkerRoutine()
{
    while (true)
    {
        GcTask req;
        tasks_.wait_dequeue(req);
        if (req.IsStopSignal())
        {
            break;
        }

        KvError err = ExecuteLocalGC(req);
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "Local GC failed for table " << req.tbl_id_.ToString()
                       << ", error: " << static_cast<int>(err);
        }
    }
}
KvError FileGarbageCollector::ExecuteLocalGC(const GcTask &task)
{
    namespace fs = std::filesystem;

    fs::path dir_path = task.tbl_id_.StorePath(options_->store_path);
    std::vector<uint64_t> archives;
    archives.reserve(options_->num_retained_archives + 1);
    std::vector<FileId> gc_data_files;
    gc_data_files.reserve(128);

    // Scan all archives and data files.
    for (auto &ent : fs::directory_iterator{dir_path})
    {
        const std::string name = ent.path().filename();
        if (boost::algorithm::ends_with(name, TmpSuffix))
        {
            // Skip temporary files.
            continue;
        }
        auto ret = ParseFileName(name);
        if (ret.first == FileNameManifest)
        {
            if (!ret.second.empty())
            {
                uint64_t ts = std::stoull(ret.second.data());
                if (ts <= task.mapping_ts_)
                {
                    archives.emplace_back(ts);
                }
            }
        }
        else if (ret.first == FileNameData)
        {
            FileId file_id = std::stoull(ret.second.data());
            if (file_id < task.max_file_id_)
            {
                gc_data_files.emplace_back(file_id);
            }
        }
    }

    // Clear expired archives
    if (archives.size() > options_->num_retained_archives)
    {
        std::sort(archives.begin(), archives.end(), std::greater<uint64_t>());
        while (archives.size() > options_->num_retained_archives)
        {
            uint64_t ts = archives.back();
            archives.pop_back();
            fs::path path = dir_path;
            path.append(ArchiveName(ts));
            if (fs::remove(path))
            {
                LOG(INFO) << "GC on partition " << dir_path << " removed "
                          << path;
            }
            else
            {
                LOG(ERROR) << "can not remove " << path;
            }
        }
    }

    // Get all currently used data files by archives and manifest.
    Replayer replayer(options_);
    std::string buffer;
    fs::path path = dir_path;
    path.append(FileNameManifest);
    std::unordered_set<FileId> all_retained_files = task.retained_files_;

    for (uint64_t ts : archives)
    {
        path.replace_filename(ArchiveName(ts));
        if (!ReadFileContent(path, buffer))
        {
            return KvError::IoFail;
        }
        MemStoreMgr::Manifest manifest(buffer);
        KvError err = replayer.Replay(&manifest);
        if (err != KvError::NoError)
        {
            if (err == KvError::Corrupted)
            {
                bool ok = fs::remove(path);
                LOG(ERROR) << "found corrupted archive " << path
                           << ", removed=" << ok;
                continue;
            }
            return err;
        }
        GetRetainedFiles(all_retained_files,
                         replayer.mapping_tbl_,
                         options_->pages_per_file_shift);
    }

    // Clear unused data files by any archive.
    for (FileId file_id : gc_data_files)
    {
        if (!all_retained_files.contains(file_id))
        {
            path.replace_filename(DataFileName(file_id));
            if (!fs::remove(path))
            {
                LOG(ERROR) << "can not remove " << path;
            }
        }
    }
    return KvError::NoError;
}

// Helper functions for cloud GC optimization
KvError FileGarbageCollector::ListCloudFiles(
    const TableIdent &tbl_id,
    std::vector<std::string> &cloud_files,
    CloudStoreMgr *cloud_mgr,
    const KvOptions *options)
{
    std::string table_path = tbl_id.ToString();
    KvTask *current_task = ThdTask();

    // List all files in cloud.
    ObjectStore::ListTask list_task(table_path, &cloud_files);

    // Set KvTask pointer and initialize inflight_io_
    list_task.SetKvTask(current_task);

    cloud_mgr->GetObjectStore()->GetHttpManager()->SubmitRequest(&list_task);
    current_task->status_ = TaskStatus::Blocked;
    current_task->Yield();

    if (list_task.error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to list cloud files for table " << table_path
                   << ", error: " << static_cast<int>(list_task.error_);
        return list_task.error_;
    }

    // Parse the JSON response.
    try
    {
        Json::Value response;
        Json::Reader reader;
        if (reader.parse(list_task.response_data_, response))
        {
            if (response.isMember("list") && response["list"].isArray())
            {
                cloud_files.clear();
                for (const auto &item : response["list"])
                {
                    if (item.isMember("Name") && item["Name"].isString())
                    {
                        cloud_files.push_back(item["Name"].asString());
                    }
                }
            }
        }
        else
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

void FileGarbageCollector::ClassifyFiles(
    const std::vector<std::string> &cloud_files,
    std::vector<std::string> &archive_files,
    std::vector<uint64_t> &archive_timestamps,
    std::vector<std::string> &data_files)
{
    archive_files.clear();
    archive_timestamps.clear();
    data_files.clear();

    for (const std::string &file_name : cloud_files)
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

KvError FileGarbageCollector::DownloadArchiveFile(
    const TableIdent &tbl_id,
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

    cloud_mgr->GetObjectStore()->GetHttpManager()->SubmitRequest(
        &download_task);
    current_task->status_ = TaskStatus::Blocked;
    current_task->Yield();

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

FileId FileGarbageCollector::ParseArchiveForMaxFileId(
    const std::string &archive_content)
{
    MemStoreMgr::Manifest manifest(archive_content);
    Replayer replayer(options_);

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
    const uint8_t pages_per_file_shift = options_->pages_per_file_shift;

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

KvError FileGarbageCollector::GetOrUpdateArchivedMaxFileId(
    const TableIdent &tbl_id,
    const std::vector<std::string> &archive_files,
    const std::vector<uint64_t> &archive_timestamps,
    uint64_t mapping_ts,
    FileId &archived_max_file_id,
    CloudStoreMgr *cloud_mgr)
{
    // 1. check cached max file id.
    auto &cached_max_ids = cloud_mgr->archived_max_file_ids_;
    auto it = cached_max_ids.find(tbl_id);
    if (it != cached_max_ids.end())
    {
        archived_max_file_id = it->second;
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
        if (ts <= mapping_ts && ts > latest_ts)
        {
            latest_ts = ts;
            latest_archive = archive_files[i];
        }
    }

    if (latest_archive.empty())
    {
        // No available archive file, use default value.
        archived_max_file_id = 0;
        cached_max_ids[tbl_id] = archived_max_file_id;
        return KvError::NoError;
    }

    // 3. download the latest archive file.
    std::string archive_content;
    KvError download_err = DownloadArchiveFile(tbl_id,
                                               latest_archive,
                                               archive_content,
                                               cloud_mgr,
                                               cloud_mgr->options_);
    if (download_err != KvError::NoError)
    {
        return download_err;
    }

    // 4. parse the archive file to get the maximum file ID.
    archived_max_file_id = ParseArchiveForMaxFileId(archive_content);

    // 6. cache the result.
    cached_max_ids[tbl_id] = archived_max_file_id;

    return KvError::NoError;
}

KvError FileGarbageCollector::DeleteUnreferencedDataFiles(
    const TableIdent &tbl_id,
    const std::vector<std::string> &data_files,
    FileId max_file_id,
    const std::unordered_set<FileId> &retained_files,
    FileId archived_max_file_id,
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

        FileId file_id = std::stoull(ret.second.data());

        // Only delete files that meet the following conditions:
        // 1. File ID < max_file_id (not the current writing file)
        // 2. File ID > archived_max_file_id (greater than the archived max file
        // ID)
        // 3. Not in retained_files (files not needed in the current version)
        if (file_id < max_file_id && file_id > archived_max_file_id &&
            !retained_files.contains(file_id))
        {
            std::string remote_path = tbl_id.ToString() + "/" + file_name;
            files_to_delete.push_back(remote_path);
        }
    }

    if (files_to_delete.empty())
    {
        return KvError::NoError;
    }

    // Delete cloud files directly
    KvTask *current_task = ThdTask();

    // Create delete task for all files
    auto delete_task =
        std::make_unique<ObjectStore::DeleteTask>(files_to_delete);

    // Set KvTask pointer
    delete_task->SetKvTask(current_task);

    // Submit each file separately by updating current_index_
    for (size_t i = 0; i < delete_task->file_paths_.size(); ++i)
    {
        delete_task->current_index_ = i;
        cloud_mgr->GetObjectStore()->GetHttpManager()->SubmitRequest(
            delete_task.get());
    }

    current_task->status_ = TaskStatus::Blocked;
    current_task->Yield();

    // Check for errors after all tasks complete
    if (delete_task->error_ != KvError::NoError)
    {
        LOG(ERROR) << "Failed to delete files: "
                   << ErrorString(delete_task->error_);
        return delete_task->error_;
    }

    return KvError::NoError;
}

KvError FileGarbageCollector::ExecuteCloudGC(
    const TableIdent &tbl_id,
    uint64_t mapping_ts,
    FileId max_file_id,
    const std::unordered_set<FileId> &retained_files,
    CloudStoreMgr *cloud_mgr)
{
    // 1. list all files in cloud.
    std::vector<std::string> cloud_files;
    KvError err = ListCloudFiles(tbl_id, cloud_files, cloud_mgr, options_);
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
    FileId archived_max_file_id = 0;
    err = GetOrUpdateArchivedMaxFileId(tbl_id,
                                       archive_files,
                                       archive_timestamps,
                                       mapping_ts,
                                       archived_max_file_id,
                                       cloud_mgr);
    if (err != KvError::NoError)
    {
        return err;
    }

    // 4. delete unreferenced data files.
    err = DeleteUnreferencedDataFiles(tbl_id,
                                      data_files,
                                      max_file_id,
                                      retained_files,
                                      archived_max_file_id,
                                      cloud_mgr);
    if (err != KvError::NoError)
    {
        return err;
    }

    return KvError::NoError;
}
}  // namespace eloqstore