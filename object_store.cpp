#include "object_store.h"

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "task.h"
namespace eloqstore
{
namespace fs = std::filesystem;
ObjectStore::ObjectStore(const KvOptions *options)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    async_http_mgr_ = std::make_unique<AsyncHttpManager>(options);
}

ObjectStore::~ObjectStore()
{
    curl_global_cleanup();
}

AsyncHttpManager::AsyncHttpManager(const KvOptions *options)
    : daemon_url_(options->cloud_store_daemon_url),
      daemon_upload_url_(daemon_url_ + "/operations/uploadfile?remote=&fs="),
      daemon_download_url_(daemon_url_ + "/operations/copyfile"),
      daemon_list_url_(daemon_url_ + "/operations/list"),
      daemon_delete_url_(daemon_url_ + "/operations/deletefile"),
      options_(options)
{
    multi_handle_ = curl_multi_init();
    if (!multi_handle_)
    {
        LOG(FATAL) << "Failed to initialize cURL multi handle";
    }

    // set the max connections
    curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, 20L);
}

AsyncHttpManager::~AsyncHttpManager()
{
    Cleanup();
    if (multi_handle_)
    {
        curl_multi_cleanup(multi_handle_);
    }
}

void AsyncHttpManager::PerformRequests()
{
    curl_multi_perform(multi_handle_, &running_handles_);
}

void AsyncHttpManager::SubmitRequest(ObjectStore::Task *task)
{
    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR) << "Failed to initialize cURL easy handle";
        task->error_ = KvError::CloudErr;
        if (task->kv_task_)
        {
            task->kv_task_->Resume();
        }
        return;
    }

    // base configuration
    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &task->response_data_);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, task);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, 300L);

    switch (task->TaskType())
    {
    case ObjectStore::Task::Type::AsyncDownload:
        SetupDownloadRequest(static_cast<ObjectStore::DownloadTask *>(task),
                             easy);
        break;
    case ObjectStore::Task::Type::AsyncUpload:
        SetupMultipartUpload(static_cast<ObjectStore::UploadTask *>(task),
                             easy);
        break;
    case ObjectStore::Task::Type::AsyncList:
        SetupListRequest(static_cast<ObjectStore::ListTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncDelete:
        SetupDeleteRequest(static_cast<ObjectStore::DeleteTask *>(task), easy);
        break;
    default:
        LOG(ERROR) << "Unknown async task type";
        curl_easy_cleanup(easy);
        return;
    }

    // add to multi handle
    CURLMcode mres = curl_multi_add_handle(multi_handle_, easy);
    if (mres != CURLM_OK)
    {
        LOG(ERROR) << "Failed to add handle to multi: "
                   << curl_multi_strerror(mres);
        curl_easy_cleanup(easy);
        task->error_ = KvError::CloudErr;
        if (task->kv_task_)
        {
            task->kv_task_->Resume();
        }
        return;
    }

    // increment inflight_io_ when request is successfully submitted
    task->inflight_io_++;

    // record the active request using CURL handle as key
    active_requests_[easy] = task;
}

void AsyncHttpManager::SetupDownloadRequest(ObjectStore::DownloadTask *task,
                                            CURL *easy)
{
    Json::Value request;
    request["srcFs"] =
        options_->cloud_store_path + "/" + task->tbl_id_->ToString();
    request["srcRemote"] = task->filename_.data();

    fs::path dir_path = task->tbl_id_->StorePath(options_->store_path);
    request["dstFs"] = dir_path.string();
    request["dstRemote"] = task->filename_.data();

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, daemon_download_url_.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}

void AsyncHttpManager::SetupMultipartUpload(ObjectStore::UploadTask *task,
                                            CURL *easy)
{
    fs::path dir_path = task->tbl_id_->StorePath(options_->store_path);

    // use the new MIME API
    curl_mime *mime = curl_mime_init(easy);

    for (const std::string &filename : task->filenames_)
    {
        std::string filepath = (dir_path / filename).string();
        if (std::filesystem::exists(filepath))
        {
            curl_mimepart *part = curl_mime_addpart(mime);
            curl_mime_name(part, "file");
            curl_mime_filedata(part, filepath.c_str());
        }
    }

    std::string fs_param =
        options_->cloud_store_path + "/" + task->tbl_id_->ToString();
    std::string url = daemon_upload_url_ + fs_param;

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_MIMEPOST, mime);

    // store mine object for later clean
    task->mime_ = mime;
}

void AsyncHttpManager::SetupListRequest(ObjectStore::ListTask *task, CURL *easy)
{
    Json::Value request;
    request["fs"] = options_->cloud_store_path;
    request["remote"] = task->remote_path_;
    request["opt"] = Json::Value(Json::objectValue);
    request["opt"]["recurse"] = false;
    request["opt"]["showHash"] = false;

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, daemon_list_url_.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}
void AsyncHttpManager::SetupDeleteRequest(ObjectStore::DeleteTask *task,
                                          CURL *easy)
{
    // Process single file based on current_index_
    if (task->current_index_ >= task->file_paths_.size())
    {
        LOG(ERROR) << "DeleteTask current_index_ out of range";
        return;
    }

    size_t index = task->current_index_;

    Json::Value request;
    request["fs"] = options_->cloud_store_path;
    request["remote"] = task->file_paths_[index];

    Json::StreamWriterBuilder builder;
    task->json_data_list_[index] = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    task->headers_list_[index] = headers;

    curl_easy_setopt(easy, CURLOPT_URL, daemon_delete_url_.c_str());
    curl_easy_setopt(
        easy, CURLOPT_POSTFIELDS, task->json_data_list_[index].c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
}

void AsyncHttpManager::ProcessCompletedRequests()
{
    if (IsIdle())
    {
        return;
    }

    CURLMsg *msg;
    int msgs_left;
    while ((msg = curl_multi_info_read(multi_handle_, &msgs_left)))
    {
        if (msg->msg == CURLMSG_DONE)
        {
            CURL *easy = msg->easy_handle;
            ObjectStore::Task *task;
            curl_easy_getinfo(
                easy, CURLINFO_PRIVATE, reinterpret_cast<void **>(&task));

            if (!task)
            {
                LOG(ERROR) << "Task is null in ProcessCompletedRequests";
                curl_multi_remove_handle(multi_handle_, easy);
                curl_easy_cleanup(easy);
                continue;
            }

            int64_t response_code;
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);

            if (msg->data.result == CURLE_OK)
            {
                if (response_code == 200)
                {
                    task->error_ = KvError::NoError;
                }
                else
                {
                    switch (response_code)
                    {
                    case 400:
                    case 401:
                    case 403:
                    case 409:
                        LOG(ERROR) << "HTTP error: " << response_code;
                        task->error_ = KvError::CloudErr;
                        break;
                    case 404:
                        task->error_ =
                            KvError::NotFound;  // 404 is a normal case
                        break;
                    case 408:
                    case 504:
                    case 429:
                    case 503:
                        LOG(ERROR) << "HTTP error: " << response_code;
                        if (task->retry_count_ < task->max_retries_)
                        {
                            task->retry_count_++;

                            curl_multi_remove_handle(multi_handle_, easy);
                            curl_easy_cleanup(easy);
                            active_requests_.erase(easy);
                            task->inflight_io_--;
                            CleanupTaskResources(task);

                            SubmitRequest(task);
                            continue;
                        }

                        task->error_ = KvError::Timeout;
                        break;
                    case 500:
                    case 502:
                    case 505:
                        LOG(ERROR) << "HTTP error: " << response_code;
                        task->error_ = KvError::Timeout;
                        break;
                    default:
                        LOG(ERROR) << "HTTP error: " << response_code;
                        task->error_ = KvError::CloudErr;
                        break;
                    }
                }
            }
            else
            {
                LOG(ERROR) << "cURL error: "
                           << curl_easy_strerror(msg->data.result);
                task->error_ = KvError::CloudErr;
            }

            if (task->TaskType() == ObjectStore::Task::Type::AsyncDelete)
            {
                auto *delete_task =
                    static_cast<ObjectStore::DeleteTask *>(task);
                if (task->error_ != KvError::NoError &&
                    !delete_task->has_error_)
                {
                    delete_task->has_error_ = true;
                    delete_task->first_error_ = task->error_;
                }
            }

            // clean curl resources first
            curl_multi_remove_handle(multi_handle_, easy);
            curl_easy_cleanup(easy);
            active_requests_.erase(easy);
            task->inflight_io_--;

            if (task->inflight_io_ == 0)
            {
                CleanupTaskResources(task);

                if (task->kv_task_)
                {
                    if (task->TaskType() ==
                        ObjectStore::Task::Type::AsyncDelete)
                    {
                        auto *delete_task =
                            static_cast<ObjectStore::DeleteTask *>(task);
                        if (delete_task->has_error_)
                        {
                            task->error_ = delete_task->first_error_;
                        }
                    }
                    task->kv_task_->Resume();
                }
            }
        }
    }
}

void AsyncHttpManager::CleanupTaskResources(ObjectStore::Task *task)
{
    if (!task || task->inflight_io_ > 0)
    {
        return;
    }

    if (task->TaskType() == ObjectStore::Task::Type::AsyncUpload)
    {
        auto upload_task = static_cast<ObjectStore::UploadTask *>(task);
        if (upload_task->mime_)
        {
            curl_mime_free(upload_task->mime_);  // free the mime object
            upload_task->mime_ = nullptr;
        }
        if (upload_task->headers_)
        {
            // free the header list
            curl_slist_free_all(upload_task->headers_);
            upload_task->headers_ = nullptr;
        }
    }
    else if (task->TaskType() == ObjectStore::Task::Type::AsyncDownload)
    {
        auto download_task = static_cast<ObjectStore::DownloadTask *>(task);
        if (download_task->headers_)
        {
            curl_slist_free_all(download_task->headers_);
            download_task->headers_ = nullptr;
        }
    }
    else if (task->TaskType() == ObjectStore::Task::Type::AsyncList)
    {
        auto list_task = static_cast<ObjectStore::ListTask *>(task);
        if (list_task->headers_)
        {
            curl_slist_free_all(list_task->headers_);
            list_task->headers_ = nullptr;
        }
    }
    else if (task->TaskType() == ObjectStore::Task::Type::AsyncDelete)
    {
        auto delete_task = static_cast<ObjectStore::DeleteTask *>(task);
        for (auto &headers : delete_task->headers_list_)
        {
            if (headers)
            {
                curl_slist_free_all(headers);
            }
        }
        delete_task->headers_list_.clear();
        delete_task->json_data_list_.clear();
    }
}

void AsyncHttpManager::Cleanup()
{
    for (auto &[easy, task] : active_requests_)
    {
        // remove easy handle from multi handle
        curl_multi_remove_handle(multi_handle_, easy);

        // clean Task associated resources
        CleanupTaskResources(task);

        // clean cURL easy handle
        curl_easy_cleanup(easy);

        task->inflight_io_--;
    }
    active_requests_.clear();
}

}  // namespace eloqstore
