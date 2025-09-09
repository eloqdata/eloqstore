#include "object_store.h"

#include <glog/logging.h>

#include <chrono>
#include <filesystem>

#include "async_io_manager.h"
namespace eloqstore
{
namespace fs = std::filesystem;
std::atomic<uint64_t> ObjectStore::Task::next_request_id_{1};
ObjectStore::ObjectStore(const KvOptions *options) : options_(options)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    async_http_mgr_ =
        std::make_unique<AsyncHttpManager>("http://127.0.0.1:5572");
}

ObjectStore::~ObjectStore()
{
    curl_global_cleanup();
}

AsyncHttpManager::AsyncHttpManager(const std::string &daemon_url)
    : daemon_url_(daemon_url)
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
void AsyncHttpManager::SubmitRequest(ObjectStore::Task *task)
{
    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR) << "Failed to initialize cURL easy handle";
        task->error_ = KvError::CloudErr;
        if (task->callback_)
        {
            task->callback_(task);
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
        if (task->callback_)
        {
            task->callback_(task);
        }
        return;
    }

    // record the active request
    active_requests_[task->request_id_] = {task, easy};
}
void AsyncHttpManager::SetupDownloadRequest(ObjectStore::DownloadTask *task,
                                            CURL *easy)
{
    Json::Value request;
    request["srcFs"] = task->io_mgr_->options_->cloud_store_path + "/" +
                       task->tbl_id_->ToString();
    request["srcRemote"] = task->filename_;

    fs::path dir_path =
        task->tbl_id_->StorePath(task->io_mgr_->options_->store_path);
    request["dstFs"] = dir_path.string();
    request["dstRemote"] = task->filename_;

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    std::string endpoint = "/operations/copyfile";
    std::string url = daemon_url_ + endpoint;

    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}

void AsyncHttpManager::SetupMultipartUpload(ObjectStore::UploadTask *task,
                                            CURL *easy)
{
    fs::path dir_path =
        task->tbl_id_->StorePath(task->io_mgr_->options_->store_path);

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

    std::string fs_param = task->io_mgr_->options_->cloud_store_path + "/" +
                           task->tbl_id_->ToString();
    std::string url =
        daemon_url_ + "/operations/uploadfile?fs=" + fs_param + "&remote=";

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_MIMEPOST, mime);

    // store mine object for later clean
    task->mime_ = mime;
}
void AsyncHttpManager::SetupListRequest(ObjectStore::ListTask *task, CURL *easy)
{
    Json::Value request;
    request["fs"] = task->options_->cloud_store_path;
    request["remote"] = task->remote_path_;
    request["opt"] = Json::Value(Json::objectValue);
    request["opt"]["recurse"] = false;
    request["opt"]["showHash"] = false;

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    std::string endpoint = "/operations/list";
    std::string url = daemon_url_ + endpoint;

    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}
void AsyncHttpManager::SetupDeleteRequest(ObjectStore::DeleteTask *task,
                                          CURL *easy)
{
    Json::Value request;
    request["fs"] = task->options_->cloud_store_path;
    request["remote"] = task->file_path_;  // 只删除第一个

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    std::string endpoint = "/operations/deletefile";
    std::string url = daemon_url_ + endpoint;

    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}

void AsyncHttpManager::ProcessCompletedRequests()
{
    int running_handles;
    curl_multi_perform(multi_handle_, &running_handles);

    CURLMsg *msg;
    int msgs_left;
    while ((msg = curl_multi_info_read(multi_handle_, &msgs_left)))
    {
        if (msg->msg == CURLMSG_DONE)
        {
            CURL *easy = msg->easy_handle;
            ObjectStore::Task *task;
            curl_easy_getinfo(easy, CURLINFO_PRIVATE, (void **) &task);

            if (!task)
            {
                LOG(ERROR) << "Task is null in ProcessCompletedRequests";
                curl_multi_remove_handle(multi_handle_, easy);
                curl_easy_cleanup(easy);
                continue;
            }

            long response_code;
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
                        LOG(ERROR) << "cURL error: "
                                   << curl_easy_strerror(msg->data.result);
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
                        LOG(ERROR) << "cURL error: "
                                   << curl_easy_strerror(msg->data.result);
                        if (task->retry_count_ < task->max_retries_)
                        {
                            task->retry_count_++;

                            curl_multi_remove_handle(multi_handle_, easy);
                            CleanupTaskResources(task);
                            curl_easy_cleanup(easy);

                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(
                                    100 << task->retry_count_));
                            SubmitRequest(task);
                            continue;  // it need not call callback,and release
                                       // resources
                        }

                        task->error_ = KvError::Timeout;
                        break;
                    case 500:
                    case 502:
                    case 505:
                        LOG(ERROR) << "cURL error: "
                                   << curl_easy_strerror(msg->data.result);
                        task->error_ = KvError::Timeout;
                        break;
                    default:
                        LOG(ERROR) << "cURL error: "
                                   << curl_easy_strerror(msg->data.result);
                        task->error_ = KvError::CloudErr;
                        // task->error_ = KvError::Timeout;
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

            // clean resources
            curl_multi_remove_handle(multi_handle_, easy);

            CleanupTaskResources(task);

            curl_easy_cleanup(easy);
            active_requests_.erase(task->request_id_);

            // we think clean task done ,so the callback can be called
            if (task->callback_)
            {
                task->callback_(task);
            }
        }
    }
}
void AsyncHttpManager::CleanupTaskResources(ObjectStore::Task *task)
{
    if (!task)
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
        if (delete_task->headers_)
        {
            curl_slist_free_all(delete_task->headers_);
            delete_task->headers_ = nullptr;
        }
    }
}
void AsyncHttpManager::Cleanup()
{
    for (auto &[request_id, active_req] : active_requests_)
    {
        // remove easy handle from multi handle
        curl_multi_remove_handle(multi_handle_, active_req.easy_handle);

        // clean Task associated resources
        CleanupTaskResources(active_req.task);

        // clean cURL easy handle
        curl_easy_cleanup(active_req.easy_handle);
    }
    active_requests_.clear();
}

}  // namespace eloqstore