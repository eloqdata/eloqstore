#include "object_store.h"

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "task.h"
namespace eloqstore
{
namespace fs = std::filesystem;
namespace
{
std::string NormalizeBaseUrl(std::string url)
{
    // Add default scheme/host if only a port or host:port is provided.
    if (url.find("://") == std::string::npos)
    {
        if (!url.empty() && url.front() == ':')
        {
            url.erase(url.begin());
        }
        // If the string is only digits, treat it as a port on localhost.
        bool all_digits =
            !url.empty() &&
            std::all_of(url.begin(),
                        url.end(),
                        [](char c) {
                            return std::isdigit(static_cast<unsigned char>(c));
                        });
        if (all_digits)
        {
            url = "127.0.0.1:" + url;
        }
        if (url.find("://") == std::string::npos)
        {
            url = "http://" + url;
        }
    }
    while (!url.empty() && url.back() == '/')
    {
        url.pop_back();
    }
    return url;
}
}  // namespace

ObjectStore::ObjectStore(const KvOptions *options)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    async_http_mgr_ = std::make_unique<AsyncHttpManager>(options);
}

ObjectStore::~ObjectStore()
{
    curl_global_cleanup();
}

AsyncHttpManager::AsyncHttpManager(const KvOptions *options) : options_(options)
{
    const auto &daemon_urls = options_->cloud_store_daemon_ports;
    for (const std::string &raw_url : daemon_urls)
    {
        std::string url = NormalizeBaseUrl(raw_url);
        if (url.empty())
        {
            continue;
        }
        DaemonEndpoint endpoint{url,
                                url + "/operations/uploadfile?remote=&fs=",
                                url + "/operations/copyfile",
                                url + "/operations/list",
                                url + "/operations/deletefile",
                                url + "/operations/purge"};
        endpoints_.emplace_back(std::move(endpoint));
    }
    CHECK(!endpoints_.empty())
        << "cloud_store_daemon_ports must contain at least one endpoint";

    multi_handle_ = curl_multi_init();
    if (!multi_handle_)
    {
        LOG(FATAL) << "Failed to initialize cURL multi handle";
    }

    // set the max connections
    curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, 200L);
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
    ProcessPendingRetries();
    curl_multi_perform(multi_handle_, &running_handles_);
}

void AsyncHttpManager::SubmitRequest(ObjectStore::Task *task)
{
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    bool is_retry = task->waiting_retry_;
    task->waiting_retry_ = false;

    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR) << "Failed to initialize cURL easy handle";
        task->error_ = KvError::CloudErr;
        task->kv_task_->Resume();
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
        task->kv_task_->Resume();
        return;
    }

    // record the active request using CURL handle as key
    active_requests_[easy] = task;
    if (!is_retry)
    {
        task->kv_task_->inflight_io_++;
    }
}

void AsyncHttpManager::SetupDownloadRequest(ObjectStore::DownloadTask *task,
                                            CURL *easy)
{
    const DaemonEndpoint &endpoint =
        endpoints_[next_endpoint_++ % endpoints_.size()];
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

    curl_easy_setopt(easy, CURLOPT_URL, endpoint.download_url.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}

void AsyncHttpManager::SetupMultipartUpload(ObjectStore::UploadTask *task,
                                            CURL *easy)
{
    const DaemonEndpoint &endpoint =
        endpoints_[next_endpoint_++ % endpoints_.size()];
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
    std::string url = endpoint.upload_url + fs_param;

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_MIMEPOST, mime);

    // store mine object for later clean
    task->mime_ = mime;
}

void AsyncHttpManager::SetupListRequest(ObjectStore::ListTask *task, CURL *easy)
{
    const DaemonEndpoint &endpoint =
        endpoints_[next_endpoint_++ % endpoints_.size()];
    Json::Value request;
    request["fs"] = options_->cloud_store_path;
    request["remote"] = task->remote_path_;
    request["opt"] = Json::Value(Json::objectValue);
    request["opt"]["recurse"] = task->Recursive();
    request["opt"]["showHash"] = false;

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(easy, CURLOPT_URL, endpoint.list_url.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);

    task->headers_ = headers;
}
void AsyncHttpManager::SetupDeleteRequest(ObjectStore::DeleteTask *task,
                                          CURL *easy)
{
    const DaemonEndpoint &endpoint =
        endpoints_[next_endpoint_++ % endpoints_.size()];
    Json::Value request;
    request["fs"] = options_->cloud_store_path;
    request["remote"] = task->remote_path_;

    Json::StreamWriterBuilder builder;
    task->json_data_ = Json::writeString(builder, request);

    curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    task->headers_ = headers;

    // Choose URL based on whether it's a directory or file
    const std::string &url =
        task->is_dir_ ? endpoint.purge_url : endpoint.delete_url;

    curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->json_data_.c_str());
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

            bool schedule_retry = false;
            uint32_t retry_delay_ms = 0;

            if (msg->data.result == CURLE_OK)
            {
                if (response_code >= 200 && response_code < 300)
                {
                    task->error_ = KvError::NoError;
                }
                else if (response_code == 404)
                {
                    task->error_ = KvError::NotFound;
                }
                else if (IsHttpRetryable(response_code) &&
                         task->retry_count_ < task->max_retries_)
                {
                    task->retry_count_++;
                    retry_delay_ms = ComputeBackoffMs(task->retry_count_);
                    schedule_retry = true;
                    LOG(WARNING) << "HTTP error " << response_code
                                 << " from rclone, scheduling retry "
                                 << unsigned(task->retry_count_) << "/"
                                 << unsigned(task->max_retries_) << " in "
                                 << retry_delay_ms << " ms";
                }
                else
                {
                    LOG(ERROR) << "HTTP error: " << response_code;
                    task->error_ = ClassifyHttpError(response_code);
                }
            }
            else
            {
                if (IsCurlRetryable(msg->data.result) &&
                    task->retry_count_ < task->max_retries_)
                {
                    task->retry_count_++;
                    retry_delay_ms = ComputeBackoffMs(task->retry_count_);
                    schedule_retry = true;
                    LOG(WARNING)
                        << "cURL transport error: "
                        << curl_easy_strerror(msg->data.result)
                        << ", scheduling retry " << unsigned(task->retry_count_)
                        << "/" << unsigned(task->max_retries_) << " in "
                        << retry_delay_ms << " ms";
                }
                else
                {
                    LOG(ERROR) << "cURL error: "
                               << curl_easy_strerror(msg->data.result);
                    task->error_ = ClassifyCurlError(msg->data.result);
                }
            }

            // clean curl resources first
            curl_multi_remove_handle(multi_handle_, easy);
            curl_easy_cleanup(easy);
            active_requests_.erase(easy);
            CleanupTaskResources(task);

            if (schedule_retry)
            {
                ScheduleRetry(task, std::chrono::milliseconds(retry_delay_ms));
                continue;
            }

            task->kv_task_->FinishIo();
        }
    }
}

void AsyncHttpManager::CleanupTaskResources(ObjectStore::Task *task)
{
    curl_slist_free_all(task->headers_);

    if (task->TaskType() == ObjectStore::Task::Type::AsyncUpload)
    {
        auto upload_task = static_cast<ObjectStore::UploadTask *>(task);
        curl_mime_free(upload_task->mime_);
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

        if (task->kv_task_->inflight_io_ > 0)
        {
            task->error_ = KvError::CloudErr;
            task->kv_task_->FinishIo();
        }
    }
}

void AsyncHttpManager::ProcessPendingRetries()
{
    if (pending_retries_.empty())
    {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    auto it = pending_retries_.begin();
    while (it != pending_retries_.end() && it->first <= now)
    {
        ObjectStore::Task *task = it->second;
        it = pending_retries_.erase(it);
        SubmitRequest(task);
    }
}

void AsyncHttpManager::ScheduleRetry(ObjectStore::Task *task,
                                     std::chrono::steady_clock::duration delay)
{
    task->waiting_retry_ = true;
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    for (auto it = pending_retries_.begin(); it != pending_retries_.end();)
    {
        if (it->second == task)
        {
            it = pending_retries_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    auto deadline = std::chrono::steady_clock::now() + delay;
    pending_retries_.emplace(deadline, task);
}

uint32_t AsyncHttpManager::ComputeBackoffMs(uint8_t attempt) const
{
    if (attempt == 0)
    {
        return kInitialRetryDelayMs;
    }

    uint64_t delay = static_cast<uint64_t>(kInitialRetryDelayMs)
                     << (attempt - 1);
    delay = std::min<uint64_t>(delay, kMaxRetryDelayMs);
    return static_cast<uint32_t>(delay);
}

bool AsyncHttpManager::IsCurlRetryable(CURLcode code) const
{
    switch (code)
    {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_GOT_NOTHING:
    case CURLE_HTTP2_STREAM:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_PARTIAL_FILE:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
        return true;
    default:
        return false;
    }
}

bool AsyncHttpManager::IsHttpRetryable(int64_t response_code) const
{
    switch (response_code)
    {
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
        return true;
    default:
        return false;
    }
}

KvError AsyncHttpManager::ClassifyHttpError(int64_t response_code) const
{
    switch (response_code)
    {
    case 400:
    case 401:
    case 403:
    case 409:
        return KvError::CloudErr;
    case 404:
        return KvError::NotFound;
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
    case 505:
        return KvError::Timeout;
    default:
        return KvError::CloudErr;
    }
}

KvError AsyncHttpManager::ClassifyCurlError(CURLcode code) const
{
    switch (code)
    {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_GOT_NOTHING:
        return KvError::Timeout;
    default:
        return KvError::CloudErr;
    }
}
}  // namespace eloqstore
