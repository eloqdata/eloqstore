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
};

ObjectStore::~ObjectStore()
{
    Stop();
    curl_global_cleanup();
}

void ObjectStore::Start()
{
    if (!async_http_mgr_)
    {
        async_http_mgr_ =
            std::make_unique<AsyncHttpManager>("http://127.0.0.1:5572");
    }

    const uint16_t n_workers = options_->rclone_threads;
    workers_.reserve(n_workers);
    // now only support 1 worker,
    // because the async_http_mgr_ is not thread safe,and one worker is enough
    // to send request to rclone
    CHECK(options_->rclone_threads == 1);
    for (int i = 0; i < n_workers; i++)
    {
        workers_.emplace_back([this] { WorkLoop(); });
    }
    LOG(INFO) << "object store syncer started";
}

void ObjectStore::Stop()
{
    LOG(INFO) << "check whether should be stop";
    if (workers_.empty())
    {
        return;
    }
    // Send stop signal to all workers.
    StopSignal stop;
    for (auto &w : workers_)
    {
        submit_q_.enqueue(&stop);
    }

    for (auto &w : workers_)
    {
        w.join();
    }
    workers_.clear();

    if (async_http_mgr_)
    {
        async_http_mgr_->Cleanup();
        async_http_mgr_.reset();
        LOG(INFO) << "AsyncHttpManager cleaned up";
    }
    LOG(INFO) << "object store syncer stopped";
}

bool ObjectStore::IsRunning() const
{
    return !workers_.empty();
}

void ObjectStore::WorkLoop()
{
    std::array<Task *, 128> tasks;
    // the max concurrent requests to rclone,it need not be too many,
    // because the rclone can not proccess much request
    const int max_concurrent_requests = 20;
    auto dequeue_tasks = [this, &tasks]() -> int
    {
        size_t ntasks = submit_q_.try_dequeue_bulk(tasks.data(), tasks.size());
        // no task, check whether the http mgr is idle
        if (ntasks == 0 && async_http_mgr_->IsIdle())
        {
            while (ntasks == 0)
            {
                const auto timeout = std::chrono::milliseconds(100);
                ntasks = submit_q_.wait_dequeue_bulk_timed(
                    tasks.data(), tasks.size(), timeout);
            }
        }
        return ntasks;
    };
    while (true)
    {
        int ntasks = dequeue_tasks();

        CHECK(ntasks >= 0);

        for (size_t i = 0; i < ntasks; i++)
        {
            Task *task = tasks[i];
            if (task->TaskType() == Task::Type::Stop)
            {
                async_http_mgr_->Cleanup();
                return;
            }
            // the rclone can not proccess much request ,it will remove jobs
            while (async_http_mgr_->NumActiveRequests() >=
                   max_concurrent_requests)
            {
                // execute the completed requests to release resources
                async_http_mgr_->ProcessCompletedRequests();
                // wait for network events to avoid the cpu busy
                async_http_mgr_->WaitForNetworkEvents(10);
            }
            // submit the aysn request to http mgr
            async_http_mgr_->SubmitRequest(task);
        }

        async_http_mgr_->ProcessCompletedRequests();
        // wait for network events to avoid the cpu busy
        async_http_mgr_->WaitForNetworkEvents(10);
    }
}
ObjectStore::AsyncHttpManager::AsyncHttpManager(const std::string &daemon_url)
    : daemon_url_(daemon_url)
{
    multi_handle_ = curl_multi_init();
    if (!multi_handle_)
    {
        LOG(FATAL) << "Failed to initialize cURL multi handle";
    }

    // set the max connections
    curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, 10L);
}

ObjectStore::AsyncHttpManager::~AsyncHttpManager()
{
    Cleanup();
    if (multi_handle_)
    {
        curl_multi_cleanup(multi_handle_);
    }
}
void ObjectStore::AsyncHttpManager::SubmitRequest(Task *task)
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
    case Task::Type::AsyncDownload:
        SetupDownloadRequest(static_cast<DownloadTask *>(task), easy);
        break;
    case Task::Type::AsyncUpload:
        SetupMultipartUpload(static_cast<UploadTask *>(task), easy);
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
void ObjectStore::AsyncHttpManager::SetupDownloadRequest(DownloadTask *task,
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

void ObjectStore::AsyncHttpManager::SetupMultipartUpload(UploadTask *task,
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
void ObjectStore::AsyncHttpManager::ProcessCompletedRequests()
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
            Task *task;
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

            if (task->callback_)
            {
                task->callback_(task);
            }

            // clean resources
            curl_multi_remove_handle(multi_handle_, easy);

            CleanupTaskResources(task);

            curl_easy_cleanup(easy);
            active_requests_.erase(task->request_id_);
            delete task;
        }
    }
}
void ObjectStore::AsyncHttpManager::CleanupTaskResources(Task *task)
{
    if (!task)
    {
        return;
    }
    if (task->TaskType() == Task::Type::AsyncUpload)
    {
        auto upload_task = static_cast<UploadTask *>(task);
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
    else if (task->TaskType() == Task::Type::AsyncDownload)
    {
        auto download_task = static_cast<DownloadTask *>(task);
        if (download_task->headers_)
        {
            curl_slist_free_all(download_task->headers_);
            download_task->headers_ = nullptr;
        }
    }
}
void ObjectStore::AsyncHttpManager::WaitForNetworkEvents(int timeout_ms)
{
    if (active_requests_.empty())
    {
        return;
    }

    fd_set fdread, fdwrite, fdexcep;
    int maxfd = -1;

    FD_ZERO(&fdread);   // the set of read file descriptors
    FD_ZERO(&fdwrite);  // the set of write file descriptors
    FD_ZERO(&fdexcep);  // the set of exception file descriptors

    curl_multi_fdset(multi_handle_, &fdread, &fdwrite, &fdexcep, &maxfd);

    if (maxfd >= 0)  // have file descriptors need to listen
    {
        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        // wait for file descriptors to be ready
        select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
    }
    else
    {
        // simple wait if no file descriptors
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
    }
}

void ObjectStore::AsyncHttpManager::Cleanup()
{
    for (auto &[request_id, active_req] : active_requests_)
    {
        // remove easy handle from multi handle
        curl_multi_remove_handle(multi_handle_, active_req.easy_handle);

        // clean Task associated resources
        CleanupTaskResources(active_req.task);

        // clean cURL easy handle
        curl_easy_cleanup(active_req.easy_handle);

        delete active_req.task;
    }
    active_requests_.clear();
}

}  // namespace eloqstore