#include "storage/object_store.h"

#include <aws/core/Aws.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <ios>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "async_io_manager.h"
#include "cloud_storage_service.h"
#include "storage/shard.h"
#include "tasks/task.h"
#include "tasks/write_task.h"
#include "utils.h"

namespace eloqstore
{
namespace fs = std::filesystem;
namespace
{
std::mutex g_aws_mutex;
size_t g_aws_refcount = 0;
bool g_aws_initialized = false;
Aws::SDKOptions g_sdk_options;
bool g_aws_cleanup_registered = false;

size_t AppendToString(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t total = size * nmemb;
    if (!userp || total == 0)
    {
        return total;
    }
    auto *buffer = static_cast<std::string *>(userp);
    buffer->append(static_cast<char *>(contents), total);
    return total;
}

void CleanupAws()
{
    std::lock_guard lk(g_aws_mutex);
    if (g_aws_initialized)
    {
        Aws::ShutdownAPI(g_sdk_options);
        g_aws_initialized = false;
    }
}

CloudPathInfo ParseCloudPath(const std::string &spec)
{
    CloudPathInfo info;
    std::string_view path(spec);
    auto colon = path.find(':');
    if (colon != std::string::npos)
    {
        LOG(FATAL) << "cloud_store_path no longer supports prefixes like '"
                   << spec << "'";
    }
    while (!path.empty() && path.front() == '/')
    {
        path.remove_prefix(1);
    }
    size_t slash = path.find('/');
    if (slash == std::string::npos)
    {
        info.bucket.assign(path.data(), path.size());
    }
    else
    {
        info.bucket.assign(path.substr(0, slash).data(), slash);
        std::string_view prefix = path.substr(slash + 1);
        while (!prefix.empty() && prefix.front() == '/')
        {
            prefix.remove_prefix(1);
        }
        while (!prefix.empty() && prefix.back() == '/')
        {
            prefix.remove_suffix(1);
        }
        info.prefix.assign(prefix.data(), prefix.size());
    }
    if (info.bucket.empty())
    {
        LOG(FATAL) << "Invalid cloud_store_path: missing bucket name";
    }
    return info;
}

}  // namespace

ObjectStore::ObjectStore(const KvOptions *options, CloudStorageService *service)
    : cloud_service_(service)
{
    CHECK(cloud_service_ != nullptr);
    curl_global_init(CURL_GLOBAL_DEFAULT);
    {
        std::lock_guard lk(g_aws_mutex);
        if (g_aws_refcount == 0 && !g_aws_initialized)
        {
            Aws::InitAPI(g_sdk_options);
            g_aws_initialized = true;
        }
        g_aws_refcount++;
        if (!g_aws_cleanup_registered)
        {
            std::atexit(CleanupAws);
            g_aws_cleanup_registered = true;
        }
    }
    async_http_mgr_ = std::make_unique<AsyncHttpManager>(options, service);
}

void ObjectStore::Task::CompleteCloudTask()
{
    CHECK(kv_task_ != nullptr);
    kv_task_->FinishIo();
}

void ObjectStore::UploadTask::CompleteCloudTask()
{
    if (owner_write_task_ != nullptr)
    {
        owner_write_task_->CompletePendingUploadTask(this);
        return;
    }
    Task::CompleteCloudTask();
}

ObjectStore::~ObjectStore()
{
    async_http_mgr_.reset();
    {
        std::lock_guard lk(g_aws_mutex);
        if (g_aws_refcount > 0)
        {
            g_aws_refcount--;
        }
    }
    curl_global_cleanup();
}

bool ObjectStore::ParseListObjectsResponse(
    std::string_view payload,
    const std::string &strip_prefix,
    std::vector<std::string> *objects,
    std::vector<utils::CloudObjectInfo> *infos,
    std::string *next_continuation_token) const
{
    return async_http_mgr_->ParseListObjectsResponse(
        payload, strip_prefix, objects, infos, next_continuation_token);
}

KvError ObjectStore::EnsureBucketExists()
{
#ifdef ELOQ_SKIP_CREATE_BUCKET
    return KvError::NoError;
#endif
    if (!async_http_mgr_)
    {
        return KvError::CloudErr;
    }
    return async_http_mgr_->EnsureBucketExists();
}

KvError ObjectStore::BootstrapUpsertTermFile(std::string_view remote_path,
                                             uint64_t process_term)
{
    if (!async_http_mgr_)
    {
        return KvError::CloudErr;
    }
    return async_http_mgr_->BootstrapUpsertTermFile(remote_path, process_term);
}

void ObjectStore::SubmitTask(ObjectStore::Task *task, Shard *owner_shard)
{
    CHECK(task != nullptr);
    CHECK(owner_shard != nullptr);
    task->SetOwnerShard(owner_shard);
    CHECK(task->kv_task_ != nullptr);
    if (task->TaskType() == Task::Type::AsyncUpload)
    {
        auto *upload_task = static_cast<ObjectStore::UploadTask *>(task);
        if (upload_task->owner_write_task_ != nullptr)
        {
            upload_task->owner_write_task_->AddInflightUploadTask();
        }
        else
        {
            task->kv_task_->inflight_io_++;
        }
    }
    else
    {
        task->kv_task_->inflight_io_++;
    }
    cloud_service_->Submit(this, task);
}

void ObjectStore::StartHttpRequest(ObjectStore::Task *task)
{
    CHECK(async_http_mgr_ != nullptr);
    async_http_mgr_->SubmitRequest(task);
}

void ObjectStore::RunHttpWork()
{
    async_http_mgr_->PerformRequests();
    async_http_mgr_->ProcessCompletedRequests();
}

bool ObjectStore::HttpWorkIdle() const
{
    return async_http_mgr_->IsIdle();
}

bool ObjectStore::HasPendingWork() const
{
    bool http_pending = async_http_mgr_ && async_http_mgr_->HasPendingWork();
    bool service_pending = cloud_service_ && cloud_service_->HasPendingJobs();
    return http_pending || service_pending;
}

void ObjectStore::Shutdown()
{
    if (async_http_mgr_)
    {
        async_http_mgr_->Shutdown();
    }
}

AsyncHttpManager::AsyncHttpManager(const KvOptions *option,
                                   CloudStorageService *service)
    : cloud_service_(service)
{
    if (option->cloud_store_path.empty())
    {
        LOG(FATAL) << "cloud_store_path must be set when using cloud store";
    }
    cloud_path_ = ParseCloudPath(option->cloud_store_path);

    backend_ = CreateBackend(option, cloud_path_);
    CHECK(backend_) << "Failed to initialize cloud backend";

    multi_handle_ = curl_multi_init();
    if (!multi_handle_)
    {
        LOG(FATAL) << "Failed to initialize cURL multi handle";
    }
    curl_multi_setopt(multi_handle_, CURLMOPT_MAXCONNECTS, 200L);
}

AsyncHttpManager::~AsyncHttpManager()
{
    CHECK(active_requests_.empty())
        << "AsyncHttpManager destroyed with active requests";
    CHECK(pending_retries_.empty())
        << "AsyncHttpManager destroyed with pending retries";
    CHECK_EQ(active_request_count_.load(std::memory_order_acquire), 0);
    CHECK_EQ(pending_retry_count_.load(std::memory_order_acquire), 0);
    if (multi_handle_)
    {
        curl_multi_cleanup(multi_handle_);
    }
}

bool AsyncHttpManager::ParseListObjectsResponse(
    std::string_view payload,
    const std::string &strip_prefix,
    std::vector<std::string> *objects,
    std::vector<utils::CloudObjectInfo> *infos,
    std::string *next_continuation_token) const
{
    if (!backend_)
    {
        return false;
    }
    return backend_->ParseListObjectsResponse(
        payload, strip_prefix, objects, infos, next_continuation_token);
}

KvError AsyncHttpManager::EnsureBucketExists()
{
    if (!backend_)
    {
        LOG(ERROR) << "Cloud backend is not initialized";
        return KvError::CloudErr;
    }

    SignedRequestInfo request_info;
    if (!backend_->BuildCreateBucketRequest(&request_info) ||
        request_info.url.empty())
    {
        LOG(ERROR) << "Failed to build bucket creation request";
        return KvError::CloudErr;
    }

    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR)
            << "Failed to initialize cURL easy handle for bucket creation";
        return KvError::CloudErr;
    }

    std::string response_body;
    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, AppendToString);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, 60L);
    const char *method =
        request_info.method.empty() ? "PUT" : request_info.method.c_str();
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    const char *body_ptr =
        request_info.body.empty() ? "" : request_info.body.c_str();
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body_ptr);
    curl_easy_setopt(easy,
                     CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<int64_t>(request_info.body.size()));

    curl_slist *headers = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        headers = curl_slist_append(headers, header_line.c_str());
    }
    if (headers)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
    }

    CURLcode res = curl_easy_perform(easy);
    int64_t response_code = 0;
    if (res == CURLE_OK)
    {
        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
    }

    KvError result = KvError::NoError;
    if (res != CURLE_OK)
    {
        LOG(ERROR) << "Bucket creation request failed: "
                   << curl_easy_strerror(res);
        result = ClassifyCurlError(res);
    }
    else if ((response_code >= 200 && response_code < 300) ||
             response_code == 409)
    {
        if (response_code == 409)
        {
            LOG(INFO) << "Cloud bucket '" << cloud_path_.bucket
                      << "' already exists";
        }
        else
        {
            LOG(INFO) << "Created cloud bucket '" << cloud_path_.bucket
                      << "' (HTTP " << response_code << ")";
        }
        result = KvError::NoError;
    }
    else
    {
        LOG(ERROR) << "Bucket creation failed with HTTP " << response_code
                   << ": " << response_body;
        result = ClassifyHttpError(response_code);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(easy);
    return result;
}

KvError AsyncHttpManager::BootstrapUpsertTermFile(std::string_view remote_path,
                                                  uint64_t process_term)
{
    auto perform_download =
        [this,
         remote_path]() mutable -> std::tuple<std::string, std::string, KvError>
    {
        ObjectStore::DownloadTask task{std::string(remote_path)};
        CURL *easy = curl_easy_init();
        if (!easy)
        {
            return {"", "", KvError::CloudErr};
        }

        curl_easy_setopt(easy, CURLOPT_PROXY, "");
        curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
        curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(easy, CURLOPT_WRITEDATA, &task);
        curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(easy, CURLOPT_HEADERDATA, &task);
        curl_easy_setopt(easy, CURLOPT_TIMEOUT, 300L);

        if (!SetupDownloadRequest(&task, easy))
        {
            CleanupTaskResources(&task);
            curl_easy_cleanup(easy);
            return {"", "", task.error_};
        }

        CURLcode res = curl_easy_perform(easy);
        int64_t response_code = 0;
        if (res == CURLE_OK)
        {
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
        }
        curl_easy_cleanup(easy);
        CleanupTaskResources(&task);

        if (res != CURLE_OK)
        {
            return {"", "", ClassifyCurlError(res)};
        }
        if (response_code >= 200 && response_code < 300)
        {
            return {std::string(task.response_data_.view()),
                    std::move(task.etag_),
                    KvError::NoError};
        }
        if (response_code == 404)
        {
            return {"", "", KvError::NotFound};
        }
        return {"", "", ClassifyHttpError(response_code)};
    };

    auto perform_upload =
        [this, remote_path](
            std::string_view data,
            std::string_view if_match,
            std::string_view if_none_match) -> std::pair<KvError, int64_t>
    {
        ObjectStore::UploadTask task{std::string(remote_path), true};
        task.data_buffer_.append(data);
        task.file_size_ = data.size();
        task.if_match_ = std::string(if_match);
        task.if_none_match_ = std::string(if_none_match);

        CURL *easy = curl_easy_init();
        if (!easy)
        {
            return {KvError::CloudErr, 0};
        }

        curl_easy_setopt(easy, CURLOPT_PROXY, "");
        curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
        curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(easy, CURLOPT_WRITEDATA, &task);
        curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(easy, CURLOPT_HEADERDATA, &task);
        curl_easy_setopt(easy, CURLOPT_TIMEOUT, 300L);

        if (!SetupUploadRequest(&task, easy))
        {
            CleanupTaskResources(&task);
            curl_easy_cleanup(easy);
            return {task.error_, 0};
        }

        CURLcode res = curl_easy_perform(easy);
        int64_t response_code = 0;
        if (res == CURLE_OK)
        {
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
        }
        curl_easy_cleanup(easy);
        CleanupTaskResources(&task);

        if (res != CURLE_OK)
        {
            return {ClassifyCurlError(res), response_code};
        }
        if (response_code >= 200 && response_code < 300)
        {
            return {KvError::NoError, response_code};
        }
        if (response_code == 404)
        {
            return {KvError::NotFound, response_code};
        }
        return {ClassifyHttpError(response_code), response_code};
    };

    constexpr uint64_t kMaxAttempts = 10;
    for (uint64_t attempt = 0; attempt < kMaxAttempts; ++attempt)
    {
        auto [body, etag, read_err] = perform_download();
        if (read_err == KvError::NotFound)
        {
            auto [create_err, response_code] =
                perform_upload(std::to_string(process_term), "", "*");
            if (create_err == KvError::NoError)
            {
                return KvError::NoError;
            }
            if (create_err == KvError::CloudErr &&
                (response_code == 409 || response_code == 412))
            {
                continue;
            }
            return create_err;
        }
        if (read_err != KvError::NoError)
        {
            return read_err;
        }

        std::string_view term_str = body;
        while (!term_str.empty() &&
               (term_str.front() == ' ' || term_str.front() == '\t' ||
                term_str.front() == '\r' || term_str.front() == '\n'))
        {
            term_str.remove_prefix(1);
        }
        while (!term_str.empty() &&
               (term_str.back() == ' ' || term_str.back() == '\t' ||
                term_str.back() == '\r' || term_str.back() == '\n'))
        {
            term_str.remove_suffix(1);
        }

        uint64_t current_term = 0;
        try
        {
            current_term = std::stoull(std::string(term_str));
        }
        catch (const std::exception &)
        {
            return KvError::Corrupted;
        }

        if (current_term > process_term)
        {
            return KvError::ExpiredTerm;
        }
        if (current_term == process_term)
        {
            return KvError::NoError;
        }

        auto [update_err, response_code] =
            perform_upload(std::to_string(process_term), etag, "");
        if (update_err == KvError::NoError)
        {
            return KvError::NoError;
        }
        if (update_err == KvError::NotFound ||
            (update_err == KvError::CloudErr &&
             (response_code == 404 || response_code == 409 ||
              response_code == 412)))
        {
            continue;
        }
        return update_err;
    }

    return KvError::CloudErr;
}

void AsyncHttpManager::PerformRequests()
{
    ProcessPendingRetries();
    if (!multi_handle_ || active_requests_.empty())
    {
        return;
    }
    curl_multi_perform(multi_handle_, &running_handles_);
}

void AsyncHttpManager::SubmitRequest(ObjectStore::Task *task)
{
    task->error_ = KvError::NoError;
    task->response_data_.clear();

    task->waiting_retry_ = false;

    CURL *easy = curl_easy_init();
    if (!easy)
    {
        LOG(ERROR) << "Failed to initialize cURL easy handle";
        task->error_ = KvError::CloudErr;
        OnTaskFinished(task);
        return;
    }

    curl_easy_setopt(easy, CURLOPT_PROXY, "");
    curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, task);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, HeaderCallback);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, task);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, task);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, 300L);

    bool setup_ok = false;
    switch (task->TaskType())
    {
    case ObjectStore::Task::Type::AsyncDownload:
        setup_ok = SetupDownloadRequest(
            static_cast<ObjectStore::DownloadTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncUpload:
        setup_ok = SetupUploadRequest(
            static_cast<ObjectStore::UploadTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncDelete:
        setup_ok = SetupDeleteRequest(
            static_cast<ObjectStore::DeleteTask *>(task), easy);
        break;
    case ObjectStore::Task::Type::AsyncList:
        setup_ok =
            SetupListRequest(static_cast<ObjectStore::ListTask *>(task), easy);
        break;
    default:
        LOG(ERROR) << "Unknown async task type";
        task->error_ = KvError::CloudErr;
        break;
    }

    if (!setup_ok)
    {
        CleanupTaskResources(task);
        curl_easy_cleanup(easy);
        OnTaskFinished(task);
        return;
    }

    CURLMcode mres = curl_multi_add_handle(multi_handle_, easy);
    if (mres != CURLM_OK)
    {
        LOG(ERROR) << "Failed to add handle to multi: "
                   << curl_multi_strerror(mres);
        curl_easy_cleanup(easy);
        task->error_ = KvError::CloudErr;
        OnTaskFinished(task);
        return;
    }

    active_requests_[easy] = task;
    active_request_count_.fetch_add(1, std::memory_order_release);
}

std::string AsyncHttpManager::ComposeKey(const TableIdent *tbl_id,
                                         std::string_view filename) const
{
    CHECK(tbl_id != nullptr);
    std::string key;
    auto append_component = [&](std::string_view part)
    {
        if (part.empty())
        {
            return;
        }
        if (!key.empty())
        {
            key.push_back('/');
        }
        key.append(part.data(), part.size());
    };

    append_component(cloud_path_.prefix);
    std::string tbl = tbl_id->ToString();
    append_component(tbl);
    append_component(filename);
    return key;
}

std::string AsyncHttpManager::ComposeKeyFromRemote(
    std::string_view remote_path, bool ensure_trailing_slash) const
{
    std::string key;
    auto append_component = [&](std::string_view part)
    {
        if (part.empty())
        {
            return;
        }
        if (!key.empty() && key.back() != '/')
        {
            key.push_back('/');
        }
        key.append(part.data(), part.size());
    };

    append_component(cloud_path_.prefix);

    std::string_view trimmed = remote_path;
    while (!trimmed.empty() && trimmed.front() == '/')
    {
        trimmed.remove_prefix(1);
    }
    while (!trimmed.empty() && trimmed.back() == '/')
    {
        trimmed.remove_suffix(1);
    }
    append_component(trimmed);

    if (ensure_trailing_slash && !key.empty() && key.back() != '/')
    {
        key.push_back('/');
    }
    return key;
}

bool AsyncHttpManager::SetupDownloadRequest(ObjectStore::DownloadTask *task,
                                            CURL *easy)
{
    std::string key = task->tbl_id_ != nullptr
                          ? ComposeKey(task->tbl_id_, task->filename_)
                          : ComposeKeyFromRemote(task->remote_path_, false);
    SignedRequestInfo request_info;
    if (!backend_->BuildObjectRequest(
            CloudHttpMethod::kGet, key, &request_info) ||
        request_info.url.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    task->json_data_ = request_info.url;
    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    task->headers_ = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        task->headers_ = curl_slist_append(task->headers_, header_line.c_str());
    }
    if (task->headers_)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers_);
    }
    return true;
}

bool AsyncHttpManager::SetupUploadRequest(ObjectStore::UploadTask *task,
                                          CURL *easy)
{
    const std::string &filename =
        task->tbl_id_ != nullptr ? task->filename_ : task->remote_path_;
    // Inline full-buffer upload only.
    // When file_size_ is not prefilled by caller, infer it from buffer size.
    if (task->file_size_ == 0)
    {
        task->file_size_ = task->data_buffer_.size();
    }
    if (task->data_buffer_.empty() && task->file_size_ > 0)
    {
        LOG(ERROR) << "UploadTask missing inline upload buffer for file "
                   << filename << " file_size=" << task->file_size_;
        task->error_ = KvError::InvalidArgs;
        return false;
    }
    if (task->data_buffer_.size() != task->file_size_)
    {
        LOG(ERROR) << "UploadTask buffer size mismatch for file " << filename
                   << " buffer_size=" << task->data_buffer_.size()
                   << " file_size=" << task->file_size_;
        task->error_ = KvError::InvalidArgs;
        return false;
    }

    std::string key = task->tbl_id_ != nullptr
                          ? ComposeKey(task->tbl_id_, filename)
                          : ComposeKeyFromRemote(task->remote_path_, false);
    SignedRequestInfo request_info;
    if (!backend_->BuildObjectRequest(
            CloudHttpMethod::kPut, key, &request_info) ||
        request_info.url.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    task->json_data_ = request_info.url;
    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(easy, CURLOPT_UPLOAD, 0L);
    curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->data_buffer_.data());
    curl_easy_setopt(easy,
                     CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<curl_off_t>(task->file_size_));
    task->headers_ = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        task->headers_ = curl_slist_append(task->headers_, header_line.c_str());
    }
    task->headers_ = curl_slist_append(task->headers_, "Expect:");

    // Add conditional headers if provided
    if (!task->if_match_.empty())
    {
        std::string header = "If-Match: " + task->if_match_;
        task->headers_ = curl_slist_append(task->headers_, header.c_str());
    }
    else if (!task->if_none_match_.empty())
    {
        std::string header = "If-None-Match: " + task->if_none_match_;
        task->headers_ = curl_slist_append(task->headers_, header.c_str());
    }

    if (task->headers_)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers_);
    }
    return true;
}

bool AsyncHttpManager::SetupDeleteRequest(ObjectStore::DeleteTask *task,
                                          CURL *easy)
{
    std::string key = ComposeKeyFromRemote(task->remote_path_, false);
    if (key.empty())
    {
        task->error_ = KvError::InvalidArgs;
        return false;
    }
    SignedRequestInfo request_info;
    if (!backend_->BuildObjectRequest(
            CloudHttpMethod::kDelete, key, &request_info) ||
        request_info.url.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    task->json_data_ = request_info.url;
    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "DELETE");
    task->headers_ = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        task->headers_ = curl_slist_append(task->headers_, header_line.c_str());
    }
    if (task->headers_)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers_);
    }
    return true;
}

bool AsyncHttpManager::SetupListRequest(ObjectStore::ListTask *task, CURL *easy)
{
    std::string strip_prefix =
        ComposeKeyFromRemote(task->remote_path_, task->ensure_trailing_slash_);

    task->json_data_ = strip_prefix;

    SignedRequestInfo request_info;
    if (!backend_->BuildListRequest(strip_prefix,
                                    task->Recursive(),
                                    task->continuation_token_,
                                    &request_info))
    {
        task->error_ = KvError::CloudErr;
        return false;
    }
    if (request_info.url.empty())
    {
        task->error_ = KvError::CloudErr;
        return false;
    }

    curl_easy_setopt(easy, CURLOPT_URL, request_info.url.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    task->headers_ = nullptr;
    for (const auto &header_line : request_info.headers)
    {
        task->headers_ = curl_slist_append(task->headers_, header_line.c_str());
    }
    if (task->headers_)
    {
        curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers_);
    }
    return true;
}

size_t AsyncHttpManager::WriteCallback(void *contents,
                                       size_t size,
                                       size_t nmemb,
                                       void *userp)
{
    size_t total = size * nmemb;
    if (userp == nullptr || total == 0)
    {
        return total;
    }
    auto *task = static_cast<ObjectStore::Task *>(userp);
    task->response_data_.append(static_cast<char *>(contents), total);
    return total;
}

size_t AsyncHttpManager::HeaderCallback(char *buffer,
                                        size_t size,
                                        size_t nitems,
                                        void *userdata)
{
    ObjectStore::Task *task = static_cast<ObjectStore::Task *>(userdata);
    if (!task)
    {
        return size * nitems;
    }

    // Extract ETag header: "ETag: "value"\r\n"
    std::string_view header_line(buffer, size * nitems);
    constexpr std::string_view etag_prefix = "ETag:";
    if (header_line.size() >= etag_prefix.size() &&
        header_line.substr(0, etag_prefix.size()) == etag_prefix)
    {
        // Find the value after "ETag: "
        size_t value_start = etag_prefix.size();
        while (value_start < header_line.size() &&
               (header_line[value_start] == ' ' ||
                header_line[value_start] == '\t'))
        {
            ++value_start;
        }

        // Extract value (may be quoted)
        size_t value_end = value_start;
        while (value_end < header_line.size() &&
               header_line[value_end] != '\r' && header_line[value_end] != '\n')
        {
            ++value_end;
        }

        if (value_end > value_start)
        {
            std::string_view etag_value =
                header_line.substr(value_start, value_end - value_start);
            // Remove quotes if present
            if (etag_value.size() >= 2 && etag_value.front() == '"' &&
                etag_value.back() == '"')
            {
                etag_value = etag_value.substr(1, etag_value.size() - 2);
            }
            task->etag_ = std::string(etag_value);
        }
    }

    return size * nitems;
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

            int64_t response_code = 0;
            curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code);
            task->response_code_ = response_code;  // Store response code for
                                                   // CAS conflict detection

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
                    LOG(WARNING)
                        << "HTTP error " << response_code
                        << ", scheduling retry " << unsigned(task->retry_count_)
                        << "/" << unsigned(task->max_retries_) << " in "
                        << retry_delay_ms << " ms" << ", task=" << task->Info();
                }
                else
                {
                    constexpr size_t kMaxHttpErrorBodyLogBytes = 512;
                    std::string_view response_body =
                        task->response_data_.view();
                    if (response_body.size() > kMaxHttpErrorBodyLogBytes)
                    {
                        response_body =
                            response_body.substr(0, kMaxHttpErrorBodyLogBytes);
                    }
                    LOG(ERROR) << "HTTP error: " << response_code
                               << ", task=" << task->Info()
                               << ", request=" << task->json_data_
                               << ", response_body=" << response_body;
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
                        << retry_delay_ms << " ms" << ", task=" << task->Info();
                }
                else
                {
                    LOG(ERROR) << "cURL error: "
                               << curl_easy_strerror(msg->data.result);
                    task->error_ = ClassifyCurlError(msg->data.result);
                }
            }

            curl_multi_remove_handle(multi_handle_, easy);
            curl_easy_cleanup(easy);
            active_requests_.erase(easy);
            active_request_count_.fetch_sub(1, std::memory_order_acq_rel);
            CleanupTaskResources(task);

            if (schedule_retry)
            {
                ScheduleRetry(task, std::chrono::milliseconds(retry_delay_ms));
                continue;
            }

            if (task->retry_count_ > 0)
            {
                LOG(INFO) << "Retry succeeded after "
                          << static_cast<unsigned>(task->retry_count_)
                          << " attempts: " << task->Info();
                task->retry_count_ = 0;
            }

            OnTaskFinished(task);
        }
    }
}

void AsyncHttpManager::Shutdown()
{
    if (!pending_retries_.empty())
    {
        std::vector<ObjectStore::Task *> retries;
        retries.reserve(pending_retries_.size());
        for (auto &entry : pending_retries_)
        {
            retries.push_back(entry.second);
        }
        pending_retries_.clear();

        for (auto *task : retries)
        {
            if (task == nullptr)
            {
                continue;
            }
            CleanupTaskResources(task);
            task->error_ = KvError::CloudErr;
            OnTaskFinished(task);
        }
    }
    pending_retry_count_.store(0, std::memory_order_release);

    CHECK(active_requests_.empty())
        << "Shutdown called with active requests; cloud tasks were not drained";
    CHECK_EQ(active_request_count_.load(std::memory_order_acquire), 0);
    CHECK(pending_retries_.empty());
    CHECK_EQ(pending_retry_count_.load(std::memory_order_acquire), 0);
}

void AsyncHttpManager::CleanupTaskResources(ObjectStore::Task *task)
{
    if (task == nullptr)
    {
        return;
    }
    curl_slist_free_all(task->headers_);
    task->headers_ = nullptr;
}

void AsyncHttpManager::OnTaskFinished(ObjectStore::Task *task)
{
    cloud_service_->NotifyTaskFinished(task);
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
        pending_retry_count_.fetch_sub(1, std::memory_order_acq_rel);
        LOG(INFO) << "Retrying task after backoff (attempt "
                  << unsigned(task->retry_count_) << "/"
                  << unsigned(task->max_retries_) << "): " << task->Info();
        // SubmitRequest will handle rescheduling if slot acquisition fails
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
            pending_retry_count_.fetch_sub(1, std::memory_order_acq_rel);
        }
        else
        {
            ++it;
        }
    }

    auto deadline = std::chrono::steady_clock::now() + delay;
    pending_retries_.emplace(deadline, task);
    pending_retry_count_.fetch_add(1, std::memory_order_release);
}

uint32_t AsyncHttpManager::ComputeBackoffMs(uint8_t attempt)
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

bool AsyncHttpManager::IsCurlRetryable(CURLcode code)
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

bool AsyncHttpManager::IsHttpRetryable(int64_t response_code)
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

KvError AsyncHttpManager::ClassifyHttpError(int64_t response_code)
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
    case 507:
        return KvError::OssInsufficientStorage;
    default:
        return KvError::CloudErr;
    }
}

KvError AsyncHttpManager::ClassifyCurlError(CURLcode code)
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
