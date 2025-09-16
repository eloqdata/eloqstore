#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <curl/curl.h>
#include <jsoncpp/json/json.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

int testNum = 300;
// 异步上传结果结构
struct UploadResult
{
    std::string file_path;
    std::string key;
    bool success;
    std::string error_message;
    std::chrono::milliseconds duration;
};

// 抽象异步上传接口
class AsyncFileUploader
{
public:
    virtual ~AsyncFileUploader() = default;
    virtual void uploadFileAsync(
        const std::string &bucket_name,
        const std::string &key,
        const std::string &file_path,
        std::function<void(const UploadResult &)> callback) = 0;
    virtual std::string getEngineType() const = 0;
    virtual void waitForCompletion() = 0;
};

// AWS S3 异步上传实现
// AWS S3 异步上传实现
class AsyncAWSS3Uploader : public AsyncFileUploader
{
private:
    std::unique_ptr<Aws::S3::S3Client> s3_client;
    std::mutex callback_mutex;
    std::condition_variable completion_cv;
    std::atomic<int> pending_uploads{0};

    // 使用静态映射来管理回调
    static std::mutex static_callback_mutex;
    static std::unordered_map<std::string,
                              std::function<void(const UploadResult &)>>
        static_callbacks;
    static std::unordered_map<std::string, AsyncAWSS3Uploader *>
        static_uploaders;

    static void uploadFinishedCallback(
        const Aws::S3::S3Client *s3Client,
        const Aws::S3::Model::PutObjectRequest &request,
        const Aws::S3::Model::PutObjectOutcome &outcome,
        const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context)
    {
        std::string file_path = context->GetUUID();

        UploadResult result;
        result.file_path = file_path;
        result.key = request.GetKey();
        result.success = outcome.IsSuccess();

        if (!result.success)
        {
            auto err = outcome.GetError();
            result.error_message =
                err.GetExceptionName() + ": " + err.GetMessage();
        }

        // 查找对应的回调和上传器
        std::function<void(const UploadResult &)> callback;
        AsyncAWSS3Uploader *uploader = nullptr;

        {
            std::lock_guard<std::mutex> lock(static_callback_mutex);
            auto callback_it = static_callbacks.find(file_path);
            auto uploader_it = static_uploaders.find(file_path);

            if (callback_it != static_callbacks.end() &&
                uploader_it != static_uploaders.end())
            {
                callback = callback_it->second;
                uploader = uploader_it->second;
                static_callbacks.erase(callback_it);
                static_uploaders.erase(uploader_it);
            }
        }

        if (callback && uploader)
        {
            callback(result);
            uploader->decrementPendingUploads();
        }
    }

    void decrementPendingUploads()
    {
        // 这个地方不上锁就有可能发生对方
        {
            std::unique_lock<std::mutex> lock(callback_mutex);
            pending_uploads--;
        }
        completion_cv.notify_all();
    }

public:
    AsyncAWSS3Uploader(const std::string &endpoint,
                       const std::string &access_key,
                       const std::string &secret_key)
    {
        Aws::SDKOptions options;
        Aws::InitAPI(options);

        Aws::Client::ClientConfiguration config;
        config.endpointOverride = endpoint;
        config.scheme = Aws::Http::Scheme::HTTP;
        config.verifySSL = false;
        config.region = "us-east-1";
        config.maxConnections = 25;  // 增加并发连接数

        Aws::Auth::AWSCredentials credentials(access_key, secret_key);
        s3_client = std::make_unique<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false);
    }

    ~AsyncAWSS3Uploader()
    {
        waitForCompletion();
        Aws::SDKOptions options;
        Aws::ShutdownAPI(options);
    }

    void uploadFileAsync(
        const std::string &bucket_name,
        const std::string &key,
        const std::string &file_path,
        std::function<void(const UploadResult &)> callback) override
    {
        if (!std::filesystem::exists(file_path))
        {
            UploadResult result;
            result.file_path = file_path;
            result.key = key;
            result.success = false;
            result.error_message = "文件不存在: " + file_path;
            callback(result);
            return;
        }

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(key);

        auto input_data = Aws::MakeShared<Aws::FStream>(
            "AsyncUploadInputStream",
            file_path.c_str(),
            std::ios_base::in | std::ios_base::binary);

        if (!*input_data)
        {
            UploadResult result;
            result.file_path = file_path;
            result.key = key;
            result.success = false;
            result.error_message = "无法打开文件: " + file_path;
            callback(result);
            return;
        }

        request.SetBody(input_data);
        request.SetContentType("application/octet-stream");

        auto context = Aws::MakeShared<Aws::Client::AsyncCallerContext>(
            "AsyncUploadContext");
        context->SetUUID(file_path);

        // 注册回调和上传器
        {
            std::lock_guard<std::mutex> lock(static_callback_mutex);
            static_callbacks[file_path] = callback;
            static_uploaders[file_path] = this;
            pending_uploads++;
        }

        s3_client->PutObjectAsync(request, uploadFinishedCallback, context);
    }

    std::string getEngineType() const override
    {
        return "AWS";
    }

    void waitForCompletion() override
    {
        std::unique_lock<std::mutex> lock(callback_mutex);
        completion_cv.wait(lock,
                           [this] { return pending_uploads.load() == 0; });
    }
};

// 静态成员定义
std::mutex AsyncAWSS3Uploader::static_callback_mutex;
std::unordered_map<std::string, std::function<void(const UploadResult &)>>
    AsyncAWSS3Uploader::static_callbacks;
std::unordered_map<std::string, AsyncAWSS3Uploader *>
    AsyncAWSS3Uploader::static_uploaders;

// Rclone 异步上传实现（使用curl multi接口）
class AsyncRcloneUploader : public AsyncFileUploader
{
private:
    std::string daemon_url;
    CURLM *multi_handle;
    std::thread worker_thread;
    std::atomic<bool> should_stop{false};
    std::mutex requests_mutex;
    std::condition_variable completion_cv;
    std::atomic<int> pending_uploads{0};

    int max_active_requests = 20;
    struct UploadRequest
    {
        CURL *easy_handle;
        std::string response_data;
        std::string file_path;
        std::string key;
        std::function<void(const UploadResult &)> callback;
        struct curl_slist *headers;
        std::string post_data;
        std::chrono::steady_clock::time_point start_time;
    };

    std::unordered_map<CURL *, std::unique_ptr<UploadRequest>> active_requests;

    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                void *userp)
    {
        auto *request = static_cast<UploadRequest *>(userp);
        request->response_data.append(static_cast<char *>(contents),
                                      size * nmemb);
        return size * nmemb;
    }

    void workerLoop()
    {
        while (!should_stop.load())
        {
            int running_handles;
            CURLMcode mc = curl_multi_perform(multi_handle, &running_handles);

            if (mc != CURLM_OK)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            // 检查完成的请求
            CURLMsg *msg;
            int msgs_left;
            while ((msg = curl_multi_info_read(multi_handle, &msgs_left)))
            {
                if (msg->msg == CURLMSG_DONE)
                {
                    handleRequestComplete(msg->easy_handle, msg->data.result);
                }
            }

            // 等待活动
            if (running_handles > 0)
            {
                // wait 100ms
                curl_multi_wait(multi_handle, nullptr, 0, 100, nullptr);
            }
            // else
            // {  // no active task
            //     std::this_thread::sleep_for(std::chrono::milliseconds(10));
            // }
        }
    }

    void handleRequestComplete(CURL *easy_handle, CURLcode result)
    {
        std::lock_guard<std::mutex> lock(requests_mutex);

        auto it = active_requests.find(easy_handle);
        if (it == active_requests.end())
        {
            return;
        }

        auto request = std::move(it->second);
        active_requests.erase(it);

        curl_multi_remove_handle(multi_handle, easy_handle);

        UploadResult upload_result;
        upload_result.file_path = request->file_path;
        upload_result.key = request->key;
        upload_result.duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - request->start_time);

        if (result == CURLE_OK)
        {
            long response_code;
            curl_easy_getinfo(
                easy_handle, CURLINFO_RESPONSE_CODE, &response_code);
            upload_result.success = (response_code == 200);
            if (!upload_result.success)
            {
                upload_result.error_message =
                    "HTTP错误: " + std::to_string(response_code) +
                    ", 响应: " + request->response_data;
            }
        }
        else
        {
            upload_result.success = false;
            upload_result.error_message =
                "cURL错误: " + std::string(curl_easy_strerror(result));
        }

        // 清理资源
        curl_slist_free_all(request->headers);
        curl_easy_cleanup(easy_handle);

        pending_uploads--;
        std::cout << "pending_uploads: " << pending_uploads << std::endl;
        completion_cv.notify_all();

        // 调用回调函数
        request->callback(upload_result);
    }

public:
    AsyncRcloneUploader(const std::string &url = "http://127.0.0.1:5572")
        : daemon_url(url)
    {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        multi_handle = curl_multi_init();

        // 设置并发连接数
        curl_multi_setopt(multi_handle, CURLMOPT_MAXCONNECTS, 1000L);

        worker_thread = std::thread(&AsyncRcloneUploader::workerLoop, this);
    }

    ~AsyncRcloneUploader()
    {
        waitForCompletion();
        should_stop = true;
        if (worker_thread.joinable())
        {
            worker_thread.join();
        }

        curl_multi_cleanup(multi_handle);
        curl_global_cleanup();
    }

    void uploadFileAsync(
        const std::string &bucket_name,
        const std::string &key,
        const std::string &file_path,
        std::function<void(const UploadResult &)> callback) override
    {
        if (!std::filesystem::exists(file_path))
        {
            UploadResult result;
            result.file_path = file_path;
            result.key = key;
            result.success = false;
            result.error_message = "文件不存在: " + file_path;
            callback(result);
            return;
        }
        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(requests_mutex);
                if (active_requests.size() <
                    static_cast<size_t>(max_active_requests))
                {
                    break;  // 可以继续执行上传
                }
            }
            // 睡眠一小段时间后重试
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        // 构造请求JSON
        std::filesystem::path abs_path = std::filesystem::absolute(file_path);
        std::string parent_dir = abs_path.parent_path().string();
        std::string filename = abs_path.filename().string();

        Json::Value request_json;
        request_json["srcFs"] = parent_dir;
        request_json["srcRemote"] = filename;
        request_json["dstFs"] = "minio:" + bucket_name;
        request_json["dstRemote"] = key;

        Json::StreamWriterBuilder builder;
        std::string json_string = Json::writeString(builder, request_json);

        // 创建请求
        auto request = std::make_unique<UploadRequest>();
        request->easy_handle = curl_easy_init();
        request->file_path = file_path;
        request->key = key;
        request->callback = callback;
        request->post_data = json_string;
        request->start_time = std::chrono::steady_clock::now();

        // 设置headers
        request->headers =
            curl_slist_append(nullptr, "Content-Type: application/json");

        // 配置curl
        std::string url = daemon_url + "/operations/copyfile";
        curl_easy_setopt(request->easy_handle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(request->easy_handle,
                         CURLOPT_POSTFIELDS,
                         request->post_data.c_str());
        curl_easy_setopt(
            request->easy_handle, CURLOPT_HTTPHEADER, request->headers);
        curl_easy_setopt(
            request->easy_handle, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(
            request->easy_handle, CURLOPT_WRITEDATA, request.get());
        curl_easy_setopt(request->easy_handle, CURLOPT_TIMEOUT, 600L);
        curl_easy_setopt(request->easy_handle, CURLOPT_NOSIGNAL, 1L);

        {
            std::lock_guard<std::mutex> lock(requests_mutex);
            CURL *handle = request->easy_handle;
            active_requests[handle] = std::move(request);
            pending_uploads++;

            curl_multi_add_handle(multi_handle, handle);
        }
    }

    std::string getEngineType() const override
    {
        return "Rclone";
    }

    void waitForCompletion() override
    {
        std::cout << "开始等待完成，当前 pending_uploads: "
                  << pending_uploads.load() << std::endl;
        std::unique_lock<std::mutex> lock(requests_mutex);

        if (pending_uploads.load() == 0)
        {
            std::cout << "已经完成，直接退出" << std::endl;
            return;
        }
        completion_cv.wait(lock,
                           [this]
                           {
                               std::cout << "test wake" << std::endl;
                               return pending_uploads.load() == 0;
                           });
        std::cout << "exit" << std::endl;
    }
};

// 异步上传测试类
class AsyncUploadTester
{
private:
    std::unique_ptr<AsyncFileUploader> uploader;
    std::string bucket_name;
    std::mutex results_mutex;
    std::vector<UploadResult> results;
    std::condition_variable completion_cv;
    std::atomic<int> pending_files{0};

public:
    AsyncUploadTester(std::unique_ptr<AsyncFileUploader> uploader,
                      const std::string &bucket)
        : uploader(std::move(uploader)), bucket_name(bucket)
    {
    }

    ~AsyncUploadTester()
    {
        // uploader->waitForCompletion();
        std::cout << "析构函数AsycnUploadTest" << std::endl;
    }

    void uploadDirectory(const std::string &directory_path)
    {
        std::cout << "\n=== 开始 " << uploader->getEngineType()
                  << " 异步上传测试 ===" << std::endl;

        std::vector<std::string> files;
        try
        {
            for (const auto &entry :
                 std::filesystem::directory_iterator(directory_path))
            {
                if (entry.is_regular_file())
                {
                    files.push_back(entry.path().string());
                }
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "读取目录失败: " << e.what() << std::endl;
            return;
        }

        if (files.empty())
        {
            std::cout << "目录中没有文件" << std::endl;
            return;
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        // pending_files = files.size();
        pending_files = testNum;
        results.clear();
        results.reserve(files.size());

        // 启动所有异步上传
        // for (const auto &file_path : files)
        for (int i = 0; i < testNum; i++)
        {
            std::string file_path = files[i];

            std::filesystem::path path(file_path);
            std::string filename = path.filename().string();
            std::string key = uploader->getEngineType() + "_async_" + filename;
            uploader->uploadFileAsync(
                bucket_name,
                key,
                file_path,
                [this, filename](const UploadResult &result)
                { handleUploadComplete(result, filename); });
        }

        // 等待所有上传完成
        {
            std::unique_lock<std::mutex> lock(results_mutex);
            std::cout << "等待所有上传完成，当前 pending_files: "
                      << pending_files.load() << std::endl;
            completion_cv.wait(lock,
                               [this] { return pending_files.load() == 0; });
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        // 统计结果
        int success_count = 0;
        for (const auto &result : results)
        {
            if (result.success)
            {
                success_count++;
            }
        }

        std::cout << "\n"
                  << uploader->getEngineType() << " 异步上传完成:" << std::endl;
        std::cout << "- 成功: " << success_count << "/" << files.size()
                  << " 文件" << std::endl;
        std::cout << "- 总耗时: " << duration.count() << " ms" << std::endl;

        if (duration.count() > 0)
        {
            double throughput =
                (double) success_count / (duration.count() / 1000.0);
            std::cout << "- 吞吐量: " << std::fixed << std::setprecision(2)
                      << throughput << " 文件/秒" << std::endl;
        }

        // 显示失败的文件
        for (const auto &result : results)
        {
            if (!result.success)
            {
                std::cout << "- 失败: " << result.file_path << " ("
                          << result.error_message << ")" << std::endl;
            }
        }
    }

private:
    void handleUploadComplete(const UploadResult &result,
                              const std::string &filename)
    {
        {
            std::lock_guard<std::mutex> lock(results_mutex);
            results.push_back(result);
        }

        std::cout << "[" << (results.size()) << "] " << filename
                  << (result.success ? " 上传成功" : " 上传失败") << " ("
                  << result.duration.count() << "ms)" << std::endl;

        pending_files--;
        completion_cv.notify_all();
    }
};

void printUsage(const char *program_name)
{
    std::cout << "用法: " << program_name << " <引擎类型> <目录路径>"
              << std::endl;
    std::cout << "引擎类型:" << std::endl;
    std::cout << "  aws     - 使用AWS SDK异步接口" << std::endl;
    std::cout << "  rclone  - 使用Rclone HTTP API + curl multi" << std::endl;
    std::cout << "  both    - 测试两种引擎" << std::endl;
    std::cout << "\n示例:" << std::endl;
    std::cout << "  " << program_name
              << " aws /home/sjh/eloqstore/rclonetest/rclone/data" << std::endl;
    std::cout << "  " << program_name
              << " rclone /home/sjh/eloqstore/rclonetest/rclone/data"
              << std::endl;
    std::cout << "  " << program_name
              << " both /home/sjh/eloqstore/rclonetest/rclone/data"
              << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printUsage(argv[0]);
        return 1;
    }

    std::string engine_type = argv[1];
    std::string directory_path = argv[2];
    std::string bucket_name = "db-stress";

    if (argc == 4)
    {
        testNum = std::stoi(argv[3]);
    }
    std::cout << "=== 异步文件上传测试 ===" << std::endl;
    std::cout << "目标桶: " << bucket_name << std::endl;
    std::cout << "源目录: " << directory_path << std::endl;

    if (engine_type == "aws")
    {
        auto uploader = std::make_unique<AsyncAWSS3Uploader>(
            "http://127.0.0.1:9000", "ROOTNAME", "CHANGEME123");
        AsyncUploadTester tester(std::move(uploader), bucket_name);
        tester.uploadDirectory(directory_path);
        std::cout << "aws 测试完成" << std::endl;
    }
    else if (engine_type == "rclone")
    {
        auto uploader = std::make_unique<AsyncRcloneUploader>();
        AsyncUploadTester tester(std::move(uploader), bucket_name);
        tester.uploadDirectory(directory_path);
        std::cout << "rclone 测试完成" << std::endl;
    }
    else if (engine_type == "both")
    {
        // 测试AWS异步
        {
            auto uploader = std::make_unique<AsyncAWSS3Uploader>(
                "http://127.0.0.1:9000", "ROOTNAME", "CHANGEME123");
            AsyncUploadTester tester(std::move(uploader), bucket_name);
            tester.uploadDirectory(directory_path);
        }

        // 测试Rclone异步
        {
            auto uploader = std::make_unique<AsyncRcloneUploader>();
            AsyncUploadTester tester(std::move(uploader), bucket_name);
            tester.uploadDirectory(directory_path);
        }
    }
    else
    {
        std::cout << "错误: 未知引擎类型 '" << engine_type << "'" << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    return 0;
}