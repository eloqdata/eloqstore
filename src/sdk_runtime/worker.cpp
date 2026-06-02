#include "internal.h"

namespace eloqstore::sdk
{

using namespace runtime_internal;

// Worker-side runtime stub:
// - parses the exported shared-memory descriptor
// - owns only IPC transport state
// - never owns authoritative slot/index state

KVCacheWorker::KVCacheWorker(KVCacheOptions options)
    : options_(std::move(options)), impl_(new Impl())
{
    // The worker object keeps only attach metadata plus IPC configuration.
}

KVCacheWorker::~KVCacheWorker()
{
    // Destruction is equivalent to dropping the current attachment state.
    DetachBufferPool();
    delete impl_;
    impl_ = nullptr;
}

bool KVCacheWorker::AttachBufferPool(const std::string &descriptor,
                                     std::string *error_message)
{
    // Workers parse the exported descriptor once so later submit calls can obey
    // manager-defined slot sizing and group-count constraints.
    if (descriptor.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "buffer pool descriptor must not be empty";
        }
        return false;
    }
    try
    {
        // Workers only parse the exported descriptor here. The actual mmap and
        // CUDA registration happen in the higher-level connector so Python can
        // directly operate on the shared slot bytes.
        const auto parts = Split(descriptor, '|');
        if (parts.size() < 9)
        {
            if (error_message != nullptr)
            {
                *error_message = "buffer pool descriptor format is invalid";
            }
            return false;
        }
        options_.shared_memory_name = parts[0];
        options_.shared_memory_bytes = static_cast<size_t>(std::stoull(parts[2]));
        options_.slot_size = static_cast<uint32_t>(std::stoul(parts[3]));
        options_.slot_count = static_cast<uint32_t>(std::stoul(parts[4]));
        options_.slot_alignment = static_cast<uint32_t>(std::stoul(parts[5]));
        options_.num_threads = static_cast<uint16_t>(std::stoul(parts[6]));
        options_.submission_queue_depth = static_cast<uint32_t>(std::stoul(parts[7]));
        options_.partition_count = static_cast<uint32_t>(std::stoul(parts[8]));
    }
    catch (const std::exception &e)
    {
        if (error_message != nullptr)
        {
            *error_message = std::string("failed to parse buffer pool descriptor: ") + e.what();
        }
        return false;
    }
    buffer_pool_descriptor_ = descriptor;
    attached_ = true;
    return true;
}

void KVCacheWorker::DetachBufferPool()
{
    // Only worker-local descriptor state is cleared here; higher layers own the
    // actual mmap/cudaHostRegister lifecycle for attached shared pages.
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (impl_ != nullptr)
    {
        impl_->ipc_socket.reset();
        impl_->ipc_context.reset();
    }
    attached_ = false;
    buffer_pool_descriptor_.clear();
}

uint32_t KVCacheWorker::RecommendPartition(uint32_t worker_lane) const
{
    // Keep routing deterministic so one worker lane usually hits the same partition.
    if (options_.partition_count == 0)
    {
        return 0;
    }
    return worker_lane % options_.partition_count;
}

bool KVCacheWorker::SubmitSaveRequest(const std::string &key,
                                      uint32_t partition_id,
                                      uint32_t payload_bytes,
                                      KVCacheRequest *out_request,
                                      std::string *error_message)
{
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(options_,
                                     impl_ != nullptr ? &impl_->ipc_context : nullptr,
                                     impl_ != nullptr ? &impl_->ipc_socket : nullptr,
                                     error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"submit_save",
                            key,
                            std::to_string(partition_id),
                            std::to_string(payload_bytes)},
                           &response_frames,
                           error_message))
    {
        if (impl_ != nullptr)
        {
            impl_->ipc_socket.reset();
            impl_->ipc_context.reset();
        }
        return false;
    }
    if (response_frames.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "missing submit-save ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1 ? response_frames[1]
                                                        : "submit-save ipc request failed";
        }
        return false;
    }
    if (response_frames[0] != "ok" ||
        !DecodeRequestRecord(response_frames, 1, key, out_request))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid submit-save ipc response";
        }
        return false;
    }
    return true;
}

bool KVCacheWorker::SubmitLoadRequest(const std::string &key,
                                      uint32_t partition_id,
                                      uint32_t payload_bytes,
                                      KVCacheRequest *out_request,
                                      std::string *error_message)
{
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(options_,
                                     impl_ != nullptr ? &impl_->ipc_context : nullptr,
                                     impl_ != nullptr ? &impl_->ipc_socket : nullptr,
                                     error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"submit_load",
                            key,
                            std::to_string(partition_id),
                            std::to_string(payload_bytes)},
                           &response_frames,
                           error_message))
    {
        if (impl_ != nullptr)
        {
            impl_->ipc_socket.reset();
            impl_->ipc_context.reset();
        }
        return false;
    }
    if (response_frames.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "missing submit-load ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1 ? response_frames[1]
                                                        : "submit-load ipc request failed";
        }
        return false;
    }
    if (response_frames[0] != "ok" ||
        !DecodeRequestRecord(response_frames, 1, key, out_request))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid submit-load ipc response";
        }
        return false;
    }
    return true;
}

bool KVCacheWorker::MarkSaveRequestReady(uint64_t request_id,
                                         std::string *error_message)
{
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(options_,
                                     impl_ != nullptr ? &impl_->ipc_context : nullptr,
                                     impl_ != nullptr ? &impl_->ipc_socket : nullptr,
                                     error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"mark_save_ready", std::to_string(request_id)},
                           &response_frames,
                           error_message))
    {
        if (impl_ != nullptr)
        {
            impl_->ipc_socket.reset();
            impl_->ipc_context.reset();
        }
        return false;
    }
    if (response_frames.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "missing mark-save-ready ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1 ? response_frames[1]
                                                        : "mark-save-ready ipc request failed";
        }
        return false;
    }
    return response_frames[0] == "ok";
}

bool KVCacheWorker::PollCompletion(KVCacheCompletion *out_completion,
                                   std::string *error_message)
{
    // Poll exactly one completion record from the manager. Empty is reported as
    // false without an error so higher layers can spin or sleep as needed.
    if (out_completion == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "completion output pointer is null";
        }
        return false;
    }
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(options_,
                                     impl_ != nullptr ? &impl_->ipc_context : nullptr,
                                     impl_ != nullptr ? &impl_->ipc_socket : nullptr,
                                     error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"poll_completion"},
                           &response_frames,
                           error_message))
    {
        if (impl_ != nullptr)
        {
            impl_->ipc_socket.reset();
            impl_->ipc_context.reset();
        }
        if (error_message != nullptr &&
            IsBenignPollCompletionTransportError(*error_message))
        {
            error_message->clear();
        }
        return false;
    }
    if (response_frames.empty())
    {
        if (error_message != nullptr)
        {
            *error_message = "missing poll-completion ipc response";
        }
        return false;
    }
    if (response_frames[0] == "empty")
    {
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1 ? response_frames[1]
                                                        : "poll-completion ipc request failed";
        }
        return false;
    }
    if (response_frames[0] != "ok" ||
        !DecodeCompletionRecord(response_frames, 1, out_completion))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid poll-completion ipc response";
        }
        return false;
    }
    return true;
}

}  // namespace eloqstore::sdk
