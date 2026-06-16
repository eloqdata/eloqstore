#include "internal.h"

namespace eloqstore::sdk
{

using namespace runtime_internal;

// Worker-side runtime stub:
// - parses the exported shared-memory descriptor
// - owns only IPC transport state
// - never owns authoritative buffer-entry/index state

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
    // Workers parse the exported descriptor once so later requests can obey the
    // manager-defined entry sizing and group-count constraints.
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
        // directly operate on the shared buffer bytes.
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
        options_.shared_memory_bytes =
            static_cast<size_t>(std::stoull(parts[2]));
        options_.entry_size = static_cast<uint32_t>(std::stoul(parts[3]));
        options_.entry_count = static_cast<uint32_t>(std::stoul(parts[4]));
        options_.entry_alignment = static_cast<uint32_t>(std::stoul(parts[5]));
        options_.num_threads = static_cast<uint16_t>(std::stoul(parts[6]));
        options_.submission_queue_depth =
            static_cast<uint32_t>(std::stoul(parts[7]));
        options_.partition_count = static_cast<uint32_t>(std::stoul(parts[8]));
    }
    catch (const std::exception &e)
    {
        if (error_message != nullptr)
        {
            *error_message =
                std::string("failed to parse buffer pool descriptor: ") +
                e.what();
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

bool KVCacheWorker::BeginSave(const std::string &key,
                              uint32_t payload_bytes,
                              KVCacheBufferHandle *out_buffer,
                              std::string *error_message)
{
    if (out_buffer == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "begin-save output buffer is null";
        }
        return false;
    }
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(
            options_,
            impl_ != nullptr ? &impl_->ipc_context : nullptr,
            impl_ != nullptr ? &impl_->ipc_socket : nullptr,
            error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"begin_save", key, std::to_string(payload_bytes)},
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
            *error_message = "missing begin-save ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1
                                 ? response_frames[1]
                                 : "begin-save ipc request failed";
        }
        return false;
    }
    if (response_frames[0] != "ok" ||
        !DecodeBufferHandleRecord(response_frames, 1, out_buffer))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid begin-save ipc response";
        }
        return false;
    }
    return true;
}

bool KVCacheWorker::FinishSave(uint64_t request_id, std::string *error_message)
{
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(
            options_,
            impl_ != nullptr ? &impl_->ipc_context : nullptr,
            impl_ != nullptr ? &impl_->ipc_socket : nullptr,
            error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"finish_save", std::to_string(request_id)},
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
            *error_message = "missing finish-save ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1
                                 ? response_frames[1]
                                 : "finish-save ipc request failed";
        }
        return false;
    }
    return response_frames[0] == "ok";
}

bool KVCacheWorker::BeginLoad(const std::string &key,
                              uint32_t payload_bytes,
                              uint64_t *out_request_id,
                              std::string *error_message)
{
    if (out_request_id == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "begin-load output request id is null";
        }
        return false;
    }
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(
            options_,
            impl_ != nullptr ? &impl_->ipc_context : nullptr,
            impl_ != nullptr ? &impl_->ipc_socket : nullptr,
            error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"begin_load", key, std::to_string(payload_bytes)},
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
            *error_message = "missing begin-load ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1
                                 ? response_frames[1]
                                 : "begin-load ipc request failed";
        }
        return false;
    }
    if (response_frames.size() != 2)
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid begin-load ipc response";
        }
        return false;
    }
    const auto request_id = ParseUint64(response_frames[1]);
    if (!request_id.has_value())
    {
        if (error_message != nullptr)
        {
            *error_message = "failed to parse begin-load request id";
        }
        return false;
    }
    *out_request_id = *request_id;
    return true;
}

bool KVCacheWorker::CheckRequest(uint64_t request_id,
                                 KVCacheRequestState *out_state,
                                 std::string *error_message)
{
    if (out_state == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "check-request output state is null";
        }
        return false;
    }
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(
            options_,
            impl_ != nullptr ? &impl_->ipc_context : nullptr,
            impl_ != nullptr ? &impl_->ipc_socket : nullptr,
            error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"check_request", std::to_string(request_id)},
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
            *error_message = "missing check-request ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1
                                 ? response_frames[1]
                                 : "check-request ipc request failed";
        }
        return false;
    }
    if (response_frames[0] != "ok" ||
        !DecodeRequestStateRecord(response_frames, 1, out_state))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid check-request ipc response";
        }
        return false;
    }
    return true;
}

bool KVCacheWorker::CheckRequests(const std::vector<uint64_t> &request_ids,
                                  std::vector<KVCacheRequestState> *out_states,
                                  std::string *error_message)
{
    if (out_states == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "check-requests output vector is null";
        }
        return false;
    }
    std::vector<std::string> request_frames;
    request_frames.reserve(request_ids.size() + 1);
    request_frames.push_back("check_requests");
    for (const auto request_id : request_ids)
    {
        request_frames.push_back(std::to_string(request_id));
    }

    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(
            options_,
            impl_ != nullptr ? &impl_->ipc_context : nullptr,
            impl_ != nullptr ? &impl_->ipc_socket : nullptr,
            error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           request_frames,
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
    if (response_frames.empty() || response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1
                                 ? response_frames[1]
                                 : "check-requests ipc request failed";
        }
        return false;
    }
    if (response_frames.size() < 2)
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid check-requests ipc response";
        }
        return false;
    }
    const auto count = ParseUint64(response_frames[1]);
    if (!count.has_value())
    {
        if (error_message != nullptr)
        {
            *error_message = "failed to parse check-requests response count";
        }
        return false;
    }
    if (response_frames.size() != 2 + (*count * 5))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid check-requests response size";
        }
        return false;
    }

    out_states->clear();
    out_states->reserve(*count);
    size_t offset = 2;
    for (uint64_t i = 0; i < *count; ++i)
    {
        KVCacheRequestState state;
        if (!DecodeRequestStateRecord(response_frames, offset, &state))
        {
            if (error_message != nullptr)
            {
                *error_message =
                    "failed to decode check-requests response state";
            }
            return false;
        }
        out_states->push_back(std::move(state));
        offset += 5;
    }
    return true;
}

bool KVCacheWorker::GetReadyBuffer(uint64_t request_id,
                                   KVCacheBufferHandle *out_buffer,
                                   std::string *error_message)
{
    if (out_buffer == nullptr)
    {
        if (error_message != nullptr)
        {
            *error_message = "get-ready-buffer output buffer is null";
        }
        return false;
    }
    std::vector<std::string> response_frames;
    std::lock_guard<std::mutex> lock(ipc_mutex_);
    if (!EnsureWorkerSocketConnected(
            options_,
            impl_ != nullptr ? &impl_->ipc_context : nullptr,
            impl_ != nullptr ? &impl_->ipc_socket : nullptr,
            error_message) ||
        !ExchangeWorkerIpc(impl_ != nullptr ? impl_->ipc_socket.get() : nullptr,
                           {"get_ready_buffer", std::to_string(request_id)},
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
            *error_message = "missing get-ready-buffer ipc response";
        }
        return false;
    }
    if (response_frames[0] == "error")
    {
        if (error_message != nullptr)
        {
            *error_message = response_frames.size() > 1
                                 ? response_frames[1]
                                 : "get-ready-buffer ipc request failed";
        }
        return false;
    }
    if (response_frames[0] != "ok" ||
        !DecodeBufferHandleRecord(response_frames, 1, out_buffer))
    {
        if (error_message != nullptr)
        {
            *error_message = "invalid get-ready-buffer ipc response";
        }
        return false;
    }
    return true;
}

}  // namespace eloqstore::sdk
