#include "eloqstore_capi.h"

#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "eloq_store.h"
#include "global_registered_memory.h"
#include "io_string_buffer.h"
#include "kv_options.h"
#include "sdk_runtime.h"
#include "types.h"

using eloqstore::BatchWriteRequest;
using eloqstore::EloqStore;
using eloqstore::FloorRequest;
using eloqstore::GlobalRegisteredMemory;
using eloqstore::IoStringBuffer;
using eloqstore::KvError;
using eloqstore::KvOptions;
using eloqstore::ReadRequest;
using eloqstore::ScanRequest;
using eloqstore::TableIdent;
using eloqstore::WriteDataEntry;
using eloqstore::WriteOp;
using eloqstore::sdk::KVCacheManager;
using eloqstore::sdk::KVCacheOptions;
using eloqstore::sdk::KVCacheWorker;

// ============================================================
// Thread-local storage for error messages (no mutex: each thread
// has its own copy, and typical usage is sequential set-then-get)
// ============================================================
static thread_local std::string g_last_error_message;

static void set_last_error(const std::string &msg)
{
    g_last_error_message = msg;
}

static void clear_last_error()
{
    g_last_error_message.clear();
}

// ============================================================
// Owned scan result storage (avoids dangling pointers into request)
// ============================================================
struct OwnedScanResult
{
    std::vector<CScanEntry> entries;
    std::vector<uint8_t> key_value_buffer;
};
static std::mutex g_scan_result_mutex;
static std::unordered_map<CScanResult *, std::unique_ptr<OwnedScanResult>>
    g_owned_scan_results;

enum class AsyncHandleKind
{
    BatchWrite,
    LargeRead,
    PinnedRead,
};

struct AsyncHandleData
{
    AsyncHandleKind kind{AsyncHandleKind::BatchWrite};
    std::unique_ptr<BatchWriteRequest> req;
    std::unique_ptr<ReadRequest> read_req;
    std::unique_ptr<IoStringBuffer> large_value;
    std::string pinned_metadata;
    uint8_t *pinned_dest{nullptr};
    size_t pinned_dest_size{0};
    uint64_t pinned_ts{0};
    uint64_t pinned_expire_ts{0};
    GlobalRegisteredMemory *mem{nullptr};
    uint16_t reg_mem_index_base{0};
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    bool result_claimed{false};
    CEloqStoreStatus status{CEloqStoreStatus_Ok};
    CLargeValueResult result{nullptr, 0, 0, 0, false, CValueKind_NotFound};
    std::string error_message;
};

static void recycle_large_entries(BatchWriteRequest *req,
                                  GlobalRegisteredMemory *mem,
                                  uint16_t reg_mem_index_base)
{
    if (!req || !mem)
        return;
    for (auto &entry : req->batch_)
    {
        entry.RecycleLargeValue(mem, reg_mem_index_base);
    }
}

// ============================================================
// Error code conversion
// ============================================================
static CEloqStoreStatus kv_error_to_c(KvError err)
{
    switch (err)
    {
    case KvError::NoError:
        return CEloqStoreStatus_Ok;
    case KvError::InvalidArgs:
        return CEloqStoreStatus_InvalidArgs;
    case KvError::NotFound:
        return CEloqStoreStatus_NotFound;
    case KvError::NotRunning:
        return CEloqStoreStatus_NotRunning;
    case KvError::Corrupted:
        return CEloqStoreStatus_Corrupted;
    case KvError::EndOfFile:
        return CEloqStoreStatus_EndOfFile;
    case KvError::OutOfSpace:
        return CEloqStoreStatus_OutOfSpace;
    case KvError::OutOfMem:
        return CEloqStoreStatus_OutOfMem;
    case KvError::OpenFileLimit:
        return CEloqStoreStatus_OpenFileLimit;
    case KvError::TryAgain:
        return CEloqStoreStatus_TryAgain;
    case KvError::Busy:
        return CEloqStoreStatus_Busy;
    case KvError::Timeout:
        return CEloqStoreStatus_Timeout;
    case KvError::NoPermission:
        return CEloqStoreStatus_NoPermission;
    case KvError::CloudErr:
        return CEloqStoreStatus_CloudErr;
    case KvError::IoFail:
        return CEloqStoreStatus_IoFail;
    case KvError::ExpiredTerm:
        return CEloqStoreStatus_ExpiredTerm;
    case KvError::OssInsufficientStorage:
        return CEloqStoreStatus_OssInsufficientStorage;
    case KvError::AlreadyExists:
        return CEloqStoreStatus_AlreadyExists;
    default:
        return CEloqStoreStatus_InvalidArgs;
    }
}

static uint8_t kv_cache_request_status_to_c(
    eloqstore::sdk::KVCacheRequestStatus status)
{
    switch (status)
    {
    case eloqstore::sdk::KVCacheRequestStatus::Pending:
        return CKVCacheRequestStatus_Pending;
    case eloqstore::sdk::KVCacheRequestStatus::Ready:
        return CKVCacheRequestStatus_Ready;
    case eloqstore::sdk::KVCacheRequestStatus::Failed:
        return CKVCacheRequestStatus_Failed;
    default:
        return 0;
    }
}

// Store C++ objects to handle mapping (for flattened API)
static std::mutex g_request_mutex;
static thread_local ReadRequest *g_last_read_req = nullptr;
static thread_local FloorRequest *g_last_floor_req = nullptr;

extern "C"
{
    // ============================================================
    // Options API (using new naming)
    // ============================================================

    CEloqStoreHandle CEloqStore_Options_Create(void)
    {
        clear_last_error();
        try
        {
            return reinterpret_cast<CEloqStoreHandle>(new KvOptions());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_Options_Destroy(CEloqStoreHandle opts)
    {
        if (opts)
        {
            delete reinterpret_cast<KvOptions *>(opts);
        }
    }

    void CEloqStore_Options_SetNumThreads(CEloqStoreHandle opts, uint16_t n)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->num_threads = n;
    }

    void CEloqStore_Options_SetBufferPoolSize(CEloqStoreHandle opts,
                                              uint64_t size)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->buffer_pool_size = size;
    }

    void CEloqStore_Options_SetDataPageSize(CEloqStoreHandle opts,
                                            uint16_t size)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->data_page_size = size;
    }

    void CEloqStore_Options_SetManifestLimit(CEloqStoreHandle opts,
                                             uint32_t limit)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->manifest_limit = limit;
    }

    void CEloqStore_Options_SetFdLimit(CEloqStoreHandle opts, uint32_t limit)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->fd_limit = limit;
    }

    void CEloqStore_Options_SetPagesPerFileShift(CEloqStoreHandle opts,
                                                 uint8_t shift)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->pages_per_file_shift = shift;
    }

    void CEloqStore_Options_SetOverflowPointers(CEloqStoreHandle opts,
                                                uint8_t n)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->overflow_pointers = n;
    }

    void CEloqStore_Options_SetDataAppendMode(CEloqStoreHandle opts,
                                              bool enable)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->data_append_mode = enable;
    }

    void CEloqStore_Options_SetEnableCompression(CEloqStoreHandle opts,
                                                 bool enable)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->enable_compression = enable;
    }

    void CEloqStore_Options_SetSegmentSize(CEloqStoreHandle opts, uint32_t size)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->segment_size = size;
    }

    void CEloqStore_Options_SetRegisteredMemoryChunkSize(CEloqStoreHandle opts,
                                                         uint64_t size)
    {
        (void) opts;
        (void) size;
    }

    void CEloqStore_Options_SetSegmentsPerFileShift(CEloqStoreHandle opts,
                                                    uint8_t shift)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->segments_per_file_shift =
                shift;
    }

    void CEloqStore_Options_SetGlobalRegisteredMemory(
        CEloqStoreHandle opts,
        uint32_t shard_id,
        CGlobalRegisteredMemoryHandle mem)
    {
        if (!opts || !mem)
            return;
        auto *cpp_opts = reinterpret_cast<KvOptions *>(opts);
        if (cpp_opts->global_registered_memories.size() <= shard_id)
        {
            cpp_opts->global_registered_memories.resize(shard_id + 1, nullptr);
        }
        cpp_opts->global_registered_memories[shard_id] =
            reinterpret_cast<GlobalRegisteredMemory *>(mem);
    }

    void CEloqStore_Options_AddPinnedMemoryChunk(CEloqStoreHandle opts,
                                                 const char *data,
                                                 size_t size)
    {
        if (!opts || !data || size == 0)
            return;
        auto *cpp_opts = reinterpret_cast<KvOptions *>(opts);
        cpp_opts->pinned_memory_chunks.emplace_back(const_cast<char *>(data),
                                                    size);
    }

    void CEloqStore_Options_SetGcGlobalMemSizePerShard(CEloqStoreHandle opts,
                                                       uint64_t size)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->gc_global_mem_size_per_shard =
                static_cast<size_t>(size);
    }

    void CEloqStore_Options_SetPinnedTailScratchSlots(CEloqStoreHandle opts,
                                                      uint16_t slots)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->pinned_tail_scratch_slots =
                slots;
    }

    void CEloqStore_Options_AddStorePath(CEloqStoreHandle opts,
                                         const char *path)
    {
        if (opts && path)
            reinterpret_cast<KvOptions *>(opts)->store_path.push_back(path);
    }

    void CEloqStore_Options_SetCloudStorePath(CEloqStoreHandle opts,
                                              const char *path)
    {
        if (opts && path)
            reinterpret_cast<KvOptions *>(opts)->cloud_store_path = path;
    }

    void CEloqStore_Options_SetCloudProvider(CEloqStoreHandle opts,
                                             const char *provider)
    {
        if (opts && provider)
            reinterpret_cast<KvOptions *>(opts)->cloud_provider = provider;
    }

    void CEloqStore_Options_SetCloudRegion(CEloqStoreHandle opts,
                                           const char *region)
    {
        if (opts && region)
            reinterpret_cast<KvOptions *>(opts)->cloud_region = region;
    }

    void CEloqStore_Options_SetCloudCredentials(CEloqStoreHandle opts,
                                                const char *access_key,
                                                const char *secret_key)
    {
        if (opts)
        {
            if (access_key)
                reinterpret_cast<KvOptions *>(opts)->cloud_access_key =
                    access_key;
            if (secret_key)
                reinterpret_cast<KvOptions *>(opts)->cloud_secret_key =
                    secret_key;
        }
    }

    void CEloqStore_Options_SetCloudAutoCredentials(CEloqStoreHandle opts,
                                                    bool enable)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->cloud_auto_credentials =
                enable;
    }

    void CEloqStore_Options_SetCloudVerifySsl(CEloqStoreHandle opts,
                                              bool verify)
    {
        if (opts)
            reinterpret_cast<KvOptions *>(opts)->cloud_verify_ssl = verify;
    }

    bool CEloqStore_Options_LoadFromIni(CEloqStoreHandle opts, const char *path)
    {
        clear_last_error();
        if (!opts || !path)
        {
            set_last_error("Invalid options or ini path");
            return false;
        }
        try
        {
            return reinterpret_cast<KvOptions *>(opts)->LoadFromIni(path) == 0;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return false;
        }
    }

    bool CEloqStore_Options_Validate(CEloqStoreHandle opts)
    {
        if (!opts)
            return false;
        return EloqStore::ValidateOptions(*reinterpret_cast<KvOptions *>(opts));
    }

    CKVCacheOptionsHandle CEloqStore_KVCacheOptions_Create(void)
    {
        clear_last_error();
        try
        {
            return reinterpret_cast<CKVCacheOptionsHandle>(
                new KVCacheOptions());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_KVCacheOptions_Destroy(CKVCacheOptionsHandle opts)
    {
        delete reinterpret_cast<KVCacheOptions *>(opts);
    }

    void CEloqStore_KVCacheOptions_AddStorePath(CKVCacheOptionsHandle opts,
                                                const char *path)
    {
        if (opts != nullptr && path != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->store_paths.emplace_back(
                path);
        }
    }

    void CEloqStore_KVCacheOptions_SetTableName(CKVCacheOptionsHandle opts,
                                                const char *table_name)
    {
        if (opts != nullptr && table_name != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->table_name = table_name;
        }
    }

    void CEloqStore_KVCacheOptions_SetBranch(CKVCacheOptionsHandle opts,
                                             const char *branch)
    {
        if (opts != nullptr && branch != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->branch = branch;
        }
    }

    void CEloqStore_KVCacheOptions_SetIpcPath(CKVCacheOptionsHandle opts,
                                              const char *ipc_path)
    {
        if (opts != nullptr && ipc_path != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->ipc_path = ipc_path;
        }
    }

    void CEloqStore_KVCacheOptions_SetSharedMemoryName(
        CKVCacheOptionsHandle opts, const char *name)
    {
        if (opts != nullptr && name != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->shared_memory_name = name;
        }
    }

    void CEloqStore_KVCacheOptions_SetNumThreads(CKVCacheOptionsHandle opts,
                                                 uint16_t n)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->num_threads = n;
        }
    }

    void CEloqStore_KVCacheOptions_SetPartitionCount(CKVCacheOptionsHandle opts,
                                                     uint32_t n)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->partition_count = n;
        }
    }

    void CEloqStore_KVCacheOptions_SetTerm(CKVCacheOptionsHandle opts,
                                           uint64_t term)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->term = term;
        }
    }

    void CEloqStore_KVCacheOptions_SetPartitionGroupId(
        CKVCacheOptionsHandle opts, uint32_t partition_group_id)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->partition_group_id =
                partition_group_id;
        }
    }

    void CEloqStore_KVCacheOptions_SetSharedMemoryBytes(
        CKVCacheOptionsHandle opts, uint64_t bytes)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->shared_memory_bytes =
                static_cast<size_t>(bytes);
        }
    }

    void CEloqStore_KVCacheOptions_SetEntrySize(CKVCacheOptionsHandle opts,
                                                uint32_t entry_size)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->entry_size = entry_size;
        }
    }

    void CEloqStore_KVCacheOptions_SetEntryCount(CKVCacheOptionsHandle opts,
                                                 uint32_t entry_count)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->entry_count = entry_count;
        }
    }

    void CEloqStore_KVCacheOptions_SetEntryAlignment(CKVCacheOptionsHandle opts,
                                                     uint32_t entry_alignment)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->entry_alignment =
                entry_alignment;
        }
    }

    void CEloqStore_KVCacheOptions_SetSubmissionQueueDepth(
        CKVCacheOptionsHandle opts, uint32_t depth)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->submission_queue_depth =
                depth;
        }
    }

    void CEloqStore_KVCacheOptions_SetEagerIoUringRegister(
        CKVCacheOptionsHandle opts, bool enable)
    {
        if (opts != nullptr)
        {
            reinterpret_cast<KVCacheOptions *>(opts)->eager_io_uring_register =
                enable;
        }
    }

    CKVCacheManagerHandle CEloqStore_KVCacheManager_Create(
        CKVCacheOptionsHandle opts)
    {
        clear_last_error();
        if (opts == nullptr)
        {
            set_last_error("runtime options handle is null");
            return nullptr;
        }
        try
        {
            auto *options = reinterpret_cast<KVCacheOptions *>(opts);
            return reinterpret_cast<CKVCacheManagerHandle>(
                new KVCacheManager(*options));
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_KVCacheManager_Destroy(CKVCacheManagerHandle runtime)
    {
        delete reinterpret_cast<KVCacheManager *>(runtime);
    }

    bool CEloqStore_KVCacheManager_Start(CKVCacheManagerHandle runtime)
    {
        // Start the native manager runtime and surface any C++ error string
        // through the thread-local C API last-error buffer.
        clear_last_error();
        if (runtime == nullptr)
        {
            set_last_error("kv cache manager handle is null");
            return false;
        }
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheManager *>(runtime)->Start(&error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    void CEloqStore_KVCacheManager_Stop(CKVCacheManagerHandle runtime)
    {
        // Stop is best-effort and intentionally silent for null handles.
        if (runtime != nullptr)
        {
            reinterpret_cast<KVCacheManager *>(runtime)->Stop();
        }
    }

    bool CEloqStore_KVCacheManager_RegisterIoUringBuffers(
        CKVCacheManagerHandle runtime)
    {
        // Register the manager-owned shared-memory region with the native store
        // so later save/load requests can reuse the same pinned buffer view.
        clear_last_error();
        if (runtime == nullptr)
        {
            set_last_error("kv cache manager handle is null");
            return false;
        }
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheManager *>(runtime)->RegisterIoUringBuffers(
                &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    const char *CEloqStore_KVCacheManager_ExportBufferPool(
        CKVCacheManagerHandle runtime)
    {
        // Return an owned C string because Python must keep the descriptor
        // after the C++ temporary std::string has gone out of scope.
        clear_last_error();
        if (runtime == nullptr)
        {
            set_last_error("kv cache manager handle is null");
            return nullptr;
        }
        try
        {
            return ::strdup(reinterpret_cast<KVCacheManager *>(runtime)
                                ->ExportBufferPoolDescriptor()
                                .c_str());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    bool CEloqStore_KVCacheManager_BeginSave(CKVCacheManagerHandle runtime,
                                             const char *key,
                                             uint32_t payload_bytes,
                                             CKVCacheBufferHandle *out_buffer)
    {
        clear_last_error();
        if (runtime == nullptr || key == nullptr || out_buffer == nullptr)
        {
            set_last_error("invalid begin-save arguments");
            return false;
        }
        eloqstore::sdk::KVCacheBufferHandle buffer;
        std::string error_message;
        const bool ok = reinterpret_cast<KVCacheManager *>(runtime)->BeginSave(
            key, payload_bytes, &buffer, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
            return false;
        }
        out_buffer->request_id = buffer.request_id;
        out_buffer->offset_bytes = buffer.offset_bytes;
        out_buffer->payload_bytes = buffer.payload_bytes;
        return true;
    }

    bool CEloqStore_KVCacheManager_FinishSave(CKVCacheManagerHandle runtime,
                                              uint64_t request_id)
    {
        clear_last_error();
        if (runtime == nullptr)
        {
            set_last_error("kv cache manager handle is null");
            return false;
        }
        std::string error_message;
        const bool ok = reinterpret_cast<KVCacheManager *>(runtime)->FinishSave(
            request_id, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    bool CEloqStore_KVCacheManager_BeginLoad(CKVCacheManagerHandle runtime,
                                             const char *key,
                                             uint32_t payload_bytes,
                                             uint64_t *out_request_id)
    {
        clear_last_error();
        if (runtime == nullptr || key == nullptr || out_request_id == nullptr)
        {
            set_last_error("invalid begin-load arguments");
            return false;
        }
        std::string error_message;
        const bool ok = reinterpret_cast<KVCacheManager *>(runtime)->BeginLoad(
            key, payload_bytes, out_request_id, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    bool CEloqStore_KVCacheManager_CheckRequest(CKVCacheManagerHandle runtime,
                                                uint64_t request_id,
                                                CKVCacheRequestState *out_state)
    {
        clear_last_error();
        if (runtime == nullptr || out_state == nullptr)
        {
            set_last_error("invalid check-request arguments");
            return false;
        }
        eloqstore::sdk::KVCacheRequestState state;
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheManager *>(runtime)->CheckRequest(
                request_id, &state, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
            return false;
        }
        out_state->request_id = state.request_id;
        out_state->status = kv_cache_request_status_to_c(state.status);
        out_state->offset_bytes = state.offset_bytes;
        out_state->payload_bytes = state.payload_bytes;
        return true;
    }

    bool CEloqStore_KVCacheManager_GetReadyBuffer(
        CKVCacheManagerHandle runtime,
        uint64_t request_id,
        CKVCacheBufferHandle *out_buffer)
    {
        clear_last_error();
        if (runtime == nullptr || out_buffer == nullptr)
        {
            set_last_error("invalid get-ready-buffer arguments");
            return false;
        }
        eloqstore::sdk::KVCacheBufferHandle buffer;
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheManager *>(runtime)->GetReadyBuffer(
                request_id, &buffer, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
            return false;
        }
        out_buffer->request_id = buffer.request_id;
        out_buffer->offset_bytes = buffer.offset_bytes;
        out_buffer->payload_bytes = buffer.payload_bytes;
        return true;
    }

    bool CEloqStore_KVCacheManager_ContainsKey(CKVCacheManagerHandle runtime,
                                               const char *key,
                                               bool *out_exists)
    {
        clear_last_error();
        if (runtime == nullptr || key == nullptr || out_exists == nullptr)
        {
            set_last_error("invalid manager contains-key arguments");
            return false;
        }
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheManager *>(runtime)->ContainsKey(
                key, out_exists, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    CKVCacheWorkerHandle CEloqStore_KVCacheWorker_Create(
        CKVCacheOptionsHandle opts)
    {
        // Create a worker-side control-plane stub from shared runtime options.
        clear_last_error();
        if (opts == nullptr)
        {
            set_last_error("runtime options handle is null");
            return nullptr;
        }
        try
        {
            auto *options = reinterpret_cast<KVCacheOptions *>(opts);
            return reinterpret_cast<CKVCacheWorkerHandle>(
                new KVCacheWorker(*options));
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_KVCacheWorker_Destroy(CKVCacheWorkerHandle runtime)
    {
        // Worker destruction is a direct delete of the native stub.
        delete reinterpret_cast<KVCacheWorker *>(runtime);
    }

    bool CEloqStore_KVCacheWorker_AttachBufferPool(CKVCacheWorkerHandle runtime,
                                                   const char *descriptor)
    {
        // Load one manager-exported descriptor into the native worker stub.
        clear_last_error();
        if (runtime == nullptr)
        {
            set_last_error("worker runtime handle is null");
            return false;
        }
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheWorker *>(runtime)->AttachBufferPool(
                descriptor ? descriptor : "", &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    void CEloqStore_KVCacheWorker_DetachBufferPool(CKVCacheWorkerHandle runtime)
    {
        // Detach is best-effort and intentionally silent for null handles.
        if (runtime != nullptr)
        {
            reinterpret_cast<KVCacheWorker *>(runtime)->DetachBufferPool();
        }
    }

    bool CEloqStore_KVCacheWorker_BeginSave(CKVCacheWorkerHandle runtime,
                                            const char *key,
                                            uint32_t payload_bytes,
                                            CKVCacheBufferHandle *out_buffer)
    {
        clear_last_error();
        if (runtime == nullptr || key == nullptr || out_buffer == nullptr)
        {
            set_last_error("invalid worker begin-save arguments");
            return false;
        }
        eloqstore::sdk::KVCacheBufferHandle buffer;
        std::string error_message;
        const bool ok = reinterpret_cast<KVCacheWorker *>(runtime)->BeginSave(
            key, payload_bytes, &buffer, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
            return false;
        }
        out_buffer->request_id = buffer.request_id;
        out_buffer->offset_bytes = buffer.offset_bytes;
        out_buffer->payload_bytes = buffer.payload_bytes;
        return true;
    }

    bool CEloqStore_KVCacheWorker_FinishSave(CKVCacheWorkerHandle runtime,
                                             uint64_t request_id)
    {
        clear_last_error();
        if (runtime == nullptr)
        {
            set_last_error("kv cache worker handle is null");
            return false;
        }
        std::string error_message;
        const bool ok = reinterpret_cast<KVCacheWorker *>(runtime)->FinishSave(
            request_id, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    bool CEloqStore_KVCacheWorker_BeginLoad(CKVCacheWorkerHandle runtime,
                                            const char *key,
                                            uint32_t payload_bytes,
                                            uint64_t *out_request_id)
    {
        clear_last_error();
        if (runtime == nullptr || key == nullptr || out_request_id == nullptr)
        {
            set_last_error("invalid worker begin-load arguments");
            return false;
        }
        std::string error_message;
        const bool ok = reinterpret_cast<KVCacheWorker *>(runtime)->BeginLoad(
            key, payload_bytes, out_request_id, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
        }
        return ok;
    }

    bool CEloqStore_KVCacheWorker_CheckRequest(CKVCacheWorkerHandle runtime,
                                               uint64_t request_id,
                                               CKVCacheRequestState *out_state)
    {
        clear_last_error();
        if (runtime == nullptr || out_state == nullptr)
        {
            set_last_error("invalid worker check-request arguments");
            return false;
        }
        eloqstore::sdk::KVCacheRequestState state;
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheWorker *>(runtime)->CheckRequest(
                request_id, &state, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
            return false;
        }
        out_state->request_id = state.request_id;
        out_state->status = kv_cache_request_status_to_c(state.status);
        out_state->offset_bytes = state.offset_bytes;
        out_state->payload_bytes = state.payload_bytes;
        return true;
    }

    bool CEloqStore_KVCacheWorker_GetReadyBuffer(
        CKVCacheWorkerHandle runtime,
        uint64_t request_id,
        CKVCacheBufferHandle *out_buffer)
    {
        clear_last_error();
        if (runtime == nullptr || out_buffer == nullptr)
        {
            set_last_error("invalid worker get-ready-buffer arguments");
            return false;
        }
        eloqstore::sdk::KVCacheBufferHandle buffer;
        std::string error_message;
        const bool ok =
            reinterpret_cast<KVCacheWorker *>(runtime)->GetReadyBuffer(
                request_id, &buffer, &error_message);
        if (!ok)
        {
            set_last_error(error_message);
            return false;
        }
        out_buffer->request_id = buffer.request_id;
        out_buffer->offset_bytes = buffer.offset_bytes;
        out_buffer->payload_bytes = buffer.payload_bytes;
        return true;
    }

    void CEloqStore_FreeCString(const char *value)
    {
        // Free strings allocated by C API helpers such as ExportBufferPool.
        if (value != nullptr)
        {
            ::free(const_cast<char *>(value));
        }
    }

    // ============================================================
    // Engine lifecycle
    // ============================================================

    CEloqStoreHandle CEloqStore_Create(CEloqStoreHandle options)
    {
        clear_last_error();
        if (!options)
        {
            set_last_error("Invalid options: null pointer");
            return nullptr;
        }
        try
        {
            auto *store =
                new EloqStore(*reinterpret_cast<KvOptions *>(options));
            return reinterpret_cast<CEloqStoreHandle>(store);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_Destroy(CEloqStoreHandle store)
    {
        if (store)
        {
            delete reinterpret_cast<EloqStore *>(store);
        }
    }

    CEloqStoreStatus CEloqStore_Start(CEloqStoreHandle store)
    {
        clear_last_error();
        if (!store)
        {
            return CEloqStoreStatus_InvalidArgs;
        }
        try
        {
            auto err = reinterpret_cast<EloqStore *>(store)->Start(
                eloqstore::MainBranchName, 0);
            if (err != KvError::NoError)
            {
                set_last_error("Failed to start store");
            }
            return kv_error_to_c(err);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_StartWithBranch(CEloqStoreHandle store,
                                                const char *branch,
                                                uint64_t term,
                                                uint32_t partition_group_id)
    {
        clear_last_error();
        if (!store)
        {
            return CEloqStoreStatus_InvalidArgs;
        }
        try
        {
            std::string_view branch_name =
                branch != nullptr ? std::string_view(branch)
                                  : std::string_view(eloqstore::MainBranchName);
            auto err = reinterpret_cast<EloqStore *>(store)->Start(
                branch_name, term, partition_group_id);
            if (err != KvError::NoError)
            {
                set_last_error("Failed to start store");
            }
            return kv_error_to_c(err);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    void CEloqStore_Stop(CEloqStoreHandle store)
    {
        if (store)
        {
            reinterpret_cast<EloqStore *>(store)->Stop();
        }
    }

    bool CEloqStore_IsStopped(CEloqStoreHandle store)
    {
        return store ? reinterpret_cast<EloqStore *>(store)->IsStopped() : true;
    }

    uint16_t CEloqStore_GlobalRegMemIndexBase(CEloqStoreHandle store,
                                              size_t shard_id)
    {
        return store ? reinterpret_cast<EloqStore *>(store)
                           ->GlobalRegMemIndexBase(shard_id)
                     : 0;
    }

    // ============================================================
    // Table identifier
    // ============================================================

    CTableIdentHandle CEloqStore_TableIdent_Create(const char *table_name,
                                                   uint32_t partition_id)
    {
        clear_last_error();
        if (!table_name)
        {
            set_last_error("Invalid table name: null pointer");
            return nullptr;
        }
        try
        {
            return reinterpret_cast<CTableIdentHandle>(
                new TableIdent(table_name, partition_id));
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_TableIdent_Destroy(CTableIdentHandle ident)
    {
        if (ident)
        {
            delete reinterpret_cast<TableIdent *>(ident);
        }
    }

    const char *CEloqStore_TableIdent_GetName(CTableIdentHandle ident)
    {
        if (!ident)
            return nullptr;
        // Return a pointer to the internal std::string's buffer.
        // The pointer is valid as long as the TableIdent object exists and is
        // not modified. Callers should copy the string immediately if they need
        // to keep it beyond the lifetime of the TableIdent object or if they
        // may call GetName again.
        return reinterpret_cast<TableIdent *>(ident)->tbl_name_.c_str();
    }

    uint32_t CEloqStore_TableIdent_GetPartition(CTableIdentHandle ident)
    {
        return ident ? reinterpret_cast<TableIdent *>(ident)->partition_id_ : 0;
    }

    // ============================================================
    // Flattened write API (simple operations)
    // Implemented using BatchWriteRequest
    // ============================================================

    CEloqStoreStatus CEloqStore_Put(CEloqStoreHandle store,
                                    CTableIdentHandle table,
                                    const uint8_t *key,
                                    size_t key_len,
                                    const uint8_t *value,
                                    size_t value_len,
                                    uint64_t timestamp)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            // Create temporary BatchWriteRequest to execute Put
            std::vector<WriteDataEntry> batch;
            WriteDataEntry entry;
            entry.key_ =
                std::string(reinterpret_cast<const char *>(key), key_len);
            if (value && value_len > 0)
            {
                entry.val_ = std::string(reinterpret_cast<const char *>(value),
                                         value_len);
            }
            entry.timestamp_ = timestamp;
            entry.expire_ts_ = 0;
            entry.op_ = WriteOp::Upsert;
            batch.push_back(std::move(entry));

            BatchWriteRequest req;
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_Delete(CEloqStoreHandle store,
                                       CTableIdentHandle table,
                                       const uint8_t *key,
                                       size_t key_len,
                                       uint64_t timestamp)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            // Create temporary BatchWriteRequest to execute Delete
            std::vector<WriteDataEntry> batch;
            WriteDataEntry entry;
            entry.key_ =
                std::string(reinterpret_cast<const char *>(key), key_len);
            entry.val_ = "";
            entry.timestamp_ = timestamp;
            entry.expire_ts_ = 0;
            entry.op_ = WriteOp::Delete;
            batch.push_back(std::move(entry));

            BatchWriteRequest req;
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    // ============================================================
    // Batch write API
    // ============================================================

    CEloqStoreStatus CEloqStore_PutBatch(CEloqStoreHandle store,
                                         CTableIdentHandle table,
                                         const uint8_t *const *keys,
                                         const size_t *key_lens,
                                         const uint8_t *const *values,
                                         const size_t *value_lens,
                                         size_t count,
                                         uint64_t timestamp)
    {
        clear_last_error();
        if (!store || !table || !keys || !key_lens || count == 0)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            std::vector<WriteDataEntry> batch;
            batch.reserve(count);

            for (size_t i = 0; i < count; ++i)
            {
                WriteDataEntry entry;
                entry.key_ = std::string(
                    reinterpret_cast<const char *>(keys[i]), key_lens[i]);
                if (values && value_lens && value_lens[i] > 0)
                {
                    entry.val_ =
                        std::string(reinterpret_cast<const char *>(values[i]),
                                    value_lens[i]);
                }
                else
                {
                    entry.val_ = "";
                }
                entry.timestamp_ = timestamp;
                entry.expire_ts_ = 0;
                entry.op_ = WriteOp::Upsert;
                batch.push_back(std::move(entry));
            }

            BatchWriteRequest req;
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_PutEntries(CEloqStoreHandle store,
                                           CTableIdentHandle table,
                                           const CWriteEntry *entries,
                                           size_t count)
    {
        clear_last_error();
        if (!store || !table || !entries || count == 0)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            std::vector<WriteDataEntry> batch;
            batch.reserve(count);

            for (size_t i = 0; i < count; ++i)
            {
                WriteDataEntry entry;
                entry.key_ =
                    std::string(reinterpret_cast<const char *>(entries[i].key),
                                entries[i].key_len);
                if (entries[i].value && entries[i].value_len > 0)
                {
                    entry.val_ = std::string(
                        reinterpret_cast<const char *>(entries[i].value),
                        entries[i].value_len);
                }
                else
                {
                    entry.val_ = "";
                }
                entry.timestamp_ = entries[i].timestamp;
                entry.expire_ts_ = entries[i].expire_ts;
                entry.op_ = static_cast<WriteOp>(entries[i].op);
                batch.push_back(std::move(entry));
            }

            BatchWriteRequest req;
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_DeleteBatch(CEloqStoreHandle store,
                                            CTableIdentHandle table,
                                            const uint8_t *const *keys,
                                            const size_t *key_lens,
                                            size_t count,
                                            uint64_t timestamp)
    {
        clear_last_error();
        if (!store || !table || !keys || !key_lens || count == 0)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            std::vector<WriteDataEntry> batch;
            batch.reserve(count);

            for (size_t i = 0; i < count; ++i)
            {
                WriteDataEntry entry;
                entry.key_ = std::string(
                    reinterpret_cast<const char *>(keys[i]), key_lens[i]);
                entry.val_ = "";
                entry.timestamp_ = timestamp;
                entry.expire_ts_ = 0;
                entry.op_ = WriteOp::Delete;
                batch.push_back(std::move(entry));
            }

            BatchWriteRequest req;
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    // ============================================================
    // Flattened read API (simple operations)
    // Implemented using ReadRequest/FloorRequest
    // ============================================================

    CEloqStoreStatus CEloqStore_Get(CEloqStoreHandle store,
                                    CTableIdentHandle table,
                                    const uint8_t *key,
                                    size_t key_len,
                                    CGetResult *out_result)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            // Create temporary ReadRequest to execute Get
            ReadRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));

            cpp_store->ExecSync(&req);
            auto err = req.Error();

            if (err == KvError::NoError)
            {
                uint8_t *value_copy = new uint8_t[req.value_.size()];
                std::memcpy(value_copy, req.value_.data(), req.value_.size());
                out_result->value = value_copy;
                out_result->value_len = req.value_.size();
                out_result->timestamp = req.ts_;
                out_result->expire_ts = req.expire_ts_;
                out_result->found = true;
                out_result->owns_value = true;
            }
            else if (err == KvError::NotFound)
            {
                out_result->value = nullptr;
                out_result->value_len = 0;
                out_result->timestamp = 0;
                out_result->expire_ts = 0;
                out_result->found = false;
                out_result->owns_value = false;
            }
            else
            {
                return kv_error_to_c(err);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_GetInto(CEloqStoreHandle store,
                                        CTableIdentHandle table,
                                        const uint8_t *key,
                                        size_t key_len,
                                        uint8_t *out_value,
                                        size_t out_capacity,
                                        CGetResult *out_result)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            ReadRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));

            cpp_store->ExecSync(&req);
            auto err = req.Error();

            if (err == KvError::NoError)
            {
                out_result->value_len = req.value_.size();
                out_result->timestamp = req.ts_;
                out_result->expire_ts = req.expire_ts_;
                out_result->found = true;

                if (req.value_.size() > out_capacity)
                {
                    out_result->value = nullptr;
                    set_last_error("output buffer too small for value");
                    return CEloqStoreStatus_OutOfSpace;
                }

                if (req.value_.size() > 0)
                {
                    if (!out_value)
                    {
                        set_last_error(
                            "output buffer is null for non-empty value");
                        return CEloqStoreStatus_InvalidArgs;
                    }
                    std::memcpy(
                        out_value, req.value_.data(), req.value_.size());
                }
                out_result->value = out_value;
                out_result->owns_value = false;
            }
            else if (err == KvError::NotFound)
            {
                out_result->value = nullptr;
                out_result->value_len = 0;
                out_result->timestamp = 0;
                out_result->expire_ts = 0;
                out_result->found = false;
                out_result->owns_value = false;
            }
            else
            {
                return kv_error_to_c(err);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_GetLarge(CEloqStoreHandle store,
                                         CTableIdentHandle table,
                                         const uint8_t *key,
                                         size_t key_len,
                                         CLargeValueResult *out_result)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            ReadRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            req.large_value_dest_.emplace<IoStringBuffer>();

            cpp_store->ExecSync(&req);
            auto err = req.Error();

            if (err == KvError::NoError)
            {
                out_result->timestamp = req.ts_;
                out_result->expire_ts = req.expire_ts_;
                out_result->found = true;

                if (auto *iosb =
                        std::get_if<IoStringBuffer>(&req.large_value_dest_))
                {
                    auto *large = new IoStringBuffer(std::move(*iosb));
                    out_result->value =
                        reinterpret_cast<CIoStringBufferHandle>(large);
                    out_result->value_len = large->Size();
                    out_result->kind = CValueKind_Large;
                }
                else
                {
                    out_result->value = nullptr;
                    out_result->value_len = req.value_.size();
                    out_result->kind = CValueKind_Small;
                    set_last_error(
                        "value is not stored as a zero-copy large value");
                    return CEloqStoreStatus_InvalidArgs;
                }
            }
            else if (err == KvError::NotFound)
            {
                out_result->value = nullptr;
                out_result->value_len = 0;
                out_result->timestamp = 0;
                out_result->expire_ts = 0;
                out_result->found = false;
                out_result->kind = CValueKind_NotFound;
            }
            else
            {
                return kv_error_to_c(err);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CAsyncHandle CEloqStore_GetLargeAsync(CEloqStoreHandle store,
                                          CTableIdentHandle table,
                                          const uint8_t *key,
                                          size_t key_len,
                                          CGlobalRegisteredMemoryHandle mem,
                                          uint16_t reg_mem_index_base)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !mem)
        {
            return nullptr;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            auto *handle = new AsyncHandleData();
            handle->kind = AsyncHandleKind::LargeRead;
            handle->mem = reinterpret_cast<GlobalRegisteredMemory *>(mem);
            handle->reg_mem_index_base = reg_mem_index_base;
            handle->read_req = std::make_unique<ReadRequest>();
            handle->read_req->SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            handle->read_req->large_value_dest_.emplace<IoStringBuffer>();

            bool submitted = cpp_store->ExecAsyn(
                handle->read_req.get(),
                0,
                [handle](eloqstore::KvRequest *done_req)
                {
                    auto *read = static_cast<ReadRequest *>(done_req);
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    auto err = read->Error();
                    if (err == KvError::NoError)
                    {
                        if (auto *iosb = std::get_if<IoStringBuffer>(
                                &read->large_value_dest_))
                        {
                            handle->large_value =
                                std::make_unique<IoStringBuffer>(
                                    std::move(*iosb));
                            handle->result.value =
                                reinterpret_cast<CIoStringBufferHandle>(
                                    handle->large_value.get());
                            handle->result.value_len =
                                handle->large_value->Size();
                            handle->result.timestamp = read->ts_;
                            handle->result.expire_ts = read->expire_ts_;
                            handle->result.found = true;
                            handle->result.kind = CValueKind_Large;
                            handle->status = CEloqStoreStatus_Ok;
                        }
                        else
                        {
                            handle->result.value = nullptr;
                            handle->result.value_len = read->value_.size();
                            handle->result.timestamp = read->ts_;
                            handle->result.expire_ts = read->expire_ts_;
                            handle->result.found = true;
                            handle->result.kind = CValueKind_Small;
                            handle->status = CEloqStoreStatus_InvalidArgs;
                            handle->error_message =
                                "value is not stored as a zero-copy large "
                                "value";
                        }
                    }
                    else if (err == KvError::NotFound)
                    {
                        handle->result = CLargeValueResult{
                            nullptr, 0, 0, 0, false, CValueKind_NotFound};
                        handle->status = CEloqStoreStatus_Ok;
                    }
                    else
                    {
                        handle->status = kv_error_to_c(err);
                    }
                    handle->done = true;
                    handle->cv.notify_all();
                });
            if (!submitted)
            {
                std::lock_guard<std::mutex> lock(handle->mutex);
                handle->status = CEloqStoreStatus_NotRunning;
                handle->done = true;
                handle->cv.notify_all();
            }
            return reinterpret_cast<CAsyncHandle>(handle);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    CEloqStoreStatus CEloqStore_AsyncGetLargeResult(
        CAsyncHandle handle, CLargeValueResult *out_result)
    {
        clear_last_error();
        if (!handle || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }
        auto *async = reinterpret_cast<AsyncHandleData *>(handle);
        CEloqStoreStatus status = CEloqStore_AsyncWait(handle);
        if (status != CEloqStoreStatus_Ok)
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(async->mutex);
        if (async->kind != AsyncHandleKind::LargeRead || async->result_claimed)
        {
            return CEloqStoreStatus_InvalidArgs;
        }
        *out_result = async->result;
        if (async->large_value)
        {
            out_result->value = reinterpret_cast<CIoStringBufferHandle>(
                async->large_value.release());
            async->result.value = nullptr;
        }
        async->result_claimed = true;
        return CEloqStoreStatus_Ok;
    }

    CEloqStoreStatus CEloqStore_GetPinnedLarge(CEloqStoreHandle store,
                                               CTableIdentHandle table,
                                               const uint8_t *key,
                                               size_t key_len,
                                               uint8_t *out_value,
                                               size_t out_value_size,
                                               CPinnedLargeResult *out_result)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_value ||
            out_value_size == 0 || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            ReadRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            req.large_value_dest_ = std::make_pair(
                reinterpret_cast<char *>(out_value), out_value_size);

            cpp_store->ExecSync(&req);
            auto err = req.Error();

            if (err == KvError::NoError)
            {
                out_result->timestamp = req.ts_;
                out_result->expire_ts = req.expire_ts_;
                out_result->found = true;

                if (!req.value_.empty())
                {
                    auto *copy = new uint8_t[req.value_.size()];
                    std::memcpy(copy, req.value_.data(), req.value_.size());
                    out_result->metadata = copy;
                    out_result->metadata_len = req.value_.size();
                    out_result->owns_metadata = true;
                }
                else
                {
                    out_result->metadata = nullptr;
                    out_result->metadata_len = 0;
                    out_result->owns_metadata = false;
                }
            }
            else if (err == KvError::NotFound)
            {
                out_result->metadata = nullptr;
                out_result->metadata_len = 0;
                out_result->timestamp = 0;
                out_result->expire_ts = 0;
                out_result->found = false;
                out_result->owns_metadata = false;
            }
            else
            {
                return kv_error_to_c(err);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_GetPinnedLargeOnly(
        CEloqStoreHandle store,
        CTableIdentHandle table,
        const uint8_t *key,
        size_t key_len,
        uint8_t *out_value,
        size_t out_value_size,
        CPinnedLargeResult *out_result)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_value ||
            out_value_size == 0 || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            ReadRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            req.large_value_dest_ = std::make_pair(
                reinterpret_cast<char *>(out_value), out_value_size);
            req.large_value_only_ = true;

            cpp_store->ExecSync(&req);
            auto err = req.Error();

            if (err == KvError::NoError)
            {
                out_result->metadata = nullptr;
                out_result->metadata_len = 0;
                out_result->owns_metadata = false;
                out_result->timestamp = req.ts_;
                out_result->expire_ts = req.expire_ts_;
                out_result->found = true;
            }
            else if (err == KvError::NotFound)
            {
                out_result->metadata = nullptr;
                out_result->metadata_len = 0;
                out_result->timestamp = 0;
                out_result->expire_ts = 0;
                out_result->found = false;
                out_result->owns_metadata = false;
            }
            else
            {
                return kv_error_to_c(err);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CAsyncHandle CEloqStore_GetPinnedLargeAsync(CEloqStoreHandle store,
                                                CTableIdentHandle table,
                                                const uint8_t *key,
                                                size_t key_len,
                                                uint8_t *out_value,
                                                size_t out_value_size)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_value ||
            out_value_size == 0)
        {
            return nullptr;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            auto *handle = new AsyncHandleData();
            handle->kind = AsyncHandleKind::PinnedRead;
            handle->pinned_dest = out_value;
            handle->pinned_dest_size = out_value_size;
            handle->read_req = std::make_unique<ReadRequest>();
            handle->read_req->SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            handle->read_req->large_value_dest_ = std::make_pair(
                reinterpret_cast<char *>(out_value), out_value_size);

            bool submitted = cpp_store->ExecAsyn(
                handle->read_req.get(),
                0,
                [handle](eloqstore::KvRequest *done_req)
                {
                    auto *read = static_cast<ReadRequest *>(done_req);
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    auto err = read->Error();
                    if (err == KvError::NoError)
                    {
                        handle->pinned_ts = read->ts_;
                        handle->pinned_expire_ts = read->expire_ts_;
                        if (!read->value_.empty())
                        {
                            handle->pinned_metadata = std::move(read->value_);
                        }
                        handle->status = CEloqStoreStatus_Ok;
                    }
                    else if (err == KvError::NotFound)
                    {
                        handle->status = CEloqStoreStatus_NotFound;
                    }
                    else
                    {
                        handle->status = kv_error_to_c(err);
                    }
                    handle->done = true;
                    handle->cv.notify_all();
                });
            if (!submitted)
            {
                std::lock_guard<std::mutex> lock(handle->mutex);
                handle->status = CEloqStoreStatus_NotRunning;
                handle->done = true;
                handle->cv.notify_all();
            }
            return reinterpret_cast<CAsyncHandle>(handle);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    CAsyncHandle CEloqStore_GetPinnedLargeOnlyAsync(CEloqStoreHandle store,
                                                    CTableIdentHandle table,
                                                    const uint8_t *key,
                                                    size_t key_len,
                                                    uint8_t *out_value,
                                                    size_t out_value_size)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_value ||
            out_value_size == 0)
        {
            return nullptr;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            auto *handle = new AsyncHandleData();
            handle->kind = AsyncHandleKind::PinnedRead;
            handle->pinned_dest = out_value;
            handle->pinned_dest_size = out_value_size;
            handle->read_req = std::make_unique<ReadRequest>();
            handle->read_req->SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            handle->read_req->large_value_dest_ = std::make_pair(
                reinterpret_cast<char *>(out_value), out_value_size);
            handle->read_req->large_value_only_ = true;

            bool submitted = cpp_store->ExecAsyn(
                handle->read_req.get(),
                0,
                [handle](eloqstore::KvRequest *done_req)
                {
                    auto *read = static_cast<ReadRequest *>(done_req);
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    auto err = read->Error();
                    if (err == KvError::NoError)
                    {
                        handle->pinned_ts = read->ts_;
                        handle->pinned_expire_ts = read->expire_ts_;
                        handle->status = CEloqStoreStatus_Ok;
                    }
                    else if (err == KvError::NotFound)
                    {
                        handle->status = CEloqStoreStatus_NotFound;
                    }
                    else
                    {
                        handle->status = kv_error_to_c(err);
                    }
                    handle->done = true;
                    handle->cv.notify_all();
                });
            if (!submitted)
            {
                std::lock_guard<std::mutex> lock(handle->mutex);
                handle->status = CEloqStoreStatus_NotRunning;
                handle->done = true;
                handle->cv.notify_all();
            }
            return reinterpret_cast<CAsyncHandle>(handle);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    CEloqStoreStatus CEloqStore_AsyncGetPinnedResult(
        CAsyncHandle handle, CPinnedLargeResult *out_result)
    {
        clear_last_error();
        if (!handle || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }
        auto *async = reinterpret_cast<AsyncHandleData *>(handle);
        CEloqStoreStatus status = CEloqStore_AsyncWait(handle);
        if (status != CEloqStoreStatus_Ok)
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(async->mutex);
        if (async->kind != AsyncHandleKind::PinnedRead || async->result_claimed)
        {
            return CEloqStoreStatus_InvalidArgs;
        }
        out_result->timestamp = async->pinned_ts;
        out_result->expire_ts = async->pinned_expire_ts;
        if (!async->pinned_metadata.empty())
        {
            auto *copy = new uint8_t[async->pinned_metadata.size()];
            std::memcpy(copy,
                        async->pinned_metadata.data(),
                        async->pinned_metadata.size());
            out_result->metadata = copy;
            out_result->metadata_len = async->pinned_metadata.size();
            out_result->owns_metadata = true;
        }
        else
        {
            out_result->metadata = nullptr;
            out_result->metadata_len = 0;
            out_result->owns_metadata = false;
        }
        out_result->found = true;
        async->result_claimed = true;
        return CEloqStoreStatus_Ok;
    }

    CEloqStoreStatus CEloqStore_PutLarge(CEloqStoreHandle store,
                                         CTableIdentHandle table,
                                         const uint8_t *key,
                                         size_t key_len,
                                         CIoStringBufferHandle value,
                                         CGlobalRegisteredMemoryHandle mem,
                                         uint16_t reg_mem_index_base,
                                         uint64_t timestamp)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !value || !mem)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);
        auto *large_value = reinterpret_cast<IoStringBuffer *>(value);
        auto *global_mem = reinterpret_cast<GlobalRegisteredMemory *>(mem);

        try
        {
            WriteDataEntry entry;
            entry.key_ =
                std::string(reinterpret_cast<const char *>(key), key_len);
            entry.large_val_ = std::move(*large_value);
            entry.timestamp_ = timestamp;
            entry.expire_ts_ = 0;
            entry.op_ = WriteOp::Upsert;

            BatchWriteRequest req;
            std::vector<WriteDataEntry> batch;
            batch.push_back(std::move(entry));
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            auto err = req.Error();
            if (!req.batch_.empty())
            {
                req.batch_[0].RecycleLargeValue(global_mem, reg_mem_index_base);
            }
            return kv_error_to_c(err);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_PutPinnedLarge(CEloqStoreHandle store,
                                               CTableIdentHandle table,
                                               const uint8_t *key,
                                               size_t key_len,
                                               const uint8_t *value,
                                               size_t value_len,
                                               const uint8_t *metadata,
                                               size_t metadata_len,
                                               uint64_t timestamp)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !value ||
            value_len == 0)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            std::string meta_str;
            if (metadata && metadata_len > 0)
            {
                meta_str.assign(reinterpret_cast<const char *>(metadata),
                                metadata_len);
            }

            WriteDataEntry entry(
                std::string(reinterpret_cast<const char *>(key), key_len),
                std::move(meta_str),
                std::make_pair(reinterpret_cast<const char *>(value),
                               value_len),
                timestamp,
                WriteOp::Upsert);

            BatchWriteRequest req;
            std::vector<WriteDataEntry> batch;
            batch.push_back(std::move(entry));
            req.SetArgs(*cpp_table, std::move(batch));

            cpp_store->ExecSync(&req);
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_Exists(CEloqStoreHandle store,
                                       CTableIdentHandle table,
                                       const uint8_t *key,
                                       size_t key_len,
                                       bool *out_exists)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_exists)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            ReadRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));
            cpp_store->ExecSync(&req);
            if (req.Error() == KvError::NoError)
            {
                *out_exists = true;
                return CEloqStoreStatus_Ok;
            }
            if (req.Error() == KvError::NotFound)
            {
                *out_exists = false;
                return CEloqStoreStatus_Ok;
            }
            set_last_error("Exists query failed");
            return kv_error_to_c(req.Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CEloqStoreStatus CEloqStore_Floor(CEloqStoreHandle store,
                                      CTableIdentHandle table,
                                      const uint8_t *key,
                                      size_t key_len,
                                      CFloorResult *out_result)
    {
        clear_last_error();
        if (!store || !table || !key || key_len == 0 || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_table = reinterpret_cast<TableIdent *>(table);

        try
        {
            // Create temporary FloorRequest to execute Floor
            FloorRequest req;
            req.SetArgs(
                *cpp_table,
                std::string(reinterpret_cast<const char *>(key), key_len));

            cpp_store->ExecSync(&req);
            auto err = req.Error();

            if (err == KvError::NoError)
            {
                uint8_t *key_copy = new uint8_t[req.floor_key_.size()];
                std::memcpy(
                    key_copy, req.floor_key_.data(), req.floor_key_.size());
                uint8_t *value_copy = new uint8_t[req.value_.size()];
                std::memcpy(value_copy, req.value_.data(), req.value_.size());
                out_result->key = key_copy;
                out_result->key_len = req.floor_key_.size();
                out_result->value = value_copy;
                out_result->value_len = req.value_.size();
                out_result->timestamp = req.ts_;
                out_result->expire_ts = req.expire_ts_;
                out_result->found = true;
            }
            else if (err == KvError::NotFound)
            {
                out_result->key = nullptr;
                out_result->key_len = 0;
                out_result->value = nullptr;
                out_result->value_len = 0;
                out_result->timestamp = 0;
                out_result->expire_ts = 0;
                out_result->found = false;
            }
            else
            {
                return kv_error_to_c(err);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    // ============================================================
    // Scan request API (complex operations - preserve Request pattern)
    // ============================================================

    CScanRequestHandle CEloqStore_ScanRequest_Create(void)
    {
        clear_last_error();
        try
        {
            return reinterpret_cast<CScanRequestHandle>(new ScanRequest());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_ScanRequest_Destroy(CScanRequestHandle req)
    {
        if (req)
        {
            delete reinterpret_cast<ScanRequest *>(req);
        }
    }

    void CEloqStore_ScanRequest_SetTable(CScanRequestHandle req,
                                         CTableIdentHandle table)
    {
        if (req && table)
        {
            reinterpret_cast<ScanRequest *>(req)->SetArgs(
                *reinterpret_cast<TableIdent *>(table), "", "", true, false);
        }
    }

    void CEloqStore_ScanRequest_SetRange(CScanRequestHandle req,
                                         const uint8_t *begin_key,
                                         size_t begin_key_len,
                                         bool begin_inclusive,
                                         const uint8_t *end_key,
                                         size_t end_key_len,
                                         bool end_inclusive)
    {
        if (req)
        {
            auto *cpp_req = reinterpret_cast<ScanRequest *>(req);
            std::string begin_str, end_str;
            if (begin_key && begin_key_len > 0)
            {
                begin_str.assign(reinterpret_cast<const char *>(begin_key),
                                 begin_key_len);
            }
            if (end_key && end_key_len > 0)
            {
                end_str.assign(reinterpret_cast<const char *>(end_key),
                               end_key_len);
            }
            // Apply range: call SetArgs again with currently set table, write
            // begin/end
            cpp_req->SetArgs(cpp_req->TableId(),
                             begin_str,
                             end_str,
                             begin_inclusive,
                             end_inclusive);
        }
    }

    void CEloqStore_ScanRequest_SetPagination(CScanRequestHandle req,
                                              size_t max_entries,
                                              size_t max_size)
    {
        if (req)
        {
            reinterpret_cast<ScanRequest *>(req)->SetPagination(max_entries,
                                                                max_size);
        }
    }

    void CEloqStore_ScanRequest_SetPrefetch(CScanRequestHandle req,
                                            size_t num_pages)
    {
        if (req)
        {
            reinterpret_cast<ScanRequest *>(req)->SetPrefetchPageNum(num_pages);
        }
    }

    CEloqStoreStatus CEloqStore_ExecScan(CEloqStoreHandle store,
                                         CScanRequestHandle req,
                                         CScanResult *out_result)
    {
        clear_last_error();
        if (!store || !req || !out_result)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_req = reinterpret_cast<ScanRequest *>(req);

        try
        {
            cpp_store->ExecSync(cpp_req);
            auto err = cpp_req->Error();
            if (err != KvError::NoError && err != KvError::NotFound)
            {
                return kv_error_to_c(err);
            }

            const auto &entries = cpp_req->Entries();
            auto [count, size] = cpp_req->ResultSize();

            // Free any previous owned result for this pointer (avoid leak if
            // caller reuses)
            {
                std::lock_guard<std::mutex> lock(g_scan_result_mutex);
                g_owned_scan_results.erase(out_result);
            }
            auto owned = std::make_unique<OwnedScanResult>();
            owned->entries.reserve(entries.size());
            owned->key_value_buffer.reserve(static_cast<size_t>(size) +
                                            entries.size() * 2 *
                                                sizeof(size_t));
            for (const auto &entry : entries)
            {
                const size_t key_len = entry.key_.size();
                const size_t value_len = entry.value_.size();
                const size_t key_off = owned->key_value_buffer.size();
                owned->key_value_buffer.insert(
                    owned->key_value_buffer.end(),
                    reinterpret_cast<const uint8_t *>(entry.key_.data()),
                    reinterpret_cast<const uint8_t *>(entry.key_.data()) +
                        key_len);
                const size_t value_off = owned->key_value_buffer.size();
                owned->key_value_buffer.insert(
                    owned->key_value_buffer.end(),
                    reinterpret_cast<const uint8_t *>(entry.value_.data()),
                    reinterpret_cast<const uint8_t *>(entry.value_.data()) +
                        value_len);
                CScanEntry e;
                e.key = owned->key_value_buffer.data() + key_off;
                e.key_len = key_len;
                e.value = owned->key_value_buffer.data() + value_off;
                e.value_len = value_len;
                e.timestamp = entry.timestamp_;
                e.expire_ts = entry.expire_ts_;
                owned->entries.push_back(e);
            }
            out_result->entries = owned->entries.data();
            out_result->num_entries = owned->entries.size();
            out_result->total_size = size;
            out_result->has_more = cpp_req->HasRemaining();
            {
                std::lock_guard<std::mutex> lock(g_scan_result_mutex);
                g_owned_scan_results[out_result] = std::move(owned);
            }
            return CEloqStoreStatus_Ok;
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    void CEloqStore_FreeScanResult(CScanResult *result)
    {
        if (result)
        {
            std::lock_guard<std::mutex> lock(g_scan_result_mutex);
            g_owned_scan_results.erase(result);
            result->entries = nullptr;
            result->num_entries = 0;
            result->total_size = 0;
            result->has_more = false;
        }
    }

    // ============================================================
    // BatchWrite request API (complex operations - preserve Request pattern)
    // ============================================================

    CBatchWriteHandle CEloqStore_BatchWrite_Create(void)
    {
        clear_last_error();
        try
        {
            return reinterpret_cast<CBatchWriteHandle>(new BatchWriteRequest());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_BatchWrite_Destroy(CBatchWriteHandle req)
    {
        if (req)
        {
            delete reinterpret_cast<BatchWriteRequest *>(req);
        }
    }

    void CEloqStore_BatchWrite_SetTable(CBatchWriteHandle req,
                                        CTableIdentHandle table)
    {
        if (req && table)
        {
            std::vector<WriteDataEntry> batch;
            reinterpret_cast<BatchWriteRequest *>(req)->SetArgs(
                *reinterpret_cast<TableIdent *>(table), std::move(batch));
        }
    }

    void CEloqStore_BatchWrite_AddEntry(CBatchWriteHandle req,
                                        const uint8_t *key,
                                        size_t key_len,
                                        const uint8_t *value,
                                        size_t value_len,
                                        uint64_t timestamp,
                                        CWriteOp op,
                                        uint64_t expire_ts)
    {
        if (req && key && key_len > 0)
        {
            auto *cpp_req = reinterpret_cast<BatchWriteRequest *>(req);
            WriteDataEntry entry;
            entry.key_ =
                std::string(reinterpret_cast<const char *>(key), key_len);
            if (value && value_len > 0)
            {
                entry.val_ = std::string(reinterpret_cast<const char *>(value),
                                         value_len);
            }
            entry.timestamp_ = timestamp;
            entry.op_ = static_cast<WriteOp>(op);
            entry.expire_ts_ = expire_ts;
            cpp_req->batch_.push_back(std::move(entry));
        }
    }

    void CEloqStore_BatchWrite_AddLargeEntry(CBatchWriteHandle req,
                                             const uint8_t *key,
                                             size_t key_len,
                                             CIoStringBufferHandle value,
                                             uint64_t timestamp,
                                             CWriteOp op,
                                             uint64_t expire_ts)
    {
        if (req && key && key_len > 0 && value)
        {
            auto *cpp_req = reinterpret_cast<BatchWriteRequest *>(req);
            auto *large_value = reinterpret_cast<IoStringBuffer *>(value);
            WriteDataEntry entry;
            entry.key_ =
                std::string(reinterpret_cast<const char *>(key), key_len);
            entry.large_val_ = std::move(*large_value);
            entry.timestamp_ = timestamp;
            entry.op_ = static_cast<WriteOp>(op);
            entry.expire_ts_ = expire_ts;
            cpp_req->batch_.push_back(std::move(entry));
        }
    }

    void CEloqStore_BatchWrite_AddPinnedLargeEntry(CBatchWriteHandle req,
                                                   const uint8_t *key,
                                                   size_t key_len,
                                                   const uint8_t *value,
                                                   size_t value_len,
                                                   const uint8_t *metadata,
                                                   size_t metadata_len,
                                                   uint64_t timestamp,
                                                   CWriteOp op,
                                                   uint64_t expire_ts)
    {
        if (req && key && key_len > 0 && value && value_len > 0)
        {
            auto *cpp_req = reinterpret_cast<BatchWriteRequest *>(req);
            std::string meta_str;
            if (metadata && metadata_len > 0)
            {
                meta_str.assign(reinterpret_cast<const char *>(metadata),
                                metadata_len);
            }
            WriteDataEntry entry(
                std::string(reinterpret_cast<const char *>(key), key_len),
                std::move(meta_str),
                std::make_pair(reinterpret_cast<const char *>(value),
                               value_len),
                timestamp,
                static_cast<WriteOp>(op),
                expire_ts);
            cpp_req->batch_.push_back(std::move(entry));
        }
    }

    void CEloqStore_BatchWrite_Clear(CBatchWriteHandle req)
    {
        if (req)
            reinterpret_cast<BatchWriteRequest *>(req)->Clear();
    }

    void CEloqStore_BatchWrite_RecycleLargeEntries(
        CBatchWriteHandle req,
        CGlobalRegisteredMemoryHandle mem,
        uint16_t reg_mem_index_base)
    {
        if (!req || !mem)
            return;
        auto *cpp_req = reinterpret_cast<BatchWriteRequest *>(req);
        auto *global_mem = reinterpret_cast<GlobalRegisteredMemory *>(mem);
        recycle_large_entries(cpp_req, global_mem, reg_mem_index_base);
    }

    CEloqStoreStatus CEloqStore_ExecBatchWrite(CEloqStoreHandle store,
                                               CBatchWriteHandle req)
    {
        clear_last_error();
        if (!store || !req)
        {
            return CEloqStoreStatus_InvalidArgs;
        }

        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_req = reinterpret_cast<BatchWriteRequest *>(req);

        try
        {
            cpp_store->ExecSync(cpp_req);
            return kv_error_to_c(cpp_req->Error());
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return CEloqStoreStatus_InvalidArgs;
        }
    }

    CAsyncHandle CEloqStore_ExecBatchWriteAsync(
        CEloqStoreHandle store,
        CBatchWriteHandle req,
        CGlobalRegisteredMemoryHandle mem,
        uint16_t reg_mem_index_base)
    {
        clear_last_error();
        if (!store || !req)
        {
            return nullptr;
        }
        auto *cpp_store = reinterpret_cast<EloqStore *>(store);
        auto *cpp_req = reinterpret_cast<BatchWriteRequest *>(req);
        auto *handle = new AsyncHandleData();
        handle->kind = AsyncHandleKind::BatchWrite;
        handle->req.reset(cpp_req);
        handle->mem = reinterpret_cast<GlobalRegisteredMemory *>(mem);
        handle->reg_mem_index_base = reg_mem_index_base;
        bool submitted = cpp_store->ExecAsyn(
            handle->req.get(),
            0,
            [handle](eloqstore::KvRequest *done_req)
            {
                auto *batch = static_cast<BatchWriteRequest *>(done_req);
                recycle_large_entries(
                    batch, handle->mem, handle->reg_mem_index_base);
                {
                    std::lock_guard<std::mutex> lock(handle->mutex);
                    handle->status = kv_error_to_c(done_req->Error());
                    handle->done = true;
                }
                handle->cv.notify_all();
            });
        if (!submitted)
        {
            recycle_large_entries(
                handle->req.get(), handle->mem, handle->reg_mem_index_base);
            {
                std::lock_guard<std::mutex> lock(handle->mutex);
                handle->status = CEloqStoreStatus_NotRunning;
                handle->done = true;
            }
            handle->cv.notify_all();
        }
        return reinterpret_cast<CAsyncHandle>(handle);
    }

    bool CEloqStore_AsyncIsDone(CAsyncHandle handle)
    {
        if (!handle)
            return true;
        auto *async = reinterpret_cast<AsyncHandleData *>(handle);
        std::lock_guard<std::mutex> lock(async->mutex);
        return async->done;
    }

    CEloqStoreStatus CEloqStore_AsyncWait(CAsyncHandle handle)
    {
        if (!handle)
            return CEloqStoreStatus_InvalidArgs;
        auto *async = reinterpret_cast<AsyncHandleData *>(handle);
        std::unique_lock<std::mutex> lock(async->mutex);
        async->cv.wait(lock, [async]() { return async->done; });
        if (!async->error_message.empty())
        {
            set_last_error(async->error_message);
        }
        return async->status;
    }

    CEloqStoreStatus CEloqStore_AsyncStatus(CAsyncHandle handle)
    {
        if (!handle)
            return CEloqStoreStatus_InvalidArgs;
        auto *async = reinterpret_cast<AsyncHandleData *>(handle);
        std::lock_guard<std::mutex> lock(async->mutex);
        if (!async->error_message.empty())
        {
            set_last_error(async->error_message);
        }
        return async->status;
    }

    void CEloqStore_AsyncDestroy(CAsyncHandle handle)
    {
        if (!handle)
            return;
        auto *async = reinterpret_cast<AsyncHandleData *>(handle);
        CEloqStore_AsyncWait(handle);
        if (async->kind == AsyncHandleKind::LargeRead && async->large_value &&
            async->mem)
        {
            async->large_value->Recycle(async->mem, async->reg_mem_index_base);
        }
        delete async;
    }

    // ============================================================
    // Zero-copy registered memory and large-value buffers
    // ============================================================

    CGlobalRegisteredMemoryHandle CEloqStore_GlobalMemory_Create(
        uint32_t segment_size, uint64_t chunk_size, uint64_t total_size)
    {
        clear_last_error();
        try
        {
            auto *mem =
                new GlobalRegisteredMemory(segment_size,
                                           static_cast<size_t>(chunk_size),
                                           static_cast<size_t>(total_size));
            return reinterpret_cast<CGlobalRegisteredMemoryHandle>(mem);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_GlobalMemory_Destroy(CGlobalRegisteredMemoryHandle mem)
    {
        delete reinterpret_cast<GlobalRegisteredMemory *>(mem);
    }

    uint32_t CEloqStore_GlobalMemory_SegmentSize(
        CGlobalRegisteredMemoryHandle mem)
    {
        return mem ? reinterpret_cast<GlobalRegisteredMemory *>(mem)
                         ->SegmentSize()
                   : 0;
    }

    size_t CEloqStore_GlobalMemory_TotalSegments(
        CGlobalRegisteredMemoryHandle mem)
    {
        return mem ? reinterpret_cast<GlobalRegisteredMemory *>(mem)
                         ->TotalSegments()
                   : 0;
    }

    size_t CEloqStore_GlobalMemory_FreeSegments(
        CGlobalRegisteredMemoryHandle mem)
    {
        return mem ? reinterpret_cast<GlobalRegisteredMemory *>(mem)
                         ->FreeSegments()
                   : 0;
    }

    size_t CEloqStore_GlobalMemory_ChunkCount(CGlobalRegisteredMemoryHandle mem)
    {
        if (!mem)
            return 0;
        return reinterpret_cast<GlobalRegisteredMemory *>(mem)
            ->MemChunks()
            .size();
    }

    bool CEloqStore_GlobalMemory_ChunkAt(CGlobalRegisteredMemoryHandle mem,
                                         size_t index,
                                         CMemoryChunk *out_chunk)
    {
        if (!mem || !out_chunk)
            return false;
        const auto chunks =
            reinterpret_cast<GlobalRegisteredMemory *>(mem)->MemChunks();
        if (index >= chunks.size())
            return false;
        out_chunk->data = reinterpret_cast<uint8_t *>(chunks[index].base_);
        out_chunk->len = chunks[index].size_;
        return true;
    }

    CIoStringBufferHandle CEloqStore_GlobalMemory_AllocateIoString(
        CGlobalRegisteredMemoryHandle mem,
        size_t size,
        uint16_t reg_mem_index_base)
    {
        clear_last_error();
        if (!mem)
        {
            set_last_error("registered memory is null");
            return nullptr;
        }
        try
        {
            auto *global_mem = reinterpret_cast<GlobalRegisteredMemory *>(mem);
            auto *buf = new IoStringBuffer();
            const size_t segment_size = global_mem->SegmentSize();
            const size_t segments =
                size == 0 ? 0 : (size + segment_size - 1) / segment_size;
            for (size_t i = 0; i < segments; ++i)
            {
                auto [ptr, chunk_index] = global_mem->GetSegment([]() {});
                buf->Append(
                    {ptr,
                     static_cast<uint16_t>(reg_mem_index_base + chunk_index)});
            }
            buf->SetSize(size);
            return reinterpret_cast<CIoStringBufferHandle>(buf);
        }
        catch (const std::exception &e)
        {
            set_last_error(e.what());
            return nullptr;
        }
    }

    void CEloqStore_IoStringBuffer_Destroy(CIoStringBufferHandle buf)
    {
        delete reinterpret_cast<IoStringBuffer *>(buf);
    }

    void CEloqStore_IoStringBuffer_Recycle(CIoStringBufferHandle buf,
                                           CGlobalRegisteredMemoryHandle mem,
                                           uint16_t reg_mem_index_base)
    {
        if (!buf || !mem)
            return;
        reinterpret_cast<IoStringBuffer *>(buf)->Recycle(
            reinterpret_cast<GlobalRegisteredMemory *>(mem),
            reg_mem_index_base);
    }

    size_t CEloqStore_IoStringBuffer_Size(CIoStringBufferHandle buf)
    {
        return buf ? reinterpret_cast<IoStringBuffer *>(buf)->Size() : 0;
    }

    size_t CEloqStore_IoStringBuffer_FragmentCount(CIoStringBufferHandle buf)
    {
        return buf ? reinterpret_cast<IoStringBuffer *>(buf)->Fragments().size()
                   : 0;
    }

    static bool fill_io_string_fragment(CIoStringBufferHandle buf,
                                        size_t index,
                                        size_t segment_size,
                                        uint16_t reg_mem_index_base,
                                        CIoStringFragment *out_fragment)
    {
        if (!buf || !out_fragment || segment_size == 0)
            return false;
        auto *cpp_buf = reinterpret_cast<IoStringBuffer *>(buf);
        const auto &fragments = cpp_buf->Fragments();
        if (index >= fragments.size())
            return false;

        const size_t used_before = index * segment_size;
        if (used_before >= cpp_buf->Size())
            return false;

        const auto &frag = fragments[index];
        const size_t len =
            std::min(segment_size, cpp_buf->Size() - used_before);
        out_fragment->data = reinterpret_cast<uint8_t *>(frag.data_);
        out_fragment->len = len;
        out_fragment->buf_index = frag.buf_index_;
        out_fragment->chunk_index = frag.buf_index_ >= reg_mem_index_base
                                        ? frag.buf_index_ - reg_mem_index_base
                                        : frag.buf_index_;
        out_fragment->offset = 0;
        return true;
    }

    bool CEloqStore_IoStringBuffer_FragmentAt(CIoStringBufferHandle buf,
                                              size_t index,
                                              CIoStringFragment *out_fragment)
    {
        if (!buf || !out_fragment)
            return false;
        auto *cpp_buf = reinterpret_cast<IoStringBuffer *>(buf);
        const auto &fragments = cpp_buf->Fragments();
        if (index >= fragments.size())
            return false;
        if (cpp_buf->Size() == 0)
            return false;

        size_t segment_size = 0;
        for (size_t i = 1; i < fragments.size(); ++i)
        {
            if (fragments[i].data_ > fragments[i - 1].data_)
            {
                const auto delta = static_cast<size_t>(fragments[i].data_ -
                                                       fragments[i - 1].data_);
                if (delta > 0 && (segment_size == 0 || delta < segment_size))
                {
                    segment_size = delta;
                }
            }
        }
        if (segment_size == 0 || segment_size > cpp_buf->Size())
        {
            segment_size = cpp_buf->Size();
        }
        return fill_io_string_fragment(
            buf, index, segment_size, 0, out_fragment);
    }

    bool CEloqStore_IoStringBuffer_FragmentAtEx(CIoStringBufferHandle buf,
                                                size_t index,
                                                uint32_t segment_size,
                                                uint16_t reg_mem_index_base,
                                                CIoStringFragment *out_fragment)
    {
        return fill_io_string_fragment(
            buf, index, segment_size, reg_mem_index_base, out_fragment);
    }

    // ============================================================
    // Error message query
    // ============================================================

    const char *CEloqStore_GetLastError(CEloqStoreHandle store)
    {
        return g_last_error_message.empty() ? nullptr
                                            : g_last_error_message.c_str();
    }

    // ============================================================
    // Memory free functions (for Get/Floor results)
    // ============================================================

    void CEloqStore_FreeGetResult(CGetResult *result)
    {
        if (result)
        {
            if (result->value && result->owns_value)
            {
                delete[] result->value;
                result->value = nullptr;
            }
            result->value_len = 0;
            result->owns_value = false;
            result->found = false;
        }
    }

    void CEloqStore_FreeFloorResult(CFloorResult *result)
    {
        if (result)
        {
            if (result->key)
            {
                delete[] result->key;
                result->key = nullptr;
            }
            if (result->value)
            {
                delete[] result->value;
                result->value = nullptr;
            }
            result->key_len = 0;
            result->value_len = 0;
            result->found = false;
        }
    }

    void CEloqStore_FreePinnedResult(CPinnedLargeResult *result)
    {
        if (result)
        {
            if (result->metadata && result->owns_metadata)
            {
                delete[] result->metadata;
                result->metadata = nullptr;
            }
            result->metadata_len = 0;
            result->owns_metadata = false;
            result->found = false;
        }
    }
}  // extern "C"
