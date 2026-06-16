#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    // ============================================================
    // Error code definitions
    // ============================================================
    typedef enum CEloqStoreStatus
    {
        CEloqStoreStatus_Ok = 0,
        CEloqStoreStatus_InvalidArgs,
        CEloqStoreStatus_NotFound,
        CEloqStoreStatus_NotRunning,
        CEloqStoreStatus_Corrupted,
        CEloqStoreStatus_EndOfFile,
        CEloqStoreStatus_OutOfSpace,
        CEloqStoreStatus_OutOfMem,
        CEloqStoreStatus_OpenFileLimit,
        CEloqStoreStatus_TryAgain,
        CEloqStoreStatus_Busy,
        CEloqStoreStatus_Timeout,
        CEloqStoreStatus_NoPermission,
        CEloqStoreStatus_CloudErr,
        CEloqStoreStatus_IoFail,
        CEloqStoreStatus_ExpiredTerm,
        CEloqStoreStatus_OssInsufficientStorage,
        CEloqStoreStatus_AlreadyExists,
    } CEloqStoreStatus;

    // ============================================================
    // Opaque handle types
    // ============================================================
    typedef void *CEloqStoreHandle;
    typedef void *CTableIdentHandle;
    typedef void *CScanRequestHandle;
    typedef void *CBatchWriteHandle;
    typedef void *CGlobalRegisteredMemoryHandle;
    typedef void *CIoStringBufferHandle;
    typedef void *CAsyncHandle;
    typedef void *CKVCacheOptionsHandle;
    typedef void *CKVCacheManagerHandle;
    typedef void *CKVCacheWorkerHandle;

    typedef enum CValueKind
    {
        CValueKind_NotFound = 0,
        CValueKind_Small = 1,
        CValueKind_Large = 2,
    } CValueKind;

    // ============================================================
    // Write operation enum
    // ============================================================
    typedef enum CWriteOp
    {
        CWriteOp_Upsert = 0,
        CWriteOp_Delete = 1,
    } CWriteOp;

    // ============================================================
    // Input structures
    // ============================================================
    typedef struct CKvEntry
    {
        const uint8_t *key;
        size_t key_len;
        const uint8_t *value;
        size_t value_len;
        uint64_t timestamp;
        uint64_t expire_ts;
    } CKvEntry;

    typedef struct CWriteEntry
    {
        const uint8_t *key;
        size_t key_len;
        const uint8_t *value;
        size_t value_len;
        uint64_t timestamp;
        CWriteOp op;
        uint64_t expire_ts;
    } CWriteEntry;

    // ============================================================
    // Output structures
    // ============================================================

    // Get operation result
    typedef struct CGetResult
    {
        const uint8_t *value;
        size_t value_len;
        uint64_t timestamp;
        uint64_t expire_ts;
        bool found;
        bool owns_value;
    } CGetResult;

    typedef struct CLargeValueResult
    {
        CIoStringBufferHandle value;
        size_t value_len;
        uint64_t timestamp;
        uint64_t expire_ts;
        bool found;
        CValueKind kind;
    } CLargeValueResult;

    typedef struct CPinnedLargeResult
    {
        const uint8_t *metadata;
        size_t metadata_len;
        uint64_t timestamp;
        uint64_t expire_ts;
        bool found;
        bool owns_metadata;
    } CPinnedLargeResult;

    typedef struct CIoStringFragment
    {
        uint8_t *data;
        size_t len;
        uint16_t buf_index;
        uint32_t chunk_index;
        size_t offset;
    } CIoStringFragment;

    typedef struct CMemoryChunk
    {
        uint8_t *data;
        size_t len;
    } CMemoryChunk;

    typedef enum CKVCacheRequestStatus
    {
        CKVCacheRequestStatus_Pending = 1,
        CKVCacheRequestStatus_Ready = 2,
        CKVCacheRequestStatus_Failed = 3,
    } CKVCacheRequestStatus;

    typedef struct CKVCacheBufferHandle
    {
        uint64_t request_id;
        uint64_t offset_bytes;
        uint32_t payload_bytes;
    } CKVCacheBufferHandle;

    typedef struct CKVCacheRequestState
    {
        uint64_t request_id;
        uint8_t status;
        uint64_t offset_bytes;
        uint32_t payload_bytes;
    } CKVCacheRequestState;

    typedef struct CKVCacheRuntimeMetrics
    {
        uint64_t flush_batches_submitted;
        uint64_t flush_batches_completed;
        uint64_t flush_batches_failed;
        uint64_t flush_entries_submitted;
        uint64_t flush_entries_completed;
        uint64_t flush_entries_failed;
        uint64_t flush_batch_latency_ns_total;
        uint64_t contains_memory_hits;
        uint64_t contains_store_hits;
        uint64_t contains_store_misses;
        uint64_t contains_store_errors;
        uint64_t contains_store_lookup_ns_total;
        uint64_t load_memory_hits;
        uint64_t load_store_hits;
        uint64_t load_store_errors;
        uint64_t load_store_bytes;
        uint64_t load_store_latency_ns_total;
        uint64_t dirty_entries_current;
        uint64_t flush_queue_entries_current;
        uint64_t flush_inflight_entries_current;
    } CKVCacheRuntimeMetrics;

    // Floor operation result
    typedef struct CFloorResult
    {
        const uint8_t *key;
        size_t key_len;
        const uint8_t *value;
        size_t value_len;
        uint64_t timestamp;
        uint64_t expire_ts;
        bool found;
    } CFloorResult;

    // Scan result entries
    typedef struct CScanEntry
    {
        const uint8_t *key;
        size_t key_len;
        const uint8_t *value;
        size_t value_len;
        uint64_t timestamp;
        uint64_t expire_ts;
    } CScanEntry;

    // Scan operation result
    typedef struct CScanResult
    {
        CScanEntry *entries;
        size_t num_entries;
        size_t total_size;
        bool has_more;
    } CScanResult;

    // ============================================================
    // Options API
    // ============================================================

    CEloqStoreHandle CEloqStore_Options_Create(void);
    void CEloqStore_Options_Destroy(CEloqStoreHandle opts);

    void CEloqStore_Options_SetNumThreads(CEloqStoreHandle opts, uint16_t n);
    void CEloqStore_Options_SetBufferPoolSize(CEloqStoreHandle opts,
                                              uint64_t size);
    void CEloqStore_Options_SetDataPageSize(CEloqStoreHandle opts,
                                            uint16_t size);
    void CEloqStore_Options_SetManifestLimit(CEloqStoreHandle opts,
                                             uint32_t limit);
    void CEloqStore_Options_SetFdLimit(CEloqStoreHandle opts, uint32_t limit);
    void CEloqStore_Options_SetPagesPerFileShift(CEloqStoreHandle opts,
                                                 uint8_t shift);
    void CEloqStore_Options_SetOverflowPointers(CEloqStoreHandle opts,
                                                uint8_t n);
    void CEloqStore_Options_SetDataAppendMode(CEloqStoreHandle opts,
                                              bool enable);
    void CEloqStore_Options_SetEnableCompression(CEloqStoreHandle opts,
                                                 bool enable);
    void CEloqStore_Options_SetSegmentSize(CEloqStoreHandle opts,
                                           uint32_t size);
    void CEloqStore_Options_SetRegisteredMemoryChunkSize(CEloqStoreHandle opts,
                                                         uint64_t size);
    void CEloqStore_Options_SetSegmentsPerFileShift(CEloqStoreHandle opts,
                                                    uint8_t shift);
    void CEloqStore_Options_SetGlobalRegisteredMemory(
        CEloqStoreHandle opts,
        uint32_t shard_id,
        CGlobalRegisteredMemoryHandle mem);
    void CEloqStore_Options_AddPinnedMemoryChunk(CEloqStoreHandle opts,
                                                 const char *data,
                                                 size_t size);
    void CEloqStore_Options_SetGcGlobalMemSizePerShard(CEloqStoreHandle opts,
                                                       uint64_t size);
    void CEloqStore_Options_SetPinnedTailScratchSlots(CEloqStoreHandle opts,
                                                      uint16_t slots);

    void CEloqStore_Options_AddStorePath(CEloqStoreHandle opts,
                                         const char *path);
    void CEloqStore_Options_SetCloudStorePath(CEloqStoreHandle opts,
                                              const char *path);
    void CEloqStore_Options_SetCloudProvider(CEloqStoreHandle opts,
                                             const char *provider);
    void CEloqStore_Options_SetCloudRegion(CEloqStoreHandle opts,
                                           const char *region);
    void CEloqStore_Options_SetCloudCredentials(CEloqStoreHandle opts,
                                                const char *access_key,
                                                const char *secret_key);
    void CEloqStore_Options_SetCloudAutoCredentials(CEloqStoreHandle opts,
                                                    bool enable);
    void CEloqStore_Options_SetCloudVerifySsl(CEloqStoreHandle opts,
                                              bool verify);
    bool CEloqStore_Options_LoadFromIni(CEloqStoreHandle opts,
                                        const char *path);

    bool CEloqStore_Options_Validate(CEloqStoreHandle opts);

    // ============================================================
    // SDK runtime API
    // ============================================================

    CKVCacheOptionsHandle CEloqStore_KVCacheOptions_Create(void);
    void CEloqStore_KVCacheOptions_Destroy(CKVCacheOptionsHandle opts);
    void CEloqStore_KVCacheOptions_AddStorePath(CKVCacheOptionsHandle opts,
                                                const char *path);
    void CEloqStore_KVCacheOptions_SetTableName(CKVCacheOptionsHandle opts,
                                                const char *table_name);
    void CEloqStore_KVCacheOptions_SetBranch(CKVCacheOptionsHandle opts,
                                             const char *branch);
    void CEloqStore_KVCacheOptions_SetIpcPath(CKVCacheOptionsHandle opts,
                                              const char *ipc_path);
    void CEloqStore_KVCacheOptions_SetSharedMemoryName(
        CKVCacheOptionsHandle opts, const char *name);
    void CEloqStore_KVCacheOptions_SetNumThreads(CKVCacheOptionsHandle opts,
                                                 uint16_t n);
    void CEloqStore_KVCacheOptions_SetPartitionCount(CKVCacheOptionsHandle opts,
                                                     uint32_t n);
    void CEloqStore_KVCacheOptions_SetTerm(CKVCacheOptionsHandle opts,
                                           uint64_t term);
    void CEloqStore_KVCacheOptions_SetPartitionGroupId(
        CKVCacheOptionsHandle opts, uint32_t partition_group_id);
    void CEloqStore_KVCacheOptions_SetSharedMemoryBytes(
        CKVCacheOptionsHandle opts, uint64_t bytes);
    void CEloqStore_KVCacheOptions_SetEntrySize(CKVCacheOptionsHandle opts,
                                                uint32_t entry_size);
    void CEloqStore_KVCacheOptions_SetEntryCount(CKVCacheOptionsHandle opts,
                                                 uint32_t entry_count);
    void CEloqStore_KVCacheOptions_SetEntryAlignment(CKVCacheOptionsHandle opts,
                                                     uint32_t entry_alignment);
    void CEloqStore_KVCacheOptions_SetSubmissionQueueDepth(
        CKVCacheOptionsHandle opts, uint32_t depth);
    void CEloqStore_KVCacheOptions_SetEagerIoUringRegister(
        CKVCacheOptionsHandle opts, bool enable);

    // Create one engine-core-side KV cache manager runtime from native options.
    CKVCacheManagerHandle CEloqStore_KVCacheManager_Create(
        CKVCacheOptionsHandle opts);
    // Destroy a manager runtime previously created by the C API.
    void CEloqStore_KVCacheManager_Destroy(CKVCacheManagerHandle runtime);
    // Start the manager runtime: allocate shared memory, initialize groups, and
    // bring up the optional IPC listener.
    bool CEloqStore_KVCacheManager_Start(CKVCacheManagerHandle runtime);
    // Stop the manager runtime and release its resources.
    void CEloqStore_KVCacheManager_Stop(CKVCacheManagerHandle runtime);
    // Register the manager-owned shared-memory region with native I/O paths.
    bool CEloqStore_KVCacheManager_RegisterIoUringBuffers(
        CKVCacheManagerHandle runtime);
    // Export the manager-owned buffer pool as a descriptor string for workers.
    const char *CEloqStore_KVCacheManager_ExportBufferPool(
        CKVCacheManagerHandle runtime);
    // Begin one asynchronous save and return a writable shared-buffer slice.
    bool CEloqStore_KVCacheManager_BeginSave(CKVCacheManagerHandle runtime,
                                             const char *key,
                                             uint32_t payload_bytes,
                                             CKVCacheBufferHandle *out_buffer);
    // Finish one previously started save after the worker has filled the
    // buffer.
    bool CEloqStore_KVCacheManager_FinishSave(CKVCacheManagerHandle runtime,
                                              uint64_t request_id);
    // Begin one asynchronous load and return its request id.
    bool CEloqStore_KVCacheManager_BeginLoad(CKVCacheManagerHandle runtime,
                                             const char *key,
                                             uint32_t payload_bytes,
                                             uint64_t *out_request_id);
    // Query one request state directly.
    bool CEloqStore_KVCacheManager_CheckRequest(
        CKVCacheManagerHandle runtime,
        uint64_t request_id,
        CKVCacheRequestState *out_state);
    // Return the ready shared-buffer slice for one completed request.
    bool CEloqStore_KVCacheManager_GetReadyBuffer(
        CKVCacheManagerHandle runtime,
        uint64_t request_id,
        CKVCacheBufferHandle *out_buffer);
    // Probe whether one key exists through the manager runtime.
    bool CEloqStore_KVCacheManager_ContainsKey(CKVCacheManagerHandle runtime,
                                               const char *key,
                                               bool *out_exists);

    // Create one worker-side KV cache control-plane stub from native options.
    CKVCacheWorkerHandle CEloqStore_KVCacheWorker_Create(
        CKVCacheOptionsHandle opts);
    // Destroy a worker runtime previously created by the C API.
    void CEloqStore_KVCacheWorker_Destroy(CKVCacheWorkerHandle runtime);
    // Load one manager-exported buffer-pool descriptor into the worker stub.
    bool CEloqStore_KVCacheWorker_AttachBufferPool(CKVCacheWorkerHandle runtime,
                                                   const char *descriptor);
    // Drop the currently attached descriptor from the worker stub.
    void CEloqStore_KVCacheWorker_DetachBufferPool(
        CKVCacheWorkerHandle runtime);
    // Begin one asynchronous save and return a writable shared-buffer slice.
    bool CEloqStore_KVCacheWorker_BeginSave(CKVCacheWorkerHandle runtime,
                                            const char *key,
                                            uint32_t payload_bytes,
                                            CKVCacheBufferHandle *out_buffer);
    // Finish one previously started save after the worker has filled the
    // buffer.
    bool CEloqStore_KVCacheWorker_FinishSave(CKVCacheWorkerHandle runtime,
                                             uint64_t request_id);
    // Begin one asynchronous load and return its request id.
    bool CEloqStore_KVCacheWorker_BeginLoad(CKVCacheWorkerHandle runtime,
                                            const char *key,
                                            uint32_t payload_bytes,
                                            uint64_t *out_request_id);
    // Query one request state directly.
    bool CEloqStore_KVCacheWorker_CheckRequest(CKVCacheWorkerHandle runtime,
                                               uint64_t request_id,
                                               CKVCacheRequestState *out_state);
    // Wait for a batch of requests and return their states in order.
    // Return the ready shared-buffer slice for one completed request.
    bool CEloqStore_KVCacheWorker_GetReadyBuffer(
        CKVCacheWorkerHandle runtime,
        uint64_t request_id,
        CKVCacheBufferHandle *out_buffer);
    // Free C strings returned by APIs such as ExportBufferPool.
    void CEloqStore_FreeCString(const char *value);

    // ============================================================
    // Engine lifecycle
    // ============================================================

    CEloqStoreHandle CEloqStore_Create(CEloqStoreHandle options);
    void CEloqStore_Destroy(CEloqStoreHandle store);

    CEloqStoreStatus CEloqStore_Start(CEloqStoreHandle store);
    CEloqStoreStatus CEloqStore_StartWithBranch(CEloqStoreHandle store,
                                                const char *branch,
                                                uint64_t term,
                                                uint32_t partition_group_id);
    void CEloqStore_Stop(CEloqStoreHandle store);
    bool CEloqStore_IsStopped(CEloqStoreHandle store);
    uint16_t CEloqStore_GlobalRegMemIndexBase(CEloqStoreHandle store,
                                              size_t shard_id);

    // ============================================================
    // Table identifier
    // ============================================================

    CTableIdentHandle CEloqStore_TableIdent_Create(const char *table_name,
                                                   uint32_t partition_id);
    void CEloqStore_TableIdent_Destroy(CTableIdentHandle ident);
    /**
     * Get the table name from a TableIdent handle.
     *
     * @param ident The TableIdent handle
     * @return A pointer to the table name string, or nullptr if ident is null.
     *
     * @note The returned pointer is valid as long as the TableIdent object
     * exists and is not modified. Callers should copy the string immediately if
     * they need to keep it beyond the lifetime of the TableIdent object or if
     * they may call GetName again on a different TableIdent (which would
     * invalidate the previous pointer if not copied).
     */
    const char *CEloqStore_TableIdent_GetName(CTableIdentHandle ident);
    uint32_t CEloqStore_TableIdent_GetPartition(CTableIdentHandle ident);

    // ============================================================
    // Flattened write API (simple operations)
    // ============================================================

    // Single Put operation (implemented using BatchWrite)
    CEloqStoreStatus CEloqStore_Put(CEloqStoreHandle store,
                                    CTableIdentHandle table,
                                    const uint8_t *key,
                                    size_t key_len,
                                    const uint8_t *value,
                                    size_t value_len,
                                    uint64_t timestamp);

    // Batch Put operation - write multiple key-value pairs at once
    // keys: key pointer array
    // values: value pointer array
    // count: element count
    CEloqStoreStatus CEloqStore_PutBatch(CEloqStoreHandle store,
                                         CTableIdentHandle table,
                                         const uint8_t *const *keys,
                                         const size_t *key_lens,
                                         const uint8_t *const *values,
                                         const size_t *value_lens,
                                         size_t count,
                                         uint64_t timestamp);

    // Batch Put operation (using CWriteEntry array)
    CEloqStoreStatus CEloqStore_PutEntries(CEloqStoreHandle store,
                                           CTableIdentHandle table,
                                           const CWriteEntry *entries,
                                           size_t count);

    // Single Delete operation (implemented using BatchWrite)
    CEloqStoreStatus CEloqStore_Delete(CEloqStoreHandle store,
                                       CTableIdentHandle table,
                                       const uint8_t *key,
                                       size_t key_len,
                                       uint64_t timestamp);

    // Batch Delete operation - delete multiple keys at once
    CEloqStoreStatus CEloqStore_DeleteBatch(CEloqStoreHandle store,
                                            CTableIdentHandle table,
                                            const uint8_t *const *keys,
                                            const size_t *key_lens,
                                            size_t count,
                                            uint64_t timestamp);

    // ============================================================
    // Flattened read API (simple operations)
    // ============================================================

    CEloqStoreStatus CEloqStore_Get(CEloqStoreHandle store,
                                    CTableIdentHandle table,
                                    const uint8_t *key,
                                    size_t key_len,
                                    CGetResult *out_result);
    CEloqStoreStatus CEloqStore_GetInto(CEloqStoreHandle store,
                                        CTableIdentHandle table,
                                        const uint8_t *key,
                                        size_t key_len,
                                        uint8_t *out_value,
                                        size_t out_capacity,
                                        CGetResult *out_result);
    CEloqStoreStatus CEloqStore_GetLarge(CEloqStoreHandle store,
                                         CTableIdentHandle table,
                                         const uint8_t *key,
                                         size_t key_len,
                                         CLargeValueResult *out_result);
    CAsyncHandle CEloqStore_GetLargeAsync(CEloqStoreHandle store,
                                          CTableIdentHandle table,
                                          const uint8_t *key,
                                          size_t key_len,
                                          CGlobalRegisteredMemoryHandle mem,
                                          uint16_t reg_mem_index_base);
    CEloqStoreStatus CEloqStore_AsyncGetLargeResult(
        CAsyncHandle handle, CLargeValueResult *out_result);
    CEloqStoreStatus CEloqStore_PutLarge(CEloqStoreHandle store,
                                         CTableIdentHandle table,
                                         const uint8_t *key,
                                         size_t key_len,
                                         CIoStringBufferHandle value,
                                         CGlobalRegisteredMemoryHandle mem,
                                         uint16_t reg_mem_index_base,
                                         uint64_t timestamp);
    CEloqStoreStatus CEloqStore_PutPinnedLarge(CEloqStoreHandle store,
                                               CTableIdentHandle table,
                                               const uint8_t *key,
                                               size_t key_len,
                                               const uint8_t *value,
                                               size_t value_len,
                                               const uint8_t *metadata,
                                               size_t metadata_len,
                                               uint64_t timestamp);
    CEloqStoreStatus CEloqStore_GetPinnedLarge(CEloqStoreHandle store,
                                               CTableIdentHandle table,
                                               const uint8_t *key,
                                               size_t key_len,
                                               uint8_t *out_value,
                                               size_t out_value_size,
                                               CPinnedLargeResult *out_result);
    CEloqStoreStatus CEloqStore_GetPinnedLargeOnly(
        CEloqStoreHandle store,
        CTableIdentHandle table,
        const uint8_t *key,
        size_t key_len,
        uint8_t *out_value,
        size_t out_value_size,
        CPinnedLargeResult *out_result);
    CAsyncHandle CEloqStore_GetPinnedLargeAsync(CEloqStoreHandle store,
                                                CTableIdentHandle table,
                                                const uint8_t *key,
                                                size_t key_len,
                                                uint8_t *out_value,
                                                size_t out_value_size);
    CAsyncHandle CEloqStore_GetPinnedLargeOnlyAsync(CEloqStoreHandle store,
                                                    CTableIdentHandle table,
                                                    const uint8_t *key,
                                                    size_t key_len,
                                                    uint8_t *out_value,
                                                    size_t out_value_size);
    CEloqStoreStatus CEloqStore_AsyncGetPinnedResult(
        CAsyncHandle handle, CPinnedLargeResult *out_result);
    CEloqStoreStatus CEloqStore_Exists(CEloqStoreHandle store,
                                       CTableIdentHandle table,
                                       const uint8_t *key,
                                       size_t key_len,
                                       bool *out_exists);

    CEloqStoreStatus CEloqStore_Floor(CEloqStoreHandle store,
                                      CTableIdentHandle table,
                                      const uint8_t *key,
                                      size_t key_len,
                                      CFloorResult *out_result);

    // Free Get result (allocated by C++)
    void CEloqStore_FreeGetResult(CGetResult *result);

    // Free Floor result (allocated by C++)
    void CEloqStore_FreeFloorResult(CFloorResult *result);

    // Free Pinned large result (allocated by C++)
    void CEloqStore_FreePinnedResult(CPinnedLargeResult *result);

    // ============================================================
    // Scan request API (complex operations - preserve Request pattern)
    // ============================================================

    CScanRequestHandle CEloqStore_ScanRequest_Create(void);
    void CEloqStore_ScanRequest_Destroy(CScanRequestHandle req);

    void CEloqStore_ScanRequest_SetTable(CScanRequestHandle req,
                                         CTableIdentHandle table);
    void CEloqStore_ScanRequest_SetRange(CScanRequestHandle req,
                                         const uint8_t *begin_key,
                                         size_t begin_key_len,
                                         bool begin_inclusive,
                                         const uint8_t *end_key,
                                         size_t end_key_len,
                                         bool end_inclusive);
    void CEloqStore_ScanRequest_SetPagination(CScanRequestHandle req,
                                              size_t max_entries,
                                              size_t max_size);
    void CEloqStore_ScanRequest_SetPrefetch(CScanRequestHandle req,
                                            size_t num_pages);

    CEloqStoreStatus CEloqStore_ExecScan(CEloqStoreHandle store,
                                         CScanRequestHandle req,
                                         CScanResult *out_result);

    // Free Scan result (allocated by C++)
    void CEloqStore_FreeScanResult(CScanResult *result);

    // ============================================================
    // BatchWrite request API (complex operations - preserve Request pattern)
    // ============================================================

    CBatchWriteHandle CEloqStore_BatchWrite_Create(void);
    void CEloqStore_BatchWrite_Destroy(CBatchWriteHandle req);

    void CEloqStore_BatchWrite_SetTable(CBatchWriteHandle req,
                                        CTableIdentHandle table);
    void CEloqStore_BatchWrite_AddEntry(CBatchWriteHandle req,
                                        const uint8_t *key,
                                        size_t key_len,
                                        const uint8_t *value,
                                        size_t value_len,
                                        uint64_t timestamp,
                                        CWriteOp op,
                                        uint64_t expire_ts);
    void CEloqStore_BatchWrite_AddLargeEntry(CBatchWriteHandle req,
                                             const uint8_t *key,
                                             size_t key_len,
                                             CIoStringBufferHandle value,
                                             uint64_t timestamp,
                                             CWriteOp op,
                                             uint64_t expire_ts);
    void CEloqStore_BatchWrite_AddPinnedLargeEntry(CBatchWriteHandle req,
                                                   const uint8_t *key,
                                                   size_t key_len,
                                                   const uint8_t *value,
                                                   size_t value_len,
                                                   const uint8_t *metadata,
                                                   size_t metadata_len,
                                                   uint64_t timestamp,
                                                   CWriteOp op,
                                                   uint64_t expire_ts);
    void CEloqStore_BatchWrite_Clear(CBatchWriteHandle req);
    void CEloqStore_BatchWrite_RecycleLargeEntries(
        CBatchWriteHandle req,
        CGlobalRegisteredMemoryHandle mem,
        uint16_t reg_mem_index_base);

    CEloqStoreStatus CEloqStore_ExecBatchWrite(CEloqStoreHandle store,
                                               CBatchWriteHandle req);
    CAsyncHandle CEloqStore_ExecBatchWriteAsync(
        CEloqStoreHandle store,
        CBatchWriteHandle req,
        CGlobalRegisteredMemoryHandle mem,
        uint16_t reg_mem_index_base);
    bool CEloqStore_AsyncIsDone(CAsyncHandle handle);
    CEloqStoreStatus CEloqStore_AsyncWait(CAsyncHandle handle);
    CEloqStoreStatus CEloqStore_AsyncStatus(CAsyncHandle handle);
    void CEloqStore_AsyncDestroy(CAsyncHandle handle);

    // ============================================================
    // Zero-copy registered memory and large-value buffers
    // ============================================================

    CGlobalRegisteredMemoryHandle CEloqStore_GlobalMemory_Create(
        uint32_t segment_size, uint64_t chunk_size, uint64_t total_size);
    void CEloqStore_GlobalMemory_Destroy(CGlobalRegisteredMemoryHandle mem);
    uint32_t CEloqStore_GlobalMemory_SegmentSize(
        CGlobalRegisteredMemoryHandle mem);
    size_t CEloqStore_GlobalMemory_TotalSegments(
        CGlobalRegisteredMemoryHandle mem);
    size_t CEloqStore_GlobalMemory_FreeSegments(
        CGlobalRegisteredMemoryHandle mem);
    size_t CEloqStore_GlobalMemory_ChunkCount(
        CGlobalRegisteredMemoryHandle mem);
    bool CEloqStore_GlobalMemory_ChunkAt(CGlobalRegisteredMemoryHandle mem,
                                         size_t index,
                                         CMemoryChunk *out_chunk);
    CIoStringBufferHandle CEloqStore_GlobalMemory_AllocateIoString(
        CGlobalRegisteredMemoryHandle mem,
        size_t size,
        uint16_t reg_mem_index_base);

    void CEloqStore_IoStringBuffer_Destroy(CIoStringBufferHandle buf);
    void CEloqStore_IoStringBuffer_Recycle(CIoStringBufferHandle buf,
                                           CGlobalRegisteredMemoryHandle mem,
                                           uint16_t reg_mem_index_base);
    size_t CEloqStore_IoStringBuffer_Size(CIoStringBufferHandle buf);
    size_t CEloqStore_IoStringBuffer_FragmentCount(CIoStringBufferHandle buf);
    bool CEloqStore_IoStringBuffer_FragmentAt(CIoStringBufferHandle buf,
                                              size_t index,
                                              CIoStringFragment *out_fragment);
    bool CEloqStore_IoStringBuffer_FragmentAtEx(
        CIoStringBufferHandle buf,
        size_t index,
        uint32_t segment_size,
        uint16_t reg_mem_index_base,
        CIoStringFragment *out_fragment);

    // ============================================================
    // Error message query
    // ============================================================

    const char *CEloqStore_GetLastError(CEloqStoreHandle store);

#ifdef __cplusplus
}
#endif
