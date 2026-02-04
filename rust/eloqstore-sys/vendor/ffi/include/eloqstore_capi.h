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
    } CEloqStoreStatus;

    // ============================================================
    // Opaque handle types
    // ============================================================
    typedef void *CEloqStoreHandle;
    typedef void *CTableIdentHandle;
    typedef void *CScanRequestHandle;
    typedef void *CBatchWriteHandle;

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
    } CGetResult;

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
    void CEloqStore_Options_SetCloudVerifySsl(CEloqStoreHandle opts,
                                              bool verify);

    bool CEloqStore_Options_Validate(CEloqStoreHandle opts);

    // ============================================================
    // Engine lifecycle
    // ============================================================

    CEloqStoreHandle CEloqStore_Create(CEloqStoreHandle options);
    void CEloqStore_Destroy(CEloqStoreHandle store);

    CEloqStoreStatus CEloqStore_Start(CEloqStoreHandle store);
    void CEloqStore_Stop(CEloqStoreHandle store);
    bool CEloqStore_IsStopped(CEloqStoreHandle store);

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

    CEloqStoreStatus CEloqStore_Floor(CEloqStoreHandle store,
                                      CTableIdentHandle table,
                                      const uint8_t *key,
                                      size_t key_len,
                                      CFloorResult *out_result);

    // Free Get result (allocated by C++)
    void CEloqStore_FreeGetResult(CGetResult *result);

    // Free Floor result (allocated by C++)
    void CEloqStore_FreeFloorResult(CFloorResult *result);

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
    void CEloqStore_BatchWrite_Clear(CBatchWriteHandle req);

    CEloqStoreStatus CEloqStore_ExecBatchWrite(CEloqStoreHandle store,
                                               CBatchWriteHandle req);

    // ============================================================
    // Error message query
    // ============================================================

    const char *CEloqStore_GetLastError(CEloqStoreHandle store);

#ifdef __cplusplus
}
#endif
