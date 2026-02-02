#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================
// 错误码定义
// ============================================================
typedef enum CEloqStoreStatus {
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
} CEloqStoreStatus;

// ============================================================
// 不透明句柄类型
// ============================================================
typedef void* CEloqStoreHandle;
typedef void* CTableIdentHandle;
typedef void* CScanRequestHandle;
typedef void* CBatchWriteHandle;

// ============================================================
// 写入操作枚举
// ============================================================
typedef enum CWriteOp {
    CWriteOp_Upsert = 0,
    CWriteOp_Delete = 1,
} CWriteOp;

// ============================================================
// 输入结构体
// ============================================================
typedef struct CKvEntry {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
} CKvEntry;

typedef struct CWriteEntry {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    CWriteOp op;
    uint64_t expire_ts;
} CWriteEntry;

// ============================================================
// 输出结构体
// ============================================================

// Get 操作结果
typedef struct CGetResult {
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
    bool found;
} CGetResult;

// Floor 操作结果
typedef struct CFloorResult {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
    bool found;
} CFloorResult;

// Scan 结果条目
typedef struct CScanEntry {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
} CScanEntry;

// Scan 操作结果
typedef struct CScanResult {
    CScanEntry* entries;
    size_t num_entries;
    size_t total_size;
    bool has_more;
} CScanResult;

// ============================================================
// 选项 API
// ============================================================

CEloqStoreHandle CEloqStore_Options_Create(void);
void CEloqStore_Options_Destroy(CEloqStoreHandle opts);

void CEloqStore_Options_SetNumThreads(CEloqStoreHandle opts, uint16_t n);
void CEloqStore_Options_SetBufferPoolSize(CEloqStoreHandle opts, uint64_t size);
void CEloqStore_Options_SetDataPageSize(CEloqStoreHandle opts, uint16_t size);
void CEloqStore_Options_SetManifestLimit(CEloqStoreHandle opts, uint32_t limit);
void CEloqStore_Options_SetFdLimit(CEloqStoreHandle opts, uint32_t limit);
void CEloqStore_Options_SetPagesPerFileShift(CEloqStoreHandle opts, uint8_t shift);
void CEloqStore_Options_SetOverflowPointers(CEloqStoreHandle opts, uint8_t n);
void CEloqStore_Options_SetDataAppendMode(CEloqStoreHandle opts, bool enable);
void CEloqStore_Options_SetEnableCompression(CEloqStoreHandle opts, bool enable);

void CEloqStore_Options_AddStorePath(CEloqStoreHandle opts, const char* path);
void CEloqStore_Options_SetCloudStorePath(CEloqStoreHandle opts, const char* path);
void CEloqStore_Options_SetCloudProvider(CEloqStoreHandle opts, const char* provider);
void CEloqStore_Options_SetCloudRegion(CEloqStoreHandle opts, const char* region);
void CEloqStore_Options_SetCloudCredentials(CEloqStoreHandle opts, const char* access_key, const char* secret_key);
void CEloqStore_Options_SetCloudVerifySsl(CEloqStoreHandle opts, bool verify);

bool CEloqStore_Options_Validate(CEloqStoreHandle opts);

// ============================================================
// 引擎生命周期
// ============================================================

CEloqStoreHandle CEloqStore_Create(CEloqStoreHandle options);
void CEloqStore_Destroy(CEloqStoreHandle store);

CEloqStoreStatus CEloqStore_Start(CEloqStoreHandle store);
void CEloqStore_Stop(CEloqStoreHandle store);
bool CEloqStore_IsStopped(CEloqStoreHandle store);

// ============================================================
// 表标识符
// ============================================================

CTableIdentHandle CEloqStore_TableIdent_Create(const char* table_name, uint32_t partition_id);
void CEloqStore_TableIdent_Destroy(CTableIdentHandle ident);
const char* CEloqStore_TableIdent_GetName(CTableIdentHandle ident);
uint32_t CEloqStore_TableIdent_GetPartition(CTableIdentHandle ident);

// ============================================================
// 扁平化写入 API（简单操作）
// ============================================================

// 单条 Put 操作（内部使用 BatchWrite 实现）
CEloqStoreStatus CEloqStore_Put(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t timestamp
);

// 批量 Put 操作 - 一次性写入多对 key-value
// keys: key 指针数组
// values: value 指针数组
// count: 元素数量
CEloqStoreStatus CEloqStore_PutBatch(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* const* keys,
    const size_t* key_lens,
    const uint8_t* const* values,
    const size_t* value_lens,
    size_t count,
    uint64_t timestamp
);

// 批量 Put 操作（使用 CWriteEntry 数组）
CEloqStoreStatus CEloqStore_PutEntries(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const CWriteEntry* entries,
    size_t count
);

// 单条 Delete 操作（内部使用 BatchWrite 实现）
CEloqStoreStatus CEloqStore_Delete(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    uint64_t timestamp
);

// 批量 Delete 操作 - 一次性删除多个 key
CEloqStoreStatus CEloqStore_DeleteBatch(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* const* keys,
    const size_t* key_lens,
    size_t count,
    uint64_t timestamp
);

// ============================================================
// 扁平化读取 API（简单操作）
// ============================================================

CEloqStoreStatus CEloqStore_Get(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    CGetResult* out_result
);

CEloqStoreStatus CEloqStore_Floor(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    CFloorResult* out_result
);

// 释放 Get 结果（由 C++ 分配）
void CEloqStore_FreeGetResult(CGetResult* result);

// 释放 Floor 结果（由 C++ 分配）
void CEloqStore_FreeFloorResult(CFloorResult* result);

// ============================================================
// Scan 请求 API（复杂操作 - 保留 Request 模式）
// ============================================================

CScanRequestHandle CEloqStore_ScanRequest_Create(void);
void CEloqStore_ScanRequest_Destroy(CScanRequestHandle req);

void CEloqStore_ScanRequest_SetTable(CScanRequestHandle req, CTableIdentHandle table);
void CEloqStore_ScanRequest_SetRange(
    CScanRequestHandle req,
    const uint8_t* begin_key,
    size_t begin_key_len,
    bool begin_inclusive,
    const uint8_t* end_key,
    size_t end_key_len,
    bool end_inclusive
);
void CEloqStore_ScanRequest_SetPagination(CScanRequestHandle req, size_t max_entries, size_t max_size);
void CEloqStore_ScanRequest_SetPrefetch(CScanRequestHandle req, size_t num_pages);

CEloqStoreStatus CEloqStore_ExecScan(
    CEloqStoreHandle store,
    CScanRequestHandle req,
    CScanResult* out_result
);

// 释放 Scan 结果（由 C++ 分配）
void CEloqStore_FreeScanResult(CScanResult* result);

// ============================================================
// BatchWrite 请求 API（复杂操作 - 保留 Request 模式）
// ============================================================

CBatchWriteHandle CEloqStore_BatchWrite_Create(void);
void CEloqStore_BatchWrite_Destroy(CBatchWriteHandle req);

void CEloqStore_BatchWrite_SetTable(CBatchWriteHandle req, CTableIdentHandle table);
void CEloqStore_BatchWrite_AddEntry(
    CBatchWriteHandle req,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t timestamp,
    CWriteOp op,
    uint64_t expire_ts
);
void CEloqStore_BatchWrite_Clear(CBatchWriteHandle req);

CEloqStoreStatus CEloqStore_ExecBatchWrite(
    CEloqStoreHandle store,
    CBatchWriteHandle req
);

// ============================================================
// 错误信息查询
// ============================================================

const char* CEloqStore_GetLastError(CEloqStoreHandle store);

#ifdef __cplusplus
}
#endif
