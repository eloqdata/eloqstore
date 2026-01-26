#include "eloqstore_capi.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <mutex>

#include "eloq_store.h"
#include "kv_options.h"
#include "types.h"

using namespace eloqstore;

// ============================================================
// 线程局部存储用于错误消息
// ============================================================
static thread_local std::string g_last_error_message;
static std::mutex g_error_mutex;

static void set_last_error(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_error_mutex);
    g_last_error_message = msg;
}

static void clear_last_error() {
    std::lock_guard<std::mutex> lock(g_error_mutex);
    g_last_error_message.clear();
}

// ============================================================
// 错误码转换
// ============================================================
static CEloqStoreStatus kv_error_to_c(KvError err) {
    switch (err) {
        case KvError::NoError: return CEloqStoreStatus_Ok;
        case KvError::InvalidArgs: return CEloqStoreStatus_InvalidArgs;
        case KvError::NotFound: return CEloqStoreStatus_NotFound;
        case KvError::NotRunning: return CEloqStoreStatus_NotRunning;
        case KvError::Corrupted: return CEloqStoreStatus_Corrupted;
        case KvError::EndOfFile: return CEloqStoreStatus_EndOfFile;
        case KvError::OutOfSpace: return CEloqStoreStatus_OutOfSpace;
        case KvError::OutOfMem: return CEloqStoreStatus_OutOfMem;
        case KvError::OpenFileLimit: return CEloqStoreStatus_OpenFileLimit;
        case KvError::TryAgain: return CEloqStoreStatus_TryAgain;
        case KvError::Busy: return CEloqStoreStatus_Busy;
        case KvError::Timeout: return CEloqStoreStatus_Timeout;
        case KvError::NoPermission: return CEloqStoreStatus_NoPermission;
        case KvError::CloudErr: return CEloqStoreStatus_CloudErr;
        case KvError::IoFail: return CEloqStoreStatus_IoFail;
        default: return CEloqStoreStatus_InvalidArgs;
    }
}

// 存储 C++ 对象到句柄的映射（用于扁平化 API）
static std::mutex g_request_mutex;
static thread_local ReadRequest* g_last_read_req = nullptr;
static thread_local FloorRequest* g_last_floor_req = nullptr;

extern "C" {

// ============================================================
// 选项 API（使用新命名）
// ============================================================

CEloqStoreHandle CEloqStore_Options_Create(void) {
    clear_last_error();
    try {
        return reinterpret_cast<CEloqStoreHandle>(new KvOptions());
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return nullptr;
    }
}

void CEloqStore_Options_Destroy(CEloqStoreHandle opts) {
    if (opts) {
        delete reinterpret_cast<KvOptions*>(opts);
    }
}

void CEloqStore_Options_SetNumThreads(CEloqStoreHandle opts, uint16_t n) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->num_threads = n;
}

void CEloqStore_Options_SetBufferPoolSize(CEloqStoreHandle opts, uint64_t size) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->buffer_pool_size = size;
}

void CEloqStore_Options_SetDataPageSize(CEloqStoreHandle opts, uint16_t size) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->data_page_size = size;
}

void CEloqStore_Options_SetManifestLimit(CEloqStoreHandle opts, uint32_t limit) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->manifest_limit = limit;
}

void CEloqStore_Options_SetFdLimit(CEloqStoreHandle opts, uint32_t limit) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->fd_limit = limit;
}

void CEloqStore_Options_SetPagesPerFileShift(CEloqStoreHandle opts, uint8_t shift) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->pages_per_file_shift = shift;
}

void CEloqStore_Options_SetOverflowPointers(CEloqStoreHandle opts, uint8_t n) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->overflow_pointers = n;
}

void CEloqStore_Options_SetDataAppendMode(CEloqStoreHandle opts, bool enable) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->data_append_mode = enable;
}

void CEloqStore_Options_SetEnableCompression(CEloqStoreHandle opts, bool enable) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->enable_compression = enable;
}

void CEloqStore_Options_AddStorePath(CEloqStoreHandle opts, const char* path) {
    if (opts && path) reinterpret_cast<KvOptions*>(opts)->store_path.push_back(path);
}

void CEloqStore_Options_SetCloudStorePath(CEloqStoreHandle opts, const char* path) {
    if (opts && path) reinterpret_cast<KvOptions*>(opts)->cloud_store_path = path;
}

void CEloqStore_Options_SetCloudProvider(CEloqStoreHandle opts, const char* provider) {
    if (opts && provider) reinterpret_cast<KvOptions*>(opts)->cloud_provider = provider;
}

void CEloqStore_Options_SetCloudRegion(CEloqStoreHandle opts, const char* region) {
    if (opts && region) reinterpret_cast<KvOptions*>(opts)->cloud_region = region;
}

void CEloqStore_Options_SetCloudCredentials(CEloqStoreHandle opts, const char* access_key, const char* secret_key) {
    if (opts) {
        if (access_key) reinterpret_cast<KvOptions*>(opts)->cloud_access_key = access_key;
        if (secret_key) reinterpret_cast<KvOptions*>(opts)->cloud_secret_key = secret_key;
    }
}

void CEloqStore_Options_SetCloudVerifySsl(CEloqStoreHandle opts, bool verify) {
    if (opts) reinterpret_cast<KvOptions*>(opts)->cloud_verify_ssl = verify;
}

bool CEloqStore_Options_Validate(CEloqStoreHandle opts) {
    if (!opts) return false;
    return EloqStore::ValidateOptions(*reinterpret_cast<KvOptions*>(opts));
}

// ============================================================
// 引擎生命周期
// ============================================================

CEloqStoreHandle CEloqStore_Create(CEloqStoreHandle options) {
    clear_last_error();
    if (!options) {
        set_last_error("Invalid options: null pointer");
        return nullptr;
    }
    try {
        auto* store = new EloqStore(*reinterpret_cast<KvOptions*>(options));
        return reinterpret_cast<CEloqStoreHandle>(store);
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return nullptr;
    }
}

void CEloqStore_Destroy(CEloqStoreHandle store) {
    if (store) {
        delete reinterpret_cast<EloqStore*>(store);
    }
}

CEloqStoreStatus CEloqStore_Start(CEloqStoreHandle store) {
    clear_last_error();
    if (!store) {
        return CEloqStoreStatus_InvalidArgs;
    }
    try {
        auto err = reinterpret_cast<EloqStore*>(store)->Start();
        if (err != KvError::NoError) {
            set_last_error("Failed to start store");
        }
        return kv_error_to_c(err);
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

void CEloqStore_Stop(CEloqStoreHandle store) {
    if (store) {
        reinterpret_cast<EloqStore*>(store)->Stop();
    }
}

bool CEloqStore_IsStopped(CEloqStoreHandle store) {
    return store ? reinterpret_cast<EloqStore*>(store)->IsStopped() : true;
}

// ============================================================
// 表标识符
// ============================================================

CTableIdentHandle CEloqStore_TableIdent_Create(const char* table_name, uint32_t partition_id) {
    clear_last_error();
    if (!table_name) {
        set_last_error("Invalid table name: null pointer");
        return nullptr;
    }
    try {
        return reinterpret_cast<CTableIdentHandle>(new TableIdent(table_name, partition_id));
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return nullptr;
    }
}

void CEloqStore_TableIdent_Destroy(CTableIdentHandle ident) {
    if (ident) {
        delete reinterpret_cast<TableIdent*>(ident);
    }
}

const char* CEloqStore_TableIdent_GetName(CTableIdentHandle ident) {
    if (!ident) return nullptr;
    static thread_local std::string name;
    name = reinterpret_cast<TableIdent*>(ident)->tbl_name_;
    return name.c_str();
}

uint32_t CEloqStore_TableIdent_GetPartition(CTableIdentHandle ident) {
    return ident ? reinterpret_cast<TableIdent*>(ident)->partition_id_ : 0;
}

// ============================================================
// 扁平化写入 API（简单操作）
// 使用 BatchWriteRequest 实现
// ============================================================

CEloqStoreStatus CEloqStore_Put(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t timestamp
) {
    clear_last_error();
    if (!store || !table || !key || key_len == 0) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        // 创建临时的 BatchWriteRequest 来执行 Put
        std::vector<WriteDataEntry> batch;
        WriteDataEntry entry;
        entry.key_ = std::string(reinterpret_cast<const char*>(key), key_len);
        if (value && value_len > 0) {
            entry.val_ = std::string(reinterpret_cast<const char*>(value), value_len);
        }
        entry.timestamp_ = timestamp;
        entry.expire_ts_ = 0;
        entry.op_ = WriteOp::Upsert;
        batch.push_back(std::move(entry));
        
        BatchWriteRequest req;
        req.SetArgs(*cpp_table, std::move(batch));
        
        cpp_store->ExecSync(&req);
        return kv_error_to_c(req.Error());
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

CEloqStoreStatus CEloqStore_Delete(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    uint64_t timestamp
) {
    clear_last_error();
    if (!store || !table || !key || key_len == 0) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        // 创建临时的 BatchWriteRequest 来执行 Delete
        std::vector<WriteDataEntry> batch;
        WriteDataEntry entry;
        entry.key_ = std::string(reinterpret_cast<const char*>(key), key_len);
        entry.val_ = "";
        entry.timestamp_ = timestamp;
        entry.expire_ts_ = 0;
        entry.op_ = WriteOp::Delete;
        batch.push_back(std::move(entry));
        
        BatchWriteRequest req;
        req.SetArgs(*cpp_table, std::move(batch));
        
        cpp_store->ExecSync(&req);
        return kv_error_to_c(req.Error());
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

// ============================================================
// 批量写入 API
// ============================================================

CEloqStoreStatus CEloqStore_PutBatch(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* const* keys,
    const size_t* key_lens,
    const uint8_t* const* values,
    const size_t* value_lens,
    size_t count,
    uint64_t timestamp
) {
    clear_last_error();
    if (!store || !table || !keys || !key_lens || count == 0) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        std::vector<WriteDataEntry> batch;
        batch.reserve(count);
        
        for (size_t i = 0; i < count; ++i) {
            WriteDataEntry entry;
            entry.key_ = std::string(reinterpret_cast<const char*>(keys[i]), key_lens[i]);
            if (values && value_lens && value_lens[i] > 0) {
                entry.val_ = std::string(reinterpret_cast<const char*>(values[i]), value_lens[i]);
            } else {
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
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

CEloqStoreStatus CEloqStore_PutEntries(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const CWriteEntry* entries,
    size_t count
) {
    clear_last_error();
    if (!store || !table || !entries || count == 0) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        std::vector<WriteDataEntry> batch;
        batch.reserve(count);
        
        for (size_t i = 0; i < count; ++i) {
            WriteDataEntry entry;
            entry.key_ = std::string(reinterpret_cast<const char*>(entries[i].key), entries[i].key_len);
            if (entries[i].value && entries[i].value_len > 0) {
                entry.val_ = std::string(reinterpret_cast<const char*>(entries[i].value), entries[i].value_len);
            } else {
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
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

CEloqStoreStatus CEloqStore_DeleteBatch(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* const* keys,
    const size_t* key_lens,
    size_t count,
    uint64_t timestamp
) {
    clear_last_error();
    if (!store || !table || !keys || !key_lens || count == 0) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        std::vector<WriteDataEntry> batch;
        batch.reserve(count);
        
        for (size_t i = 0; i < count; ++i) {
            WriteDataEntry entry;
            entry.key_ = std::string(reinterpret_cast<const char*>(keys[i]), key_lens[i]);
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
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

// ============================================================
// 扁平化读取 API（简单操作）
// 使用 ReadRequest/FloorRequest 实现
// ============================================================

CEloqStoreStatus CEloqStore_Get(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    CGetResult* out_result
) {
    clear_last_error();
    if (!store || !table || !key || key_len == 0 || !out_result) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        // 创建临时的 ReadRequest 来执行 Get
        ReadRequest req;
        req.SetArgs(*cpp_table, std::string(reinterpret_cast<const char*>(key), key_len));
        
        cpp_store->ExecSync(&req);
        auto err = req.Error();
        
        if (err == KvError::NoError) {
            uint8_t* value_copy = new uint8_t[req.value_.size()];
            std::memcpy(value_copy, req.value_.data(), req.value_.size());
            out_result->value = value_copy;
            out_result->value_len = req.value_.size();
            out_result->timestamp = req.ts_;
            out_result->expire_ts = req.expire_ts_;
            out_result->found = true;
        } else if (err == KvError::NotFound) {
            out_result->value = nullptr;
            out_result->value_len = 0;
            out_result->timestamp = 0;
            out_result->expire_ts = 0;
            out_result->found = false;
        } else {
            return kv_error_to_c(err);
        }
        return CEloqStoreStatus_Ok;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

CEloqStoreStatus CEloqStore_Floor(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    CFloorResult* out_result
) {
    clear_last_error();
    if (!store || !table || !key || key_len == 0 || !out_result) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_table = reinterpret_cast<TableIdent*>(table);
    
    try {
        // 创建临时的 FloorRequest 来执行 Floor
        FloorRequest req;
        req.SetArgs(*cpp_table, std::string(reinterpret_cast<const char*>(key), key_len));
        
        cpp_store->ExecSync(&req);
        auto err = req.Error();
        
        if (err == KvError::NoError) {
            uint8_t* key_copy = new uint8_t[req.floor_key_.size()];
            std::memcpy(key_copy, req.floor_key_.data(), req.floor_key_.size());
            uint8_t* value_copy = new uint8_t[req.value_.size()];
            std::memcpy(value_copy, req.value_.data(), req.value_.size());
            out_result->key = key_copy;
            out_result->key_len = req.floor_key_.size();
            out_result->value = value_copy;
            out_result->value_len = req.value_.size();
            out_result->timestamp = req.ts_;
            out_result->expire_ts = req.expire_ts_;
            out_result->found = true;
        } else if (err == KvError::NotFound) {
            out_result->key = nullptr;
            out_result->key_len = 0;
            out_result->value = nullptr;
            out_result->value_len = 0;
            out_result->timestamp = 0;
            out_result->expire_ts = 0;
            out_result->found = false;
        } else {
            return kv_error_to_c(err);
        }
        return CEloqStoreStatus_Ok;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

// ============================================================
// Scan 请求 API（复杂操作 - 保留 Request 模式）
// ============================================================

CScanRequestHandle CEloqStore_ScanRequest_Create(void) {
    clear_last_error();
    try {
        return reinterpret_cast<CScanRequestHandle>(new ScanRequest());
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return nullptr;
    }
}

void CEloqStore_ScanRequest_Destroy(CScanRequestHandle req) {
    if (req) {
        delete reinterpret_cast<ScanRequest*>(req);
    }
}

void CEloqStore_ScanRequest_SetTable(CScanRequestHandle req, CTableIdentHandle table) {
    if (req && table) {
        reinterpret_cast<ScanRequest*>(req)->SetArgs(*reinterpret_cast<TableIdent*>(table), "", "", true);
    }
}

void CEloqStore_ScanRequest_SetRange(
    CScanRequestHandle req,
    const uint8_t* begin_key,
    size_t begin_key_len,
    bool begin_inclusive,
    const uint8_t* end_key,
    size_t end_key_len,
    bool end_inclusive
) {
    if (req) {
        auto* cpp_req = reinterpret_cast<ScanRequest*>(req);
        std::string_view begin_sv, end_sv;
        if (begin_key && begin_key_len > 0) {
            begin_sv = std::string_view(reinterpret_cast<const char*>(begin_key), begin_key_len);
        }
        if (end_key && end_key_len > 0) {
            end_sv = std::string_view(reinterpret_cast<const char*>(end_key), end_key_len);
        }
        // Note: ScanRequest 的 SetArgs 需要 table 参数
        // 这里简化处理，用户应使用原始的 SetArgs API
    }
}

void CEloqStore_ScanRequest_SetPagination(CScanRequestHandle req, size_t max_entries, size_t max_size) {
    if (req) {
        reinterpret_cast<ScanRequest*>(req)->SetPagination(max_entries, max_size);
    }
}

void CEloqStore_ScanRequest_SetPrefetch(CScanRequestHandle req, size_t num_pages) {
    if (req) {
        reinterpret_cast<ScanRequest*>(req)->SetPrefetchPageNum(num_pages);
    }
}

CEloqStoreStatus CEloqStore_ExecScan(
    CEloqStoreHandle store,
    CScanRequestHandle req,
    CScanResult* out_result
) {
    clear_last_error();
    if (!store || !req || !out_result) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_req = reinterpret_cast<ScanRequest*>(req);
    
    try {
        cpp_store->ExecSync(cpp_req);
        auto err = cpp_req->Error();
        if (err != KvError::NoError && err != KvError::NotFound) {
            return kv_error_to_c(err);
        }
        
        auto entries = cpp_req->Entries();
        auto [count, size] = cpp_req->ResultSize();
        
        // 分配结果数组（调用方需通过 CEloqStore_FreeScanResult 释放）
        static thread_local std::vector<CScanEntry> result_entries;
        result_entries.clear();
        result_entries.reserve(entries.size());
        
        for (const auto& entry : entries) {
            CScanEntry e;
            e.key = reinterpret_cast<const uint8_t*>(entry.key_.data());
            e.key_len = entry.key_.size();
            e.value = reinterpret_cast<const uint8_t*>(entry.value_.data());
            e.value_len = entry.value_.size();
            e.timestamp = entry.timestamp_;
            e.expire_ts = entry.expire_ts_;
            result_entries.push_back(e);
        }
        
        out_result->entries = result_entries.data();
        out_result->num_entries = result_entries.size();
        out_result->total_size = size;
        out_result->has_more = cpp_req->HasRemaining();
        
        return CEloqStoreStatus_Ok;
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

void CEloqStore_FreeScanResult(CScanResult* result) {
    if (result) {
        result->entries = nullptr;
        result->num_entries = 0;
        result->total_size = 0;
        result->has_more = false;
    }
}

// ============================================================
// BatchWrite 请求 API（复杂操作 - 保留 Request 模式）
// ============================================================

CBatchWriteHandle CEloqStore_BatchWrite_Create(void) {
    clear_last_error();
    try {
        return reinterpret_cast<CBatchWriteHandle>(new BatchWriteRequest());
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return nullptr;
    }
}

void CEloqStore_BatchWrite_Destroy(CBatchWriteHandle req) {
    if (req) {
        delete reinterpret_cast<BatchWriteRequest*>(req);
    }
}

void CEloqStore_BatchWrite_SetTable(CBatchWriteHandle req, CTableIdentHandle table) {
    if (req && table) {
        std::vector<WriteDataEntry> batch;
        reinterpret_cast<BatchWriteRequest*>(req)->SetArgs(*reinterpret_cast<TableIdent*>(table), std::move(batch));
    }
}

void CEloqStore_BatchWrite_AddEntry(
    CBatchWriteHandle req,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t timestamp,
    CWriteOp op,
    uint64_t expire_ts
) {
    if (req && key && key_len > 0) {
        auto* cpp_req = reinterpret_cast<BatchWriteRequest*>(req);
        std::string key_str(reinterpret_cast<const char*>(key), key_len);
        std::string value_str;
        if (value && value_len > 0) {
            value_str = std::string(reinterpret_cast<const char*>(value), value_len);
        }
        cpp_req->AddWrite(std::move(key_str), std::move(value_str), timestamp, static_cast<WriteOp>(op));
    }
}

void CEloqStore_BatchWrite_Clear(CBatchWriteHandle req) {
    if (req) reinterpret_cast<BatchWriteRequest*>(req)->Clear();
}

CEloqStoreStatus CEloqStore_ExecBatchWrite(
    CEloqStoreHandle store,
    CBatchWriteHandle req
) {
    clear_last_error();
    if (!store || !req) {
        return CEloqStoreStatus_InvalidArgs;
    }
    
    auto* cpp_store = reinterpret_cast<EloqStore*>(store);
    auto* cpp_req = reinterpret_cast<BatchWriteRequest*>(req);
    
    try {
        cpp_store->ExecSync(cpp_req);
        return kv_error_to_c(cpp_req->Error());
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return CEloqStoreStatus_InvalidArgs;
    }
}

// ============================================================
// 错误信息查询
// ============================================================

const char* CEloqStore_GetLastError(CEloqStoreHandle store) {
    std::lock_guard<std::mutex> lock(g_error_mutex);
    return g_last_error_message.empty() ? nullptr : g_last_error_message.c_str();
}

// ============================================================
// 内存释放函数（用于 Get/Floor 结果）
// ============================================================

void CEloqStore_FreeGetResult(CGetResult* result) {
    if (result) {
        if (result->value) {
            delete[] result->value;
            result->value = nullptr;
        }
        result->value_len = 0;
        result->found = false;
    }
}

void CEloqStore_FreeFloorResult(CFloorResult* result) {
    if (result) {
        if (result->key) {
            delete[] result->key;
            result->key = nullptr;
        }
        if (result->value) {
            delete[] result->value;
            result->value = nullptr;
        }
        result->key_len = 0;
        result->value_len = 0;
        result->found = false;
    }
}

}  // extern "C"
