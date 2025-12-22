---
date: 2025-12-12T18:04:05+08:00
researcher: assistant
git_commit: 30de7d775e927d18c2418a821b8753377ae6cb2c
branch: multi-write-term
repository: eloqstore
topic: "云模式分区写入与 manifest/文件 id 处理"
tags: [research, codebase, cloud-store, manifest, file-allocator, io-manager]
status: complete
last_updated: 2025-12-12
last_updated_by: assistant
---

# 研究：云模式分区写入与 manifest/文件 id 处理

**日期**: 2025-12-12T18:04:05+08:00  
**研究者**: assistant  
**Git 提交**: 30de7d775e927d18c2418a821b8753377ae6cb2c  
**分支**: multi-write-term  
**仓库**: eloqstore

## 研究问题
用户设计希望为数据/manifest 文件名添加 term 后缀，并在文件和分配器层面携带 term 元数据，以避免云模式下多个进程同时打开同一表/分区时的数据混乱。现有代码在云模式下对 manifest、文件命名、文件描述符缓存以及文件页分配是如何处理的？

## 摘要
- 目前文件名固定为 `manifest`、`data_<file_id>`，无 term 后缀。
- FD 缓存(`PartitionFiles`、`LruFD`) 仅按 `TableIdent` 和 `FileId` 做键。
- Manifest 快照包含 `max_fp_id`、字典数据、映射表；WAL 追加映射增量，未存 term。
- Replayer 仅依赖 `max_fp_id` 和映射恢复分配器；云模式下载/上传也用同名文件，不感知 term，重用缓存文件不做版本校验。
- 文件页分配从 manifest 中最大的 `file_page_id` 继续，无跨进程冲突防护。

## 详细发现

### 文件命名与解析
- 文件名前缀固定 `manifest`、`data`，分隔符为 `_`，数据文件格式 `data_<file_id>`，无 term/版本字段。  
```26:29:types.h
constexpr char FileNameSeparator = '_';
static constexpr char FileNameData[] = "data";
static constexpr char FileNameManifest[] = "manifest";
```
```9:36:common.h
inline std::pair<std::string_view, std::string_view> ParseFileName(std::string_view name)
{
    size_t pos = name.find(FileNameSeparator);
    std::string_view file_type;
    std::string_view file_id;

    if (pos == std::string::npos)
    {
        file_type = name;
    }
    else
    {
        file_type = name.substr(0, pos);
        file_id = name.substr(pos + 1);
    }

    return {file_type, file_id};
}
```

### Manifest 结构与重放
- 快照：头部 + `max_fp_id`、字典长度/内容、映射表序列化；无 term 字段，WAL 追加映射增量。  
```33:45:root_meta.cpp
std::string_view ManifestBuilder::Snapshot(PageId root_id,
                                           PageId ttl_root,
                                           const MappingSnapshot *mapping,
                                           FilePageId max_fp_id,
                                           std::string_view dict_bytes)
{
    Reset();
    buff_.reserve(4 + 8 * (mapping->mapping_tbl_.size() + 1));
    PutVarint64(&buff_, max_fp_id);
    PutVarint32(&buff_, dict_bytes.size());
    buff_.append(dict_bytes.data(), dict_bytes.size());
    mapping->Serialize(buff_);
    return Finalize(root_id, ttl_root);
}
```
```86:113:replayer.cpp
void Replayer::DeserializeSnapshot(std::string_view snapshot)
{
    [[maybe_unused]] bool ok = GetVarint64(&snapshot, &max_fp_id_);
    assert(ok);

    uint32_t dict_len = 0;
    ok = GetVarint32(&snapshot, &dict_len);
    assert(ok);
    if (dict_len > 0)
    {
        assert(snapshot.size() >= dict_len);
        dict_bytes_.assign(snapshot.data(), snapshot.data() + dict_len);
        snapshot = snapshot.substr(dict_len);
    }
    else
    {
        dict_bytes_.clear();
    }

    mapping_tbl_.reserve(opts_->init_page_count);
    while (!snapshot.empty())
    {
        uint64_t value;
        ok = GetVarint64(&snapshot, &value);
        assert(ok);
        mapping_tbl_.push_back(value);
    }
}
```
- Replayer 用 `max_fp_id` 和映射恢复分配器，假定分配单调递增，无额外版本/term 保护。  
```146:207:replayer.cpp
auto mapper = std::make_unique<PageMapper>(std::move(mapping));
...
if (opts_->data_append_mode)
{
    if (using_fp_ids.empty())
    {
        FileId min_file_id = max_fp_id_ >> opts_->pages_per_file_shift;
        mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
            opts_, min_file_id, max_fp_id_, 0);
    }
    else
    {
        std::sort(using_fp_ids.begin(), using_fp_ids.end());
        FileId min_file_id =
            using_fp_ids.front() >> opts_->pages_per_file_shift;
        uint32_t hole_cnt = 0;
        for (FileId cur_file_id = min_file_id;
             FilePageId fp_id : using_fp_ids)
        {
            FileId file_id = fp_id >> opts_->pages_per_file_shift;
            ...
        }
        assert(using_fp_ids.back() < max_fp_id_);
        mapper->file_page_allocator_ = std::make_unique<AppendAllocator>(
            opts_, min_file_id, max_fp_id_, hole_cnt);
    }
}
```

### 文件页分配
- `FilePageAllocator::Allocate` 仅自增 `max_fp_id_`，无 term 维度。  
```295:318:page_mapper.cpp
FilePageId FilePageAllocator::Allocate()
{
    return max_fp_id_++;
}
```

### Manifest 写入
- `WriteTask::FlushManifest` 根据大小/字典脏状态决定追加 WAL 或写全量快照，调用 `AppendManifest` / `SwitchManifest`，均无 term 参数。  
```212:279:write_task.cpp
if (!dict_dirty && manifest_size > 0 &&
    manifest_size + wal_builder_.CurrentSize() <= opts->manifest_limit)
{
    std::string_view blob =
        wal_builder_.Finalize(cow_meta_.root_id_, cow_meta_.ttl_root_id_);
    err = IoMgr()->AppendManifest(tbl_ident_, blob, manifest_size);
    ...
}
else
{
    MappingSnapshot *mapping = cow_meta_.mapper_->GetMapping();
    FilePageId max_fp_id =
        cow_meta_.mapper_->FilePgAllocator()->MaxFilePageId();
    std::string_view snapshot =
        wal_builder_.Snapshot(cow_meta_.root_id_,
                              cow_meta_.ttl_root_id_,
                              mapping,
                              max_fp_id,
                              dict_bytes);
    err = IoMgr()->SwitchManifest(tbl_ident_, snapshot);
    ...
}
```

### FD 缓存与云模式文件名
- `PartitionFiles` 以 `TableIdent`+`FileId` 作为键，无 term 维度。  
```263:268:async_io_manager.h
class PartitionFiles
{
public:
    const TableIdent *tbl_id_ = nullptr;
    std::unordered_map<FileId, LruFD> fds_;
};
```
- 追加/快照均按 `FileId` 和固定名打开 `manifest`/`data_<id>`。  
```1460:1550:async_io_manager.cpp
KvError IouringMgr::AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t manifest_size)
{ ... }
KvError IouringMgr::SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot)
{ ... WriteSnapshot(..., FileNameManifest, snapshot); ... }
```
- 云模式缓存/上传使用相同固定名，不校验 term 后再复用缓存。  
```2034:2085:async_io_manager.cpp
KvError CloudStoreMgr::SwitchManifest(const TableIdent &tbl_id,
                                      std::string_view snapshot)
{
    LruFD::Ref fd_ref = GetOpenedFD(tbl_id, LruFD::kManifest);
    if (fd_ref != nullptr)
    {
        KvError err = CloseFile(std::move(fd_ref));
        ...
    }
    FileKey fkey(tbl_id, ToFilename(LruFD::kManifest));
    bool dequed = DequeClosedFile(fkey);
    ...
    err = UploadFiles(tbl_id, {std::string(FileNameManifest)});
    ...
    EnqueClosedFile(std::move(fkey));
    return KvError::NoError;
}
```
- 云模式打开/下载仅按 `FileId` 和文件名推导，不感知 term。  
```2132:2199:async_io_manager.cpp
int CloudStoreMgr::OpenFile(const TableIdent &tbl_id,
                            FileId file_id,
                            bool direct)
{
    FileKey key = FileKey(tbl_id, ToFilename(file_id));
    if (DequeClosedFile(key))
    { ... }
    // File not exists locally, try to download it from cloud.
    size_t size = EstimateFileSize(file_id);
    int res = ReserveCacheSpace(size);
    ...
    KvError err = DownloadFile(tbl_id, file_id);
    switch (err)
    { ... }
}
```

## 代码参考
- `types.h`：文件名前缀与分隔符  
- `common.h`：文件名解析、`data_<id>` 生成  
- `root_meta.cpp`：manifest 快照编码 (`max_fp_id`、映射)  
- `replayer.cpp`：manifest 重放与基于 `max_fp_id` 的分配器恢复  
- `page_mapper.cpp`：文件页分配器单调递增  
- `write_task.cpp`：manifest 追加/快照写入流程  
- `async_io_manager.h/.cpp`：FD 缓存按 `FileId` 键，读写用固定名  
- `async_io_manager.cpp` (CloudStoreMgr)：云缓存/上传/下载使用同名文件

## 架构说明
- 存储以 `TableIdent`（表名 + 分区 id）确定分区目录，目录内有 `manifest`、`data_<file_id>`，归档文件为 `manifest_<timestamp>`。
- Manifest 日志结构：首个快照记录（头 + `max_fp_id`、字典、映射表），后续可有 WAL 增量。写入根据大小/字典脏状态选择追加或全量。
- Replayer 从最新 manifest 恢复映射与分配器，无跨进程协调；`max_fp_id` 决定下一次分配，映射表标识已用/空闲页。
- 每个分片的 FD 缓存按 `(TableIdent, FileId)` 存储；云模式上传/下载也基于该文件名，无版本区分。
- 云模式缺失文件会从云端下载；本地缓存 LRU 驱逐，不带版本。

## 历史背景（thoughts/）
暂无相关文档。

## 相关研究
无。

## 未解决问题
- 多进程并发写如何协调？文件名、manifest 内容、FD/缓存元数据都没有版本/term 信息。
- 若多进程各自产生 manifest，重新下载时谁覆盖谁？分配器的 `max_fp_id` 如何避免冲突？

## 方案设计（term 通过启动参数注入，而非 KvOptions）

### term 注入与生命周期
- 在构造 `EloqStore` 或调用 `Start(term)` 传入 `uint64_t term`，非云模式默认 0，云部署需为每个实例提供正的 term。
- term 存在 `EloqStore` 成员并提供 getter 给 IO 管理器/Shard；不写入 KvOptions。
- Shard 将 term 传给 `AsyncIoManager`（IouringMgr/CloudStoreMgr），管理器持有 `term_` 用于文件命名和 manifest 校验。

### 带 term 后缀的文件名
- 数据文件：`data_<file_id>_<term>`（分隔符沿用 `_`）。
- 活跃 manifest：`manifest_<term>`。
- 归档：`manifest_<term>_<timestamp>`（仅支持 term-aware 格式）。旧格式 `manifest_<timestamp>` 不再支持解析。
- 工具函数：
  - 扩展 `DataFileName(file_id, term)`；保留 legacy 重载 `DataFileName(file_id)` 用于 term==0 以方便过渡。
  - 新增 `ManifestFileName(term)` 返回 `manifest_<term>`。
  - 新增 `ArchiveName(term, ts)` 返回 `manifest_<term>_<ts>`。**注意**：legacy `ArchiveName(ts)` 重载不再支持。
  - `ParseFileName` 返回 `{type, suffix}` 对，其中 suffix 是第一个分隔符后的所有内容。
  - 新增 `ParseDataFileSuffix(suffix, file_id, term)`：返回 bool；解析 `file_id` 和 `term`，若未带 term（旧格式）则 term=0。
  - 新增 `ParseManifestFileSuffix(suffix, term, timestamp_opt)`：返回 bool；解析 `term`（缺省 term=0）和可选 `timestamp`。
  - **注意**：旧格式 `manifest_<timestamp>` 不再支持解析（已移除）。
- 云端上传/下载、本地打开/创建活跃文件均使用带 term 的文件名。

### Manifest term 校验与下载策略
- 读取 manifest 时：
  - 本地 manifest 的内嵌 term 与进程 term 不一致，则丢弃并尝试下载 `manifest_<term>`；若不存在，下载最新 manifest（term 最大或最近修改），随后执行分配器调整。
  - 若可用的最新 manifest 的 term 大于当前进程 term，直接返回错误（避免旧 term writer 消费未来 term 的数据）。
  - 若不存在 manifest，从空状态启动，并按 term 规则对齐 `max_fp_id`。

### Manifest 中持久化 term 元数据
- 不在 payload 内持久化 `manifest_term`，活跃 manifest 的文件名已包含 term（`manifest_<term>`），校验通过文件名与当前进程 term 完成。
- 持久化 `FileIdTermMapping`：在 `max_fp_id` 和字典之后，总是先写入条目数（varint count），再写 `(file_id, term)` 的 varint64 对（即使FileIdTermMapping映射为空也写 count=0）。 (**注意：这个改动导致无法兼容之前的manifest文件数据**)
- WAL 追加无需变更；term 映射变更时强制写全量快照。
- Replayer：
  - 始终从快照中反序列化 `FileIdTermMapping`（映射段总是存在，count 可以为 0 表示为空）。
  - 若文件名 term（`manifest_<term>`）与进程 term 不一致且云模式：将分配器 `max_fp_id` 提升到下一个 file_id 边界并将页偏移归零，避免冲突（`file_id = (max_fp_id >> shift) + 1; max_fp_id = file_id << shift`）。
  - 将 `FileIdTermMapping` 恢复到 `PartitionFiles` 的 shared_ptr 中（与 Replayer 和其他类共享，便于传递）。

### FileId→term 跟踪
- 新增 `FileIdTermMapping`（unordered_map<FileId,uint64_t>），支持序列化/反序列化。
- 以 `std::shared_ptr<FileIdTermMapping>` 形式存入 `PartitionFiles`（每表），随 manifest 快照持久化。
- shared_ptr 设计允许映射在 PartitionFiles、Replayer 和其他类（如 PageMapper、WriteTask）之间共享，便于传递。
- AppendAllocator 分配到新 file_id 或首次使用该 file_id 时，通过 shared_ptr 记录当前 `term_`。

### FD 缓存的 term 感知
- `LruFD` 增加 `term_`。
- `OpenFD` / `OpenOrCreateFD`：
  - 若已有 FD 但 `term_` 不符当前 `term_`，先关闭再按正确 term 重新打开。
  - `PartitionFiles::fds_` 可继续以 file_id 为键，通过 term 校验触发重开；如需简化也可改为 `(file_id, term)` 作为键。
- `CloudStoreMgr::CloseFile` 入队缓存文件时使用带 term 的文件名；LRU 驱逐/重用需考虑完整文件名（`FileKey` 已包含）。

### 文件名使用更新
- 活跃 manifest 路径用 `ManifestFileName(term_)` 代替 `FileNameManifest`。
- 活跃读写/打开数据文件使用带 term 的 `DataFileName`。
- `ArchiveName` 需要 term 参数；所有调用点必须使用 `ArchiveName(term, ts)`。
- `ParseFileName` 返回 `{type, suffix}`；使用 `ParseDataFileSuffix`（无 term 时设为 0）和 `ParseManifestFileSuffix`（term 默认为 0，timestamp 可选）进行详细解析。

### 启动与串联
- `EloqStore::Start(term)` 持久保存 term，线程/Shard 读取后传给各自的 IO 管理器。
- `AsyncIoManager::Instance` 以及 `IouringMgr`/`CloudStoreMgr` 构造函数接受 term。

### 兼容与过渡
- 读取旧文件/manifest：解析器允许旧名（但 `manifest_<timestamp>` 格式已移除，不再支持）；无 term 的 manifest 视为 `manifest_term=0` 且 `FileIdTermMapping` 为空。解析辅助函数在未带 term 时将 term 设为 0。
- 写入：term != 0 时始终使用带 term 名称/快照；term==0 时保持旧格式。
- `ArchiveName` 不再支持 legacy 单参数版本；所有调用点必须提供 term 参数。

### 冲突规避规则
- manifest 的 term 与当前 term 不匹配（云模式）时，重放后将分配器 `max_fp_id` 提升到下一个 file_id 边界，确保新写入落在新的 file_id 范围。

