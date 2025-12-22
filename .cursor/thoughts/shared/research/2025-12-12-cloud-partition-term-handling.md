---
date: 2025-12-12T18:04:05+08:00
researcher: assistant
git_commit: 30de7d775e927d18c2418a821b8753377ae6cb2c
branch: multi-write-term
repository: eloqstore
topic: "Cloud mode partition writes and manifest/file id handling"
tags: [research, codebase, cloud-store, manifest, file-allocator, io-manager]
status: complete
last_updated: 2025-12-12
last_updated_by: assistant
---

# Research: Cloud mode partition writes and manifest/file id handling

**Date**: 2025-12-12T18:04:05+08:00  
**Researcher**: assistant  
**Git Commit**: 30de7d775e927d18c2418a821b8753377ae6cb2c  
**Branch**: multi-write-term  
**Repository**: eloqstore

## Research Question
User design proposes adding a term suffix to data/manifest filenames and associating term metadata (per file and allocator) 
to prevent cross-process corruption when multiple processes open the same table/partition in cloud mode. 
What does the current codebase do for cloud mode manifests, file naming, file descriptor caching, and file-page allocation?

## Summary
Current implementation uses fixed filenames (`manifest`, `data_<file_id>`) without any term suffixing. 
File descriptor caches (`PartitionFiles`, `LruFD`) are keyed only by `TableIdent` and `FileId`. 
Manifests store a snapshot header containing `max_fp_id`, dictionary bytes, and mapping table, and write-ahead logs append mapping deltas; 
there is no concept of term. Replayer rebuilds allocator state solely from `max_fp_id` and mapping contents. 

Cloud mode downloads/uploads files under the same names and caches them locally but does not track term or reopen files based on a version. 
File page allocation resumes from the maximum observed `file_page_id` in the manifest, not guarded against concurrent writers in separate processes.

## Detailed Findings

### File naming and parsing
- Filenames use static prefixes `manifest` and `data` separated by `FileNameSeparator '_'`; data files are formatted as `data_<file_id>`. There is no term or version component in filenames.  
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

### Manifest structure and replay
- Manifest snapshot layout: header plus encoded `max_fp_id`, dictionary length/content, then serialized mapping table. Logging via `ManifestBuilder` appends mapping deltas. No term or version fields are stored.  
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
- Replayer rebuilds the allocator using the restored `max_fp_id` and mapping table, assuming monotonic allocation across processes; no extra guard/versioning.  
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

### File-page allocation
- `FilePageAllocator` increments a global `max_fp_id_` per allocation; it has no per-term segmentation.  
```295:318:page_mapper.cpp
FilePageId FilePageAllocator::Allocate()
{
    return max_fp_id_++;
}
```

### Manifest writing
- `WriteTask::FlushManifest` decides between appending a WAL record or writing a full snapshot; both paths write through `AppendManifest`/`SwitchManifest` without any term parameter.  
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

### File descriptor cache and naming in cloud mode
- `PartitionFiles` maps `FileId` to `LruFD`, keyed only by table id and numeric file id; there is no term dimension.  
```263:268:async_io_manager.h
class PartitionFiles
{
public:
    const TableIdent *tbl_id_ = nullptr;
    std::unordered_map<FileId, LruFD> fds_;
};
```
- Append and snapshot operations open files by `FileId` and fixed names (`manifest`, `data_<id>`).  
```1460:1550:async_io_manager.cpp
KvError IouringMgr::AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t manifest_size)
{ ... }
KvError IouringMgr::SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot)
{ ... WriteSnapshot(..., FileNameManifest, snapshot); ... }
```
- Cloud mode uses the same fixed filenames when caching and uploading; it ensures a fresh manifest is uploaded but does not version filenames or check any term before reusing cached files.  
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
- Download/open logic in cloud mode selects files solely by `FileId` and filename computed via `ToFilename`, without any term awareness.  
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

## Code References
- `types.h` – file name prefixes and separator.  
- `common.h` – filename parsing and `data_<id>` builder.  
- `root_meta.cpp` – manifest snapshot encoding (`max_fp_id`, mapping).  
- `replayer.cpp` – manifest replay and allocator reconstruction from `max_fp_id`.  
- `page_mapper.cpp` – file-page allocator monotonic increment.  
- `write_task.cpp` – manifest append/snapshot writing flow.  
- `async_io_manager.h/.cpp` – FD cache keyed by `FileId`; manifest/data open/write using fixed names.  
- `async_io_manager.cpp` (CloudStoreMgr) – cloud cache/upload/download using same filenames.

## Architecture Documentation
- Storage uses `TableIdent` (table name + partition id) to derive a directory per partition. Files inside are `manifest` and `data_<file_id>`; archives are `manifest_<timestamp>`.
- Manifests are log-structured: a snapshot record (header + payload containing `max_fp_id`, dictionary, mapping table) followed by optional WAL records with mapping deltas. Writers choose append vs full snapshot based on size/dirty dictionary.
- Replayer rebuilds mapping and allocator state from the latest manifest without cross-process coordination. `max_fp_id` drives the next allocation; mapping table identifies free vs mapped pages.
- FD caching per shard maps `(TableIdent, FileId)` to `LruFD`; cache eviction and cloud upload/download flows rely on filename derived from `FileId` only.
- Cloud mode downloads missing files and uploads manifests/data using the same filenames as local; local cache eviction is LRU without versioning beyond archive timestamps.

## Historical Context (from thoughts/)
No relevant documents found.

## Related Research
None recorded in this repository.

## Open Questions
- How are concurrent writers across processes coordinated today? No explicit term/versioning is present in filenames, manifest content, or FD/cache metadata.
- If multiple processes produce manifests independently, which manifest wins when re-downloaded, and how is allocator `max_fp_id` prevented from conflicting across processes?

## Proposed Design (with term injected at startup, not in KvOptions)

### Term injection and lifecycle
- Accept `uint64_t term` as an argument when constructing `EloqStore` or when calling `Start(term)`. Default to `0` for non-cloud usage; cloud deployments must supply a positive term per process/instance.
- Store the term in `EloqStore` (member field) and expose a getter for IO managers/shards; do **not** persist it in options.
- Shards pass the term to their `AsyncIoManager` (IouringMgr/CloudStoreMgr). Managers keep a `term_` member used for filename derivation and manifest validation.

### Filename scheme with term suffix
- Data files: `data_<file_id>_<term>` (using existing `FileNameSeparator '_'`).
- Manifest: `manifest_<term>` for the active manifest.
- Archives: `manifest_<term>_<timestamp>` (term-aware format only). Legacy `manifest_<timestamp>` format is NOT supported for parsing.
- Helpers:
  - Extend `DataFileName(file_id, term)`; keep legacy overload `DataFileName(file_id)` for term==0 to ease transition.
  - Add `ManifestFileName(term)` returning `manifest_<term>`.
  - Add `ArchiveName(term, ts)` returning `manifest_<term>_<ts>`. **Note**: Legacy `ArchiveName(ts)` overload is NOT supported.
  - `ParseFileName` returns `{type, suffix}` pair where suffix is everything after first separator.
  - Add `ParseDataFileSuffix(suffix, file_id, term)`: returns bool; parses file_id and term, setting term=0 when absent (legacy).
  - Add `ParseManifestFileSuffix(suffix, term, timestamp_opt)`: returns bool; parses term (term=0 when absent) and optional timestamp.
  - **Note**: Legacy `manifest_<timestamp>` format is NOT supported for parsing (removed).
- Cloud upload/download and local open/create paths always use the term-aware names for the active generation.

### Manifest term validation and download policy
- When loading a manifest:
  - If a local manifest’s embedded term (see next section) mismatches the process term, discard it and attempt to download `manifest_<term>`. If absent, download the latest manifest (highest term or latest mod time) and then adjust allocator (see below).
  - If the latest available manifest has a term greater than the current process term, return an error instead of replaying (avoid consuming a future term with an older writer).
  - If no manifest is found, start from empty state with `max_fp_id` aligned to term boundary policy.

### Persisted term metadata in manifest
- Do **not** persist `manifest_term` inside the payload; the active manifest filename already carries the term (`manifest_<term>`). Term is validated via filename and the current process term.
- Persist `FileIdTermMapping` after `max_fp_id` and dictionary: always write a varint count, then entries `(file_id, term)` as varint64 pairs (count can be 0 for an empty file_id_mapping). (**Notice: This modification is not compatible with the previous manifest file data.**)
- WAL append records do not need changes; term mapping changes force a full snapshot.
- Replayer:
  - Always deserialize `FileIdTermMapping` from the snapshot mapping section (count may be 0 for an empty mapping).
  - If filename term (from `manifest_<term>`) differs from process term in cloud mode: adjust allocator `max_fp_id` by bumping the file-id portion by +1 and zeroing the page offset to avoid collisions (`file_id = (max_fp_id >> shift) + 1; max_fp_id = file_id << shift`).
  - Restore `FileIdTermMapping` into a shared_ptr stored in `PartitionFiles` (shared with `Replayer` and other classes for easy passing).

### FileId→term tracking
- Introduce `FileIdTermMapping` (unordered_map<FileId,uint64_t>) with serialize/deserialize.
- Store the mapping as `std::shared_ptr<FileIdTermMapping>` inside `PartitionFiles` (per table) and persist via manifest snapshot.
- The shared_ptr design allows the mapping to be easily passed between PartitionFiles, Replayer, and other classes (e.g., PageMapper, WriteTask) that need access to it.
- On new file allocation (AppendAllocator) and first use of a `file_id`, record `term_` into the mapping via the shared_ptr.

### FD cache with term awareness
- Extend `LruFD` to store `term_`.
- When `OpenFD`/`OpenOrCreateFD` is requested:
  - If an FD exists but its `term_` differs from current `term_`, close it and reopen the correct term file.
  - Keying in `PartitionFiles::fds_` can remain by `file_id`, but `term_` mismatch triggers reopen; optionally key by `(file_id, term)` if simplicity is preferred.
- `CloudStoreMgr::CloseFile` enqueues cached file with its term-aware filename; LRU eviction and reuse must consider the full filename (already part of `FileKey`).

### Filename usage updates
- Replace uses of `FileNameManifest` in active manifest paths with `ManifestFileName(term_)`.
- Replace `DataFileName(file_id)` for active writes/opens with term-aware overload.
- `ArchiveName` requires term parameter; all call sites must use `ArchiveName(term, ts)`.
- `ParseFileName` returns `{type, suffix}`; use `ParseDataFileSuffix` (term defaults to 0 when absent) and `ParseManifestFileSuffix` (term defaults to 0, timestamp optional) for detailed parsing.

### Startup and shard wiring
- `EloqStore::Start(term)` stores `term_`, threads/shards read it and pass to their IO manager instances.
- `AsyncIoManager::Instance` and constructors for `IouringMgr`/`CloudStoreMgr` accept `term`.

### Compatibility/transition
- Reading legacy files/manifests: parser allows legacy names (except `manifest_<timestamp>` format which is removed); manifest replay without term sets `manifest_term=0` and empty `FileIdTermMapping`. For parsing helpers, term defaults to 0 when absent.
- Writing always uses term-aware names/snapshots when term != 0. For term==0 (non-cloud), filenames match legacy behavior.
- `ArchiveName` no longer supports legacy single-parameter version; all call sites must provide term parameter.

### Collision avoidance rule
- When manifest term != process term (cloud mode): after replay, bump allocator `max_fp_id` to next file-id boundary as described above, ensuring new writes land in a new file range for this term.

