---
name: Term-Aware Cloud Manifest Implementation
overview: Implement term-aware filenames, manifest serialization with FileIdTermMapping, replayer term validation and allocator bumping, FD cache term awareness, and cloud download logic to prevent cross-process corruption in cloud mode.
todos:
  - id: filename-helpers
    content: Implement term-aware filename helpers and enhanced parser in common.h
    status: completed
  - id: fileid-term-mapping
    content: Define FileIdTermMapping structure with serialize/deserialize and integrate into PartitionFiles
    status: completed
  - id: manifest-serialization
    content: Update ManifestBuilder::Snapshot to serialize FileIdTermMapping after mapping table
    status: completed
    dependencies:
      - fileid-term-mapping
  - id: replayer-deserialize
    content: Update Replayer::DeserializeSnapshot to deserialize FileIdTermMapping
    status: completed
    dependencies:
      - manifest-serialization
  - id: replayer-term-validation
    content: Add term validation and allocator bumping logic in Replayer::GetMapper
    status: in_progress
    dependencies:
      - replayer-deserialize
  - id: lrufd-term
    content: Add term member to LruFD and implement reopen-on-mismatch in OpenFD/OpenOrCreateFD
    status: pending
  - id: tofilename-term
    content: Update CloudStoreMgr::ToFilename to use term-aware helpers
    status: pending
    dependencies:
      - filename-helpers
      - lrufd-term
  - id: cloud-getmanifest
    content: Update CloudStoreMgr::GetManifest to select manifest by term and handle term validation
    status: pending
    dependencies:
      - filename-helpers
      - replayer-term-validation
  - id: term-recording
    content: Update file allocation paths to record term in FileIdTermMapping when new file_id is first used
    status: pending
    dependencies:
      - fileid-term-mapping
  - id: archive-naming
    content: Update CreateArchive to use term-aware archive names
    status: pending
    dependencies:
      - filename-helpers
  - id: filename-usage
    content: Replace all FileNameManifest and DataFileName usages with term-aware versions in active paths
    status: pending
    dependencies:
      - filename-helpers
      - tofilename-term
  - id: prewarm-filegc
    content: Update prewarm_task.cpp and file_gc.cpp to parse term-aware filenames
    status: pending
    dependencies:
      - filename-helpers
  - id: manifest-writing
    content: Update WriteTask to detect FileIdTermMapping changes and force full snapshot
    status: pending
    dependencies:
      - fileid-term-mapping
      - manifest-serialization
  - id: tests
    content: Add comprehensive tests for filename parsing, manifest serialization, replayer term logic, and cloud integration
    status: pending
    dependencies:
      - filename-helpers
      - replayer-term-validation
      - cloud-getmanifest
---

# Term-Aware Cloud Manifest & File Handling Implementation

## Overview

Implement term-aware data/manifest filenames and manifest parsing to prevent cross-process corruption when multiple processes open the same table/partition in cloud mode. Term is injected at startup (not in KvOptions), filename carries term, and FileIdTermMapping is persisted in manifest payload.

## Implementation Plan

### Phase 1: Filename Helpers & Parsing (common.h)

**1.1 Extend filename parsing**

- Update `ParseFileName` to return `{type, id_or_ts, term_opt}` tuple
- Support parsing:
- Legacy: `data_<id>`, `manifest`, `manifest_<ts>` → term=0
- Term-aware: `data_<id>_<term>`, `manifest_<term>`, `manifest_<term>_<ts>`
- Handle multiple underscores correctly (find last separator for term)

**1.2 Add term-aware filename generators**

- `DataFileName(FileId file_id, uint64_t term)` - returns `data_<id>_<term>` when term!=0, `data_<id>` when term==0
- Keep legacy `DataFileName(FileId)` overload for backward compatibility
- `ManifestFileName(uint64_t term)` - returns `manifest_<term>` when term!=0, `manifest` when term==0
- `ArchiveName(uint64_t term, uint64_t ts)` - returns `manifest_<term>_<ts>` when term!=0, `manifest_<ts>` when term==0
- Keep legacy `ArchiveName(uint64_t ts)` overload

**Files**: `common.h`

### Phase 2: FileIdTermMapping Structure

**2.1 Define FileIdTermMapping**

- Add `FileIdTermMapping` type: `std::unordered_map<FileId, uint64_t>`
- Add serialize/deserialize methods:
- `Serialize`: varint count + varint64 pairs `(file_id, term)`
- `Deserialize`: read count, then pairs; return empty map if absent/malformed

**2.2 Integrate into PartitionFiles**

- Store `FileIdTermMapping` in `shard_ptr` (shared pointer) for easy passing between classes
- Add `std::shared_ptr<FileIdTermMapping> file_id_term_mapping_` member to `PartitionFiles` class
- Initialize as empty shared_ptr or create empty mapping on construction
- Provide getter/setter methods to access/modify the mapping
- Integrate into `Replayer` and other classes that need access to the mapping:
- Replayer can receive/restore the mapping from shard_ptr
- Other classes (e.g., PageMapper, WriteTask) can access via PartitionFiles or shard_ptr
- This design allows the mapping to be shared and easily passed between PartitionFiles, Replayer, and other components

**Files**: `async_io_manager.h` (PartitionFiles), `replayer.h`, `replayer.cpp`, new helper file or `common.h` for serialization

### Phase 3: Manifest Payload Serialization (root_meta.cpp)

**3.1 Update ManifestBuilder::Snapshot**

- After `mapping->Serialize(buff_)`, append `FileIdTermMapping`:
- PutVarint64 count
- For each `(file_id, term)` pair: PutVarint64(file_id), PutVarint64(term)
- If mapping is empty, write count=0 (backward compatible)

**3.2 Update Replayer::DeserializeSnapshot**

- After reading mapping table, check if snapshot has remaining data
- If yes, deserialize `FileIdTermMapping`:
- GetVarint64 count
- Read count pairs of (file_id, term)
- If absent or malformed, treat as empty mapping (term=0 for all files)
- Store in shard_ptr (shared with PartitionFiles) for later use in GetMapper

**Files**: `root_meta.cpp`, `replayer.cpp`, `replayer.h`

### Phase 4: Replayer Term Validation & Allocator Bumping

**4.1 Add term tracking to Replayer**

- Add `uint64_t manifest_term_` member (extracted from filename)
- Access `FileIdTermMapping` via shard_ptr (shared with PartitionFiles)
- Add `uint64_t process_term_` parameter to constructor or GetMapper

**4.2 Update GetMapper with term logic**

- Accept `process_term` parameter (from IO manager)
- Access `FileIdTermMapping` from shard_ptr (via PartitionFiles or direct access)
- If `manifest_term != process_term` (and cloud mode):
- Bump allocator: `file_id = (max_fp_id_ >> pages_per_file_shift) + 1; max_fp_id_ = file_id << pages_per_file_shift`
- This ensures new allocations start at next file boundary
- Restore/update `FileIdTermMapping` in shard_ptr (shared with PartitionFiles) so it's accessible to all classes

**4.3 Manifest filename term extraction**

- Update `GetManifest` in IO managers to extract term from filename
- Pass manifest term to Replayer (via new method or parameter)

**Files**: `replayer.h`, `replayer.cpp`, `async_io_manager.cpp` (GetManifest methods)

### Phase 5: LruFD Term Awareness

**5.1 Add term to LruFD**

- Add `uint64_t term_` member to `LruFD` class
- Initialize to 0 in constructor (legacy default)
- Update constructor to accept optional term parameter

**5.2 Update OpenFD/OpenOrCreateFD**

- Check if existing FD has different term than current `term_`
- If mismatch: close existing FD, clear from cache, reopen with correct term filename
- Set `term_` on newly opened FDs

**5.3 Update ToFilename in CloudStoreMgr**

- Make `ToFilename(FileId file_id)` use term-aware helpers
- Use `DataFileName(file_id, term_)` and `ManifestFileName(term_)`
- Update all call sites

**Files**: `async_io_manager.h` (LruFD), `async_io_manager.cpp` (OpenFD, OpenOrCreateFD, ToFilename)

### Phase 6: Cloud Download & Manifest Selection

**6.1 Update CloudStoreMgr::GetManifest**

- Try to download `manifest_<term>` first (where term is process term)
- If not found, list all manifests and find latest:
- Parse filenames to extract terms
- If latest term > process term: return error (avoid future term consumption)
- If latest term < process term: download it, will trigger allocator bump in replayer
- Extract term from selected manifest filename
- Pass term to Replayer

**6.2 Update CloudStoreMgr::DownloadFile**

- Use term-aware filename when downloading
- For data files: `DataFileName(file_id, term_)`
- For manifest: `ManifestFileName(term_)` (or specific term if known)

**6.3 Update CloudStoreMgr::OpenFile**

- Use term-aware `ToFilename` (already updated in Phase 5)
- Ensure FileKey uses full term-aware filename

**Files**: `async_io_manager.cpp` (CloudStoreMgr methods)

### Phase 7: File Allocation & Term Recording

**7.1 Update AppendAllocator**

- On first allocation for a new `file_id`, record term in FileIdTermMapping (via shard_ptr in PartitionFiles)
- Access PartitionFiles via PageMapper or pass term through allocation path
- Access the mapping via shard_ptr stored in PartitionFiles
- Ensure term is recorded when `file_id` is first used

**7.2 Update file creation paths**

- When creating new data files, ensure term is recorded in mapping
- Update `CreateFile` and related methods

**Files**: `page_mapper.cpp`, `async_io_manager.cpp` (file creation)

### Phase 8: Archive Naming

**8.1 Update CreateArchive**

- Use `ArchiveName(term_, ts)` when term != 0
- Use legacy `ArchiveName(ts)` when term == 0
- Ensure archive filenames are term-aware

**8.2 Update archive reading**

- Support both `manifest_<term>_<ts>` and legacy `manifest_<ts>`
- Parse term from archive filename if present, default to 0

**Files**: `async_io_manager.cpp` (CreateArchive), `file_gc.cpp` (archive parsing)

### Phase 9: Filename Usage Updates

**9.1 Replace FileNameManifest constants**

- Find all uses of `FileNameManifest` for active manifest operations
- Replace with `ManifestFileName(term_)` calls
- Keep `FileNameManifest` for legacy compatibility checks

**9.2 Replace DataFileName calls**

- Update active write/open paths to use term-aware `DataFileName(file_id, term_)`
- Archive and read paths can use legacy names (backward compatible)

**9.3 Update prewarm and file classification**

- Update `prewarm_task.cpp` to parse term-aware filenames
- Update `file_gc.cpp` `ClassifyFiles` to handle term-aware names
- Ensure both legacy and term-aware names are recognized

**Files**: `async_io_manager.cpp`, `prewarm_task.cpp`, `file_gc.cpp`, `write_task.cpp`

### Phase 10: Manifest Writing & Term Mapping Changes

**10.1 Detect term mapping changes**

- Track when FileIdTermMapping changes (new file_id added with different term)
- Force full snapshot when mapping changes (cannot use WAL append)
- Update `WriteTask::FlushManifest` logic

**10.2 Update manifest writing paths**

- Ensure `SwitchManifest` uses term-aware filename
- Ensure `AppendManifest` works with term-aware manifest (WAL format unchanged)

**Files**: `write_task.cpp`, `async_io_manager.cpp`

### Phase 11: Testing

**11.1 Filename parsing tests**

- Test `ParseFileName` with all formats (legacy + term-aware)
- Test filename generators with term=0 and term>0
- Test archive name generation

**11.2 Manifest serialization tests**

- Test roundtrip: serialize FileIdTermMapping, deserialize, verify
- Test backward compatibility: deserialize manifest without mapping
- Test with empty mapping

**11.3 Replayer tests**

- Test allocator bumping when manifest_term != process_term
- Test error when manifest_term > process_term
- Test term mapping restoration

**11.4 Cloud integration tests**

- Test manifest download with term selection
- Test file open with term-aware names
- Test archive creation with term
- Test backward compatibility with legacy files

**Files**: New test files or extend `tests/manifest.cpp`, `tests/cloud.cpp`

## Key Design Decisions

1. **Term not in payload**: Manifest filename carries term (`manifest_<term>`), validated against process term. No `manifest_term` field in payload.

2. **Backward compatibility**: All legacy filenames (`data_<id>`, `manifest`, `manifest_<ts>`) are treated as term=0. New code reads both formats.

3. **Allocator bumping**: When manifest term < process term, bump `max_fp_id` to next file boundary to avoid collisions. Formula: `file_id = (max_fp_id >> shift) + 1; max_fp_id = file_id << shift`.

4. **Term mapping persistence**: Only in full snapshots, not WAL records. Mapping changes force full snapshot.

5. **FD cache**: Keyed by `file_id` only, but `term_` mismatch triggers reopen. FileKey already includes full filename, so term is implicitly part of key.

## Files to Modify

- `common.h` - filename helpers and parsing
- `async_io_manager.h` - LruFD term, PartitionFiles mapping
- `async_io_manager.cpp` - IO manager implementations, ToFilename, GetManifest, OpenFD
- `root_meta.cpp` - manifest serialization
- `replayer.h`, `replayer.cpp` - term validation, allocator bumping
- `page_mapper.cpp` - term recording on allocation
- `write_task.cpp` - manifest writing with mapping
- `file_gc.cpp` - archive parsing
- `prewarm_task.cpp` - filename parsing
- `tests/` - comprehensive test coverage

## Migration Notes

- Term=0 (default) maintains legacy behavior: filenames match current format
- Cloud deployments must set term > 0 per process/instance
- Existing manifests without mapping are treated as term=0 for all files
- Archive files can be in either format; readers support both