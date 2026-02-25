# Branch Feature Design Document

## Overview

The Branch feature provides lightweight data isolation by creating independent metadata (manifest) that references parent table's data files. Similar to git branches.

---

## 1. File Naming Conventions

### Branch Name Rules
- **Validation**: Only `[a-zA-Z0-9_-]+` (alphanumeric, underscore, hyphen)
- **Reserved**: `"development"` (primary branch)
- **Uniqueness**: Branch name must be unique within the table

### Manifest Files

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Development manifest | `manifest_development_<term>` | `manifest_development_5` | |
| Development manifest (legacy) | `manifest_<term>` | `manifest_5` | Backward compatible |
| Branch manifest | `manifest_<branch_name>_<term>` | `manifest_feature_5` | |
| Development archive | `manifest_development_<term>_<ts>` | `manifest_development_5_1234567890` | |
| Development archive (legacy) | `manifest_<term>_<ts>` | `manifest_5_1234567890` | Backward compatible |
| Branch archive | `manifest_<branch_name>_<term>_<ts>` | `manifest_feature_5_1234567890` | |

### Data Files

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Development data | `data_<file_id>_development_<term>` | `data_10_development_5` | |
| Development data (legacy) | `data_<file_id>_<term>` | `data_10_5` | Backward compatible |
| Branch data | `data_<file_id>_<branch_name>_<term>` | `data_10_feature_5` | |

### CURRENT_TERM Files

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Development | `CURRENT_TERM.development` | `CURRENT_TERM.development` | |
| Development (legacy) | `CURRENT_TERM` | `CURRENT_TERM` | Backward compatible |
| Branch | `CURRENT_TERM.<branch_name>` | `CURRENT_TERM.feature` | |

---

## 2. Parsing Functions (Backward Compatible)

### Manifest Parsing

```
manifest_5                → {branch_name: "development", term: 5, timestamp: null}    (legacy)
manifest_development_5   → {branch_name: "development", term: 5, timestamp: null} (development)
manifest_feature_5       → {branch_name: "feature", term: 5, timestamp: null}      (branch)
manifest_5_123           → {branch_name: "development", term: 5, timestamp: 123}    (legacy archive)
manifest_development_5_123 → {branch_name: "development", term: 5, timestamp: 123} (archive)
manifest_feature_5_123  → {branch_name: "feature", term: 5, timestamp: 123}        (branch archive)
```

### Data File Parsing

```
data_10_5                → {file_id: 10, branch_name: "development", term: 5}  (legacy)
data_10_development_5    → {file_id: 10, branch_name: "development", term: 5}
data_10_feature_5       → {file_id: 10, branch_name: "feature", term: 5}
```

### CURRENT_TERM Parsing

```
CURRENT_TERM             → branch_name: "development" (legacy)
CURRENT_TERM.development → branch_name: "development"
CURRENT_TERM.feature    → branch_name: "feature"
```

### Error Handling

| Case | Behavior |
|------|----------|
| Malformed manifest filename | Ignore file, log warning |
| Malformed data filename | Ignore file, log warning |
| Invalid branch_name (contains invalid chars) | Return error |
| Reserved branch_name (except "development") | Return error |
| Branch_name="development" conflicts | Prefer explicit `manifest_development_<term>` over legacy formats |

---

## 3. Branch Term

| Property | Value |
|----------|-------|
| Independence | Each branch has independent term counter |
| Initial value | 0 (when branch created) |
| Increment | On write to that branch with new term |
| Storage | `CURRENT_TERM.<branch_name>` file |

---

## 4. Archive Retention

| Property | Value |
|----------|-------|
| Policy | Same for all branches |
| Config | `num_retained_archives` (global) |
| Tracking | Per-branch archive list |

---

## 5. Branch Limit

| Property | Value |
|----------|-------|
| Limit | None (unlimited branches) |
| Name allocation | User-provided, validated, unique |
| File ID allocation | Per-branch, independent file_id counters |

---

## 6. Data Structures

### Current Manifest Metadata Format (Existing)

**Manifest Snapshot Format** (`include/storage/root_meta.h`):
```
Header :  [ Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) ]
Body   :  [ MaxFpId(8B) | DictLen(4B) | dict_bytes(bytes) |
             mapping_bytes_len(4B) | mapping_tbl(varint64...) |
             Serialized FileIdTermMapping bytes(4B|varint64...) ]
```

**Manifest Log (Append) Format**:
```
Header  :  [ Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B) ]
LogBody :  [ mapping_bytes_len(4B) | mapping_bytes(varint64...) |
             Serialized FileIdTermMapping bytes(4B|varint64...) ]
```

**Current Data Structure** (`include/common.h` line 21):
```cpp
using FileIdTermMapping = absl::flat_hash_map<FileId, uint64_t>;  // file_id → term
```

---

### Proposed New Manifest Metadata Format

#### FileIdBranchTermMapping

```cpp
struct FileBranchTerm {
    std::string branch_name;  // "development", "feature", etc.
    uint64_t term;
};

using FileIdBranchTermMapping = absl::flat_hash_map<FileId, FileBranchTerm>;
```

**Note**: Large `FileIdBranchTermMapping` can be optimized during manifest snapshot operations to reduce memory footprint.

#### Manifest Metadata Format

```cpp
struct ManifestMetadata {
    std::string branch_name;              // unique identifier (e.g., "development", "feature")
    uint64_t term;                         // current term for this branch
    PageId root;                           // B+ tree root
    PageId ttl_root;                       // TTL index root
    FileIdBranchTermMapping file_refs;     // files accessible by this branch
};
```

---

#### Key Differences

| Aspect | Current Format | Proposed Format |
|--------|---------------|-----------------|
| Branch Identification | Not supported | `branch_name` field |
| Branch Term | Global term | Per-branch term |
| File Mapping | `FileId → term` | `FileId → {branch_name, term}` |

---

## 7. Operations

### Create Branch

```
Input: parent_table, branch_name
Output: branch_name

Assumptions:
- Parent branch is not being written during branch creation
- Branch name is validated and unique

1. Read parent's current manifest
2. Validate branch_name (alphanumeric, underscore, hyphen only)
3. Check branch_name uniqueness (must not exist)
4. Create branch manifest:
   - branch_name = input_name
   - term = 0 (new branch starts at term 0)
   - root = parent's root
   - file_refs = COPY of parent's file_refs (inherits all references)
5. Write manifest_<branch_name>_<term>
6. Create CURRENT_TERM.<branch_name> with content "0"
7. Return branch_name
```

### Write to Branch

```
Input: branch_name, data
Output: written data

1. Allocate new file_id
2. Write data to: data_<file_id>_<branch_name>_<term>
3. Update file_refs: file_id → term
4. Append to manifest_<branch_name>_<term>
5. Increment branch's term
6. Update CURRENT_TERM.<branch_name>
```

### Delete Branch

```
Input: branch_name (cannot delete "development")

Assumptions:
- Branch being deleted is not actively being written to

1. Delete manifest_<branch_name>_<term>
2. Delete CURRENT_TERM.<branch_name>
3. DO NOT delete data files (may be referenced elsewhere)
4. Orphaned data files will be cleaned by GC
```

### Open Branch

```
Input: branch_name
Output: branch manifest

1. Find manifest_<branch_name>_<term> (latest by term)
2. Parse manifest metadata (branch_name, file_refs)
3. Load mapping snapshot from root
4. Return branch context
```

---

## 8. GC with Cross-Branch References

### Classification

```
manifest_development_5          → Development manifest, branch=development, term=5
manifest_feature_5              → Branch manifest, branch=feature, term=5
manifest_development_5_1234567890 → Development archive, ts=1234567890
manifest_feature_5_1234567890  → Branch archive, branch=feature, ts=1234567890
data_10_development_5          → Development data, file_id=10, term=5
data_10_feature_5             → Branch data, file_id=10, branch=feature, term=5
```

### Deletion Algorithm

```
1. Collect all active manifests (development + all branches)
2. Build reference map:
   For each manifest:
       For each (file_id → term) in file_refs:
           referenced_files[file_id].insert(manifest.branch_name)

3. For each data file (file_id, branch_name, term):
       if file_id in referenced_files and branch_name in referenced_files[file_id]:
           KEEP (at least one branch references it)
       else:
           DELETE (orphaned)
```

---

## 9. Cloud Storage Structure

```
bucket/prefix/table_name.partition_id/
├── CURRENT_TERM                  # Development (legacy)
├── CURRENT_TERM.development     # Explicit development
├── CURRENT_TERM.feature         # Branch feature
├── CURRENT_TERM.hotfix          # Branch hotfix
├── manifest_development_5        # Development manifest (term=5)
├── manifest_feature_3            # Branch feature (term=3)
├── manifest_feature_5            # Branch feature (term=5)
├── manifest_hotfix_2             # Branch hotfix (term=2)
├── manifest_development_5_1234567890  # Development archive
├── manifest_feature_5_1234567890      # Branch feature archive
├── data_0_development_5          # Development data
├── data_1_development_5          # Development data
├── data_0_feature_3              # Branch feature data
├── data_0_hotfix_2               # Branch hotfix data
└── ...
```

---

## 10. Backward Compatibility

| Old Format | New Interpretation | Status |
|------------|-------------------|--------|
| `manifest_5` | Development manifest (branch=development, term=5) | Works |
| `manifest_5_1234567890` | Development archive | Works |
| `data_10_5` | Development data (file_id=10, term=5) | Works |
| `CURRENT_TERM` | Development branch term | Works |

---

## 11. Implementation Tasks

### Phase 1: Infrastructure
- Add request classes: BranchRequest, GlobalBranchRequest, DeleteBranchRequest
- Add file naming functions: BranchManifestFileName, BranchDataFileName, BranchCurrentTermFileName
- Add parsing functions: ParseManifestFilename, ParseDataFilename, ParseCurrentTermFilename
- Add helper functions: IsBranchManifest, IsBranchArchive, IsBranchDataFile
- Implement branch_name validation (alphanumeric, underscore, hyphen)

### Phase 2: Manifest Updates
- Add branch_name to manifest structure
- Update serialization/deserialization
- Update to FileIdBranchTermMapping (file_id → {branch_name, term})

### Phase 3: Branch Operations
- BackgroundWrite::CreateBranch()
- IouringMgr::CreateBranch(), CloudStoreMgr::CreateBranch()
- Handle branch requests in Shard::HandleRequest()
- CURRENT_TERM management per branch
- DeleteBranch implementation

### Phase 4: Read/Write
- Read path: find manifest by branch_name
- Write path: include branch_name in data filename
- Term management per branch

### Phase 5: Archive with Branch
- Archive naming: manifest_<branch_name>_<term>_<ts>
- Archive creation includes branch_name
- GC archive protection per branch

### Phase 6: GC
- Classify files including branch detection
- Load all branch manifests
- Build reference map
- Safe deletion with cross-branch check

### Phase 7: Testing
- Unit tests for parsing (legacy + branch formats)
- Integration: create/read/write/delete branches
- GC: verify cross-branch protection
- Cloud mode tests
- Backward compatibility tests

#### Specific Test Cases

**Backward Compatibility**
- Legacy table with `manifest_5`, `data_10_5`, `CURRENT_TERM` opens correctly
- Mixed legacy and new formats in same table
- Upgrade path: legacy table → add first branch

**Parsing**
- Valid manifest names: `manifest_5`, `manifest_development_5`, `manifest_feature_5`
- Valid data names: `data_10_5`, `data_10_development_5`, `data_10_feature_5`
- Malformed names: `manifest_`, `data_abc_5`, etc. (should be ignored)
- Edge cases: branch_name=development vs legacy (explicit wins)

**Branch Operations**
- Create branch from development
- Create branch from another branch
- Write to branch: file_id allocation is per-branch
- Read from branch: inherited + own data visible
- Delete branch: manifests removed, data kept until GC
- Delete branch while parent still exists

**Concurrency**
- Parent writes do not affect newly created branch
- Branch deletion does not block other branches
- Multiple branches write simultaneously

**GC**
- Orphaned data files cleaned after branch deletion
- Cross-branch references prevent deletion (data_10_feature_5 referenced by branch hotfix)
- Archives respect `num_retained_archives` per branch
- GC with 100+ branches: performance and correctness

**Failure Recovery**
- Partial branch creation (manifest written, CURRENT_TERM missing)
- Corrupted manifest in one branch doesn't affect others
- Missing CURRENT_TERM file handling

---

## 12. Key Files to Modify

| File | Changes |
|------|---------|
| include/eloq_store.h | Request classes |
| include/common.h | Naming/parsing functions |
| include/types.h | Constants |
| include/storage/root_meta.h | Manifest structure |
| src/storage/shard.cpp | Request handling |
| src/tasks/background_write.cpp | CreateBranch() |
| src/async_io_manager.cpp | Branch I/O |
| src/file_gc.cpp | Cross-branch GC |
| src/replayer.cpp | Manifest replay with branch |

---

## 13. Operational Limits and Management

### Branch Management

| Operation | Implementation |
|-----------|----------------|
| List branches | Handled by `eloqctl` (outside of store) |
| Get branch metadata | Handled by `eloqctl` (outside of store) |
| Merge branch | Not implemented (future consideration) |

### Resource Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Max branches | Effectively unlimited | Limited by filename length and uniqueness |
| Max file_refs per manifest | Memory-dependent | Optimize via manifest snapshot |
| Storage overhead per branch | ~1 manifest file + CURRENT_TERM file | Plus per-branch data files |
| Manifest loading time | O(n) where n = number of branches | GC loads all branch manifests |

### Performance Considerations

| Aspect | Impact | Mitigation |
|--------|--------|-----------|
| GC with many branches | O(n*m) complexity (n=files, m=branches) | Parallel manifest loading, indexed reference map |
| FileIdBranchTermMapping size | Memory grows with inherited + own files | Manifest snapshot can compress/deduplicate |
| Branch creation latency | O(1) - copy parent manifest metadata | Minimal impact |
| Archive retention with active branches | Global quota may be consumed unevenly | Monitor per-branch archive count |

---

## Summary

| Aspect | Decision |
|--------|----------|
| Development files | Include branch_name=development (manifest_development_5, data_10_development_5) |
| Legacy fallback | Old format still works |
| Branch identification | branch_name (user-provided, validated, unique) |
| Branch term | Independent, starts at 0 |
| Archive | Per-branch, same retention |
| Branch limit | None |
| GC | Cross-branch reference tracking |
