# Branch Feature Design Document

## Overview

The Branch feature provides lightweight data isolation by creating independent metadata (manifest) that references parent table's data files. Similar to git branches.

---

## 1. File Naming Conventions

### Branch Name Rules
- **Validation**: Only `[a-zA-Z0-9_-]+` (alphanumeric, underscore, hyphen)
- **Reserved**: `"main"` (primary branch)
- **Uniqueness**: Branch name must be unique within the table

### Manifest Files

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Main manifest | `manifest_main_<term>` | `manifest_main_5` | |
| Branch manifest | `manifest_<branch_name>_<term>` | `manifest_feature_5` | |
| Main archive | `manifest_main_<term>_<ts>` | `manifest_main_5_1234567890` | |
| Branch archive | `manifest_<branch_name>_<term>_<ts>` | `manifest_feature_5_1234567890` | |

### Data Files

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Main data | `data_<file_id>_main_<term>` | `data_10_main_5` | |
| Branch data | `data_<file_id>_<branch_name>_<term>` | `data_10_feature_5` | |

### CURRENT_TERM Files

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Main | `CURRENT_TERM.main` | `CURRENT_TERM.main` | |
| Branch | `CURRENT_TERM.<branch_name>` | `CURRENT_TERM.feature` | |

### Version Information

All files include a version number in their header for future compatibility:

```
Manifest Header: [ Checksum(8B) | Version(4B) | Root(4B) | TTL Root(4B) | Payload Len(4B) ]
Data Header:     [ Checksum(8B) | Version(4B) | ... ]
```

- Current version: 1
- Version enables future format migrations

---

## 2. Parsing Functions

### Manifest Parsing

```
manifest_main_5   → {branch_name: "main", term: 5, timestamp: null} (main)
manifest_feature_5       → {branch_name: "feature", term: 5, timestamp: null}      (branch)
manifest_main_5_123 → {branch_name: "main", term: 5, timestamp: 123} (archive)
manifest_feature_5_123  → {branch_name: "feature", term: 5, timestamp: 123}        (branch archive)
```

### Data File Parsing

```
data_10_main_5    → {file_id: 10, branch_name: "main", term: 5}
data_10_feature_5       → {file_id: 10, branch_name: "feature", term: 5}
```

### CURRENT_TERM Parsing

```
CURRENT_TERM.main → branch_name: "main"
CURRENT_TERM.feature    → branch_name: "feature"
```

### Error Handling

| Case | Behavior |
|------|----------|
| Malformed manifest filename | Ignore file, log warning |
| Malformed data filename | Ignore file, log warning |
| Invalid branch_name (contains invalid chars) | Return error |
| Reserved branch_name (except "main") | Return error |

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

#### BranchFileMapping

```cpp
struct BranchFileRange {
    std::string branch_name;  // branch identifier
    uint64_t term;            // term when this file_id range was allocated
    FileId max_file_id;       // highest file_id allocated in this branch
};

using BranchFileMapping = std::vector<BranchFileRange>;  // sorted by max_file_id
```

**Algorithm to find branch given file_id**: Use `std::lower_bound` to find the first entry where `max_file_id >= file_id`.

**Size Reduction**: 1 entry per branch instead of 1 entry per file (e.g., 10 branches vs millions of files).

**Entry Removal**(Optinal): During manifest snapshot, entries can be removed:
1. Check all file_ids listed in the manifest
2. For each entry in BranchFileMapping, if no file_id ≤ max_file_id exists in the manifest, that entry can be deleted

**Note**: After branch creation, branches are fully independent and do not track parent branch changes.

#### Manifest Metadata Format

```cpp
struct ManifestMetadata {
    std::string branch_name;              // unique identifier (e.g., "main", "feature")
    uint64_t term;                         // current term for this branch
    PageId root;                           // B+ tree root
    PageId ttl_root;                       // TTL index root
    BranchFileMapping branch_file_ranges;  // per-branch file ranges (sorted by max_file_id)
};
```

---

#### Key Differences

| Aspect | Current Format | Proposed Format |
|--------|---------------|-----------------|
| Branch Identification | Not supported | `branch_name` field |
| Branch Term | Global term | Per-branch term |
| File Mapping | `FileId → term` | `branch_name → {max_file_id, term}` (per-branch ranges) |

---

## 7. Operations

### Create Branch

```
Input: parent_table, branch_name
Output: branch_name

Assumptions:
- Parent branch is not being written during branch creation
- Branch name validation and uniqueness are guaranteed by caller

1. Read parent's current manifest
2. Create branch manifest:
   - branch_name = input_name
   - term = 0 (new branch starts at term 0)
   - root = parent's root
   - branch_file_ranges = COPY of parent's branch_file_ranges
3. Write manifest_<branch_name>_<term>
4. Create CURRENT_TERM.<branch_name> with content "0"
5. Return branch_name
```

### Add File to Branch

```
Input: branch_name, data
Output: written data

1. Allocate new file_id (per-branch counter)
2. Write data to: data_<file_id>_<branch_name>_<term>
3. Update branch_file_ranges: set max_file_id = file_id (term unchanged)
4. Append to manifest_<branch_name>_<term>
```

### Delete Branch

```
Input: branch_name

Assumptions:
- Branch being deleted is not actively being written to

1. Delete manifest_<branch_name>_<term>
2. Delete CURRENT_TERM.<branch_name>
3. DO NOT delete data files (may be referenced elsewhere)
4. Orphaned data files will be cleaned by GC
```

### Open Branch

```
Input: branch_name, term
Output: branch manifest

1. Find manifest_<branch_name>_<term>
2. If manifest not found:
   - Create new manifest with term
   - branch_file_ranges = inherited from latest existing branch manifest
3. Parse manifest metadata (branch_name, term, branch_file_ranges)
4. Load mapping snapshot from root
5. Return branch context
```

---

## 8. GC with Cross-Branch References

### Classification

```
manifest_main_5          → Development manifest, branch=main, term=5
manifest_feature_5              → Branch manifest, branch=feature, term=5
manifest_main_5_1234567890 → Development archive, ts=1234567890
manifest_feature_5_1234567890  → Branch archive, branch=feature, ts=1234567890
data_10_main_5          → Development data, file_id=10, term=5
data_10_feature_5             → Branch data, file_id=10, branch=feature, term=5
```

### Deletion Algorithm

```
1. Collect all active manifests (main + all branches + archives)
2. Build reference set from manifest entries:
   For each manifest:
       For each file entry (file_id) in manifest:
           file_name = "data_" + file_id + "_" + branch_name + "_" + term
           referenced_files.insert(file_name)

3. Build branch max_file_id map:
   For each manifest (branch_name, term):
       max_file_id = max file_id in manifest for this (branch_name, term)
       branch_max_map[(branch_name, term)] = max_file_id

4. For each data file (file_name = data_<file_id>_<branch_name>_<term>):
       if file_name in referenced_files:
           KEEP (manifest references it)
       else if (branch_name, term) in branch_max_map and file_id > branch_max_map[(branch_name, term)]:
           KEEP (newly created file not yet in manifest)
       else:
           DELETE (orphaned)
```

---

## 9. Cloud Storage Structure

```
bucket/prefix/table_name.partition_id/
├── CURRENT_TERM.main     # Main branch
├── CURRENT_TERM.feature         # Branch feature
├── CURRENT_TERM.hotfix          # Branch hotfix
├── manifest_main_5        # main manifest (term=5)
├── manifest_feature_3            # Branch feature (term=3)
├── manifest_feature_5            # Branch feature (term=5)
├── manifest_hotfix_2             # Branch hotfix (term=2)
├── manifest_main_5_1234567890  # main archive
├── manifest_feature_5_1234567890      # Branch feature archive
├── data_0_main_5          # main data
├── data_1_main_5          # main data
├── data_0_feature_3              # Branch feature data
├── data_0_hotfix_2               # Branch hotfix data
└── ...
```

---

## 10. Backward Compatibility

| Old Format | New Interpretation | Status |
|------------|-------------------|--------|
| `manifest_5` | Development manifest (branch=main, term=5) | Works |
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

### Phase 2: Manifest Updates
- Add branch_name to manifest structure
- Update serialization/deserialization
- Add BranchFileMapping (vector of BranchFileRange sorted by max_file_id)
- Implement binary search lookup with std::lower_bound

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
- Unit tests for parsing (branch formats)
- Integration: create/read/write/delete branches
- GC: verify cross-branch protection
- Cloud mode tests
- Version header tests

#### Specific Test Cases

**Parsing**
- Valid manifest names: `manifest_main_5`, `manifest_feature_5`
- Valid data names: `data_10_main_5`, `data_10_feature_5`
- Malformed names: `manifest_`, `data_abc_5`, etc. (should be ignored)

**Branch Operations**
- Create branch from main
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
| Branch file ranges per manifest | O(M branches) | Constant per branch, not per file |
| Storage overhead per branch | ~1 manifest file + CURRENT_TERM file | Plus per-branch data files |
| Manifest loading time | O(M) where M = number of branches | GC loads all branch manifests |

### Performance Considerations

| Aspect | Impact | Mitigation |
|--------|--------|-----------|
| GC with many branches | O(N files + M branches) | Range-based reference checking is efficient |
| BranchFileMapping size | O(M branches) - significantly smaller | Millions of entries → tens of entries |
| Branch creation latency | O(1) - copy parent manifest metadata | Minimal impact |
| Archive retention with active branches | Global quota may be consumed unevenly | Monitor per-branch archive count |

---

## Summary

| Aspect | Decision |
|--------|----------|
| Main branch files | Include branch_name=main (manifest_main_5, data_10_main_5) |
| Branch identification | branch_name (user-provided, validated, unique) |
| Branch term | Independent, starts at 0 |
| Archive | Per-branch, same retention |
| Branch limit | None |
| GC | Cross-branch reference tracking |
| Version header | All files include version for future migration support |

---

## Appendix: Legacy Support Removal

**Rationale**: Legacy file format support is removed. Existing cloud customers will migrate data using export/import tools instead of in-place upgrades. This simplifies the design and removes backward compatibility overhead.
