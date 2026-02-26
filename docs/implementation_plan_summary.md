# Summary of Branch Feature Design Discussion and Conclusions

## Document Location
- Design Document: `~/workspace/eloqdata/eloqkv/data_substrate/store_handler/eloq_data_store_service/eloqstore/docs/branch_feature_design.md`
- Implementation Summary Location: `docs/` directory (one file per phase)

---

## Overview

The Branch feature provides lightweight data isolation by creating independent metadata (manifests) that reference parent table's data files, similar to git branches. The implementation will be done in phases with detailed planning, review, implementation, testing, and documentation for each phase.

---

## Key Design Decisions and Clarifications

### 1. Version Headers (Separate Task - Phase 7)
- **Implementation**: Add 4-byte version field after checksum, before root
- **Scope**: Update both manifest and data file headers
- **Format Changes**:
  - Old manifest: `[Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B)]` (24 bytes)
  - New manifest: `[Checksum(8B) | Version(4B) | Root(4B) | TTL Root(4B) | Payload Len(4B)]` (28 bytes)
  - Data files: Add version field similarly
- **Version Number**: Current version = 1

### 2. Branch Creation Concurrency
- **Assumption**: Caller guarantees no concurrent writes to parent branch during creation
- **No Locking**: Implementation does not need to acquire locks
- **Detection**: Cannot detect if parent is being written during branch creation
- **Acceptable Risk**: Concurrent CreateBranch from same parent is acceptable

### 3. File ID Allocation Strategy (**CRITICAL - Option B**)
- **Per-branch monotonic increase**: Each branch has its own file_id counter
- **Initial file_id**: New branch starts from `parent's max_file_id + 1`
- **Same file_id can exist in different branches**: Yes, same file_id can appear in multiple branches
- **Important**: File IDs are NOT globally unique across branches

**Example Scenario:**
```
Main branch: file_ids [0,1,2,3,4] (max_file_id=4)
Create "feature" from main:
  - Feature inherits BranchFileMapping: [{branch: "main", term: 5, max_file_id: 4}]
  - Feature's first write allocates file_id = 5 (parent_max + 1)
  - After write: BranchFileMapping = [
      {branch: "main", term: 5, max_file_id: 4},
      {branch: "feature", term: 0, max_file_id: 5}
    ]
  - Feature's second write allocates file_id = 6

Main branch continues independently:
  - Main's next write allocates file_id = 5 (main's current max is still 4)
  - Both branches can have file_id=5: data_5_main_6, data_5_feature_0
```

**Key Point**: When allocating file_id for branch X, look at BranchFileMapping entries where `branch_name == X`, find the max, and use `max + 1`. Start from parent's max for first allocation in new branch.

### 4. BranchFileMapping Entry Removal (Optional - Phase 8)
- **Status**: Separate optional task
- **Implementation**: During manifest snapshot, remove entries for branches with no active files
- **Priority**: Low, deferred to optimization phase

### 5. DeleteBranch Safety
- **No validation check**: Do not check if branch is in use
- **Main branch deletion**: Main branch CAN be deleted (no special protection)
- **Non-existent branch**: Log error if branch manifest is not found, do not fail
- **Data files**: NOT deleted (handled by GC to prevent premature deletion)

### 6. Open Branch - Auto-creation Logic
- **Clarification**: "Latest existing branch manifest" means latest manifest of the SAME branch name
- **Use in CURRENT_TERM**: Read term from `CURRENT_TERM.<branch_name>` file
- **No auto-creation**: No scenarios exist for opening a non-existent branch
- **Implementation**: If manifest not found at expected term, return error

### 7. Branch Name Normalization (**NEW REQUIREMENT**)
- **Case Sensitivity**: Branch names are case-insensitive
- **Normalization**: Convert all branch names to lowercase before storage
- **Validation Pattern**: `[a-zA-Z0-9_-]+` (alphanumeric, underscore, hyphen)
- **Reserved Name**: `"main"` (already lowercase)

**Examples:**
- `"Feature"` → `"feature"`
- `"Dev-Branch_123"` → `"dev-branch_123"`
- `"HOTFIX"` → `"hotfix"`
- `"invalid!"` → error (invalid character)
- `"my branch"` → error (space not allowed)

### 8. Testing Strategy
- **Timing**: Implement tests as part of each phase (not deferred to Phase 7)
- **Framework**: Follow existing patterns in `eloqstore/tests/` using Catch2
- **Test Types**: Unit tests + integration tests per phase
- **Cloud Tests**: Include both local and cloud storage tests

### 9. Migration Path
- **No backward compatibility**: Only support new branch-based format from day one
- **No migration helpers**: Do not detect or warn about old format files
- **Legacy format**: Completely removed
- **Cloud customers**: Will use export/import tools for data migration

### 10. Performance Testing
- **Benchmarking**: Defer detailed performance benchmarking for now
- **Baseline**: Create baseline in Phase 0, but no specific performance targets yet
- **Tool**: Use `benchmark/simple_bench` for performance tests
- **Future**: Will consider performance targets later

### 11. Storage Support
- **Scope**: Branch feature must support BOTH local and cloud storage modes
- **Testing**: All tests must cover both local and cloud scenarios
- **Cloud Mode**: Handle S3 operations, CURRENT_TERM CAS operations
- **Local Mode**: Handle local filesystem operations

### 12. Summary File Location
- **Location**: `docs/` directory
- **Format**: One markdown file per phase
- **Naming**: `phase{N}_{phase_name}_summary.md`
- **Content**: 
  - Implementation details
  - Code changes made
  - Test results
  - Performance metrics (if applicable)
  - Issues encountered and resolutions
  - Review feedback and changes

---

## Implementation Workflow (Per Phase)

For each phase, the workflow is:

1. **Detailed Implementation Plan**
   - Break down tasks
   - Identify files to modify
   - Design data structures and algorithms
   - Present to user for review

2. **Ask for Review** - WAIT FOR USER APPROVAL

3. **Write Code**
   - Implement the planned changes
   - Follow coding standards
   - Ask for review

4. **Ask for Review** - WAIT FOR USER APPROVAL

5. **Commit Code**
   - Create git commit with descriptive message
   - Follow repository's commit style

6. **Detailed Test Plan**
   - Design test cases (unit + integration)
   - Include revising existing tests if needed
   - Cover both local and cloud modes
   - Present to user for review

7. **Ask for Review** - WAIT FOR USER APPROVAL

8. **Write Test Code**
   - Implement test cases
   - Follow existing test patterns
   - Ask for review

9. **Ask for Review** - WAIT FOR USER APPROVAL

10. **Run Tests**
    - Execute test suite
    - Verify all tests pass
    - Document results

11. **Fix Bugs**
    - If tests fail, debug and fix
    - Re-run tests until all pass

12. **Commit Changes**
    - Commit test code
    - Update summary document

13. **Create Phase Summary**
    - Document what was implemented
    - Record test results
    - Note any issues and resolutions
    - Save to `docs/phase{N}_summary.md`

---

## Revised Implementation Phases

### **Phase 0: Baseline & Performance Benchmarking**
**Goal**: Establish baseline metrics before making changes

**Tasks**:
- Run existing benchmark suite (`benchmark/simple_bench`)
- Document system configuration
- Record baseline metrics (write/read throughput, basic GC timing)
- Save results to `docs/phase0_baseline.md`

**Note**: Defer detailed GC benchmarking with 100+ branches to later

---

### **Phase 1: Infrastructure - File Naming & Parsing**
**Goal**: Add foundation for branch-aware file naming and parsing

**Key Changes**:
- `include/common.h`:
  - Add `NormalizeBranchName()` - validates and converts to lowercase
  - Add `IsValidBranchName()` - validates pattern `[a-zA-Z0-9_-]+`
  - Add `BranchManifestFileName(branch, term)` → `manifest_<branch>_<term>`
  - Add `BranchDataFileName(file_id, branch, term)` → `data_<id>_<branch>_<term>`
  - Add `BranchCurrentTermFileName(branch)` → `CURRENT_TERM.<branch>`
  - Add `BranchArchiveName(branch, term, ts)` → `manifest_<branch>_<term>_<ts>`
  - Update `ParseDataFileSuffix()` - extract `{file_id, branch_name, term}`
  - Update `ParseManifestFileSuffix()` - extract `{branch_name, term, timestamp?}`
  - Add `ParseCurrentTermFilename()` - extract `{branch_name}`
  - Add helper functions: `IsBranchManifest()`, `IsBranchArchive()`, `IsBranchDataFile()`

- `include/types.h`:
  - Add `constexpr char MainBranchName[] = "main"`
  - Add `constexpr char CurrentTermPrefix[] = "CURRENT_TERM"`

**Tests** (`tests/branch_filename_parsing.cpp`):
- Branch name validation and normalization
- File naming function outputs
- Parsing function correctness
- Roundtrip tests (generate → parse → verify)
- Malformed filename handling (should reject gracefully)

---

### **Phase 2: Manifest Structure Updates**
**Goal**: Replace FileIdTermMapping with BranchFileMapping, add branch_name to manifests

**Key Changes**:
- `include/storage/root_meta.h`:
  - Add `struct BranchFileRange { string branch_name; uint64_t term; FileId max_file_id; }`
  - Add `using BranchFileMapping = std::vector<BranchFileRange>` (sorted by max_file_id)
  - Update `ManifestMetadata` to include `branch_name` and `branch_file_ranges`
  - Add `FindBranchForFileId()` - binary search using `std::lower_bound`

- `src/storage/root_meta.cpp`:
  - Add `SerializeBranchFileMapping()` - serialize to string
  - Add `DeserializeBranchFileMapping()` - deserialize from string
  - Update manifest serialization to include new fields
  - Update manifest deserialization

- File ID allocation:
  - Implement per-branch allocation logic
  - **New branch starts from parent's max_file_id + 1** (Option B)
  - When allocating for branch X, find max in entries where `branch_name == X`

**Tests** (`tests/branch_file_mapping.cpp`, `tests/branch_manifest.cpp`):
- BranchFileRange creation and sorting
- Binary search lookup (`FindBranchForFileId()`)
- Serialization/deserialization roundtrip
- File ID allocation (per-branch, starting from parent's max + 1)
- Manifest metadata with branch_name
- Edge cases: empty mapping, single entry, multiple branches

---

### **Phase 3: Request Classes & Branch Operations**
**Goal**: Implement CreateBranch and DeleteBranch operations

**Key Changes**:
- `include/eloq_store.h`:
  - Add `class BranchRequest : public GlobalRequest`
  - Add `class DeleteBranchRequest : public GlobalRequest`

- `src/tasks/background_write.cpp`:
  - Implement `CreateBranch(parent_branch, new_branch_name)`
    - Normalize branch name to lowercase
    - Read parent's manifest
    - Create new manifest with term=0, copy BranchFileMapping
    - Write `manifest_<branch>_0`
    - Create `CURRENT_TERM.<branch>` with content "0"
  - Implement `DeleteBranch(branch_name)`
    - Read CURRENT_TERM to get term
    - Delete manifest file
    - Delete CURRENT_TERM file
    - Do NOT delete data files (GC handles this)
    - Log error if branch not found

- `src/storage/shard.cpp`:
  - Add request handling for `kBranch` and `kDeleteBranch`

- `src/async_io_manager.cpp`:
  - Update CURRENT_TERM management for per-branch files
  - Support both local and cloud modes
  - Cloud mode: handle S3 operations for `CURRENT_TERM.<branch>`

**Tests** (`tests/branch_operations.cpp`):
- CreateBranch: from main, from another branch
- Branch name normalization (uppercase → lowercase)
- Invalid branch names (should fail)
- Duplicate branch creation (should fail)
- DeleteBranch: existing, non-existent
- Delete main branch (should succeed)
- Verify manifest and CURRENT_TERM files created/deleted
- Test both local and cloud storage modes

---

### **Phase 4: Read/Write with Branch Support**
**Goal**: Enable reading and writing to specific branches

**Key Changes**:
- `src/tasks/write_task.cpp`:
  - Update `WriteTask::AllocatePage()` to accept branch_name
  - Use per-branch file_id allocation
  - Generate data filenames with branch_name: `data_<id>_<branch>_<term>`
  - Update BranchFileMapping when allocating new file_ids

- `src/storage/shard.cpp`:
  - Update `Shard::Get()` to accept branch_name parameter
  - Load branch-specific manifest
  - Resolve data files using BranchFileMapping

- `src/storage/page_mapper.h` and implementation:
  - Update file allocation to track branch_name
  - Implement per-branch file_id counter logic

- `src/async_io_manager.cpp`:
  - Update term management (per-branch CURRENT_TERM files)
  - Update term validation for cloud mode

**Tests** (`tests/branch_read_write.cpp`):
- Write to main branch
- Write to feature branch
- Verify file_id allocation (starts from parent_max + 1)
- Verify correct filenames: `data_<id>_<branch>_<term>`
- Read from same branch (should succeed)
- Read inherited data from parent branch (should succeed)
- Cross-branch isolation:
  - Write to branch A, verify not visible in branch B
  - Create branch B from A, verify A's data visible in B
  - Write to B, verify not visible in A
- Test both local and cloud storage modes

---

### **Phase 5: Archive with Branch Support**
**Goal**: Enable per-branch archives with retention

**Key Changes**:
- `src/tasks/background_write.cpp`:
  - Update `CreateArchive()` to accept branch_name
  - Generate archive names: `manifest_<branch>_<term>_<timestamp>`
  - Track archives per-branch for retention

- `src/replayer.cpp`:
  - Update archive loading to filter by branch_name
  - Handle branch-specific archive naming

- Archive retention:
  - Apply `num_retained_archives` per branch
  - Clean up old archives during GC

**Tests** (`tests/branch_archive.cpp`):
- Create archive for main branch
- Create archive for feature branch
- Verify archive filename format
- Archive retention: create N+1 archives, verify oldest deleted
- Verify retention is per-branch (not global)
- Load archive by branch and term
- Test both local and cloud storage modes

---

### **Phase 6: GC with Cross-Branch References**
**Goal**: Safe garbage collection with cross-branch reference tracking

**Key Changes**:
- `src/file_gc.cpp`:
  - Update file classification to parse branch names
  - Load ALL branch manifests (not just current branch)
  - Build reference set from all manifests:
    - For each file_id in manifest, use BranchFileMapping to find branch/term
    - Generate data filename and add to reference set
  - Build branch_max_map: `{(branch, term) -> max_file_id}`
  - Deletion logic:
    - KEEP if referenced in any manifest
    - KEEP if file_id > branch_max_map[(branch, term)] (newly created)
    - DELETE otherwise (orphaned)
  - Handle per-branch archives (retention policy)
  - Support both local and cloud storage

**Tests** (`tests/branch_gc.cpp`):
- Basic GC: create branch, write data, delete branch, run GC
- Cross-branch references:
  - Create branch B from A
  - Delete B, run GC
  - Verify A's data files NOT deleted (inherited references)
  - Write new data to B, delete B, run GC
  - Verify B's new files deleted, A's files kept
- Same file_id in different branches:
  - Main has data_5_main_3
  - Feature has data_5_feature_2
  - Delete feature, verify only data_5_feature_2 deleted
- Archive retention per branch
- Test both local and cloud storage modes

---

### **Phase 7: Version Headers (Separate Task)**
**Goal**: Add version field to all file headers for future compatibility

**Key Changes**:
- `include/storage/root_meta.h`:
  - Update manifest header: add 4-byte version field after checksum
  - Old: `[Checksum(8B) | Root(4B) | TTL Root(4B) | Payload Len(4B)]` (24B)
  - New: `[Checksum(8B) | Version(4B) | Root(4B) | TTL Root(4B) | Payload Len(4B)]` (28B)
  - Add `constexpr uint32_t kManifestFormatVersion = 1`

- Data file headers:
  - Add version field similarly
  - Add `constexpr uint32_t kDataFileFormatVersion = 1`

- Update all serialization/deserialization code

**Tests** (`tests/version_headers.cpp`):
- Write manifest with version, verify read
- Write data file with version, verify read
- Version field roundtrip tests

---

### **Phase 8: Optional Optimizations (Separate Task)**
**Goal**: Performance improvements and optimizations

**Tasks**:
- BranchFileMapping entry removal during compaction
- Performance benchmarking (if needed later)
- Other optimizations identified during implementation

---

## File Change Summary

### Files to Create:
- `tests/branch_filename_parsing.cpp` (Phase 1)
- `tests/branch_file_mapping.cpp` (Phase 2)
- `tests/branch_manifest.cpp` (Phase 2)
- `tests/branch_operations.cpp` (Phase 3)
- `tests/branch_read_write.cpp` (Phase 4)
- `tests/branch_archive.cpp` (Phase 5)
- `tests/branch_gc.cpp` (Phase 6)
- `tests/version_headers.cpp` (Phase 7)
- `docs/phase0_baseline.md` through `docs/phase8_optimizations_summary.md`

### Files to Modify:
- `include/common.h` (Phase 1: file naming and parsing)
- `include/types.h` (Phase 1: constants)
- `include/storage/root_meta.h` (Phase 2: manifest structure)
- `src/storage/root_meta.cpp` (Phase 2: serialization)
- `include/eloq_store.h` (Phase 3: request classes)
- `src/tasks/background_write.cpp` (Phase 3: CreateBranch, DeleteBranch; Phase 5: archive)
- `src/storage/shard.cpp` (Phase 3: request handling; Phase 4: read/write)
- `src/async_io_manager.cpp` (Phase 3: CURRENT_TERM; Phase 4: term management)
- `src/tasks/write_task.cpp` (Phase 4: write path)
- `include/storage/page_mapper.h` (Phase 4: file allocation)
- `src/replayer.cpp` (Phase 5: archive loading)
- `src/file_gc.cpp` (Phase 6: GC with cross-branch)
- All header/source files (Phase 7: version headers)

---

## Critical Implementation Points

1. **Branch Name Normalization**: ALWAYS convert to lowercase before storage or comparison
2. **File ID Allocation**: Per-branch, starts from parent's max_file_id + 1, NOT global
3. **BranchFileMapping Lookup**: Use binary search on sorted vector for efficiency
4. **GC Cross-Branch**: MUST check ALL branch manifests for references before deletion
5. **Same file_id Different Branches**: Valid scenario, filenames include branch_name for uniqueness
6. **No Backward Compatibility**: Only support new format, no legacy format handling
7. **Both Storage Modes**: Every feature must work in local AND cloud modes

---

## Next Steps

**Ready to proceed with Phase 0: Baseline & Performance Benchmarking**

Phase 0 will:
1. Run existing benchmark suite
2. Document current performance metrics
3. Record system configuration
4. Create `docs/phase0_baseline.md` with results
5. Establish baseline for future comparison

**Awaiting approval to begin Phase 0.**
