---
name: Filename Usage Integration & Manifest Writing
overview: Replace all FileNameManifest and DataFileName usages with term-aware versions in active paths. Update WriteTask to detect FileIdTermMapping changes and force full snapshot. Update prewarm and file classification.
todos:
  - id: filename-manifest-replace
    content: Replace FileNameManifest constants with ManifestFileName(term_) calls
    status: completed
  - id: filename-data-replace
    content: Replace DataFileName calls with term-aware versions in active paths
    status: completed
  - id: prewarm-filegc-update
    content: Update prewarm_task.cpp and file_gc.cpp to parse term-aware filenames
    status: completed
  - id: manifest-writing-mapping-changes
    content: Update WriteTask to detect FileIdTermMapping changes and force full snapshot
    status: completed
---

# Phase 9: Filename Usage Integration & Manifest Writing

## Overview

Replace all legacy filename constant usages with term-aware versions in active write/open paths. Update WriteTask to detect FileIdTermMapping changes and force full snapshot. Update prewarm and file classification to handle term-aware filenames.

## Implementation Steps

### 9.1 Replace FileNameManifest constants

**Goal**: Replace uses of `FileNameManifest` for active manifest operations with `ManifestFileName(term_)` calls.

**Steps**:
1. Find all uses of `FileNameManifest` in active manifest paths:
   - Search for `FileNameManifest` in codebase
   - Identify active manifest operations (not archive/read-only)
   - Focus on write/append/switch operations
2. Replace with `ManifestFileName(term_)` calls:
   - Get term from IO manager (term_ member)
   - Replace `FileNameManifest` with `ManifestFileName(term_)`
   - Update all call sites
3. Keep `FileNameManifest` for legacy compatibility:
   - Keep constant definition for backward compatibility checks
   - Use in archive/read paths that need legacy support
   - Use in compatibility checks

**Files**: `async_io_manager.cpp`, `write_task.cpp`, other files using FileNameManifest

**Status**: Completed in earlier phases (Phase 5/6) and verified in current code.

**Success Criteria**:
- [x] All active manifest operations use term-aware names (e.g. `SwitchManifest` uses `ManifestFileName(term)`)
- [x] `FileNameManifest` constant remains for type comparisons/parsing
- [x] All call sites updated
- [ ] Unit tests verify filename usage
- [ ] Integration tests with manifest operations

### 9.2 Replace DataFileName calls

**Goal**: Update active write/open paths to use term-aware `DataFileName(file_id, term_)`.

**Steps**:
1. Find all uses of `DataFileName(file_id)` in active paths:
   - Search for `DataFileName` calls
   - Identify active write/open operations (not archive/read-only)
   - Focus on file creation and opening
2. Update to use term-aware version:
   - Replace `DataFileName(file_id)` with `DataFileName(file_id, term_)`
   - Get term from IO manager (term_ member)
   - Update all call sites
3. Keep legacy version for backward compatibility:
   - Archive and read paths can use legacy names
   - Backward compatible reading supported
   - Legacy overload kept for compatibility

**Files**: `async_io_manager.cpp`, `write_task.cpp`, other files using DataFileName

**Status**: Completed in earlier phases (Phase 5) and verified in current code.

**Success Criteria**:
- [x] Active write/open paths use term-aware names (e.g. `OpenFile/CreateFile` use `DataFileName(file_id, term)`)
- [x] Legacy overload remains for term=0 compatibility
- [x] All call sites updated
- [ ] Unit tests verify filename usage
- [ ] Integration tests with file operations

### 9.3 Update prewarm and file classification

**Goal**: Update prewarm_task.cpp and file_gc.cpp to parse term-aware filenames.

**Steps**:
1. Update `prewarm_task.cpp`:
   - Use `ParseFileName` helper from Phase 1
   - Parse term-aware filenames correctly
   - Handle both legacy and term-aware formats
   - Extract term from filenames if present
   - Use term directly from parsed filename (do not check FileIdTermMapping; term comes from cloud listing and is authoritative)
2. Update `file_gc.cpp` `ClassifyFiles`:
   - Use `ParseFileName` helper
   - Handle term-aware names in file classification
   - Note: legacy `manifest_<ts>` is no longer supported; archives are `manifest_<term>_<ts>`
3. Supported formats:
   - Data: `data_<id>` and `data_<id>_<term>` (term defaults to 0 if absent)
   - Manifest: `manifest_<term>` (current) and `manifest_<term>_<ts>` (archive)

**Files**: `prewarm_task.cpp`, `file_gc.cpp`

**Status**: Completed in earlier phases (Phase 5/8) and verified in current code.

**Success Criteria**:
- [x] Prewarm parses term-aware filenames
- [x] File classification handles term-aware names (and data term defaulting)
- [x] Supported formats recognized correctly
- [ ] Unit tests verify parsing
- [ ] Integration tests with prewarm and GC

### 9.4 Detect term mapping changes

**Goal**: Track when FileIdTermMapping changes and force full snapshot.

**Steps**:
1. Locate `WriteTask::FlushManifest` in `write_task.cpp`
2. Detect FileIdTermMapping changes:
   - Compare current term mapping with previous snapshot
   - Check if new file_id added with different term
   - Track term mapping state between snapshots
3. Force full snapshot when term mapping changes:
   - Cannot use WAL append when mapping changes
   - Force full snapshot write
   - Update snapshot logic
4. Update `WriteTask::FlushManifest` logic:
   - Check mapping changes before deciding append vs snapshot
   - Force snapshot if mapping changed
   - Ensure mapping is serialized in snapshot

**Files**: `write_task.cpp`

**Status**: Implemented in `WriteTask` via `file_id_term_mapping_dirty_` and verified in current code.

**Success Criteria**:
- [x] Mapping changes detected correctly (dirty flag set on first insertion)
- [x] Full snapshot forced on mapping changes
- [x] WAL append skipped when mapping changes
- [ ] Unit tests verify change detection
- [ ] Integration tests with mapping changes

### 9.5 Update manifest writing paths

**Goal**: Ensure manifest writing uses term-aware filenames.

**Steps**:
1. Ensure `SwitchManifest` uses term-aware filename:
   - Already updated in Phase 5/6
   - Verify term-aware filename used
   - Ensure consistency
2. Ensure `AppendManifest` works with term-aware manifest:
   - WAL format unchanged (no term in WAL records)
   - Manifest file uses term-aware name
   - Append operations work correctly
3. Update manifest writing:
   - Use term-aware filename for manifest file
   - Ensure all write paths use term-aware names
   - Maintain backward compatibility

**Files**: `write_task.cpp`, `async_io_manager.cpp`

**Success Criteria**:
- [x] SwitchManifest uses term-aware filename
- [x] AppendManifest works with term-aware manifest
- [x] All write paths updated
- [ ] Unit tests verify manifest writing
- [ ] Integration tests with manifest operations

## Dependencies

- Phase 1: Filename Helpers & Parsing (for filename helpers)
- Phase 2: FileIdTermMapping Structure (for mapping access)
- Phase 3: Manifest Payload Serialization (for mapping serialization)
- Phase 5: LruFD Term Awareness (for ToFilename)

## Related Phases

- Phase 10: Testing (tests filename usage)

## Design Notes

- Active paths (write/open) use term-aware names
- Archive naming uses term-aware `manifest_<term>_<ts>` (legacy archive format removed)
- Mapping changes force full snapshot (cannot append WAL)
- Term-aware manifest names used for all writes

## Testing

Create unit tests:
- Test FileNameManifest replacement
- Test DataFileName replacement
- Test prewarm with term-aware filenames
- Test file classification with both formats
- Test mapping change detection
- Test full snapshot on mapping changes
- Integration tests with all operations

