---
name: Replayer Term Validation & Allocator Bumping
overview: Add term validation and allocator bumping logic in Replayer::GetMapper to prevent cross-process file_id collisions when manifest term differs from process term.
todos:
  - id: replayer-term-tracking
    content: Add term tracking members to Replayer (manifest_term_, process_term_)
    status: in_progress
  - id: replayer-getmapper-term-logic
    content: Implement term validation and allocator bumping in GetMapper
    status: pending
    dependencies:
      - replayer-term-tracking
  - id: manifest-filename-term-extraction
    content: Update GetManifest to extract term from filename and pass to Replayer
    status: pending
    dependencies:
      - replayer-term-tracking
---

# Phase 4: Replayer Term Validation & Allocator Bumping

## Overview

Add term validation logic to Replayer to detect when manifest term differs from process term. When terms differ in cloud mode, bump the allocator to next file boundary to avoid file_id collisions between processes.

## Implementation Steps

### 4.1 Add term tracking to Replayer

**Goal**: Add members to track manifest term and process term in Replayer.

**Steps**:
1. Add `uint64_t manifest_term_` member to Replayer class:
   - Extracted from manifest filename (e.g., `manifest_<term>`)
   - Default to 0 (legacy manifests)
2. Add access to FileIdTermMapping via shared_ptr:
   - Store `std::shared_ptr<FileIdTermMapping> file_id_term_mapping_` member
   - Or access via PartitionFiles (passed from IO manager)
3. Add `uint64_t process_term_` parameter:
   - Pass to `GetMapper` method (from IO manager)
   - Or store in Replayer if available earlier
4. Update Replayer constructor if needed:
   - Accept process_term if available at construction
   - Or pass via GetMapper parameter

**Files**: `replayer.h`, `replayer.cpp`

**Success Criteria**:
- [ ] manifest_term_ member added
- [ ] FileIdTermMapping accessible via shared_ptr
- [ ] process_term_ can be passed to GetMapper
- [ ] Default values correct (term=0 for legacy)

### 4.2 Update GetMapper with term logic

**Goal**: Implement term validation and allocator bumping when terms differ.

**Steps**:
1. Update `GetMapper` signature to accept `process_term`:
   ```cpp
   std::unique_ptr<PageMapper> GetMapper(
       IndexPageManager *idx_mgr,
       const TableIdent *tbl_ident,
       uint64_t process_term = 0);
   ```
2. Access FileIdTermMapping from shared_ptr:
   - Get from PartitionFiles (via IO manager or parameter)
   - Or use member if stored in Replayer
3. Implement term validation logic:
   - If `manifest_term_ != process_term` (and cloud mode):
     - Calculate new file_id boundary:
       ```cpp
       FileId file_id = (max_fp_id_ >> opts_->pages_per_file_shift) + 1;
       max_fp_id_ = file_id << opts_->pages_per_file_shift;
       ```
     - This ensures new allocations start at next file boundary
     - Prevents collisions with files from other terms
4. Restore/update FileIdTermMapping:
   - Restore deserialized mapping to PartitionFiles (via shared_ptr)
   - Ensure mapping is accessible to all classes
5. Handle edge cases:
   - If manifest_term > process_term: log error, may need to reject (handled in Phase 6)
   - If process_term == 0: skip bumping (local mode, no collision risk)

**Files**: `replayer.h`, `replayer.cpp`

**Success Criteria**:
- [ ] GetMapper accepts process_term parameter
- [ ] Allocator bumped correctly when terms differ
- [ ] FileIdTermMapping restored to PartitionFiles
- [ ] Edge cases handled correctly
- [ ] Unit tests verify bumping logic
- [ ] Integration tests with different terms

### 4.3 Manifest filename term extraction

**Goal**: Extract term from manifest filename and pass to Replayer.

**Steps**:
1. Update `GetManifest` in IO managers (IouringMgr and CloudStoreMgr):
   - After getting manifest file, extract term from filename
   - Use `ParseFileName` helper from Phase 1
   - Extract term from `manifest_<term>` or `manifest_<term>_<ts>`
2. Pass manifest term to Replayer:
   - Store in Replayer::manifest_term_ member
   - Or pass as parameter to GetMapper
3. Update Replayer::Replay method:
   - Extract term from manifest filename if not already set
   - Store in manifest_term_ member

**Files**: `async_io_manager.cpp` (GetManifest methods), `replayer.cpp`

**Success Criteria**:
- [ ] Term extracted correctly from manifest filename
- [ ] Term passed to Replayer correctly
- [ ] Legacy manifests (no term) default to term=0
- [ ] Unit tests verify term extraction

## Dependencies

- Phase 1: Filename Helpers & Parsing (for ParseFileName)
- Phase 2: FileIdTermMapping Structure (for mapping access)
- Phase 3: Manifest Payload Serialization (for deserialized mapping)

## Related Phases

- Phase 6: Cloud Download & Manifest Selection (provides process_term)
- Phase 7: File Allocation & Term Recording (uses restored mapping)

## Design Notes

- Allocator bumping formula:
  - `file_id = (max_fp_id >> shift) + 1`
  - `max_fp_id = file_id << shift`
  - This moves to next file boundary, zeroing page offset
- Only bump in cloud mode when terms differ
- Term=0 (local mode) doesn't need bumping
- FileIdTermMapping is restored so PageMapper can check term for existing files

## Testing

Create unit tests:
- Test allocator bumping when manifest_term < process_term
- Test no bumping when manifest_term == process_term
- Test no bumping when process_term == 0 (local mode)
- Test FileIdTermMapping restoration
- Test term extraction from various filename formats
- Integration tests with multiple terms

