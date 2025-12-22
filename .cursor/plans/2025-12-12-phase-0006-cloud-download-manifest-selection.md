---
name: Cloud Download & Manifest Selection
overview: Update CloudStoreMgr::GetManifest to select manifest by term, handle term validation, and update DownloadFile/OpenFile to use term-aware filenames.
todos:
  - id: cloud-getmanifest-term-selection
    content: Update GetManifest to select manifest by term and handle term validation
    status: pending
  - id: cloud-downloadfile-term-aware
    content: Update DownloadFile to use term-aware filenames
    status: pending
  - id: cloud-openfile-term-aware
    content: Update OpenFile to use term-aware ToFilename
    status: pending
    dependencies:
      - cloud-getmanifest-term-selection
---

# Phase 6: Cloud Download & Manifest Selection

## Overview

Update CloudStoreMgr to select manifests by term, validate term against process term, and use term-aware filenames for downloads. This ensures cloud mode correctly handles term-aware files and prevents cross-process corruption.

## Implementation Steps

### 6.1 Update CloudStoreMgr::GetManifest

**Goal**: Select manifest by term, validate term, and handle term mismatches.

**Steps**:
1. Locate `CloudStoreMgr::GetManifest` in `async_io_manager.cpp`
2. Try to download `manifest_<term>` first (where term is process term):
   - Use `ManifestFileName(process_term_)` to generate filename
   - Attempt download from cloud
   - If found, extract term from filename and proceed
3. If not found, list all manifests and find latest:
   - List all files matching `manifest_*` pattern
   - Parse filenames to extract terms using `ParseFileName`
   - Find manifest with highest term
4. Validate term:
   - If latest term > process_term: return error
     - Avoid consuming future term with older writer
     - Log error and return appropriate error code
   - If latest term < process_term: download it
     - Will trigger allocator bump in replayer (Phase 4)
     - Extract term from selected manifest filename
5. Pass term to Replayer:
   - Extract term from manifest filename
   - Pass to Replayer::Replay or GetMapper
   - Term is stored in FileIdTermMapping when manifest is deserialized by Replayer

**Files**: `async_io_manager.cpp` (CloudStoreMgr::GetManifest)

**Success Criteria**:
- [x] Manifest selected by term correctly
- [x] Term validation prevents future term consumption (when process_term_ is set)
- [x] Allocator bump triggered when term < process_term (via Replayer logic)
- [x] Error returned when term > process_term
- [x] Term passed to Replayer correctly (via FileIdTermMapping deserialized from manifest)
- [ ] Unit tests verify selection logic
- [ ] Integration tests with different terms

### 6.2 Update CloudStoreMgr::DownloadFile

**Goal**: Use term-aware filenames when downloading files from cloud.

5. Manifest promotion when `selected_term < process_term_`:
   - In `GetManifest`, after calling `DownloadFile(..., selected_term)` and if `process_term_ != 0 && selected_term != process_term_`:
     - Rename the downloaded `manifest_<selected_term>` to `manifest_<process_term_>` locally using `Rename` system call (more efficient than read-write).
     - Sync the directory using `Fdatasync` to ensure rename is persisted.
     - Upload `manifest_<process_term_>` to cloud via `UploadFiles`.
     - Note: `selected_term` remains unchanged (keeps the original term from cloud). FileIdTermMapping is not set in GetManifest; it will be populated when the manifest is deserialized by Replayer.
6. Ensure local cache and cloud use the same term-aware filename:
   - `DownloadFile` always writes to the exact filename produced by `ToFilename(tbl_id, file_id, term)`.
   - Manifest promotion logic renames `manifest_<selected_term>` to `manifest_<process_term_>` locally (using Rename system call) and uploads to cloud when `selected_term < process_term_`.
   - FileIdTermMapping is populated from the manifest payload when Replayer deserializes it, not in GetManifest.

**Files**: `async_io_manager.cpp` (CloudStoreMgr::DownloadFile)

**Success Criteria**:
- [x] Term-aware filenames used for downloads via `ToFilename(tbl_id, file_id, term)`
- [x] Callers (GetManifest/OpenFile/others) pass correct term based on `FileIdTermMapping` or process context
- [x] Fallback/promotion renames `manifest_<selected_term>` to `manifest_<process_term_>` locally (using Rename system call) and uploads to cloud when `selected_term < process_term_`
- [x] FileIdTermMapping is populated from manifest payload during Replayer deserialization, not set in GetManifest
- [x] Local cache uses correct filename for both data and manifest files
- [ ] Unit tests verify download filenames and manifest promotion behavior
- [ ] Integration tests with cloud storage

### 6.3 Update CloudStoreMgr::OpenFile

**Goal**: Use term-aware ToFilename (already updated in Phase 5) and ensure FileKey uses full term-aware filename.

**Steps**:
1. Locate `CloudStoreMgr::OpenFile` in `async_io_manager.cpp`
2. Verify ToFilename is term-aware (from Phase 5):
   - ToFilename should use `DataFileName(file_id, term_)`
   - Already updated in Phase 5.3
3. Ensure FileKey uses full term-aware filename:
   - FileKey construction uses ToFilename result
   - FileKey includes full filename (term-aware)
   - Cache lookup uses term-aware FileKey
4. Update file opening logic:
   - Use term-aware filename for cloud download
   - Use term-aware filename for local cache
   - Ensure consistency between download and open

**Files**: `async_io_manager.cpp` (CloudStoreMgr::OpenFile)

**Success Criteria**:
- [ ] FileKey uses term-aware filename
- [ ] Cloud download uses correct filename
- [ ] Local cache uses correct filename
- [ ] Consistency between download and open
- [ ] Unit tests verify FileKey generation
- [ ] Integration tests with file operations

## Dependencies

- Phase 1: Filename Helpers & Parsing (for ParseFileName, ManifestFileName)
- Phase 4: Replayer Term Validation (for allocator bumping)
- Phase 5: LruFD Term Awareness (for ToFilename)

## Related Phases

- Phase 7: File Allocation & Term Recording (records term for new files)
- Phase 9: Filename Usage Updates (uses term-aware filenames)

## Design Notes

- Manifest selection prioritizes `process_term_` when it is non-zero, otherwise falls back to latest manifest term found in cloud.
- Term validation prevents consuming future terms when `process_term_` is explicitly set.
- Allocator bumping for `term < process_term_` is handled centrally in `Replayer` based on `manifest_term_` and `expect_term`.
- FileIdTermMapping is populated from manifest payload when Replayer deserializes it. The mapping provides term for existing files (including manifest) so that base `IouringMgr::GetManifest` and other helpers can open correct term-aware filenames.
- `process_term_` is injected from a later startup/term-injection phase; when it is 0, GetManifest skips strict validation and simply uses the latest manifest term.

## Testing

Create unit tests:
- Test manifest selection by term
- Test term validation (term > process_term error)
- Test allocator bumping (term < process_term)
- Test DownloadFile with term-aware filenames
- Test OpenFile with term-aware FileKey
- Integration tests with cloud storage and multiple terms

