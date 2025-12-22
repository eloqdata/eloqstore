# Term Handling Changes Summary (2025-12-17)

## Overview

This note summarizes the **final term-related behavior changes** implemented across the codebase to prevent cross-process corruption in cloud mode and to make filenames/manifest handling term-aware and deterministic.

The key outcome is:

- **All generated filenames are term-aware, including `term=0`**
- **Parsing is strict and rejects legacy bare formats for current manifest/data**
- **Manifests persist `FileIdTermMapping` and replayer validates term mismatch by bumping allocators**

---

## Final Filename Scheme

### Data files

- **Format**: `data_<file_id>_<term>`
- **Including term=0**: `data_<file_id>_0`

Generator behavior:

- `DataFileName(file_id, term)` **always emits** `data_<file_id>_<term>`
- `DataFileName(file_id)` is an overload that calls `DataFileName(file_id, 0)` and therefore emits `data_<file_id>_0`

Parser behavior:

- `ParseDataFileSuffix("<file_id>_<term>", file_id, term)` ✅
- `ParseDataFileSuffix("<file_id>", ...)` ❌ **rejected** (legacy no-term format)

### Current manifest

- **Format**: `manifest_<term>`
- **Including term=0**: `manifest_0`

Generator behavior:

- `ManifestFileName(term)` **always emits** `manifest_<term>`
- `ManifestFileName()` defaults to `term=0` and emits `manifest_0`

Parser behavior:

- `ParseManifestFileSuffix("<term>", term, ts)` ✅ (with `ts` unset)
- `ParseManifestFileSuffix("", ...)` ❌ **rejected** (legacy bare `manifest`)
- `manifest_<ts>` legacy format ❌ **not supported**

### Archive manifests

- **Format**: `manifest_<term>_<timestamp>`
- **Including term=0**: `manifest_0_<timestamp>`

Generator behavior:

- `ArchiveName(term, ts)` emits `manifest_<term>_<ts>` (always term-aware)

Parser behavior:

- `ParseManifestFileSuffix("<term>_<ts>", term, ts)` ✅ (ts set)

---

## Term Metadata: `FileIdTermMapping`

### Definition

- `FileIdTermMapping = std::unordered_map<FileId, uint64_t>`

### Persistence in manifest payload

Manifest snapshot payload layout now includes `FileIdTermMapping` **always present**:

1. `max_fp_id`
2. dictionary
3. `FileIdTermMapping` (varint count + (file_id, term) pairs; may be empty with count=0)
4. mapping table

### Where mapping is updated

- **On first allocation of a new `file_id`** during writes (dirty flag is set to force snapshot instead of WAL append).
- **On file creation** (`IouringMgr::CreateFile`) records the term for the created file id if missing.
- **On manifest selection/download** in cloud mode: `FileIdTermMapping[LruFD::kManifest]` is set to the selected manifest term.

---

## Replayer Term Validation / Allocator Bumping

Replayer tracks:

- `manifest_term_`: term extracted from the manifest filename (or set by caller)
- `expect_term_`: expected term of current process (passed in from IO manager / caller)

In **cloud mode**, if both terms are non-zero and differ:

- Replayer bumps `max_fp_id_` to the **next file boundary** to prevent cross-term file id reuse/collision.

---

## IO / FD Term Awareness

- `LruFD` caches a `term_` per-open file descriptor.
- `OpenFD` / `OpenOrCreateFD` accept a `term` argument.
- In cloud mode, if cached FD term != requested term (and term!=0 in earlier design; currently filenames are always term-aware), the FD is closed and reopened with correct term.

`CloudStoreMgr::ToFilename(tbl, file_id, term)` uses:

- `ManifestFileName(term)` for manifest file id
- `DataFileName(file_id, term)` for data file ids

---

## Cloud Manifest Selection Notes

`CloudStoreMgr::GetManifest` selects a manifest by term and supports “promotion”:

- If `process_term_ != 0`, first tries `manifest_<process_term_>`
- Else lists manifests, selects highest term (ignoring archive manifests with timestamps)
- Validates against `process_term_` (rejects `selected_term > process_term_`)
- If `selected_term < process_term_`, downloads selected manifest, writes local `manifest_<process_term_>`, uploads it, and proceeds with `selected_term = process_term_`

---

## Term Injection at Startup

Term is injected at startup and wired to IO managers:

- `EloqStore::Start(uint64_t term = 0)` stores `term_`
- `Shard::Init()` reads `store_->Term()` and calls:
  - `CloudStoreMgr::SetProcessTerm(term)` or
  - `IouringMgr::SetProcessTerm(term)`

This ensures term is available before any manifest/data operations.

---

## Tests Updated/Added

Key test adjustments:

- `tests/filename_parsing.cpp`:
  - `DataFileName(...,0)` and `ManifestFileName(0)` now expect `_0` suffix.
  - `ParseDataFileSuffix("123")` and `ParseManifestFileSuffix("")` now **expect failure** (strict parsing).
- `tests/cloud.cpp`:
  - backup naming uses `ArchiveName(0, ts)` to avoid legacy `manifest_<ts>` names.
  - references to `"manifest"` updated to use `ManifestFileName(0)` (`manifest_0`).
- `tests/replayer_term.cpp` added:
  - validates allocator bumping behavior in cloud mode when terms mismatch.

---

## Migration / Operational Implications

- Existing deployments that still have bare `manifest` or `data_<id>` files will **not be parsed** by strict parsers.
- A migration strategy (if needed) should rename:
  - `manifest` → `manifest_0`
  - `data_<id>` → `data_<id>_0`
- Cloud tests and tooling must avoid creating legacy `manifest_<ts>` files because that pattern is not supported by manifest parsing/selection logic.



