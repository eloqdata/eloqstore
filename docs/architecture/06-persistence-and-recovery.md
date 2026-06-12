# Persistence, Recovery, Terms and Branches

Source: `include/storage/root_meta.h`, `src/storage/root_meta.cpp`,
`include/storage/page_mapper.h`, `src/storage/page_mapper.cpp`,
`include/replayer.h`, `src/replayer.cpp`, `include/manifest_buffer.h`,
`include/common.h` (file naming), `include/types.h` (branch types).

## The durability anchor

A page write is durable only when a manifest record describing its mapping has
been persisted. Everything hangs off three structures:

- **`RootMeta`** — per-partition durable state bundle: main root `root_id_`,
  TTL root `ttl_root_id_`, the data `mapper_` and segment `segment_mapper_`,
  live mapping/segment snapshots, manifest size, next TTL expiration,
  compression dictionary, and `first_unflushed_*_fp_id_` watermarks consumed
  by GC. Cached globally in `RootMetaMgr` (LRU, `root_meta_cache_size`),
  pinned via `Handle` while any task uses it.
- **`PageMapper` / `MappingSnapshot`** — the logical→physical indirection.
  `MappingTbl` is a chunked array `PageId → uint64`, where each value is
  type-tagged (`ValType`): a `FilePageId`, a swizzled `MemCachedPage*`, a
  free-list `PageId` link, or Invalid. Snapshots are immutable-by-version:
  a CoW write produces a *new* snapshot that records pending changes; readers
  hold `MappingSnapshot::Ref`s. Each snapshot lists `to_free_file_pages_` and
  links to `next_snapshot_` — physical pages are recycled only after every
  older snapshot is released (no reader can see a reused page).
- **`FilePageAllocator`** — turns logical growth into physical placement.
  `AppendAllocator` (append mode; monotonically increasing FilePageIds,
  tracks `min_file_id_`/`empty_file_cnt_` for space accounting) or
  `PooledFilePages` (in-place mode; free-id pool). The allocator is
  *reconstructed from manifest state* on replay, never persisted separately.

## Manifest format

Built by `ManifestBuilder` (`include/storage/root_meta.h` documents the exact
byte layout). A manifest file = one **snapshot** record + N appended **log**
records, every record framed as:

```
checksum(8B) | root(4B) | ttl_root(4B) | payload_len(4B) | payload
```

- Snapshot payload: `max_fp_id`, compression dictionary, full data mapping
  table, `BranchManifestMetadata` (branch name, term, per-branch file ranges),
  segment `max_fp_id` + full segment mapping.
- Log payload: data-mapping deltas, `BranchManifestMetadata`, segment-mapping
  deltas.

Each committing write (`WriteTask::FlushManifest`) appends one log record;
when the file exceeds `manifest_limit`, the writer instead writes a fresh
snapshot to a temp file and atomically renames it (`SwitchManifest`). In
cloud mode each append also uploads the manifest object (doc 07).

## Replay

`Replayer::Replay(ManifestFile*)` parses the snapshot, then applies log
records in order, rebuilding mapping tables, roots, branch metadata, and file
watermarks. A corrupt or torn record stops replay at the last valid boundary
(`corrupted_log_found_`, `file_size_before_corrupted_log_`) — torn tails from
crashes are expected and safely truncated. `GetMapper`/`GetSegmentMapper`
materialize fresh `PageMapper`s with correctly seeded allocators.

Replay is the *single* restore path, used by:

- Cold open of a partition (first access after start).
- `ReopenRequest` — drop in-memory state, re-replay (possibly a newer manifest
  fetched from cloud/standby, possibly an archive tag).
- `PageManager::InstallExternalSnapshot` — install a remote snapshot into the
  RootMeta chain without local CoW writes; missing remote manifest can fall
  back to `InstallEmptySnapshot` (Reopen-clean).

Changing manifest encoding therefore changes cold start, crash recovery,
cloud adoption, and archive restore at once — design migrations accordingly.

## On-disk layout and file naming

Per partition directory `<store_path>/<tbl>.<partition>/` (placement across
multiple `store_path`s via `store_path_lut`):

| File | Name format (all in `include/common.h`) |
|------|------------------------------------------|
| Data file | `data_<file_id>_<branch>_<term>` (legacy `data_<id>_<term>`) |
| Segment file | `segment_<file_id>_<branch>_<term>` |
| Manifest | `manifest_<branch>_<term>` (legacy `manifest_<term>`) |
| Archive | `manifest_<branch>_<term>_<tag>` |
| Temp | `*.tmp` suffix during atomic write+rename |

Store-root level (cloud bucket root in cloud mode):
`CURRENT_TERM_<branch>_<pg_id>` — content is the current term number.

Data/segment files are sized by `pages_per_file_shift` /
`segments_per_file_shift`. `TypedFileId` (`include/types.h`) encodes
data-vs-segment in the LSB (`file_id << 1 | is_segment`) so both kinds share
one FD table and one branch-range lookup; sentinels near `MaxFileId` denote
the manifest and directory FDs.

## Terms

The embedding system passes a `term` to `Start()` (0 in Local mode). Files and
manifests are stamped with the writer's term. In cloud mode,
`CloudStorageService` bootstrap CAS-updates `CURRENT_TERM_<branch>_<pg_id>`
(ETag-guarded); a writer that discovers a newer published term gets
`KvError::ExpiredTerm` and must stop — this fences zombie writers after
failover. Readers/GC treat *newer-term* files conservatively (see BranchGuard
below). `ManifestTermFromFilename` etc. parse terms back out of names.

## Branches

A branch is a fork of a partition sharing history with its parent:

- `BranchManifestMetadata` in every manifest carries the branch's name, term,
  and `BranchFileMapping` — a vector of `BranchFileRange{branch, term,
  max_file_id, max_segment_file_id}` sorted by file id. Because file ids are
  allocated monotonically, a binary search by file id resolves *which
  (branch, term) owns any file* — that is how a child branch reads pages that
  physically live in files written by an ancestor branch.
- `CreateBranch` (per partition, via `BackgroundWrite::CreateBranch`) writes a
  branch manifest for the new branch that inherits the parent's mapping and
  file ranges — O(1), no data copy. `GlobalCreateBranchRequest` fans this out
  and salts the branch name (`<name>-<salt>`), returning `ResultBranch()`.
- `EloqStore::Start(branch, …)` opens the store *on* a branch; the active
  branch flows to each `IouringMgr` (`SetActiveBranch`) and stamps new files.
- `DeleteBranch` removes a branch's manifest and its uniquely-owned files
  (`DeleteBranchFiles`); shared ancestor files are protected by the
  file-range bookkeeping and GC rules.
- `RetainedFileKey{file_id, branch, term}` exists because sibling branches
  forked at the same point can allocate overlapping file ids — file identity
  for GC is the triple, not the id (doc 08).

## Archives

`ArchiveRequest`/`BackgroundWrite::CreateArchive` snapshots the current
manifest into `manifest_<branch>_<term>_<tag>` (append mode only). Archives
pin every file they reference (GC reads archive manifests — doc 08) and are
pruned to `num_retained_archives`. `ReopenRequest::SetTag` /
`CloudStoreMgr::RefreshManifest(tag)` restore from an archive;
`GlobalListArchiveTagsRequest` enumerates them.
