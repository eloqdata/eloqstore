# Overview

EloqStore is an embeddable, persistent key-value storage engine written in
C++20. It is a *library*, not a server: the embedding application (EloqKV,
EloqDoc, EloqSQL, the Rust/Python SDKs, or the data-store service) constructs
an `eloqstore::EloqStore`, starts it, and submits request objects to it.

Design goals, in priority order:

1. **Predictable point-read latency on flash** — every point read on cold data
   costs exactly one disk I/O, because all B+-tree index (non-leaf) pages are
   kept in memory and only leaf/data pages live on disk.
2. **High-throughput batch writes without blocking reads** — the tree is
   copy-on-write (CoW); a batch write builds new pages and publishes a new
   root, while concurrent readers keep traversing the old root.
3. **Object storage as primary storage** — in cloud mode, S3-compatible object
   storage holds the durable copy and the local disk is just a cache.

## Core concepts and glossary

| Term | Meaning |
|------|---------|
| **Partition** | The unit of data and of write serialization. Identified by `TableIdent {tbl_name_, partition_id_}` (`include/types.h`). Each partition is an independent B+-tree with its own manifest, files, and directory `<store_path>/<tbl_name>.<partition_id>/`. |
| **Shard** | The unit of execution. One shard = one worker thread + one io_uring + one set of caches (`include/storage/shard.h`). `KvOptions::num_threads` sets the shard count. A partition is statically routed to shard `TableIdent::ShardIndex(num_shards)` (hash). |
| **Page** | 4 KiB (configurable `data_page_size`) aligned buffer; the B+-tree node unit. Types: non-leaf index, leaf index, data (leaf), overflow (`include/storage/page.h`). |
| **PageId / FilePageId** | `PageId` is a partition-local *logical* page number; `FilePageId` is the *physical* location (file id × pages-per-file + offset). The indirection table between them is the page mapping (doc 06). |
| **Segment** | Large (default 256 KiB) append-only allocation unit for very-large values, stored in dedicated *segment files* (doc 09). |
| **Manifest** | Per-partition redo log of metadata: tree roots + page mapping snapshot + appended deltas. Replayed on open (doc 06). |
| **Term** | Monotonic ownership epoch supplied by the embedding system (e.g. raft term). File names embed the term that wrote them; in cloud mode a `CURRENT_TERM_<branch>_<pg_id>` object fences stale writers (doc 06). |
| **Branch** | A named fork of a partition's data (`MainBranchName = "main"`). Branches share immutable files with their parent via `BranchFileMapping` file-range bookkeeping; used for instant data forks (doc 06). |
| **Archive** | A retained manifest snapshot (`manifest_<branch>_<term>_<tag>`) that pins the set of files needed to reopen the partition at that point in time (docs 06, 08). |
| **Request / Task** | Callers submit `KvRequest` objects (doc 03); shards execute them as pooled `KvTask` coroutines (doc 04). |

## Store modes

`DeriveStoreMode(opts)` in `include/eloq_store.h` picks exactly one mode at
`Start()`:

| Mode | Selected when | Backing |
|------|---------------|---------|
| `Local` | default | Local files via io_uring (`IouringMgr`); or pure in-memory (`MemStoreMgr`) when `store_path` is empty |
| `Cloud` | `cloud_store_path` non-empty | S3-compatible object store is durable; local disk is an LRU cache (`CloudStoreMgr`) |
| `StandbyMaster` | `enable_local_standby` and no `standby_master_addr` | Local mode that allows replicas to rsync from it |
| `StandbyReplica` | `enable_local_standby` + `standby_master_addr` | Pulls manifests/files from the master over rsync on demand (`StandbyStoreMgr`) |

The mode decides which `AsyncIoManager` subclass each shard gets, which
auxiliary services exist (`CloudStorageService`, `StandbyService`,
`PrewarmService`, `ArchiveCrond`), and which option combinations are legal
(doc 02, 07).

## Write modes

Independent of store mode, `KvOptions::data_append_mode` selects how data
pages map to files:

- **Append mode (`true`)** — pages are always written to the end of the newest
  data file (`AppendAllocator`); rewritten pages leave holes that background
  compaction reclaims. Required pattern for cloud mode (files become immutable
  objects once sealed) and for archives/branches.
- **In-place mode (`false`)** — file pages are recycled from a free pool
  (`PooledFilePages`); fewer files, no compaction, but files mutate in place.

## Repository layout

```
include/, src/           core engine (src/storage = tree+pages+shard, src/tasks = request execution)
ffi/                     C ABI (eloqstore_capi) over the C++ API
rust/                    eloqstore-sys (raw FFI) + eloqstore (high-level SDK)
python/                  Python SDK wrapping the C ABI
tests/                   Catch2 unit/integration tests (one binary per .cpp)
benchmark/, micro-bench/ throughput/latency drivers
db_stress/               randomized stress + crash-recovery harness
tools/                   page_checksum_tool, manifest_check_tool
examples/                minimal C++ usage
docs/architecture/       this documentation
```

Key entry points when reading code:

- Public API: `include/eloq_store.h`, `include/kv_options.h`, `include/types.h`, `include/error.h`
- Engine root: `src/eloq_store.cpp`
- Per-shard runtime: `src/storage/shard.cpp`, `src/tasks/task.cpp`
- Tree write path: `src/tasks/batch_write_task.cpp`, `src/tasks/write_task.cpp`
- I/O backends: `include/async_io_manager.h`, `src/async_io_manager.cpp`
- Durability: `src/storage/root_meta.cpp`, `src/storage/page_mapper.cpp`, `src/replayer.cpp`
- GC: `src/file_gc.cpp`
