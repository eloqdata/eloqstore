# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

EloqStore is a hybrid-tier key-value storage engine in C++20: in-memory B-tree index (hot), local NVMe via io_uring (warm), S3-compatible object storage (cold). It uses a copy-on-write B-tree so batch writes never block reads. Requires Linux kernel 6.8+ (io_uring) and `rsync` (standby replication and tests).

## Architecture docs — read these first, keep them updated

`docs/architecture/` is the maintained design documentation (its `README.md` is the index). Before working on a subsystem, read `01-overview.md` plus the doc covering that subsystem — it is much faster than re-deriving the design from source, and it records invariants the code only implies. **When a code change alters behavior described in those docs (request types, manifest/page formats, KvOptions fields, GC rules, lifecycle ordering, renamed classes), update the matching doc in the same change.** Each doc lists the source files it covers. Prefer `docs/architecture/` over `.codex/knowledge-base/` (stale) and over `docs/zero_copy_*.md` (original design proposals, partially superseded).

## Build

```bash
# One-time dependency install (Ubuntu 24.04, needs sudo)
bash scripts/install_dependency_ubuntu2404.sh

# Configure + build (SKIP_CREATE_BUCKET=ON is what CI uses for unit tests)
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug -DSKIP_CREATE_BUCKET=ON
cmake --build build -j$(nproc)
```

Useful CMake options: `WITH_ASAN=ON` (needs `libboost_context-asan`), `WITH_COVERAGE=ON`, `ELOQ_MODULE_ENABLED=ON` (embedding/module mode), `WITH_UNIT_TESTS`/`WITH_EXAMPLE`/`WITH_DB_STRESS`/`WITH_BENCHMARK` (all default ON). Compile commands are exported to `build/` and `.clangd` points there.

CMake configure runs `git submodule update --init --recursive` automatically (submodules live in `external/`).

## Tests

Cloud-mode tests need MinIO running on `127.0.0.1:9900` first:

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x minio
./minio server /tmp/minio-data --address :9900 --console-address :9901
```

```bash
# All tests
ctest --test-dir build/tests/ --output-on-failure

# Each tests/*.cpp builds into its own Catch2 executable in build/tests/.
# Run one binary, or one test case by name / tag:
./build/tests/batch_write
./build/tests/batch_write "mixed batch write with read"
./build/tests/scan "[scan]"
# Or via ctest regex:
ctest --test-dir build/tests/ -R "complex scan"
```

The `large_value_*` and `segment_compact` tests register io_uring fixed buffers and need RLIMIT_MEMLOCK ≥ 2GB. Their custom main (`tests/large_value_main.cpp`) bumps the limit best-effort; CI runs with `--ulimit memlock=-1:-1`. Under WSL2 use `systemd-run --user --pipe --wait --property=LimitMEMLOCK=2G ./build/tests/large_value_e2e`.

Test fixtures: `tests/common.h` defines the canonical `KvOptions` presets (`default_opts`, `append_opts`, `cloud_options`, ...) plus S3 helpers; local data goes under `/tmp/eloqstore` or `/tmp/test-data`, and `CleanupStore()` wipes both local and MinIO state.

SDK tests (also run in CI): `python3 -m build --wheel` + pytest in `python/`; in `rust/`: `cargo test -p eloqstore --test integration_test -- --nocapture --test-threads=1`.

## Formatting

`bash scripts/format.sh` — installs clang-format 18.1.8 on first run and formats the tree. Main branch has a format check in CI; run before committing. Style is Google-based with Allman braces, 4-space indent, right pointer alignment (see `.clang-format`).

## Architecture

Full subsystem documentation lives in `docs/architecture/` (see above). The load-bearing invariants, as a quick reference:

**EloqStore is the root owner** (`include/eloq_store.h`, `src/eloq_store.cpp`). `EloqStore::Start` derives one `StoreMode` from `KVOptions` — cloud (cloud bucket set), standby replica (standby remote path set), standby master (standalone replication), else local — and that choice fixes the whole topology: which `AsyncIoManager` backend is built, whether `CloudStorageService`/`StandbyService` exist, which options are legal. Startup is dependency-ordered (options → store-space metadata → shards → remote services → shard execution → archive/prewarm); shutdown is the exact inverse so late completions never touch freed state.

**Shard-thread affinity replaces locking.** Each shard owns one worker thread and event loop. Requests become pooled `KvTask` subclasses (`TaskManager`) running as `boost::context` coroutines; `Resume`/`Yield`/`WaitIo`/`FinishIo` are the handoff points. All async completions — including cloud and standby ones arriving on service threads — must re-enter through the shard's ready queues, never resume a task on a foreign thread. Per-table write serialization and write-concurrency limits are enforced in `TaskManager` above the coroutine layer. In module mode (`EloqStoreModule`) the embedding runtime drives shard progress via one-round `Process`/`HasTask` calls instead of dedicated threads.

**Request surface** (`include/eloq_store.h`): `KvRequest` subclasses with `ExecSync`/`ExecAsyn`; `SetDone()` is the single completion gate (callback OR sync wake, never both). `KvRequest::ReadOnly()` depends on enum ordering (`Type() < RequestType::BatchWrite`) — don't reorder `RequestType`. Global requests (`DropTable`, `GlobalArchive`, `GlobalReopen`) fan out into per-partition subrequests at the store level; everything else routes to `shards_[TableId().ShardIndex(...)]`. Borrowed `string_view` request inputs must outlive completion.

**I/O stack is layered, not parallel implementations** (`include/async_io_manager.h`): `IouringMgr` is the local-filesystem base (page buffers, manifests, FD cache, direct I/O, merged writes); `CloudStoreMgr` extends it, treating local files as a cache over object storage (`ObjectStore` → `CloudStorageService` → `AsyncHttpManager`); `StandbyStoreMgr` extends it filling missing state via rsync jobs in `StandbyService`. Local manifest/page semantics are the common substrate in every mode — remote sync and cache cleanup must preserve local replayability.

**On-disk and in-memory page formats intentionally differ** (`src/storage/`). `DataPageBuilder`/`IndexPageBuilder` define the canonical encoded layout (prefix-compressed keys, restart points, timestamp deltas, overflow flags); large values go to chained overflow pages. In memory, internal nodes are swizzled `MemIndexPage` objects with pins and cached child pointers, managed by `IndexPageManager`. Any file-format change must update both builder and reader paths.

**Durability is anchored at RootMeta + manifest, not pages** (`src/storage/root_meta.cpp`, `src/storage/page_mapper.cpp`, `src/replayer.cpp`). Logical-to-file page identity is versioned through `MappingSnapshot`s for COW root transitions; `PageMapper` owns the logical page table and allocator, reconstructed from manifest replay. `Replayer` handles both cold restart and live refresh (reopen, cloud/standby snapshot install) — manifest encoding changes must keep both working. Swizzled pages can only be recycled after being unswizzled from every live mapping snapshot.

**Bindings stack** (one-way dependency onto the native engine): `ffi/` C ABI (`eloqstore_capi`, opaque handles, explicit free functions) → `rust/eloqstore-sys` (raw FFI) → `rust/eloqstore` (high-level API); `python/` SDK also wraps the C ABI. Bindings adapt API shape but must not invent a different lifecycle, completion, or ownership model.

## CI notes

PRs need the `ci-approved` label for CI to run (`.github/workflows/ci.yml`). CI builds Debug with `SKIP_CREATE_BUCKET=ON`, runs C++ tests against MinIO, then builds/tests the Python wheel and Rust SDK.
