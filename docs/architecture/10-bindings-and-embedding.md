# Bindings and Embedding

Source: `ffi/include/eloqstore_capi.h`, `ffi/src/eloqstore_capi.cpp`,
`rust/eloqstore-sys/`, `rust/eloqstore/`, `python/src/eloqstore/`,
`include/eloqstore_module.h`, `src/eloqstore_module.cpp`.

Dependency direction is strictly one-way: every layer adapts API *shape* but
must preserve the native store's lifecycle, completion, and memory-ownership
model. None of them invent a second data path.

```
C++  EloqStore / KvRequest            (the only engine API)
└── ffi/ eloqstore_capi               C ABI, opaque handles
    ├── rust/eloqstore-sys            raw extern "C" declarations + lib embedding
    │   └── rust/eloqstore            safe high-level SDK
    └── python/ (ctypes via _ffi.py)  Python SDK
```

## C ABI (`ffi/`)

- Flat C functions over opaque handles: `CEloqStoreHandle`,
  `CTableIdentHandle`, `CScanRequestHandle`, `CBatchWriteHandle`. Request
  objects survive as handles only where state spans multiple calls (scans,
  batch-write builders); simple ops are single functions.
- `CEloqStoreStatus` mirrors `KvError` (keep in sync when adding errors).
- Results backed by native allocations come with dedicated `*_free`
  functions; handle types have their own destructors. Crossing the boundary
  copies values — the zero-copy large-value paths are *not* exposed through
  the C ABI today.
- Built as the `eloqstore_capi` shared library (see root `CMakeLists.txt`),
  installed alongside `eloq_store.h` headers.

## Rust SDK (`rust/`)

- `eloqstore-sys`: re-declares the C ABI, plus build logic that either links a
  prebuilt library or embeds/builds the native one
  (`ELOQSTORE_STATIC_EXE=1` for static examples; see `embedded_lib.rs` and
  `rust/README.md`).
- `eloqstore`: two API styles over the same core — RocksDB-style convenience
  methods (`put/get/delete/scan`) and a typed request/response trait layer
  (`request.rs`, `response.rs`, `traits.rs`). `Options`/`TableIdentifier`
  wrap `KvOptions`/`TableIdent`. Integration tests:
  `cargo test -p eloqstore --test integration_test -- --test-threads=1`.

## Python SDK (`python/`)

`eloqstore.Client(Options(...))` loads `libeloqstore_capi.so` via ctypes
(`_ffi.py`; override path with `ELOQSTORE_PY_LIB`). The wheel build
(`build_hooks.py`) compiles the native library. Design notes:
`docs/design/python_sdk.md`.

## EloqModule embedding (`ELOQ_MODULE_ENABLED`)

A different integration axis: same process, *externally scheduled*. Built
only when the embedding runtime (EloqKV's tx service, with bthread) defines
`ELOQ_MODULE_ENABLED`:

- `EloqStoreModule : eloq::EloqModule` exposes `ExtThdStart/ExtThdEnd`
  (bind/unbind the thread-local `shard`), `Process(thd_id)` (run
  `Shard::WorkOneRound`), and `HasTask(thd_id)`.
- Shards own no threads; the host's workers drive shard progress. Shard
  count must match the host's worker topology (`thd_id` ↔ `shard_id`).
- Request sync waits switch from `std::atomic` wait to bthread
  mutex/condvar so blocking a request doesn't block a host worker thread.
- Shutdown is cooperative: shards are flagged `Stopping` and the host's
  workers run rounds until each reports `Stopped` (see
  `EloqStore::CleanupRuntime`).

When changing `Shard::WorkLoop`, mirror the change in `WorkOneRound` — they
are the same scheduler in two drive modes.
