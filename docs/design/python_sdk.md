# EloqStore Python SDK Design

## Summary

This document describes the initial Python SDK for EloqStore.

The SDK follows the repository's preferred language-binding direction:

- C++ engine remains the source of truth.
- A stable C API is the cross-language boundary.
- Python builds on that C API instead of binding internal C++ request
  types directly.

## Goals

- Expose a minimal, stable Python API for embedded EloqStore usage.
- Keep the first SDK useful for LMCache and other Python callers.
- Reuse the existing C API work already present in the repository.
- Avoid leaking internal request-oriented C++ APIs into Python.

## Non-Goals

- Do not expose every C++ request type in Python.
- Do not make async Python APIs part of the first release.
- Do not optimize for zero-copy restore in the first release.
- Do not make the first SDK depend on MP-mode LMCache assumptions.

## API Boundary

The SDK stack is:

- EloqStore C++ engine
- `eloqstore_capi` shared library
- pure-Python `ctypes` wrapper
- public Python `Client`

This preserves a clean language boundary and keeps the public Python API
independent from internal C++ refactors.

## Public Python API

The first SDK exposes:

- `Options`
- `Client`
- `EloqStoreError`

### `Options`

`Options` is a convenience dataclass that covers the common embedded
configuration path:

- `store_paths`
- `options_path`
- `table_name`
- `partition_id`
- `branch`
- `term`
- `partition_group_id`
- `validate`
- `num_threads`
- `data_page_size`
- `pages_per_file_shift`
- `data_append_mode`
- `overflow_pointers`
- `enable_compression`
- `buffer_pool_size`
- `manifest_limit`
- `fd_limit`

The dataclass is not a full mirror of `KvOptions`. It is intentionally
focused on the common embedded-store case with validated FFI inputs.

### `Client`

`Client` owns:

- one store handle
- one table handle
- store lifecycle (`start`, `close`)
- common key-value APIs

The first release exposes:

- `put`
- `get`
- `get_into`
- `exists`
- `delete`
- `batch_put`
- `batch_get`
- `batch_delete`

This is enough for LMCache's intended backend integration and for
general-purpose Python callers who want embedded byte-oriented storage.

## Why `ctypes` First

The first SDK uses `ctypes` rather than pybind11.

Reasons:

- lowest integration overhead once a C API already exists
- no need to map internal C++ types directly into Python
- Python package can stay mostly pure-Python
- the public API can remain stable if the implementation later moves to
  pybind11 or cffi

If profiling later shows the FFI overhead is material, the backend can
switch implementations behind the same `Client` API.

## C API Requirements

The Python SDK depends on these C API capabilities:

- create / destroy options
- load options from ini
- create / destroy store
- start store with branch selection
- stop store
- create / destroy table identifier
- put / batch put
- get
- exists
- delete / batch delete
- get last error
- free get result

The current SDK does not require:

- scan
- floor
- complex batch-write builders

Those stay available in C, but are not part of the Python-first surface
yet.

## Error Model

Python raises `EloqStoreError` when the C API returns failure status.

Each raised error includes:

- numeric status code
- symbolic status name when available
- last C API error string if present

This keeps Python code straightforward and avoids exposing raw status
handling to callers.

## Build and Packaging Model

The repository builds a shared library `eloqstore_capi`. The Python wheel bundles
`libeloqstore_capi.so` under `eloqstore/.libs/`, so no manual `ELOQSTORE_PY_LIB`
override is required when installing from PyPI.

For local development, the library is discovered via:

1. wheel-bundled path (`eloqstore/.libs/`)
2. `ELOQSTORE_PY_LIB` environment variable
3. nearby repository build directory

The Python package is published to PyPI as `eloqstore` with a `manylinux_2_31_x86_64`
platform tag.

## Future Work

- expand `Options` coverage for more `KvOptions` fields
- add scan and iterator APIs
- add bulk read helpers at the C API level
- evaluate a pybind11 or cffi backend behind the same Python API
- cross-platform wheel builds

