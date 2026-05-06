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
- `num_threads`

The dataclass is not a full mirror of `KvOptions`. It is intentionally
small and focused on the common embedded-store case.

### `Client`

`Client` owns:

- one store handle
- one table handle
- store lifecycle (`start`, `close`)
- common key-value APIs

The first release exposes:

- `put`
- `get`
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

The repository now builds a shared library target:

- `eloqstore_capi`

The Python package itself is currently pure-Python and expects the
shared library to be discoverable via:

1. `ELOQSTORE_PY_LIB`
2. a package-local bundled library path
3. a nearby repository build directory

This is sufficient for local development and SDK iteration.

A later packaging phase can bundle the shared library directly into the
wheel once the ABI and build story are stable.

## Future Work

- bundle `eloqstore_capi` into platform wheels
- expand `Options` coverage for more `KvOptions` fields
- add scan and iterator APIs
- add bulk read helpers at the C API level
- evaluate a pybind11 or cffi backend behind the same Python API

