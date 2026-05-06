# EloqStore Python SDK

This directory contains the first Python SDK for EloqStore.

## Status

The SDK is currently intended for local development and embedded usage.
It wraps the repository's C API shared library, `eloqstore_capi`.

## Build the shared library

From the repository root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target eloqstore_capi -j8
```

The resulting library is typically:

- `build/libeloqstore_capi.so`

## Install the Python package

From the `python/` directory:

```bash
pip install -e .
```

To build a distributable wheel that bundles `libeloqstore_capi.so`:

```bash
python -m pip install build
python -m build --wheel
```

The wheel build runs CMake against the repository root and embeds the
resulting shared library under `eloqstore/.libs/`.

If the shared library is not in a default search location, point the SDK
at it explicitly:

```bash
export ELOQSTORE_PY_LIB=/abs/path/to/libeloqstore_capi.so
```

## Example

```python
from eloqstore import Client, Options

client = Client(
    Options(
        store_paths=["/tmp/eloqstore"],
        table_name="example",
        partition_id=0,
        num_threads=1,
    )
)

client.put("hello", b"world")
assert client.get("hello") == b"world"
assert client.exists("hello")
client.delete("hello")
client.close()
```

## Example scripts

Under `python/examples/`:

- `basic_usage.py`: in-memory embedded usage
- `disk_persistence.py`: local disk mode with reopen/persistence
- `ini_branch_usage.py`: loading options from ini and starting a named branch
