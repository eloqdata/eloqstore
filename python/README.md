# eloqstore — Python SDK

[![PyPI](https://img.shields.io/pypi/v/eloqstore)](https://pypi.org/project/eloqstore/)
[![Python](https://img.shields.io/pypi/pyversions/eloqstore)](https://pypi.org/project/eloqstore/)

Python SDK for [EloqStore](https://github.com/eloqdata/eloqstore), a high-performance hybrid-tier key-value storage engine. Combines object storage (S3) with local NVMe SSDs for exceptional write throughput and sub-millisecond read latency.

## Installation

```bash
pip install eloqstore
```

Requires Linux kernel 6.8+ (uses `io_uring`).

## Quick Start

```python
from eloqstore import Client, Options

client = Client(Options(table_name="demo", partition_id=0, num_threads=1))
client.put("hello", b"world")
assert client.get("hello") == b"world"
assert client.exists("hello")
client.delete("hello")
client.close()
```

## Disk Persistence

```python
from eloqstore import Client, Options

client = Client(
    Options(
        store_paths=["/data/eloqstore"],
        table_name="mydb",
        partition_id=0,
        num_threads=4,
    )
)
client.put("key", b"value")
client.close()
# Data persists — reopen later with the same store_paths
```

## INI Configuration

```python
from eloqstore import Client, Options

client = Client(
    Options(
        store_paths=["/data/eloqstore"],
        options_path="/etc/eloqstore.ini",
        table_name="mydb",
        partition_id=0,
        branch="feature-x",
        term=7,
    )
)
```

## API

| Method | Description |
|---|---|
| `Client(options)` | Create a client with `Options` |
| `put(key, value, *, timestamp)` | Upsert a key-value pair |
| `get(key)` | Get value by key, returns `None` if missing |
| `get_into(key, out_buffer)` | Read value into a pre-allocated buffer |
| `exists(key)` | Check if key exists |
| `delete(key, *, timestamp)` | Delete a key |
| `batch_put(items, *, timestamp)` | Batch upsert multiple key-value pairs |
| `batch_get(keys)` | Batch get multiple values |
| `batch_delete(keys, *, timestamp)` | Batch delete multiple keys |
| `close()` | Close the store, release resources |

## Building from Source

```bash
cd python
python -m pip install build
python -m build --wheel
pip install dist/*.whl
```

If the shared library is not auto-discovered, set:

```bash
export ELOQSTORE_PY_LIB=/path/to/libeloqstore_capi.so
```

## License

BSL 2.0 or AGPL 3.0 (dual-licensed). See [LICENSE.md](https://github.com/eloqdata/eloqstore/blob/main/LICENSE.md).
