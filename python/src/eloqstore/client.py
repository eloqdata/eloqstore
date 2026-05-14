from __future__ import annotations

from ctypes import POINTER, c_size_t, c_uint8, c_uint64, c_bool, byref
from dataclasses import dataclass, field
from time import time_ns
from typing import Any, Iterable, Mapping, Sequence

from ._errors import EloqStoreError
from ._ffi import (
    CGetResult,
    alloc_bytes,
    as_input_buffer,
    as_output_buffer,
    lib,
    last_error,
)


def _to_bytes(data: str | bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return data
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, memoryview):
        return data.tobytes()
    raise TypeError(f"Expected str or bytes, got {type(data)!r}")


def _ok(status: int) -> None:
    if status != 0:
        raise EloqStoreError(status, last_error())


def _validate_uint(name: str, value: int, max_value: int) -> int:
    if not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value)!r}")
    if value < 0 or value > max_value:
        raise ValueError(f"{name} must be between 0 and {max_value}, got {value}")
    return value


@dataclass(slots=True)
class Options:
    # ── required ──
    store_paths: Sequence[str] = field(default_factory=list)
    # ── table / branch ──
    options_path: str | None = None
    table_name: str = "default"
    partition_id: int = 0
    branch: str = "main"
    term: int = 0
    partition_group_id: int = 0
    validate: bool = True
    # ── engine ──
    num_threads: int | None = None
    # ── B+Tree storage ──
    data_page_size: int | None = None  # bytes, max 65535 (uint16)
    pages_per_file_shift: int | None = None  # data file = page_size << shift
    data_append_mode: bool | None = None
    overflow_pointers: int | None = None  # max 128
    enable_compression: bool | None = None
    # ── resource limits ──
    buffer_pool_size: int | None = None  # bytes, index page cache per shard
    manifest_limit: int | None = None  # bytes, WAL file size limit
    fd_limit: int | None = None  # max open files


class Client:
    def __init__(self, options: Options):
        self._lib = lib()
        self._options = options
        self._closed = False
        self._opts_handle = None
        self._store_handle = None
        self._table_handle = None
        self._opts_handle = self._lib.CEloqStore_Options_Create()
        if not self._opts_handle:
            raise EloqStoreError(1, last_error())

        try:
            if options.options_path:
                ok = self._lib.CEloqStore_Options_LoadFromIni(
                    self._opts_handle, options.options_path.encode("utf-8")
                )
                if not ok:
                    raise EloqStoreError(1, last_error())

            for path in options.store_paths:
                self._lib.CEloqStore_Options_AddStorePath(
                    self._opts_handle, path.encode("utf-8")
                )

            if options.num_threads is not None:
                self._lib.CEloqStore_Options_SetNumThreads(
                    self._opts_handle,
                    _validate_uint("num_threads", options.num_threads, 65535),
                )
            if options.data_page_size is not None:
                self._lib.CEloqStore_Options_SetDataPageSize(
                    self._opts_handle,
                    _validate_uint("data_page_size", options.data_page_size, 65535),
                )
            if options.pages_per_file_shift is not None:
                self._lib.CEloqStore_Options_SetPagesPerFileShift(
                    self._opts_handle,
                    _validate_uint(
                        "pages_per_file_shift", options.pages_per_file_shift, 255
                    ),
                )
            if options.data_append_mode is not None:
                self._lib.CEloqStore_Options_SetDataAppendMode(
                    self._opts_handle, options.data_append_mode
                )
            if options.overflow_pointers is not None:
                self._lib.CEloqStore_Options_SetOverflowPointers(
                    self._opts_handle,
                    _validate_uint("overflow_pointers", options.overflow_pointers, 128),
                )
            if options.enable_compression is not None:
                self._lib.CEloqStore_Options_SetEnableCompression(
                    self._opts_handle, options.enable_compression
                )
            if options.buffer_pool_size is not None:
                self._lib.CEloqStore_Options_SetBufferPoolSize(
                    self._opts_handle,
                    _validate_uint(
                        "buffer_pool_size", options.buffer_pool_size, 2**64 - 1
                    ),
                )
            if options.manifest_limit is not None:
                self._lib.CEloqStore_Options_SetManifestLimit(
                    self._opts_handle,
                    _validate_uint("manifest_limit", options.manifest_limit, 2**32 - 1),
                )
            if options.fd_limit is not None:
                self._lib.CEloqStore_Options_SetFdLimit(
                    self._opts_handle,
                    _validate_uint("fd_limit", options.fd_limit, 2**32 - 1),
                )

            if options.validate and not self._lib.CEloqStore_Options_Validate(
                self._opts_handle
            ):
                raise EloqStoreError(1, last_error())

            self._store_handle = self._lib.CEloqStore_Create(self._opts_handle)
            if not self._store_handle:
                raise EloqStoreError(1, last_error())

            self._table_handle = self._lib.CEloqStore_TableIdent_Create(
                options.table_name.encode("utf-8"), options.partition_id
            )
            if not self._table_handle:
                raise EloqStoreError(1, last_error())

            _ok(
                self._lib.CEloqStore_StartWithBranch(
                    self._store_handle,
                    options.branch.encode("utf-8"),
                    options.term,
                    options.partition_group_id,
                )
            )
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._table_handle:
            self._lib.CEloqStore_TableIdent_Destroy(self._table_handle)
            self._table_handle = None
        if self._store_handle:
            if not self._lib.CEloqStore_IsStopped(self._store_handle):
                self._lib.CEloqStore_Stop(self._store_handle)
            self._lib.CEloqStore_Destroy(self._store_handle)
            self._store_handle = None
        if self._opts_handle:
            self._lib.CEloqStore_Options_Destroy(self._opts_handle)
            self._opts_handle = None

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def put(
        self,
        key: str | bytes,
        value: str | bytes | bytearray | memoryview | Any,
        *,
        timestamp: int | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        value_arr, value_ptr, value_len = as_input_buffer(value)
        _ok(
            self._lib.CEloqStore_Put(
                self._store_handle,
                self._table_handle,
                key_ptr,
                key_len,
                value_ptr,
                value_len,
                ts,
            )
        )

    def get(self, key: str | bytes) -> bytes | None:
        if self._closed:
            raise RuntimeError("store is closed")
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        result = CGetResult()
        status = self._lib.CEloqStore_Get(
            self._store_handle, self._table_handle, key_ptr, key_len, result
        )
        _ok(status)
        try:
            if not result.found:
                return None
            return bytes(result.value[: result.value_len])
        finally:
            self._lib.CEloqStore_FreeGetResult(result)

    def get_into(self, key: str | bytes, out_buffer: Any) -> int | None:
        if self._closed:
            raise RuntimeError("store is closed")
        key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
        out_arr, out_ptr, out_len = as_output_buffer(out_buffer)
        result = CGetResult()
        status = self._lib.CEloqStore_GetInto(
            self._store_handle,
            self._table_handle,
            key_ptr,
            key_len,
            out_ptr,
            out_len,
            result,
        )
        _ok(status)
        if not result.found:
            return None
        return int(result.value_len)

    def exists(self, key: str | bytes) -> bool:
        if self._closed:
            raise RuntimeError("store is closed")
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        out = c_bool(False)
        _ok(
            self._lib.CEloqStore_Exists(
                self._store_handle, self._table_handle, key_ptr, key_len, byref(out)
            )
        )
        return bool(out)

    def delete(self, key: str | bytes, *, timestamp: int | None = None) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        _ok(
            self._lib.CEloqStore_Delete(
                self._store_handle,
                self._table_handle,
                key_ptr,
                key_len,
                ts,
            )
        )

    def batch_put(
        self,
        items: Mapping[str | bytes, Any] | Iterable[tuple[str | bytes, Any]],
        *,
        timestamp: int | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        pairs = list(items.items()) if isinstance(items, Mapping) else list(items)
        if not pairs:
            return

        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)

        key_ptrs = []
        key_lens = []
        value_ptrs = []
        value_lens = []
        key_arrays = []
        value_arrays = []

        for key, value in pairs:
            key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
            value_arr, value_ptr, value_len = as_input_buffer(value)
            key_arrays.append(key_arr)
            value_arrays.append(value_arr)
            key_ptrs.append(key_ptr)
            key_lens.append(key_len)
            value_ptrs.append(value_ptr)
            value_lens.append(value_len)

        KeyPtrArray = POINTER(c_uint8) * len(key_ptrs)
        ValuePtrArray = POINTER(c_uint8) * len(value_ptrs)
        LenArray = c_size_t * len(key_lens)

        _ok(
            self._lib.CEloqStore_PutBatch(
                self._store_handle,
                self._table_handle,
                KeyPtrArray(*key_ptrs),
                LenArray(*key_lens),
                ValuePtrArray(*value_ptrs),
                LenArray(*value_lens),
                len(pairs),
                ts,
            )
        )

    def batch_get(self, keys: Sequence[str | bytes]) -> list[bytes | None]:
        if self._closed:
            raise RuntimeError("store is closed")
        return [self.get(key) for key in keys]

    def batch_delete(
        self, keys: Sequence[str | bytes], *, timestamp: int | None = None
    ) -> None:
        if self._closed:
            raise RuntimeError("store is closed")
        if not keys:
            return

        ts = timestamp if timestamp is not None else time_ns()
        _validate_uint("timestamp", ts, 2**64 - 1)

        key_ptrs = []
        key_lens = []
        key_arrays = []
        for key in keys:
            key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
            key_arrays.append(key_arr)
            key_ptrs.append(key_ptr)
            key_lens.append(key_len)

        KeyPtrArray = POINTER(c_uint8) * len(key_ptrs)
        LenArray = c_size_t * len(key_lens)

        _ok(
            self._lib.CEloqStore_DeleteBatch(
                self._store_handle,
                self._table_handle,
                KeyPtrArray(*key_ptrs),
                LenArray(*key_lens),
                len(keys),
                ts,
            )
        )
