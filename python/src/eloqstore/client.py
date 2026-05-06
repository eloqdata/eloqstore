from __future__ import annotations

from ctypes import POINTER, c_size_t, c_uint8, c_uint64
from dataclasses import dataclass, field
from time import time_ns
from typing import Iterable, Mapping, Sequence

from ._errors import EloqStoreError
from ._ffi import CGetResult, alloc_bytes, lib, last_error


def _to_bytes(data: str | bytes) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode("utf-8")
    raise TypeError(f"Expected str or bytes, got {type(data)!r}")


def _ok(status: int) -> None:
    if status != 0:
        raise EloqStoreError(status, last_error())


@dataclass(slots=True)
class Options:
    store_paths: Sequence[str] = field(default_factory=list)
    options_path: str | None = None
    table_name: str = "default"
    partition_id: int = 0
    branch: str = "main"
    term: int = 0
    partition_group_id: int = 0
    num_threads: int | None = None
    validate: bool = True


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
                    self._opts_handle, options.num_threads
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

    def put(self, key: str | bytes, value: bytes, *, timestamp: int | None = None) -> None:
        key_b = _to_bytes(key)
        value_b = _to_bytes(value)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        value_arr, value_ptr, value_len = alloc_bytes(value_b)
        _ok(
            self._lib.CEloqStore_Put(
                self._store_handle,
                self._table_handle,
                key_ptr,
                key_len,
                value_ptr,
                value_len,
                timestamp if timestamp is not None else time_ns(),
            )
        )

    def get(self, key: str | bytes) -> bytes | None:
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
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

    def exists(self, key: str | bytes) -> bool:
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        return bool(
            self._lib.CEloqStore_Exists(
                self._store_handle, self._table_handle, key_ptr, key_len
            )
        )

    def delete(self, key: str | bytes, *, timestamp: int | None = None) -> None:
        key_b = _to_bytes(key)
        key_arr, key_ptr, key_len = alloc_bytes(key_b)
        _ok(
            self._lib.CEloqStore_Delete(
                self._store_handle,
                self._table_handle,
                key_ptr,
                key_len,
                timestamp if timestamp is not None else time_ns(),
            )
        )

    def batch_put(
        self,
        items: Mapping[str | bytes, bytes] | Iterable[tuple[str | bytes, bytes]],
        *,
        timestamp: int | None = None,
    ) -> None:
        pairs = list(items.items()) if isinstance(items, Mapping) else list(items)
        if not pairs:
            return

        key_ptrs = []
        key_lens = []
        value_ptrs = []
        value_lens = []
        key_arrays = []
        value_arrays = []

        for key, value in pairs:
            key_arr, key_ptr, key_len = alloc_bytes(_to_bytes(key))
            value_arr, value_ptr, value_len = alloc_bytes(_to_bytes(value))
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
                timestamp if timestamp is not None else time_ns(),
            )
        )

    def batch_get(self, keys: Sequence[str | bytes]) -> list[bytes | None]:
        return [self.get(key) for key in keys]

    def batch_delete(
        self, keys: Sequence[str | bytes], *, timestamp: int | None = None
    ) -> None:
        if not keys:
            return

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
                timestamp if timestamp is not None else time_ns(),
            )
        )
