from pathlib import Path
import tempfile

import pytest

from eloqstore import Client, EloqStoreError, Options, RegisteredMemory


def test_in_memory_client_roundtrip():
    client = Client(Options(table_name="demo", partition_id=0, num_threads=1))
    try:
        client.put("hello", b"world")
        assert client.exists("hello") is True
        assert client.get("hello") == b"world"

        client.delete("hello")
        assert client.exists("hello") is False
        assert client.get("hello") is None

        client.batch_put({"k1": b"v1", "k2": b"v2"})
        assert client.batch_get(["k1", "k2", "missing"]) == [b"v1", b"v2", None]

        client.batch_delete(["k1", "k2"])
        assert client.batch_get(["k1", "k2"]) == [None, None]
    finally:
        client.close()


def test_buffer_inputs_and_get_into_roundtrip():
    client = Client(Options(table_name="demo", partition_id=0, num_threads=1))
    try:
        payload = bytearray(b"buffer-payload")
        client.put("hello", payload)

        out = bytearray(len(payload))
        written = client.get_into("hello", out)
        assert written == len(payload)
        assert bytes(out) == bytes(payload)

        batch_payload = bytearray(b"batch-payload")
        client.batch_put([("k1", memoryview(batch_payload)), ("k2", b"v2")])
        out2 = bytearray(len(batch_payload))
        written2 = client.get_into("k1", out2)
        assert written2 == len(batch_payload)
        assert bytes(out2) == bytes(batch_payload)
        assert client.batch_get(["k1", "k2"]) == [bytes(batch_payload), b"v2"]
    finally:
        client.close()


def test_get_into_missing_key_returns_none():
    client = Client(Options(table_name="demo", partition_id=0, num_threads=1))
    try:
        out = bytearray(8)
        assert client.get_into("missing", out) is None
    finally:
        client.close()


def test_small_values_can_target_multiple_partitions():
    client = Client(Options(table_name="multi_part", partition_id=0, num_threads=2))
    try:
        client.put("same-key", b"p0", partition_id=0)
        client.put("same-key", b"p1", partition_id=1)
        client.batch_put({"batched": b"p2"}, partition_id=2)

        assert client.get("same-key", partition_id=0) == b"p0"
        assert client.get("same-key", partition_id=1) == b"p1"
        assert client.get("same-key", partition_id=2) is None
        assert client.batch_get(["batched"], partition_id=2) == [b"p2"]

        out = bytearray(2)
        assert client.get_into("same-key", out, partition_id=1) == 2
        assert bytes(out) == b"p1"

        client.delete("same-key", partition_id=0)
        client.batch_delete(["batched"], partition_id=2)
        assert client.exists("same-key", partition_id=0) is False
        assert client.exists("same-key", partition_id=1) is True
        assert client.exists("batched", partition_id=2) is False
    finally:
        client.close()


def test_large_value_async_roundtrip_uses_fragment_lengths():
    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-large-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)
    memory = RegisteredMemory(total_size=16384, chunk_size=16384, segment_size=4096)
    client = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="large_async",
            partition_id=0,
            num_threads=1,
            segment_size=4096,
            registered_memory_chunk_size=16384,
            data_append_mode=True,
            buffer_pool_size=16 << 20,
            registered_memory=memory,
        )
    )
    try:
        payload = bytes((idx % 251 for idx in range(9000)))
        value = client.allocate_large_value(len(payload))
        value.copy_from(payload)
        assert [len(view) for view in value.memoryviews()] == [4096, 4096, 808]

        handle = client.batch_put_large_async([("large-key", value)])
        try:
            handle.wait()
        finally:
            handle.close()

        read_handle = client.get_large_async("large-key")
        try:
            result = read_handle.result_large()
            assert result is not None
            try:
                assert [len(view) for view in result.memoryviews()] == [4096, 4096, 808]
                assert result.to_bytes() == payload
            finally:
                result.close()
        finally:
            read_handle.close()
    finally:
        client.close()
        memory.close()


def test_large_value_copy_from_supports_nonzero_offset():
    memory = RegisteredMemory(total_size=8192, chunk_size=8192, segment_size=4096)
    try:
        value = memory.allocate(5000)
        try:
            value.copy_from(b"abcd", offset=4094)
            data = value.to_bytes()
            assert data[4094:4098] == b"abcd"
        finally:
            value.close()
    finally:
        memory.close()


def test_get_large_async_missing_key_returns_none():
    memory = RegisteredMemory(total_size=8192, chunk_size=8192, segment_size=4096)
    client = Client(
        Options(
            table_name="large_missing",
            partition_id=0,
            num_threads=1,
            segment_size=4096,
            registered_memory_chunk_size=8192,
            registered_memory=memory,
        )
    )
    try:
        handle = client.get_large_async("missing")
        try:
            assert handle.result_large() is None
        finally:
            handle.close()
    finally:
        client.close()
        memory.close()


def test_disk_mode_roundtrip_and_reopen():
    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-disk-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)

    client = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="demo",
            partition_id=0,
            num_threads=1,
        )
    )
    try:
        client.put("hello", b"world")
        assert client.get("hello") == b"world"
    finally:
        client.close()

    reopened = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="demo",
            partition_id=0,
            num_threads=1,
        )
    )
    try:
        assert reopened.exists("hello") is True
        assert reopened.get("hello") == b"world"
    finally:
        reopened.close()


def test_options_path_and_branch_start():
    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-ini-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)
    ini_path = root / "eloqstore.ini"
    ini_path.write_text(
        "[run]\nnum_threads = 1\nbuffer_pool_size = 4MB\n\n"
        "[permanent]\ndata_page_size = 4KB\n",
        encoding="utf-8",
    )

    client = Client(
        Options(
            store_paths=[str(store_path)],
            options_path=str(ini_path),
            table_name="demo",
            partition_id=0,
            branch="feature-x",
            term=7,
            partition_group_id=3,
        )
    )
    try:
        client.put("branch-key", b"value")
        assert client.get("branch-key") == b"value"
    finally:
        client.close()


def test_bad_ini_path_raises(tmp_path):
    bad_ini = tmp_path / "does-not-exist-eloqstore.ini"
    with pytest.raises(EloqStoreError):
        Client(
            Options(
                options_path=str(bad_ini),
                table_name="demo",
                partition_id=0,
            )
        )


def test_close_is_idempotent():
    client = Client(Options(table_name="demo", partition_id=0, num_threads=1))
    client.close()
    client.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("data_page_size", -1),
        ("data_page_size", 65536),
        ("pages_per_file_shift", -1),
        ("pages_per_file_shift", 256),
        ("overflow_pointers", -1),
        ("overflow_pointers", 129),
        ("manifest_limit", -1),
        ("manifest_limit", 2**32),
        ("fd_limit", -1),
        ("fd_limit", 2**32),
        ("buffer_pool_size", -1),
        ("buffer_pool_size", 2**64),
        ("num_threads", -1),
        ("num_threads", 65536),
    ],
)
def test_invalid_numeric_options_raise_value_error(field: str, value: int):
    kwargs = {"table_name": "demo", "partition_id": 0, field: value}
    with pytest.raises(ValueError):
        Client(Options(**kwargs))


def test_pinned_write_read_roundtrip():
    from pathlib import Path
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-pinned-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)

    from eloqstore import PinnedMemoryPool

    pool = PinnedMemoryPool(total_size=64 << 10, chunk_size=64 << 10, num_pools=1)
    client = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="pinned_rw",
            partition_id=0,
            num_threads=1,
            segment_size=4096,
            data_append_mode=True,
            buffer_pool_size=16 << 20,
            pinned_memory_pool=pool,
            gc_global_mem_size_per_shard=32 << 20,
            pinned_tail_scratch_slots=8,
        )
    )
    try:
        payload = bytes((idx % 251 + 1 for idx in range(8192)))
        key = "pinned-key"

        buf = pool.allocate(len(payload))
        import ctypes

        import numpy as np

        arr = np.ctypeslib.as_array(
            ctypes.cast(buf.ptr, ctypes.POINTER(ctypes.c_uint8)),
            shape=(buf.nbytes,),
        )
        arr[:] = np.frombuffer(payload, dtype=np.uint8)

        client.put_pinned_large(key, buf.ptr, buf.nbytes)
        assert client.exists(key) is True

        read_buf = pool.allocate(len(payload))
        found, metadata = client.get_pinned_large_into(key, read_buf)
        assert found is True
        assert metadata == b""

        read_arr = np.ctypeslib.as_array(
            ctypes.cast(read_buf.ptr, ctypes.POINTER(ctypes.c_uint8)),
            shape=(read_buf.nbytes,),
        )
        assert bytes(read_arr) == payload
    finally:
        client.close()
        pool.close()


def test_pinned_write_with_metadata():
    from pathlib import Path
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-pinned-meta-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)

    from eloqstore import PinnedMemoryPool

    pool = PinnedMemoryPool(total_size=64 << 10, chunk_size=64 << 10)
    client = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="pinned_meta",
            partition_id=0,
            num_threads=1,
            segment_size=4096,
            data_append_mode=True,
            buffer_pool_size=16 << 20,
            pinned_memory_pool=pool,
            gc_global_mem_size_per_shard=32 << 20,
            pinned_tail_scratch_slots=8,
        )
    )
    try:
        meta = b"tensor[8192,fp16]"
        payload = bytes((idx % 251 + 1 for idx in range(8192)))
        key = "meta-key"

        buf = pool.allocate(len(payload))
        import ctypes

        import numpy as np

        arr = np.ctypeslib.as_array(
            ctypes.cast(buf.ptr, ctypes.POINTER(ctypes.c_uint8)),
            shape=(buf.nbytes,),
        )
        arr[:] = np.frombuffer(payload, dtype=np.uint8)

        client.put_pinned_large(key, buf.ptr, buf.nbytes, metadata=meta)
        assert client.exists(key) is True

        read_buf = pool.allocate(len(payload))
        found, metadata = client.get_pinned_large_into(key, read_buf)
        assert found is True
        assert metadata == meta

        read_arr = np.ctypeslib.as_array(
            ctypes.cast(read_buf.ptr, ctypes.POINTER(ctypes.c_uint8)),
            shape=(read_buf.nbytes,),
        )
        assert bytes(read_arr) == payload
    finally:
        client.close()
        pool.close()


def test_pinned_async_read():
    from pathlib import Path
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-pinned-async-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)

    from eloqstore import PinnedMemoryPool

    pool = PinnedMemoryPool(total_size=64 << 10, chunk_size=64 << 10)
    client = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="pinned_async",
            partition_id=0,
            num_threads=1,
            segment_size=4096,
            data_append_mode=True,
            buffer_pool_size=16 << 20,
            pinned_memory_pool=pool,
            gc_global_mem_size_per_shard=32 << 20,
            pinned_tail_scratch_slots=8,
        )
    )
    try:
        payload = bytes((idx % 251 + 1 for idx in range(4096)))
        key = "async-key"

        buf = pool.allocate(len(payload))
        import ctypes

        import numpy as np

        arr = np.ctypeslib.as_array(
            ctypes.cast(buf.ptr, ctypes.POINTER(ctypes.c_uint8)),
            shape=(buf.nbytes,),
        )
        arr[:] = np.frombuffer(payload, dtype=np.uint8)

        client.put_pinned_large(key, buf.ptr, buf.nbytes)

        read_buf = pool.allocate(len(payload))
        handle = client.get_pinned_large_into_async(key, read_buf)
        handle.wait()
        found, metadata = handle.result_pinned()
        assert found is True
        assert metadata == b""
        handle.close()

        read_arr = np.ctypeslib.as_array(
            ctypes.cast(read_buf.ptr, ctypes.POINTER(ctypes.c_uint8)),
            shape=(read_buf.nbytes,),
        )
        assert bytes(read_arr) == payload

        missing_buf = pool.allocate(16)
        handle2 = client.get_pinned_large_into_async("no-such-key", missing_buf)
        status = handle2._lib.CEloqStore_AsyncWait(handle2._handle)
        assert status != 0
        handle2.close()
    finally:
        client.close()
        pool.close()


def test_pinned_pool_free_reuses_regions():
    from eloqstore import PinnedMemoryPool

    pool = PinnedMemoryPool(total_size=16 << 10, chunk_size=16 << 10, num_pools=1)
    try:
        first = pool.allocate(4096)
        second = pool.allocate(4096)
        assert second.ptr != first.ptr

        pool.free(first)
        reused = pool.allocate(4096)
        assert reused.ptr == first.ptr

        pool.free(second)
        pool.free(reused)
        merged = pool.allocate(8192)
        assert merged.ptr == first.ptr
    finally:
        pool.close()
