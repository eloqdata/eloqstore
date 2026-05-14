from pathlib import Path
import tempfile

import pytest

from eloqstore import Client, EloqStoreError, Options


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
