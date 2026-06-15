from __future__ import annotations

import mmap
import os
import sys
from multiprocessing import Pipe, Process
from pathlib import Path

from eloqstore import (
    KVCacheManager,
    KVCacheManagerOptions,
    KVCacheWorker,
    KVCacheWorkerOptions,
)


STORE_DIR = "/tmp/opencode/eloqstore-sdk-store"
IPC_PATH = "ipc:///tmp/eloqstore-sdk-smoke.sock"
SHM_NAME = "/eloqstore-sdk-smoke"
PAYLOAD = b"hello-eloqstore-kvcache"
KEY = "smoke:key"

def _make_manager_options() -> KVCacheManagerOptions:
    return KVCacheManagerOptions.from_budget(
        store_paths=[STORE_DIR],
        table_name="sdk_smoke",
        memory_bytes=8 << 20,
        cpu_threads=2,
        branch="main",
        ipc_path=IPC_PATH,
        shared_memory_name=SHM_NAME,
        eager_io_uring_register=True,
    )


def _make_worker_options() -> KVCacheWorkerOptions:
    return KVCacheWorkerOptions.from_budget(
        memory_bytes=8 << 20,
        cpu_threads=2,
        ipc_path=IPC_PATH,
        shared_memory_name=SHM_NAME,
    )


def run_same_process() -> None:
    os.makedirs(STORE_DIR, exist_ok=True)
    manager_options = _make_manager_options()
    worker_options = _make_worker_options()
    with KVCacheManager(manager_options) as manager:
        manager.start()
        manager.register_io_uring_buffers()
        descriptor = manager.export_buffer_pool()

        with KVCacheWorker(worker_options) as worker:
            worker.attach_buffer_pool(descriptor)
            shared = worker.shared_buffer()
            save_req = worker.begin_save(KEY, len(PAYLOAD))
            print(f"same save_req={save_req}", flush=True)
            shared.slice(save_req.offset, len(PAYLOAD))[:] = PAYLOAD
            worker.finish_save(save_req.request_id)
            save_result = worker.wait_requests([save_req.request_id])[save_req.request_id]
            print(
                f"same save_ready offset={save_result.offset} length={save_result.length}",
                flush=True,
            )
            assert manager.contains_key(KEY) is True

            load_req = worker.begin_load(KEY, len(PAYLOAD))
            print(f"same load_req={load_req}", flush=True)
            load_result = worker.wait_requests([load_req])[load_req]
            print(
                f"same load_ready offset={load_result.offset} length={load_result.length}",
                flush=True,
            )
            loaded = bytes(shared.slice(load_result.offset, len(PAYLOAD)))
            assert loaded == PAYLOAD, (loaded, PAYLOAD)


def run_shard_eviction_same_process() -> None:
    os.makedirs(STORE_DIR, exist_ok=True)
    manager_options = _make_manager_options()
    worker_options = _make_worker_options()
    with KVCacheManager(manager_options) as manager:
        manager.start()
        manager.register_io_uring_buffers()
        descriptor = manager.export_buffer_pool()

        with KVCacheWorker(worker_options) as worker:
            worker.attach_buffer_pool(descriptor)
            shared = worker.shared_buffer()
            shard0_capacity = (
                manager_options.entry_count // manager_options.num_threads
            )
            keys = [f"evict:key:{i}" for i in range(shard0_capacity + 1)]
            payloads = [f"payload-{i}".encode("utf-8") for i in range(len(keys))]

            for key, payload in zip(keys, payloads):
                req = worker.begin_save(key, len(payload))
                shared.slice(req.offset, len(payload))[:] = payload
                worker.finish_save(req.request_id)
                worker.wait_requests([req.request_id])

            for key in keys:
                assert manager.contains_key(key) is True

            reloaded_key = keys[0]
            reloaded_payload = payloads[0]
            load_req = worker.begin_load(reloaded_key, len(reloaded_payload))
            load_result = worker.wait_requests([load_req])[load_req]
            assert load_result.length == len(reloaded_payload), load_result.length
            loaded = bytes(shared.slice(load_result.offset, len(reloaded_payload)))
            assert loaded == reloaded_payload, (loaded, reloaded_payload)


def _manager_process(conn, descriptor_path: str) -> None:
    manager_options = _make_manager_options()
    with KVCacheManager(manager_options) as manager:
        manager.start()
        manager.register_io_uring_buffers()
        descriptor = manager.export_buffer_pool()
        Path(descriptor_path).write_text(descriptor, encoding="utf-8")
        conn.send("ready")
        conn.recv()


def run_two_process() -> None:
    os.makedirs(STORE_DIR, exist_ok=True)
    parent_conn, child_conn = Pipe()
    descriptor_path = "/tmp/opencode/eloqstore-sdk-descriptor.txt"
    proc = Process(target=_manager_process, args=(child_conn, descriptor_path), daemon=True)
    proc.start()
    assert parent_conn.recv() == "ready"

    try:
        worker_options = _make_worker_options()
        descriptor = Path(descriptor_path).read_text(encoding="utf-8")
        with KVCacheWorker(worker_options) as worker:
            worker.attach_buffer_pool(descriptor)
            shared = worker.shared_buffer()
            save_req = worker.begin_save(KEY + ":mp", len(PAYLOAD))
            print(f"mp save_req={save_req}", flush=True)
            shared.slice(save_req.offset, len(PAYLOAD))[:] = PAYLOAD
            worker.finish_save(save_req.request_id)
            save_result = worker.wait_requests([save_req.request_id])[save_req.request_id]
            print(f"mp save_ready offset={save_result.offset} length={save_result.length}", flush=True)

            load_req = worker.begin_load(KEY + ":mp", len(PAYLOAD))
            print(f"mp load_req={load_req}", flush=True)
            load_result = worker.wait_requests([load_req])[load_req]
            print(f"mp load_ready offset={load_result.offset} length={load_result.length}", flush=True)
            loaded = bytes(shared.slice(load_result.offset, len(PAYLOAD)))
            assert loaded == PAYLOAD, (loaded, PAYLOAD)
    finally:
        parent_conn.send("stop")
        proc.join(timeout=10)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)
        Path(descriptor_path).unlink(missing_ok=True)


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "same"
    if mode == "same":
        run_same_process()
        print("same-process sdk smoke passed")
        return 0
    if mode == "same-evict":
        run_shard_eviction_same_process()
        print("same-process shard-eviction sdk smoke passed")
        return 0
    if mode == "mp":
        run_two_process()
        print("two-process sdk smoke passed")
        return 0
    raise SystemExit(f"unknown mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
