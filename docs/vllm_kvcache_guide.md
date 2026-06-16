# EloqStore vLLM KV Cache Guide

This is the canonical document for integrating EloqStore with vLLM as an
external KV cache.

It covers:

- system architecture
- local build and install
- vLLM startup configuration
- benchmark commands
- current measured results

`vllm` itself does not carry a second EloqStore connector implementation.
The connector lives in:

```text
eloqstore/python/src/eloqstore/vllm_connector.py
```

and is loaded dynamically through:

```text
eloqstore.vllm_connector
```

## Architecture

The integration has three layers:

1. EloqStore engine

- durable local/object-store KV persistence
- shard execution and async I/O

2. EloqStore KV-cache runtime

- one shared pinned host buffer pool
- manager/worker local IPC
- async save/load request lifecycle
- background publish of dirty blocks into EloqStore

3. vLLM connector adapter

- scheduler-side prefix matching
- worker-side save/load hooks
- GPU KV tensor marshaling

The hot path is:

```text
save:
GPU KV cache -> shared host buffer -> EloqStore -> storage

load:
storage -> shared host buffer -> GPU KV cache
```

The current design is block-oriented:

- one shared-memory slot corresponds to one vLLM KV block
- one slot stores the concatenated bytes of that block across all layers
- slot count is derived from `memory_bytes / aligned_block_payload_bytes`

This replaced the old generic-entry layout, which wasted the memory budget on a
small number of oversized slots and exhausted allocatable entries early.

## Install

Use the official upstream `vllm` release tag `v0.23.0` baseline.
EloqStore does not require a custom `vllm` fork or branch.

Create or reuse a target Python environment, then install `vllm` `0.23.0` as a
wheel:

```bash
uv pip install "vllm==0.23.0" --torch-backend=auto
```

If you also clone the `vllm` repository for benchmark scripts or inspection,
use upstream directly:

```bash
git clone https://github.com/vllm-project/vllm.git
cd vllm
git checkout v0.23.0
```

When validating an installed wheel, do not run Python from inside a `vllm`
source checkout unless that is the environment you intentionally want to test.
Otherwise the source tree can shadow the installed wheel and cause import errors
such as `ModuleNotFoundError: No module named 'vllm._C'`.

Install the local EloqStore package:

```bash
cd /path/to/eloqstore
uv pip install -e python
```

The package bundles `libeloqstore_capi.so` and its private shared-library
dependencies into `python/src/eloqstore/.libs`, so normal runtime use does not
require `LD_LIBRARY_PATH`.

The KV-cache runtime also depends on system ZeroMQ (`libzmq`) for the local
manager/worker control plane. Follow the repository dependency install script
before building or installing the package.

To force loading a specific locally built native library during debugging:

```bash
export ELOQSTORE_PY_LIB=/path/to/eloqstore/build/libeloqstore_capi.so
```

Install the multi-turn benchmark dependencies:

```bash
uv pip install -r /path/to/vllm/benchmarks/multi_turn/requirements.txt
```

## Memlock

The GPU KV-cache path requires pinned memory registration with io_uring.
If `RLIMIT_MEMLOCK` is too small, EloqStore startup fails.

On the test machine used during validation, the default limit was only 8 MiB.

Raise it before starting `vllm`. This is mandatory for EloqStore KV-cache
startup; there is no supported mode that skips io_uring buffer registration.

```bash
sudo prlimit --memlock=unlimited:unlimited -- bash -lc 'prlimit --pid $$ --memlock'
```

If runtime `prlimit` is not allowed in your environment, configure the limit
system-wide through PAM or your service manager.

## vLLM Configuration

Use the external connector hook:

```json
{
  "kv_connector": "EloqStoreConnector",
  "kv_connector_module_path": "eloqstore.vllm_connector",
  "kv_role": "kv_both",
  "kv_connector_extra_config": {
    "store_paths": ["/path/to/eloqstore-store"],
    "memory_bytes": 5368709120,
    "cpu_threads": 2
  }
}
```

Supported settings:

- `store_paths`: where EloqStore persists KV data
- `memory_bytes`: total shared-buffer budget for block-sized slots
- `cpu_threads`: total EloqStore runtime CPU parallelism

## Startup

Before debugging the connector, it is useful to verify stock `vllm` on the same
GPU and model first.

Then start EloqStore-backed `vllm` with raised memlock. Example:

```bash
sudo prlimit --memlock=unlimited:unlimited -- bash -lc 'nohup \
  /path/to/venv/bin/vllm serve \
  Qwen/Qwen3-4B \
  --served-model-name qwen3-4b-eloq \
  --port 8015 \
  --dtype half \
  --gpu-memory-utilization 0.60 \
  --max-model-len 10384 \
  --max-num-seqs 64 \
  --max-num-batched-tokens 2048 \
  --enforce-eager \
  --kv-transfer-config '"'"'{
    "kv_connector": "EloqStoreConnector",
    "kv_connector_module_path": "eloqstore.vllm_connector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
      "store_paths": ["/path/to/eloqstore-store"],
      "memory_bytes": 5368709120,
      "cpu_threads": 2
    }
  }'"'"' \
  > /path/to/logs/eloqstore.log 2>&1 < /dev/null &'
```

Check the server:

```bash
/path/to/venv/bin/python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8015/v1/models', timeout=20).read().decode())"
```

## Benchmark

The built-in benchmark used during validation is:

```text
vllm/benchmarks/multi_turn/benchmark_serving_multi_turn.py
```

Example command:

```bash
/path/to/venv/bin/python \
  /path/to/vllm/benchmarks/multi_turn/benchmark_serving_multi_turn.py \
  --model Qwen/Qwen3-4B \
  --served-model-name qwen3-4b-eloq \
  --url http://127.0.0.1:8015 \
  --input-file /path/to/high_hit_over5g_conversations.json \
  --num-clients 1 \
  --max-active-conversations 64 \
  --max-num-requests 128 \
  --max-turns 4 \
  --request-timeout-sec 300 \
  --stats-json-output /path/to/high_hit_stats_eloq.json
```

The benchmark input can either be:

- a synthetic generation config (`filetype: generate_conversations`)
- or a literal list of OpenAI-format conversations

For cache-hit testing, the literal conversation list is more useful because it
lets you guarantee repeated multi-turn reuse.

## Current Measured Results

Using an explicit high-hit workload with:

- `64` conversations
- total first-turn hotset above `5 GiB`
- second-turn short questions that strongly reuse the first-turn history

Current best validated EloqStore result:

```text
requests_per_sec = 6.879
ttft_ms mean     = 128.95
latency_ms mean  = 143.21
```

Reference CPU memory offloading result on the same workload:

```text
requests_per_sec = 8.665
ttft_ms mean     = 92.78
latency_ms mean  = 113.26
```

So the current EloqStore implementation is still slower than CPU offloading,
but the gap has been reduced substantially by:

- block-mapped shared memory
- memory-only prefix matching
- lighter save/load data path handling

## Failure Modes

Known pitfalls seen during validation:

1. Low `memlock` breaks io_uring pinned-buffer registration.
2. Using the model's theoretical max context length may still fail if the GPU KV
   cache budget cannot serve even one request at that length; prefer the maximum
   length reported by `vllm` for the current memory budget.
3. If cache-hit results unexpectedly collapse to zero, verify that save and
   match use the same block identity scheme and that the benchmark is actually
   exercising second-turn or later requests.

## Smoke Test

After installation, confirm the connector module is importable:

```bash
python - <<'PY'
from eloqstore.vllm_connector import EloqStoreConnector
print(EloqStoreConnector)
PY
```
