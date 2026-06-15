# EloqStore vLLM KV Cache Install

This is the canonical install and local usage document for the EloqStore-backed
vLLM KV cache connector.

## Scope

This document only describes the current external-connector integration model:

- `vllm` is installed separately
- the connector implementation comes from `eloqstore`
- `vllm` loads `eloqstore.vllm_connector` dynamically

## Package Boundary

`eloqstore` provides:

- the native runtime
- the Python SDK
- the vLLM connector module

The SDK is responsible for the kvcache runtime-side mechanics:

- derive runtime shape from `memory_bytes` and `cpu_threads`
- attach and map the shared buffer on workers
- expose shared-buffer slices to the connector
- batch-wait async save/load requests

The connector module path is:

```text
eloqstore.vllm_connector
```

The adapter is not imported by `eloqstore.__init__`, so normal SDK use does not
require `vllm`.

## Runtime Model

The connector uses one shared host buffer as the CPU staging area:

```text
save:
GPU KV cache -> shared host buffer -> EloqStore -> storage

load:
storage -> shared host buffer -> GPU KV cache
```

Worker-side behavior:

- `start_load_kv()` submits async loads
- `wait_for_layer_load()` waits only when a layer needs the data
- `save_kv_layer()` appends bytes into per-block staging buffers
- `wait_for_save()` submits async saves and waits once

## Install

Install `vllm` in the target environment:

```bash
uv pip install vllm --torch-backend=auto
```

This was verified in the local test environment used for the commands below.
The active interpreter ended up at:

```text
/home/starrysky/workspace/llm/vllm/.venv/bin/python
```

When validating an installed `vllm` wheel, do not run Python from inside a
`vllm` source checkout unless that is the environment you intentionally want to
test. Running from the repository root can shadow the installed package and lead
to import errors such as `ModuleNotFoundError: No module named 'vllm._C'`.

Install the local EloqStore Python package:

```bash
cd /home/starrysky/workspace/llm/eloqstore
uv pip install -e python
```

The package bundles `libeloqstore_capi.so` together with its private native
dependencies, so normal runtime use does not require `LD_LIBRARY_PATH`.

To override the packaged native library during local testing:

```bash
export ELOQSTORE_PY_LIB=/home/starrysky/workspace/llm/eloqstore/build/libeloqstore_capi.so
```

Install the multi-turn benchmark dependencies before running the benchmark:

```bash
uv pip install -r /home/starrysky/workspace/llm/vllm/benchmarks/multi_turn/requirements.txt
```

## Raise Memlock For Pinned Buffers

For the GPU KV-cache path, EloqStore must successfully register pinned memory
with io_uring. On this machine, the default memlock limit was only 8 MiB:

```text
RESOURCE DESCRIPTION                           SOFT    HARD UNITS
MEMLOCK  max locked-in-memory address space 8388608 8388608 bytes
```

That is not enough for the normal EloqStore KV-cache runtime, because the
runtime registers:

- the connector shared-memory pool
- additional internal registered buffers used by EloqStore background I/O

If memlock is too small, EloqStore startup fails during io_uring buffer
registration.

For local testing, raise memlock before starting vLLM. The verified command on
this machine was:

```bash
sudo prlimit --memlock=unlimited:unlimited -- bash -lc 'prlimit --pid $$ --memlock'
```

If your environment does not allow runtime `prlimit`, configure the limit
system-wide instead, for example through PAM limits or your service manager.

## Verified GPU Baseline

Before debugging the connector, verify that stock vLLM can serve the same model
on the same GPU. This separates generic vLLM/model issues from connector
integration issues.

Verified stock vLLM command:

```bash
nohup /home/starrysky/workspace/llm/vllm/.venv/bin/vllm serve \
  /home/starrysky/.cache/huggingface/hub/models--Qwen--Qwen3-4B/snapshots/1cfa9a7208912126459214e8b04321603b3df60c \
  --served-model-name qwen3-4b-stock \
  --port 8014 \
  --dtype half \
  --gpu-memory-utilization 0.60 \
  --max-model-len 512 \
  --max-num-seqs 1 \
  --max-num-batched-tokens 128 \
  --enforce-eager \
  > /tmp/opencode/vllm-qwen-stock.log 2>&1 < /dev/null &
```

Verified stock request:

```bash
/home/starrysky/workspace/llm/vllm/.venv/bin/python -c "import requests; resp=requests.post('http://127.0.0.1:8014/v1/chat/completions', json={'model':'qwen3-4b-stock','messages':[{'role':'user','content':'Hello, answer in one word.'}],'stream':False,'temperature':0.0,'max_tokens':8}, timeout=120); print(resp.status_code); print(resp.text)"
```

## vLLM Configuration

Configure vLLM to load the external connector module:

```json
{
  "kv_connector": "EloqStoreConnector",
  "kv_connector_module_path": "eloqstore.vllm_connector",
  "kv_role": "kv_both",
  "kv_connector_extra_config": {
    "store_paths": ["/tmp/eloqstore-kvcache"],
    "memory_bytes": 67108864,
    "cpu_threads": 2
  }
}
```

Supported connector settings:

- `store_paths`: where EloqStore persists KV data
- `memory_bytes`: total shared-buffer budget for block-sized KV slots
- `cpu_threads`: total EloqStore runtime CPU parallelism

Everything else is derived internally.

The runtime derives a block-mapped shared-memory layout from the live vLLM KV
cache shape:

- one shared-memory slot per vLLM block
- one slot payload contains all participating layer bytes for that block
- slot count is determined from `memory_bytes / aligned_block_payload_bytes`

This is intentionally different from the previous generic-entry layout, which
could waste the budget on a very small number of oversized entries and run out
of allocatable slots early.

## Verified GPU Launch With EloqStore

The following command was verified with `Qwen3-4B` on GPU after raising
memlock. It uses the normal EloqStore pinned-memory path and does not disable
io_uring registration.

```bash
sudo prlimit --memlock=unlimited:unlimited -- bash -lc 'nohup \
  /home/starrysky/workspace/llm/vllm/.venv/bin/vllm serve \
  /home/starrysky/.cache/huggingface/hub/models--Qwen--Qwen3-4B/snapshots/1cfa9a7208912126459214e8b04321603b3df60c \
  --served-model-name qwen3-4b-eloq \
  --port 8015 \
  --dtype half \
  --gpu-memory-utilization 0.60 \
  --max-model-len 512 \
  --max-num-seqs 1 \
  --max-num-batched-tokens 128 \
  --enforce-eager \
  --kv-transfer-config '"'"'{
    "kv_connector": "EloqStoreConnector",
    "kv_connector_module_path": "eloqstore.vllm_connector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
      "store_paths": ["/tmp/opencode/eloqstore-sdk-store"],
      "memory_bytes": 67108864,
      "cpu_threads": 2
    }
  }'"'"' \
  > /tmp/opencode/eloqstore-qwen-memlock.log 2>&1 < /dev/null &'
```

The server was then verified with:

```bash
/home/starrysky/workspace/llm/vllm/.venv/bin/python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8015/v1/models', timeout=20).read().decode())"
```

And a direct OpenAI-compatible request:

```bash
/home/starrysky/workspace/llm/vllm/.venv/bin/python -c "import requests; resp=requests.post('http://127.0.0.1:8015/v1/chat/completions', json={'model':'qwen3-4b-eloq','messages':[{'role':'user','content':'Hello, answer in one word.'}],'stream':False,'temperature':0.0,'max_tokens':8}, timeout=120); print(resp.status_code); print(resp.text)"
```

## Verified Multi-Turn Benchmark

The built-in benchmark used was:

```text
vllm/benchmarks/multi_turn/benchmark_serving_multi_turn.py
```

For a light local smoke workload, the following synthetic config was used:

```json
{
  "filetype": "generate_conversations",
  "num_conversations": 6,
  "text_files": [
    "/home/starrysky/workspace/llm/vllm/README.md"
  ],
  "print_stats": false,
  "prompt_input": {
    "num_turns": {
      "distribution": "uniform",
      "min": 4,
      "max": 6
    },
    "common_prefix_num_tokens": {
      "distribution": "constant",
      "value": 64
    },
    "prefix_num_tokens": {
      "distribution": "uniform",
      "min": 96,
      "max": 160
    },
    "num_tokens": {
      "distribution": "uniform",
      "min": 32,
      "max": 48
    }
  },
  "prompt_output": {
    "num_tokens": {
      "distribution": "uniform",
      "min": 16,
      "max": 24
    }
  }
}
```

Verified benchmark command:

```bash
/home/starrysky/workspace/llm/vllm/.venv/bin/python \
  /home/starrysky/workspace/llm/vllm/benchmarks/multi_turn/benchmark_serving_multi_turn.py \
  --model /home/starrysky/.cache/huggingface/hub/models--Qwen--Qwen3-4B/snapshots/1cfa9a7208912126459214e8b04321603b3df60c \
  --served-model-name qwen3-4b-eloq \
  --url http://127.0.0.1:8015 \
  --input-file /tmp/opencode/multi_turn_smoke.json \
  --num-clients 1 \
  --max-active-conversations 2 \
  --max-num-requests 6 \
  --max-turns 4 \
  --request-timeout-sec 180 \
  --warmup-step \
  --stats-json-output /tmp/opencode/multi_turn_stats.json
```

Observed benchmark summary:

```text
runtime_sec = 1.117
requests_per_sec = 4.475
warmup_runtime_sec = 1.479
total_runtime_incl_warmup_sec = 2.596
```

Sample per-request stats from `/tmp/opencode/multi_turn_stats.json`:

```text
ttft_ms: about 28-30 ms
tpot_ms: about 10 ms
approx_cached_percent: about 85%-90%
```

## Failure Modes Seen During Verification

These failures were reproduced while validating the GPU setup:

1. Stock vLLM works, but EloqStore startup fails with low memlock.
   This is a system configuration problem, not a Qwen3-4B problem.

2. Setting `eager_io_uring_register=false` is not a valid workaround for the
   current connector path.
   Scheduler prefix matching calls `contains_key()`, and the current runtime
   requires io_uring buffers to be registered before `contains_key` can run.

3. The memlock requirement is higher than `memory_bytes` alone.
   EloqStore also registers internal pinned buffers used by background I/O, so
   memlock must cover the whole registered-buffer footprint, not just the
   shared host buffer exported to vLLM workers.

## Smoke Test

After installation, confirm the connector module is importable:

```bash
python - <<'PY'
from eloqstore.vllm_connector import EloqStoreConnector
print(EloqStoreConnector)
PY
```
