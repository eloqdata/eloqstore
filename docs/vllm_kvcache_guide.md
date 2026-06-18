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

The built-in benchmark is
`vllm/benchmarks/multi_turn/benchmark_serving_multi_turn.py`.

Pre-built conversation files for repeatable testing ship with the
`eloqstore` package in `eloqstore/bench_configs/`:

| File | Conversations | First-turn tokens | Total KV cache | vs 5 GB |
|------|--------------|-------------------|----------------|---------|
| `fit_5g_conversations.json` | 3 | ~9000 | ~3.4 GB | Within budget |
| `overflow_5g_conversations.json` | 7 | ~9000 | ~7.9 GB | Exceeds budget |

Each conversation: long first-turn analysis, short assistant reply, short
second-turn follow-up question.

Per-token KV cache size (Qwen3-4B, half):
```
2 × 36 layers × 8 kv_heads × 128 dim × 2 bytes = 147,456 bytes ≈ 144 KB
```

### Prerequisites

```bash
uv pip install -r /path/to/vllm/benchmarks/multi_turn/requirements.txt
```

### Running a Benchmark

Start the server (EloqStore or CPU offloading, see [Startup](#startup) above).

```bash
# Cold run (empty store / empty CPU buffer)
VENV=/path/to/venv/bin
VLLM_BENCH=/path/to/vllm/benchmarks/multi_turn/benchmark_serving_multi_turn.py
CONFIGS=/path/to/eloqstore/python/src/eloqstore/bench_configs

$VENV/python $VLLM_BENCH \
  --model Qwen/Qwen3-4B \
  --served-model-name qwen3-4b-eloq \
  --url http://127.0.0.1:8015 \
  --input-file $CONFIGS/overflow_5g_conversations.json \
  --num-clients 1 \
  --max-active-conversations 7 \
  --max-turns 4 \
  --no-early-stop \
  --request-timeout-sec 300 \
  --stats-json-output /path/to/eloq_overflow_cold.json

# Warm run (data now in store / CPU buffer)
$VENV/python $VLLM_BENCH \
  --model Qwen/Qwen3-4B \
  --served-model-name qwen3-4b-eloq \
  --url http://127.0.0.1:8015 \
  --input-file $CONFIGS/overflow_5g_conversations.json \
  --num-clients 1 \
  --max-active-conversations 7 \
  --max-turns 4 \
  --no-early-stop \
  --request-timeout-sec 300 \
  --stats-json-output /path/to/eloq_overflow_warm.json
```

`--max-active-conversations` must equal the number of conversations in the
input file.  `--no-early-stop` forces all turns to complete.

Repeat with the CPU offloading connector
(`--kv-transfer-config '{"kv_connector":"OffloadingConnector",...}'`) on a
different port for comparison.

### Generating Reports

The `eloqstore.bench_report` module (also available as the
`eloqstore-bench-report` CLI) reads the `--stats-json-output` files and prints
comparison tables.

```bash
# Full 4-way comparison (cold + warm for both systems)
eloqstore-bench-report \
  --eloq-cold   eloq_cold.json \
  --eloq-warm   eloq_warm.json \
  --offload-cold offload_cold.json \
  --offload-warm offload_warm.json
```

Or from Python:

```python
from eloqstore.bench_report import load_stats, print_report

print_report(
    eloq_cold=load_stats("eloq_cold.json"),
    eloq_warm=load_stats("eloq_warm.json"),
    offload_cold=load_stats("offload_cold.json"),
    offload_warm=load_stats("offload_warm.json"),
)
```

## Current Measured Results

Test setup:

- Model: Qwen3-4B, dtype half, enforce eager
- GPU: RTX 5080 (16 GB), gpu-memory-utilization 0.60
- KV cache budget: 5 GB (EloqStore shared buffer / CPU RAM)
- vLLM base: v0.22.0 with EloqStore batch API
- Per-token KV cache: ~144 KB (36 layers × 8 kv_heads × 128 dim × 2 bytes)

### Workload A — Fit (KV cache fits within 5 GB)

3 conversations, each ~9000-token first turn.  Total KV cache ~3.4 GB < 5 GB.

| System | Cold TTFT | Warm TTFT | Speedup |
|--------|-----------|-----------|---------|
| CPU Offloading | 505ms | 62ms | **8.10×** |
| EloqStore | 937ms | 222ms | **4.22×** |

All blocks fit in the CPU ring buffer.  CPU offloading loads from RAM
(~100ns latency).  EloqStore loads from NVMe via io_uring (~10µs latency).
Both show significant speedup; CPU is faster due to storage medium.

### Workload B — Overflow (KV cache exceeds 5 GB)

7 conversations, each ~9000-token first turn.  Total KV cache ~7.9 GB >> 5 GB.

| System | Cold TTFT | Warm TTFT | Speedup |
|--------|-----------|-----------|---------|
| CPU Offloading | 634ms | 568ms | **1.12×** |
| EloqStore | 1013ms | 331ms | **3.06×** |

The hotset exceeds the 5 GB CPU buffer.  CPU ring buffer evicts ~2.9 GB
(37%) of blocks.  Evicted blocks must be recomputed on GPU — negates nearly
all cache benefit.  EloqStore persists to NVMe SSD — no eviction — 3.06×
speedup.

### Workload C — High Cache Hit (small requests, large hotset)

64 conversations, ~1000-token first turns, second-turn short questions.
Total KV cache ~8.95 GB >> 5 GB.

| System | Cold TTFT | Warm TTFT | Speedup |
|--------|-----------|-----------|---------|
| CPU Offloading | 92ms | 91ms | **1.02×** |
| EloqStore | 166ms | 75ms | **2.22×** |

On this workload, EloqStore warm-cache **outperforms** CPU offloading
(75ms vs 91ms).  The batch `ContainsKeys` and `BeginLoads` APIs eliminate
per-key synchronization overhead on the warm path.

### Summary

```
                    FIT (3.4 GB)          OVERFLOW (7.9 GB)      HIGH HIT (8.95 GB)
                    cold    warm  speedup  cold    warm  speedup  cold    warm  speedup
CPU Offloading      505ms   62ms  8.10x   634ms  568ms  1.12x    92ms    91ms  1.02x
EloqStore           937ms  222ms  4.22x  1013ms  331ms  3.06x   166ms    75ms  2.22x
```

1. **When KV cache fits in RAM**: CPU offloading wins (8.10× vs 4.22×).
   RAM is ~100× faster than NVMe SSD for random reads.

2. **When KV cache exceeds RAM**: CPU offloading loses nearly all benefit
   (1.12×) due to ring-buffer eviction.  EloqStore maintains substantial
   speedup (3.06×) because SSDs have orders of magnitude more capacity.

3. **EloqStore can outperform CPU offloading**: On the high-hit workload,
   EloqStore warm-cache (75ms) beats CPU offloading (91ms).  Batch APIs and
   persistent storage together make EloqStore competitive even against
   RAM-backed caches when the working set is large.

4. **Capacity is the differentiator**: CPU offloading provides fast access
   for hot caches that fit within available RAM.  EloqStore provides
   predictable cache reuse regardless of working-set size, bounded only by
   available NVMe storage.

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
