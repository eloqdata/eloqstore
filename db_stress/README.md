# db_stress

`db_stress` is a stress/load tool for EloqStore. It drives mixed read/write
workloads across multiple tables/partitions and can run in local or cloud
mode. Append mode is recommended for realistic write-heavy tests.

## Build

```bash
cmake -B build
cmake --build build -j"$(nproc)"
```

Binary path: `build/db_stress/db_stress`

## Minimal run (append mode, local)

```bash
./build/db_stress/db_stress \
  --data_append_mode=true \
  --db_path=./data/stress_test \
  --n_tables=1 \
  --n_partitions=1 \
  --max_key=10000 \
  --keys_per_batch=2000 \
  --ops_per_partition=100000
```

This uses defaults from `include/kv_options.h` for the remaining storage
options. Data is created under `--db_path`.

## Minimal run (append mode, cloud)

```bash
./build/db_stress/db_stress \
  --data_append_mode=true \
  --cloud_store_path=mybucket/eloqstore-stress \
  --db_path=./data/stress_test \
  --n_tables=1 \
  --n_partitions=1 \
  --max_key=10000 \
  --keys_per_batch=2000 \
  --ops_per_partition=100000
```

`--cloud_store_path` enables cloud mode. Cloud defaults (provider/endpoint/keys)
come from `include/kv_options.h`. If you need custom credentials or endpoint,
use an ini file (see below).

## Configuration

There are two groups of knobs:

1) Workload flags (stress behavior)
- `--n_tables`, `--n_partitions`
- `--ops_per_partition`, `--keys_per_batch`, `--max_key`, `--active_width`
- `--shortest_value`, `--longest_value`
- `--write_percent`, `--point_read_percent`, `--floor_read_percent`
- `--num_readers_per_partition`, `--max_verify_ops_per_write`
- `--test_batched_ops_stress` (batched vs non-batched)
- `--syn_scan` (sync vs async scan)

2) Storage/KV options (EloqStore settings)
- Flags mapped from `include/kv_options.h`:
  - `--db_path`, `--cloud_store_path`, `--num_threads`
  - `--data_append_mode`, `--pages_per_file_shift`, `--data_page_size`
  - `--buffer_pool_size`, `--io_queue_size`, `--fd_limit`
  - `--manifest_limit`, `--init_page_count`
  - `--max_inflight_write`, `--max_write_batch_pages`
  - `--file_amplify_factor`, `--local_space_limit`, `--reserve_space_ratio`
  - `--overflow_pointers`, `--data_page_restart_interval`,
    `--index_page_restart_interval`

