#!/usr/bin/env bash
#
# IO QoS device calibration sweep (docs/design/io_qos.md, evaluation step 1).
#
# Runs a fixed 4KB random-read fio job while stepping a rate-limited
# sequential-write job through increasing MB/s targets, and emits a CSV of
#   write_target_MBps, write_actual_MBps, read_iops, read_p50_us,
#   read_p99_us, read_p999_us
# The knee of the p99-vs-write-rate curve is the per-device background write
# budget; divide by num_threads for the per-shard bg budget / M3 rate limit.
#
# IMPORTANT — read before trusting the numbers:
#  * PRECONDITION the drive: a fresh/trimmed SSD serves reads from empty FTL
#    mappings and absorbs writes into a pristine SLC cache, both of which
#    flatter the results. Fill the test file region at least once (the script
#    lays out the file with a full sequential write pass unless it already
#    exists) and ideally run `--precondition` (two full overwrite passes)
#    on a drive that has seen real use.
#  * SLC-CACHE EXHAUSTION: consumer drives fold SLC->TLC once the cache
#    fills; read tails get much worse and unstable after that point. Size
#    per-step runtime (--step-secs, default 60) and the write rates so the
#    total bytes written per step exceed the SLC cache if you want
#    steady-state numbers; watch for a step whose actual write MB/s sags
#    below the target — that is the fold-over signature.
#  * The script always runs against files below --dir, so the result includes
#    filesystem effects. Use a dedicated directory on the target device.
#
# Requires: fio, python3.
#
# Usage:
#   scripts/io_calibration_sweep.sh --dir /mnt/nvme/fio-test \
#       [--size 32G] [--step-secs 60] [--read-qd 32] \
#       [--rates "0 50 100 200 400 800 1600"] [--precondition]

set -euo pipefail

DIR=""
SIZE="32G"
STEP_SECS=60
READ_QD=32
RATES="0 50 100 200 400 800 1600"
PRECONDITION=0

while [[ $# -gt 0 ]]; do
    case "$1" in
    --dir) DIR="$2"; shift 2 ;;
    --size) SIZE="$2"; shift 2 ;;
    --step-secs) STEP_SECS="$2"; shift 2 ;;
    --read-qd) READ_QD="$2"; shift 2 ;;
    --rates) RATES="$2"; shift 2 ;;
    --precondition) PRECONDITION=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

[[ -n "$DIR" ]] || { echo "usage: $0 --dir <path> [...]" >&2; exit 1; }
command -v fio >/dev/null || { echo "fio not installed" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not installed" >&2; exit 1; }

mkdir -p "$DIR"
READ_FILE="$DIR/calib_read.bin"
WRITE_FILE="$DIR/calib_write.bin"

layout() {
    local file="$1" passes="$2"
    for ((i = 0; i < passes; i++)); do
        echo "# layout pass $((i + 1))/$passes of $file" >&2
        fio --name=layout --filename="$file" --size="$SIZE" --rw=write \
            --bs=1M --iodepth=8 --direct=1 --ioengine=libaio \
            --output-format=terse >/dev/null
    done
}

# Lay out both files so reads never hit holes; precondition = extra passes.
PASSES=$((PRECONDITION ? 2 : 1))
[[ -f "$READ_FILE" && $PRECONDITION -eq 0 ]] || layout "$READ_FILE" "$PASSES"
[[ -f "$WRITE_FILE" && $PRECONDITION -eq 0 ]] || layout "$WRITE_FILE" "$PASSES"

echo "write_target_MBps,write_actual_MBps,read_iops,read_p50_us,read_p99_us,read_p999_us"

for rate in $RATES; do
    OUT=$(mktemp)
    if [[ "$rate" == "0" ]]; then
        # Baseline: reads only.
        fio --output-format=json --output="$OUT" \
            --name=randread --filename="$READ_FILE" --size="$SIZE" \
            --rw=randread --bs=4k --iodepth="$READ_QD" --direct=1 \
            --ioengine=libaio --time_based --runtime="$STEP_SECS" \
            >/dev/null
    else
        fio --output-format=json --output="$OUT" \
            --name=randread --filename="$READ_FILE" --size="$SIZE" \
            --rw=randread --bs=4k --iodepth="$READ_QD" --direct=1 \
            --ioengine=libaio --time_based --runtime="$STEP_SECS" \
            --name=seqwrite --filename="$WRITE_FILE" --size="$SIZE" \
            --rw=write --bs=1M --iodepth=4 --direct=1 \
            --ioengine=libaio --time_based --runtime="$STEP_SECS" \
            --rate=,"${rate}m" \
            >/dev/null
    fi
    python3 - "$OUT" "$rate" <<'EOF'
import json
import sys

with open(sys.argv[1]) as f:
    data = json.load(f)
rate = sys.argv[2]
read_iops = 0.0
read_p = {"50.000000": 0, "99.000000": 0, "99.900000": 0}
write_mbps = 0.0
for job in data["jobs"]:
    if job["jobname"] == "randread":
        r = job["read"]
        read_iops = r["iops"]
        # clat_ns percentiles -> us
        pct = r.get("clat_ns", {}).get("percentile", {})
        for k in read_p:
            read_p[k] = pct.get(k, 0) / 1000.0
    elif job["jobname"] == "seqwrite":
        write_mbps = job["write"]["bw_bytes"] / (1 << 20)
print(
    f"{rate},{write_mbps:.0f},{read_iops:.0f},"
    f"{read_p['50.000000']:.0f},{read_p['99.000000']:.0f},"
    f"{read_p['99.900000']:.0f}"
)
EOF
    rm -f "$OUT"
done
