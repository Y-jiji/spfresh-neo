#!/bin/bash
# Batch stream update experiment for SIFT-1B using SPFresh
#
# Dataset: SIFT-1B (uint8, 128d, 1B points, 10K queries)
# Data:    /media/tj-yang/T9/data/sift.1B.128.u8.bin      (~120 GB)
# Query:   /media/tj-yang/T9/data/sift.10K.128.u8.query
#
# Usage:
#   bash script/run_sift1b.sh
#
# Build first:
#   mkdir -p build && cd build
#   cmake -DCMAKE_BUILD_TYPE=Release ..
#   make -j$(nproc) experiment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN="${PROJECT_DIR}/build/experiment"

if [ ! -f "$BIN" ]; then
    echo "Binary not found at $BIN"
    echo "Build first: cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j\$(nproc) experiment"
    exit 1
fi

DATA_DIR="/media/tj-yang/T9/data"
DATA_FILE="${DATA_DIR}/sift.1B.128.u8.bin"
QUERY_FILE="${DATA_DIR}/sift.10K.128.u8.query"

for f in "$DATA_FILE" "$QUERY_FILE"; do
    if [ ! -f "$f" ]; then
        echo "Missing: $f"
        exit 1
    fi
done

# SPDK environment
export SPFRESH_SPDK_CONF="${SCRIPT_DIR}/bdev-uring.json"
export SPFRESH_SPDK_BDEV=Uring0
export DPDK_IOVA_MODE=va
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib"

# Directories
IDX_DIR="/media/tj-yang/T9/index/spfresh_sift1b"
OUT_DIR="/media/tj-yang/T9/result"
OUT_PREFIX="${OUT_DIR}/result_spfresh_sift1b"

mkdir -p "$IDX_DIR" "$OUT_DIR"

# Clean previous index
rm -rf "${IDX_DIR:?}"/*

echo "=== SIFT-1B SPFresh Batch Stream Update Experiment ==="
echo "Data file:     $DATA_FILE"
echo "Query file:    $QUERY_FILE"
echo "Index dir:     $IDX_DIR"
echo "Output prefix: $OUT_PREFIX"
echo ""

"$BIN" \
    --dim 128 \
    --count 20000000 \
    --db-vectors "$DATA_FILE" \
    --query-vectors "$QUERY_FILE" \
    --k 1,10 \
    --threads "$(nproc)" \
    --index-dir "$IDX_DIR" \
    --spdk-map "$IDX_DIR/spdk_mapping.bin" \
    --query-output "$OUT_PREFIX" \
    --stats-output "${OUT_PREFIX}_stats.tsv" \
    --value-type UInt8 \
    --spdk-batch-size 256 \
    --spdk-capacity 10000000 \
    --dist-calc-method L2 \
    --ratio 0.1 \
    --tree-number 1 \
    --bkt-kmeans-k 32 \
    --bkt-leaf-size 8 \
    --select-threshold 12 \
    --split-factor 9 \
    --split-threshold 18 \
    --internal-result-num 64 \
    --replica-count 8 \
    --posting-page-limit 3 \
    --insert-threads 4 \
    --append-threads 2 \
    --reassign-threads 0 \
    --reassign-k 64 \
    --merge-threshold 10 \
    --buffer-length 1 \
    --result-num 10 \
    --search-internal-result-num 32,64 \
    --max-dist-ratio 1000000

echo ""
echo "Output files:"
ls -lh "${OUT_PREFIX}"* 2>/dev/null || echo "  (none)"
echo ""
echo "Done."
