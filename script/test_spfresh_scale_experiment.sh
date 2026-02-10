#!/bin/bash
# Test script for the experiment binary
# Runs a small-scale test with random vectors to verify the build+add+query pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/build"
INDEX_DIR="$PROJECT_DIR/experiment_index"

# SPDK environment
export SPFRESH_SPDK_CONF="$SCRIPT_DIR/bdev-uring.json"
export SPFRESH_SPDK_BDEV=Uring0
export DPDK_IOVA_MODE=va
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/usr/local/lib"

# Clean previous index
rm -rf "$INDEX_DIR"
mkdir -p "$INDEX_DIR"

gdb -ex=run --args "$BUILD_DIR/experiment" \
    --dim 32 \
    --count 2000 \
    --batches 5 \
    --query-count 5 \
    --k 1,10 \
    --threads 2 \
    --index-dir "$INDEX_DIR" \
    --spdk-map "$INDEX_DIR/spdk_mapping.bin" \
    --mapping-output "$INDEX_DIR/mapping.txt" \
    --query-output "$INDEX_DIR/results.txt" \
    --value-type Float \
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
    --search-internal-result-num 64 \
    --max-dist-ratio 1000000
