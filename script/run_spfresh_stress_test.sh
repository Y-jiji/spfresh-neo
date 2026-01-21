#!/bin/bash

# Wrapper script to run spfresh_stress_test_uint8 with SPDK
# This script sets up the environment and runs the stress test

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "This script must be run as root (use sudo)"
    exit 1
fi

# Default configuration
DATA_FILE="${DATA_FILE:-./build/vectors_320M_uint8.bin}"
OUTPUT_LOG="${OUTPUT_LOG:-./output.log}"
STATS_LOG="${STATS_LOG:-./stats.log}"
K="${K:-10}"
DIMENSION="${DIMENSION:-128}"
HEAD_VECTOR_COUNT="${HEAD_VECTOR_COUNT:-1000}"
INDEX_PATH="${INDEX_PATH:-./build/stress_test_index}"
SPDK_MAPPING_PATH="${SPDK_MAPPING_PATH:-${INDEX_PATH}/spdk_mapping.txt}"
SSD_INFO_FILE="${SSD_INFO_FILE:-${INDEX_PATH}/ssd_info.txt}"
SPDK_BATCH_SIZE="${SPDK_BATCH_SIZE:-128}"
NUM_THREADS="${NUM_THREADS:-16}"

# SPDK Environment variables
export SPFRESH_SPDK_USE_SSD_IMPL="${SPFRESH_SPDK_USE_SSD_IMPL:-1}"
export SPFRESH_SPDK_CONF="${SPFRESH_SPDK_CONF:-${INDEX_PATH}/spdk_config.json}"
export SPFRESH_SPDK_BDEV="${SPFRESH_SPDK_BDEV:-Nvme0n1}"
export SPFRESH_SPDK_IO_DEPTH="${SPFRESH_SPDK_IO_DEPTH:-1024}"

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Run SPFresh stress test with SPDK backend"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -d, --data FILE         Data file (default: $DATA_FILE)"
    echo "  -o, --output FILE       Output log file (default: $OUTPUT_LOG)"
    echo "  -s, --stats FILE        Statistics log file (default: $STATS_LOG)"
    echo "  -k, --k NUM             Number of nearest neighbors (default: $K)"
    echo "  -D, --dimension NUM     Vector dimension (default: $DIMENSION)"
    echo "  -H, --head NUM          Head vector count (default: $HEAD_VECTOR_COUNT)"
    echo "  -i, --index PATH        Index directory (default: $INDEX_PATH)"
    echo "  -b, --batch NUM         SPDK batch size (default: $SPDK_BATCH_SIZE)"
    echo "  -t, --threads NUM       Number of threads (default: $NUM_THREADS)"
    echo ""
    echo "Environment variables for SPDK:"
    echo "  SPFRESH_SPDK_USE_SSD_IMPL=1   (enable SPDK)"
    echo "  SPFRESH_SPDK_CONF             (SPDK config JSON)"
    echo "  SPFRESH_SPDK_BDEV             (SPDK block device name)"
    echo "  SPFRESH_SPDK_IO_DEPTH         (SPDK IO queue depth)"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -d|--data)
            DATA_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_LOG="$2"
            shift 2
            ;;
        -s|--stats)
            STATS_LOG="$2"
            shift 2
            ;;
        -k|--k)
            K="$2"
            shift 2
            ;;
        -D|--dimension)
            DIMENSION="$2"
            shift 2
            ;;
        -H|--head)
            HEAD_VECTOR_COUNT="$2"
            shift 2
            ;;
        -i|--index)
            INDEX_PATH="$2"
            SPDK_MAPPING_PATH="${INDEX_PATH}/spdk_mapping.txt"
            SSD_INFO_FILE="${INDEX_PATH}/ssd_info.txt"
            SPFRESH_SPDK_CONF="${INDEX_PATH}/spdk_config.json"
            shift 2
            ;;
        -b|--batch)
            SPDK_BATCH_SIZE="$2"
            shift 2
            ;;
        -t|--threads)
            NUM_THREADS="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validation
if [ ! -f "$DATA_FILE" ]; then
    print_error "Data file not found: $DATA_FILE"
    exit 1
fi

if [ ! -f "./build/spfresh_stress_test_uint8" ]; then
    print_error "spfresh_stress_test_uint8 binary not found in ./build/"
    print_error "Please build the project first"
    exit 1
fi

if [ ! -f "$SPFRESH_SPDK_CONF" ]; then
    print_error "SPDK config file not found: $SPFRESH_SPDK_CONF"
    print_error "Please run: sudo ./script/setup_spdk.sh"
    exit 1
fi

if [ ! -f "$SPDK_MAPPING_PATH" ]; then
    print_error "SPDK mapping file not found: $SPDK_MAPPING_PATH"
    print_error "Please run: sudo ./script/setup_spdk.sh"
    exit 1
fi

if [ ! -f "$SSD_INFO_FILE" ]; then
    print_error "SSD info file not found: $SSD_INFO_FILE"
    print_error "Please run: sudo ./script/setup_spdk.sh"
    exit 1
fi

# Check SPDK setup
if [ "$SPFRESH_SPDK_USE_SSD_IMPL" = "1" ]; then
    print_info "Checking SPDK setup..."

    # Check hugepages
    HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages)
    if [ "$HUGEPAGES" -lt 512 ]; then
        print_warn "Low hugepages configured: $HUGEPAGES (recommended: >= 512)"
        print_warn "Run: sudo ./script/setup_spdk.sh"
    else
        print_info "Hugepages: $HUGEPAGES"
    fi

    # Check UIO driver
    if ! lsmod | grep -q uio_pci_generic && ! lsmod | grep -q vfio_pci; then
        print_error "No userspace driver loaded (uio_pci_generic or vfio-pci)"
        print_error "Run: sudo ./script/setup_spdk.sh"
        exit 1
    fi
fi

# Display configuration
echo ""
print_info "================================================"
print_info "SPFresh Stress Test Configuration"
print_info "================================================"
echo ""
print_info "Test Parameters:"
print_info "  Data file:          $DATA_FILE"
print_info "  Output log:         $OUTPUT_LOG"
print_info "  Statistics log:     $STATS_LOG"
print_info "  K (neighbors):      $K"
print_info "  Dimension:          $DIMENSION"
print_info "  Head vectors:       $HEAD_VECTOR_COUNT"
print_info "  Index path:         $INDEX_PATH"
print_info "  SPDK batch size:    $SPDK_BATCH_SIZE"
print_info "  Worker threads:     $NUM_THREADS"
echo ""
print_info "SPDK Configuration:"
print_info "  Use SPDK:           $SPFRESH_SPDK_USE_SSD_IMPL"
print_info "  SPDK Config:        $SPFRESH_SPDK_CONF"
print_info "  SPDK BDev:          $SPFRESH_SPDK_BDEV"
print_info "  SPDK IO Depth:      $SPFRESH_SPDK_IO_DEPTH"
print_info "  SPDK Mapping:       $SPDK_MAPPING_PATH"
print_info "  SSD Info:           $SSD_INFO_FILE"
echo ""

# Ensure index directory exists
mkdir -p "$INDEX_PATH/head"

# Run the stress test
print_info "Starting SPFresh stress test..."
echo ""

./build/spfresh_stress_test_uint8 \
    "$DATA_FILE" \
    "$OUTPUT_LOG" \
    "$STATS_LOG" \
    "$K" \
    "$DIMENSION" \
    "$HEAD_VECTOR_COUNT" \
    "$INDEX_PATH" \
    "$SPDK_MAPPING_PATH" \
    "$SSD_INFO_FILE" \
    "$SPDK_BATCH_SIZE" \
    "$NUM_THREADS"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    print_info "================================================"
    print_info "Stress test completed successfully!"
    print_info "================================================"
    print_info "Output log: $OUTPUT_LOG"
    print_info "Statistics log: $STATS_LOG"
else
    print_error "================================================"
    print_error "Stress test failed with exit code: $EXIT_CODE"
    print_error "================================================"
fi

exit $EXIT_CODE
