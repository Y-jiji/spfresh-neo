#!/bin/bash

# SPDK Status Checker
# Shows current SPDK configuration status

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_good() {
    echo -e "${GREEN}✓${NC} $1"
}

print_bad() {
    echo -e "${RED}✗${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "  $1"
}

echo ""
print_header "SPDK Environment Status"
echo ""

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    print_good "Running as root"
else
    print_warn "Not running as root (some checks may be limited)"
fi

echo ""

# Check hugepages
print_header "Hugepages"
HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages 2>/dev/null || echo "0")
HUGEPAGES_FREE=$(grep HugePages_Free /proc/meminfo | awk '{print $2}')
HUGEPAGES_SIZE=$(grep Hugepagesize /proc/meminfo | awk '{print $2}')
HUGEPAGES_TOTAL_MB=$((HUGEPAGES * HUGEPAGES_SIZE / 1024))

if [ "$HUGEPAGES" -ge 512 ]; then
    print_good "Hugepages configured: $HUGEPAGES (${HUGEPAGES_TOTAL_MB}MB total)"
    print_info "Free: $HUGEPAGES_FREE, Size: ${HUGEPAGES_SIZE}KB each"
else
    print_bad "Insufficient hugepages: $HUGEPAGES (recommended: >= 512)"
fi

# Check if hugetlbfs is mounted
if mount | grep -q hugetlbfs; then
    print_good "Hugetlbfs mounted: $(mount | grep hugetlbfs | awk '{print $3}')"
else
    print_bad "Hugetlbfs not mounted"
fi

echo ""

# Check kernel modules
print_header "Kernel Modules"
if lsmod | grep -q uio_pci_generic; then
    print_good "uio_pci_generic loaded"
else
    print_bad "uio_pci_generic not loaded"
fi

if lsmod | grep -q "^uio "; then
    print_good "uio loaded"
else
    print_bad "uio not loaded"
fi

if lsmod | grep -q vfio_pci; then
    print_good "vfio-pci loaded"
else
    print_info "vfio-pci not loaded (optional)"
fi

echo ""

# Check NVMe devices
print_header "NVMe Devices"
echo ""

for nvme_dev in /sys/class/nvme/nvme*; do
    if [ ! -e "$nvme_dev" ]; then
        continue
    fi

    NVME_NAME=$(basename $nvme_dev)
    PCI_ADDR=$(basename $(readlink $nvme_dev/device))
    DRIVER=$(basename $(readlink $nvme_dev/device/driver 2>/dev/null) 2>/dev/null || echo "none")
    VENDOR=$(cat $nvme_dev/device/vendor 2>/dev/null)
    DEVICE=$(cat $nvme_dev/device/device 2>/dev/null)
    MODEL=$(cat $nvme_dev/model 2>/dev/null | tr -d ' ')

    echo "Device: $NVME_NAME ($PCI_ADDR)"
    print_info "Model: $MODEL"
    print_info "Vendor/Device: $VENDOR/$DEVICE"

    if [ "$DRIVER" = "uio_pci_generic" ] || [ "$DRIVER" = "vfio-pci" ]; then
        print_good "Driver: $DRIVER (userspace, ready for SPDK)"
    elif [ "$DRIVER" = "nvme" ]; then
        print_warn "Driver: $DRIVER (kernel driver, need to bind to userspace)"
    else
        print_info "Driver: $DRIVER"
    fi

    # Check MSI-X support
    if [ -d "$nvme_dev/device/msi_irqs" ]; then
        MSI_COUNT=$(ls $nvme_dev/device/msi_irqs | wc -l)
        print_good "MSI support: Yes ($MSI_COUNT IRQs)"
    else
        print_info "MSI support: Unknown"
    fi

    if [ -d "$nvme_dev/device/msix_irqs" ]; then
        MSIX_COUNT=$(ls $nvme_dev/device/msix_irqs | wc -l)
        print_good "MSI-X support: Yes ($MSIX_COUNT IRQs)"
    else
        print_warn "MSI-X support: Not detected (may need vfio-pci)"
    fi

    # Check driver_override
    DRIVER_OVERRIDE=$(cat $nvme_dev/device/driver_override 2>/dev/null || echo "(none)")
    if [ "$DRIVER_OVERRIDE" != "(none)" ] && [ -n "$DRIVER_OVERRIDE" ]; then
        print_info "Driver override: $DRIVER_OVERRIDE"
    fi

    echo ""
done

# Check SPDK processes
print_header "SPDK Processes"
if pgrep -f "spdk|spfresh" > /dev/null; then
    print_good "SPDK/SPFresh processes running:"
    ps aux | grep -E "spdk|spfresh" | grep -v grep | awk '{print "  "$11" (PID: "$2")"}'
else
    print_info "No SPDK/SPFresh processes currently running"
fi

echo ""

# Check SPDK configuration files
print_header "SPDK Configuration Files"
if [ -n "$1" ]; then
    INDEX_PATH="$1"
else
    INDEX_PATH="./build/stress_test_index"
fi

if [ -f "$INDEX_PATH/spdk_config.json" ]; then
    print_good "SPDK config: $INDEX_PATH/spdk_config.json"
else
    print_bad "SPDK config not found: $INDEX_PATH/spdk_config.json"
fi

if [ -f "$INDEX_PATH/spdk_mapping.txt" ]; then
    print_good "SPDK mapping: $INDEX_PATH/spdk_mapping.txt"
else
    print_bad "SPDK mapping not found: $INDEX_PATH/spdk_mapping.txt"
fi

if [ -f "$INDEX_PATH/ssd_info.txt" ]; then
    print_good "SSD info: $INDEX_PATH/ssd_info.txt"
else
    print_bad "SSD info not found: $INDEX_PATH/ssd_info.txt"
fi

echo ""

# Check environment variables
print_header "SPDK Environment Variables"
if [ -n "$SPFRESH_SPDK_USE_SSD_IMPL" ]; then
    print_good "SPFRESH_SPDK_USE_SSD_IMPL=$SPFRESH_SPDK_USE_SSD_IMPL"
else
    print_info "SPFRESH_SPDK_USE_SSD_IMPL not set"
fi

if [ -n "$SPFRESH_SPDK_CONF" ]; then
    print_info "SPFRESH_SPDK_CONF=$SPFRESH_SPDK_CONF"
fi

if [ -n "$SPFRESH_SPDK_BDEV" ]; then
    print_info "SPFRESH_SPDK_BDEV=$SPFRESH_SPDK_BDEV"
fi

if [ -n "$SPFRESH_SPDK_IO_DEPTH" ]; then
    print_info "SPFRESH_SPDK_IO_DEPTH=$SPFRESH_SPDK_IO_DEPTH"
fi

echo ""

# Recommendations
print_header "Recommendations"
echo ""

NEED_SETUP=false

if [ "$HUGEPAGES" -lt 512 ]; then
    print_warn "Run: sudo ./script/setup_spdk.sh"
    NEED_SETUP=true
fi

ALL_KERNEL_DRIVER=true
for nvme_dev in /sys/class/nvme/nvme*/device/driver; do
    if [ -e "$nvme_dev" ]; then
        DRIVER=$(basename $(readlink $nvme_dev 2>/dev/null) 2>/dev/null || echo "none")
        if [ "$DRIVER" = "uio_pci_generic" ] || [ "$DRIVER" = "vfio-pci" ]; then
            ALL_KERNEL_DRIVER=false
        fi
    fi
done

if $ALL_KERNEL_DRIVER; then
    print_warn "No NVMe devices bound to userspace driver"
    print_warn "Run: sudo ./script/setup_spdk.sh <pci_address>"
    NEED_SETUP=true
fi

if ! $NEED_SETUP; then
    print_good "SPDK environment is properly configured!"
    echo ""
    print_info "To run stress test: sudo ./script/run_spfresh_stress_test.sh"
fi

echo ""
