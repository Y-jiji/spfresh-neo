#!/bin/bash
# Setup script to configure huge pages for SPDK/DPDK
# This configuration is temporary and will not persist through reboot

set -e

USER=$(whoami)
HUGEPAGE_COUNT=1024  # Number of 2MB huge pages (2GB total)

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --hugepage-count)
            HUGEPAGE_COUNT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--hugepage-count <count>]"
            echo "  --hugepage-count: Number of 2MB huge pages to allocate (default: 1024)"
            exit 1
            ;;
    esac
done

echo "Setting up huge pages for $USER..."
echo ""

# Check current huge page configuration
CURRENT_HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages)
echo "Current huge pages: $CURRENT_HUGEPAGES"

# Allocate huge pages if needed
if [ "$CURRENT_HUGEPAGES" -lt "$HUGEPAGE_COUNT" ]; then
    echo "Allocating $HUGEPAGE_COUNT huge pages (2MB each, $(($HUGEPAGE_COUNT * 2))MB total)..."
    sudo sh -c "echo $HUGEPAGE_COUNT > /proc/sys/vm/nr_hugepages"

    # Verify allocation
    ALLOCATED=$(cat /proc/sys/vm/nr_hugepages)
    if [ "$ALLOCATED" -lt "$HUGEPAGE_COUNT" ]; then
        echo "Warning: Only $ALLOCATED huge pages allocated (requested $HUGEPAGE_COUNT)"
        echo "This may be due to memory fragmentation. Try rebooting or reducing HUGEPAGE_COUNT."
    else
        echo "Successfully allocated $ALLOCATED huge pages"
    fi
else
    echo "Sufficient huge pages already allocated ($CURRENT_HUGEPAGES)"
fi

# Ensure hugepages mount point exists and is accessible
HUGEPAGE_MOUNT="/dev/hugepages"
if [ ! -d "$HUGEPAGE_MOUNT" ]; then
    echo "Creating hugepage mount point..."
    sudo mkdir -p "$HUGEPAGE_MOUNT"
fi

# Check if already mounted
if ! mount | grep -q "on $HUGEPAGE_MOUNT type hugetlbfs"; then
    echo "Mounting hugetlbfs..."
    sudo mount -t hugetlbfs -o uid=$(id -u),gid=$(id -g),mode=0777 none "$HUGEPAGE_MOUNT"
else
    # Remount with current user permissions
    echo "Remounting hugetlbfs with user permissions..."
    sudo umount "$HUGEPAGE_MOUNT" 2>/dev/null || true
    sudo mount -t hugetlbfs -o uid=$(id -u),gid=$(id -g),mode=0777 none "$HUGEPAGE_MOUNT"
fi

# Verify mount and permissions
echo "Verifying hugepage mount..."
if mount | grep -q "$HUGEPAGE_MOUNT"; then
    MOUNT_INFO=$(mount | grep "$HUGEPAGE_MOUNT")
    echo "  Mount: $MOUNT_INFO"

    # Test write access
    TEST_FILE="$HUGEPAGE_MOUNT/.test_$$"
    if touch "$TEST_FILE" 2>/dev/null; then
        rm -f "$TEST_FILE"
        echo "Huge pages configured and accessible (write test passed)"
    else
        echo "Warning: Cannot write to $HUGEPAGE_MOUNT"
        echo "Attempting to fix permissions..."
        sudo chmod 1777 "$HUGEPAGE_MOUNT"
        if touch "$TEST_FILE" 2>/dev/null; then
            rm -f "$TEST_FILE"
            echo "Permissions fixed"
        else
            echo "Error: Still cannot write to $HUGEPAGE_MOUNT"
            exit 1
        fi
    fi
else
    echo "Error: Failed to mount $HUGEPAGE_MOUNT"
    exit 1
fi

echo ""
echo "=========================================="
echo "Huge pages setup complete!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  Huge pages: $(cat /proc/sys/vm/nr_hugepages) x 2MB allocated"
echo "  Huge pages mount: $HUGEPAGE_MOUNT (accessible by $USER)"
echo ""
echo "To cleanup (optional, will reset on reboot anyway):"
echo "  sudo umount $HUGEPAGE_MOUNT"
echo "  sudo sh -c 'echo 0 > /proc/sys/vm/nr_hugepages'"
