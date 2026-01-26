#!/bin/bash
# Setup script to enable SPDK io_uring access to /dev/sda without root privileges
# and configure huge pages for the current user
# This configuration is temporary and will not persist through reboot

set -e

DEVICE="/dev/sda"
USER=$(whoami)
HUGEPAGE_COUNT=1024  # Number of 2MB huge pages (2GB total)

echo "Setting up SPDK environment for $USER..."
echo ""

# ===========================
# Huge Pages Configuration
# ===========================
echo "[1/2] Configuring huge pages..."

# Check current huge page configuration
CURRENT_HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages)
echo "  Current huge pages: $CURRENT_HUGEPAGES"

# Allocate huge pages if needed
if [ "$CURRENT_HUGEPAGES" -lt "$HUGEPAGE_COUNT" ]; then
    echo "  Allocating $HUGEPAGE_COUNT huge pages (2MB each, $(($HUGEPAGE_COUNT * 2))MB total)..."
    sudo sh -c "echo $HUGEPAGE_COUNT > /proc/sys/vm/nr_hugepages"

    # Verify allocation
    ALLOCATED=$(cat /proc/sys/vm/nr_hugepages)
    if [ "$ALLOCATED" -lt "$HUGEPAGE_COUNT" ]; then
        echo "  Warning: Only $ALLOCATED huge pages allocated (requested $HUGEPAGE_COUNT)"
        echo "  This may be due to memory fragmentation. Try rebooting or reducing HUGEPAGE_COUNT."
    else
        echo "  ✓ Successfully allocated $ALLOCATED huge pages"
    fi
else
    echo "  ✓ Sufficient huge pages already allocated ($CURRENT_HUGEPAGES)"
fi

# Ensure hugepages mount point exists and is accessible
HUGEPAGE_MOUNT="/dev/hugepages"
if [ ! -d "$HUGEPAGE_MOUNT" ]; then
    echo "  Creating hugepage mount point..."
    sudo mkdir -p "$HUGEPAGE_MOUNT"
fi

# Check if already mounted
if ! mount | grep -q "on $HUGEPAGE_MOUNT type hugetlbfs"; then
    echo "  Mounting hugetlbfs..."
    sudo mount -t hugetlbfs -o uid=$(id -u),gid=$(id -g),mode=0775 nodev "$HUGEPAGE_MOUNT"
else
    # Remount with current user permissions
    echo "  Remounting hugetlbfs with user permissions..."
    sudo mount -o remount,uid=$(id -u),gid=$(id -g),mode=0775 "$HUGEPAGE_MOUNT"
fi

echo "  ✓ Huge pages configured and accessible"
echo ""

# ===========================
# Device Permissions
# ===========================
echo "[2/2] Setting up device permissions for io_uring..."

# Check if device exists
if [ ! -b "$DEVICE" ]; then
    echo "Error: $DEVICE is not a block device or doesn't exist"
    exit 1
fi

# Check if io_uring is available
if ! grep -q "io_uring" /proc/filesystems 2>/dev/null && ! grep -q "CONFIG_IO_URING=y" /boot/config-$(uname -r) 2>/dev/null; then
    echo "Warning: io_uring support may not be available on this kernel"
fi

# Grant temporary read/write permissions to the current user using ACL
# This requires sudo but is temporary (won't persist through reboot)
echo "Setting temporary ACL permissions on $DEVICE for user $USER..."
sudo setfacl -m u:$USER:rw $DEVICE

# Verify permissions
if ! getfacl $DEVICE 2>/dev/null | grep -q "user:$USER:rw"; then
    echo "Error: Failed to set ACL permissions"
    exit 1
fi

echo "  ✓ ACL permissions set successfully"
echo "  ✓ $USER now has read/write access to $DEVICE"

# Display current permissions
echo ""
echo "Current permissions for $DEVICE:"
getfacl $DEVICE 2>/dev/null | grep -E "^(# file:|user:|group:|other:)"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  • Huge pages: $(cat /proc/sys/vm/nr_hugepages) x 2MB allocated"
echo "  • Huge pages mount: $HUGEPAGE_MOUNT (accessible by $USER)"
echo "  • Device access: $USER has read/write access to $DEVICE via ACL"
echo ""
echo "You can now run SPDK with io_uring on $DEVICE as user $USER"
echo ""
echo "To cleanup after use (optional):"
echo "  # Remove device ACL:"
echo "  sudo setfacl -b $DEVICE"
echo ""
echo "  # Unmount hugepages (will reset on reboot anyway):"
echo "  sudo umount $HUGEPAGE_MOUNT"
echo ""
echo "  # Free huge pages (will reset on reboot anyway):"
echo "  sudo sh -c 'echo 0 > /proc/sys/vm/nr_hugepages'"
echo ""
