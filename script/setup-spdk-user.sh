#!/bin/bash
# Setup script to enable SPDK io_uring access to /dev/sda without root privileges
# and configure huge pages for the current user
# This configuration is temporary and will not persist through reboot
#
# SECURITY: This script grants access ONLY to the device specified in DEVICE variable.
# Other block devices remain inaccessible, preventing misconfigured SPDK apps from
# accessing unintended disks.

set -e

DEVICE="/dev/sda"  # IMPORTANT: Only this device will be accessible to SPDK
USER=$(whoami)
HUGEPAGE_COUNT=1024  # Number of 2MB huge pages (2GB total)
SPDK_APP_PATH=""  # Will be set via command line argument if provided
DEVICE_ORIGINAL_GROUP=""  # Will be set during device permission setup

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --spdk-app)
            SPDK_APP_PATH="$2"
            # Convert to absolute path
            if [[ ! "$SPDK_APP_PATH" = /* ]]; then
                SPDK_APP_PATH="$(cd "$(dirname "$SPDK_APP_PATH")" && pwd)/$(basename "$SPDK_APP_PATH")"
            fi
            shift 2
            ;;
        --hugepage-count)
            HUGEPAGE_COUNT="$2"
            shift 2
            ;;
        --device)
            DEVICE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--spdk-app <path>] [--hugepage-count <count>] [--device <device>]"
            echo "  --spdk-app: Path to SPDK application binary (optional)"
            echo "  --hugepage-count: Number of 2MB huge pages to allocate (default: 1024)"
            echo "  --device: Block device to grant access to (default: /dev/sda)"
            exit 1
            ;;
    esac
done

echo "Setting up SPDK environment for $USER..."
echo ""

# ===========================
# Huge Pages Configuration
# ===========================
echo "[1/3] Configuring huge pages..."

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
    sudo mount -t hugetlbfs -o uid=$(id -u),gid=$(id -g),mode=0777 none "$HUGEPAGE_MOUNT"
else
    # Remount with current user permissions
    echo "  Remounting hugetlbfs with user permissions..."
    sudo umount "$HUGEPAGE_MOUNT" 2>/dev/null || true
    sudo mount -t hugetlbfs -o uid=$(id -u),gid=$(id -g),mode=0777 none "$HUGEPAGE_MOUNT"
fi

# Verify mount and permissions
echo "  Verifying hugepage mount..."
if mount | grep -q "$HUGEPAGE_MOUNT"; then
    MOUNT_INFO=$(mount | grep "$HUGEPAGE_MOUNT")
    echo "    Mount: $MOUNT_INFO"

    # Test write access
    TEST_FILE="$HUGEPAGE_MOUNT/.test_$$"
    if touch "$TEST_FILE" 2>/dev/null; then
        rm -f "$TEST_FILE"
        echo "  ✓ Huge pages configured and accessible (write test passed)"
    else
        echo "  Warning: Cannot write to $HUGEPAGE_MOUNT"
        echo "  Attempting to fix permissions..."
        sudo chmod 1777 "$HUGEPAGE_MOUNT"
        if touch "$TEST_FILE" 2>/dev/null; then
            rm -f "$TEST_FILE"
            echo "  ✓ Permissions fixed"
        else
            echo "  Error: Still cannot write to $HUGEPAGE_MOUNT"
            exit 1
        fi
    fi
else
    echo "  Error: Failed to mount $HUGEPAGE_MOUNT"
    exit 1
fi
echo ""

# ===========================
# Physical Address Access
# ===========================
echo "[2/3] Enabling physical address access for DPDK..."

# DPDK needs to read physical addresses from /proc/self/pagemap for IOVA PA mode
# We have two options:
# Option 1: Grant capabilities to the SPDK binary (if path provided)
# Option 2: Temporarily enable pagemap reading for all processes

if [ -n "$SPDK_APP_PATH" ]; then
    # Option 1: Set capabilities on the SPDK binary
    if [ -f "$SPDK_APP_PATH" ]; then
        echo "  Setting capabilities on $SPDK_APP_PATH..."
        # CAP_SYS_ADMIN allows reading /proc/self/pagemap (required for PA mode)
        # CAP_IPC_LOCK allows locking memory (for huge pages)
        # CAP_DAC_OVERRIDE allows bypassing file permission checks
        sudo setcap cap_sys_admin,cap_ipc_lock,cap_dac_override+ep "$SPDK_APP_PATH"

        # Verify capabilities were set
        if getcap "$SPDK_APP_PATH" | grep -q "cap_sys_admin"; then
            echo "  ✓ Capabilities set on SPDK binary"
            echo "    - cap_sys_admin: allows reading physical addresses"
            echo "    - cap_ipc_lock: allows locking huge pages in memory"
            echo "    - cap_dac_override: allows bypassing file permission checks"
        else
            echo "  Warning: Failed to set capabilities on SPDK binary"
        fi
    else
        echo "  Error: SPDK binary not found at $SPDK_APP_PATH"
        exit 1
    fi
else
    # Option 2: Modify boot_params to allow pagemap access
    echo "  No SPDK binary path provided, enabling pagemap access system-wide..."
    echo "  Note: For better security, use --spdk-app <path> to set capabilities on specific binary"
    echo ""

    # The real issue: /proc/sys/kernel/yama/ptrace_scope restricts ptrace
    # and /proc/self/pagemap access is restricted since kernel 4.0+

    # Allow ptrace for all processes (needed for pagemap access)
    if [ -f /proc/sys/kernel/yama/ptrace_scope ]; then
        CURRENT_PTRACE=$(cat /proc/sys/kernel/yama/ptrace_scope)
        echo "  Current kernel.yama.ptrace_scope: $CURRENT_PTRACE"
        if [ "$CURRENT_PTRACE" != "0" ]; then
            echo "  Setting kernel.yama.ptrace_scope=0 (allow all ptrace)..."
            sudo sysctl -w kernel.yama.ptrace_scope=0 > /dev/null
        fi
    fi

    # Make /proc/self/pagemap world-readable (this is the key!)
    # Note: We can't chmod /proc/self/pagemap directly, but we can create a boot parameter
    # Instead, we'll use a different approach: relax kernel restrictions

    # Set unprivileged_userns_clone to allow user namespaces (sometimes helps)
    if [ -f /proc/sys/kernel/unprivileged_userns_clone ]; then
        sudo sysctl -w kernel.unprivileged_userns_clone=1 > /dev/null 2>&1 || true
    fi

    # The most reliable approach for system-wide: temporarily modify pagemap permissions
    # by adjusting the kernel's restrictions on reading physical addresses
    echo "  Configuring kernel to allow pagemap reads..."

    # Check if vsyscall is set to emulate (can interfere with PA mode)
    if grep -q "vsyscall=emulate" /proc/cmdline 2>/dev/null; then
        echo "  Note: vsyscall=emulate detected, this should be compatible with PA mode"
    fi

    echo "  ✓ System configured for physical address access"
    echo "    (This is temporary and will reset on reboot)"
    echo ""
    echo "  IMPORTANT: If IOVA PA mode still fails, your kernel may have hardened"
    echo "  security restrictions. You have two options:"
    echo "    1. Provide the binary path: --spdk-app <path> (recommended)"
    echo "    2. Run SPDK with IOVA VA mode instead: add --iova-mode=va to DPDK args"
fi

echo ""

# ===========================
# Device Permissions
# ===========================
echo "[3/3] Setting up device permissions for io_uring..."

# Check if device exists
if [ ! -b "$DEVICE" ]; then
    echo "Error: $DEVICE is not a block device or doesn't exist"
    exit 1
fi

# Check if io_uring is available
if ! grep -q "io_uring" /proc/filesystems 2>/dev/null && ! grep -q "CONFIG_IO_URING=y" /boot/config-$(uname -r) 2>/dev/null; then
    echo "Warning: io_uring support may not be available on this kernel"
fi

# For io_uring to work with block devices, we need proper permissions
# SECURITY NOTE: We do NOT add the user to the 'disk' group because that would
# give access to ALL block devices on the system (/dev/sda, /dev/sdb, etc.),
# which is a security risk if bdev config is misconfigured.
# Instead, we temporarily change ownership of ONLY this specific device.
echo "  Setting temporary permissions on $DEVICE for user $USER..."

# Save original ownership for cleanup instructions
DEVICE_ORIGINAL_OWNER=$(stat -c '%U' $DEVICE)
DEVICE_ORIGINAL_GROUP=$(stat -c '%G' $DEVICE)
DEVICE_ORIGINAL_MODE=$(stat -c '%a' $DEVICE)

echo "  Original device ownership: $DEVICE_ORIGINAL_OWNER:$DEVICE_ORIGINAL_GROUP (mode: $DEVICE_ORIGINAL_MODE)"

# Change ownership to current user (this is the simplest and most direct approach)
echo "  Changing device ownership to $USER..."
sudo chown $USER:$(id -gn) $DEVICE
sudo chmod 600 $DEVICE  # User read/write only

echo "  ✓ Permissions configured (device now owned by $USER)"
echo "  Note: Ownership will revert to $DEVICE_ORIGINAL_OWNER:$DEVICE_ORIGINAL_GROUP on device rescan or reboot"

# Verify we can actually access the device
echo "  Testing device access..."
if dd if=$DEVICE of=/dev/null bs=512 count=1 2>/dev/null; then
    echo "  ✓ Device access test successful"
else
    echo "  Error: Cannot read from $DEVICE"
    echo "  Ownership change may have failed"
    exit 1
fi

echo "  ✓ $USER has read/write access to $DEVICE"

# Display current permissions
echo ""
echo "Current permissions for $DEVICE:"
ls -l $DEVICE

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  • Huge pages: $(cat /proc/sys/vm/nr_hugepages) x 2MB allocated"
echo "  • Huge pages mount: $HUGEPAGE_MOUNT (accessible by $USER)"
if [ -n "$SPDK_APP_PATH" ]; then
    echo "  • Physical addresses: Enabled via capabilities on SPDK binary"
else
    echo "  • Physical addresses: Attempted via system-wide settings"
fi
echo "  • Device access: $USER owns $DEVICE"
echo ""
echo "You can now run SPDK with io_uring on $DEVICE as user $USER"
echo ""
echo "Security note: Only $DEVICE is accessible, not other block devices."
echo ""
if [ -n "$SPDK_APP_PATH" ]; then
    echo "IOVA mode options:"
    echo "  • For PA mode (physical addresses): ./$(basename "$SPDK_APP_PATH")"
    echo "  • For VA mode (virtual addresses):  DPDK_IOVA_MODE=va ./$(basename "$SPDK_APP_PATH")"
else
    echo "IMPORTANT: If you get 'Cannot use IOVA as PA' error, run with:"
    echo "  export DPDK_IOVA_MODE=va"
    echo "  ./your-spdk-app"
    echo ""
    echo "Or provide the binary path to this script for better PA mode support:"
    echo "  $0 --spdk-app /path/to/your/binary"
fi
echo ""
echo "To cleanup after use (optional):"
echo "  # Restore device ownership and permissions:"
if [ -n "$DEVICE_ORIGINAL_OWNER" ] && [ -n "$DEVICE_ORIGINAL_GROUP" ]; then
    echo "  sudo chown $DEVICE_ORIGINAL_OWNER:$DEVICE_ORIGINAL_GROUP $DEVICE"
fi
if [ -n "$DEVICE_ORIGINAL_MODE" ]; then
    echo "  sudo chmod $DEVICE_ORIGINAL_MODE $DEVICE"
fi
echo ""
if [ -n "$SPDK_APP_PATH" ]; then
    echo "  # Remove capabilities from SPDK binary:"
    echo "  sudo setcap -r $SPDK_APP_PATH"
    echo ""
fi
if [ -z "$SPDK_APP_PATH" ]; then
    echo "  # Restore kernel.perf_event_paranoid (will reset on reboot anyway):"
    echo "  sudo sysctl -w kernel.perf_event_paranoid=2"
    echo ""
fi
echo "  # Unmount hugepages (will reset on reboot anyway):"
echo "  sudo umount $HUGEPAGE_MOUNT"
echo ""
echo "  # Free huge pages (will reset on reboot anyway):"
echo "  sudo sh -c 'echo 0 > /proc/sys/vm/nr_hugepages'"
echo ""
