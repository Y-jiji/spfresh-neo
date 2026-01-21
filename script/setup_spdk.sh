#!/bin/bash

# SPDK Setup Script for SPFresh
# This script configures SPDK environment to run spfresh_stress_test_uint8

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

# Configuration
HUGEPAGES_NUM=${HUGEPAGES_NUM:-1024}  # Number of 2MB hugepages (default: 1024 = 2GB)
NVME_DEVICE=${NVME_DEVICE:-"0000:b4:00.0"}  # Default to Samsung NVMe (change if needed)
DRIVER=${DRIVER:-"uio_pci_generic"}  # uio_pci_generic or vfio-pci

# Detect available NVMe devices
print_info "Detecting NVMe devices..."
echo ""
echo "Available NVMe devices:"
lspci | grep -i "Non-Volatile memory controller"
echo ""

# Function to setup hugepages
setup_hugepages() {
    print_info "Setting up hugepages..."

    # Check current hugepages
    CURRENT_HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages)
    print_info "Current hugepages: $CURRENT_HUGEPAGES"

    if [ "$CURRENT_HUGEPAGES" -lt "$HUGEPAGES_NUM" ]; then
        print_info "Configuring $HUGEPAGES_NUM hugepages (2MB each)..."
        echo $HUGEPAGES_NUM > /proc/sys/vm/nr_hugepages

        # Verify
        ACTUAL_HUGEPAGES=$(cat /proc/sys/vm/nr_hugepages)
        if [ "$ACTUAL_HUGEPAGES" -lt "$HUGEPAGES_NUM" ]; then
            print_warn "Could only allocate $ACTUAL_HUGEPAGES hugepages (requested $HUGEPAGES_NUM)"
            print_warn "This is usually fine, but if you encounter memory issues, try rebooting and running this script early"
        else
            print_info "Successfully allocated $ACTUAL_HUGEPAGES hugepages"
        fi
    else
        print_info "Hugepages already configured: $CURRENT_HUGEPAGES"
    fi

    # Mount hugepages if not already mounted
    if ! mount | grep -q hugetlbfs; then
        print_info "Mounting hugetlbfs..."
        mkdir -p /mnt/huge
        mount -t hugetlbfs nodev /mnt/huge
    else
        print_info "Hugetlbfs already mounted"
    fi
}

# Function to load kernel modules
load_kernel_modules() {
    print_info "Loading kernel modules..."

    if [ "$DRIVER" = "uio_pci_generic" ]; then
        if ! lsmod | grep -q uio; then
            modprobe uio
            print_info "Loaded uio module"
        fi
        if ! lsmod | grep -q uio_pci_generic; then
            modprobe uio_pci_generic
            print_info "Loaded uio_pci_generic module"
        fi
    elif [ "$DRIVER" = "vfio-pci" ]; then
        if ! lsmod | grep -q vfio_pci; then
            modprobe vfio-pci
            print_info "Loaded vfio-pci module"
        fi
    else
        print_error "Unknown driver: $DRIVER"
        exit 1
    fi

    print_info "Kernel modules loaded successfully"
}

# Function to check device compatibility
check_device_compatibility() {
    local device=$1

    print_info "Checking device compatibility..."

    # Check if device exists
    if [ ! -d "/sys/bus/pci/devices/$device" ]; then
        print_error "Device $device not found"
        return 1
    fi

    # Check if device supports MSI or MSI-X (required for uio_pci_generic)
    if [ "$DRIVER" = "uio_pci_generic" ]; then
        # Check for MSI-X support first (preferred)
        if [ -d "/sys/bus/pci/devices/$device/msix_irqs" ]; then
            MSIX_COUNT=$(ls "/sys/bus/pci/devices/$device/msix_irqs" 2>/dev/null | wc -l)
            print_info "Device supports MSI-X ($MSIX_COUNT IRQs)"
        # Check for MSI support
        elif [ -d "/sys/bus/pci/devices/$device/msi_irqs" ]; then
            MSI_COUNT=$(ls "/sys/bus/pci/devices/$device/msi_irqs" 2>/dev/null | wc -l)
            print_info "Device supports MSI ($MSI_COUNT IRQs)"
        # No MSI/MSI-X detected - uio_pci_generic won't work
        else
            print_warn "Device does not have MSI/MSI-X enabled"
            print_warn "uio_pci_generic requires MSI or MSI-X interrupts"
            print_warn "Automatically switching to vfio-pci driver..."
            DRIVER="vfio-pci"

            # Load vfio-pci if needed
            if ! lsmod | grep -q vfio_pci; then
                modprobe vfio-pci || {
                    print_error "Failed to load vfio-pci module"
                    return 1
                }
            fi
        fi
    fi

    return 0
}

# Function to unbind device from kernel driver
unbind_device() {
    local device=$1
    local current_driver=$(basename $(readlink /sys/bus/pci/devices/$device/driver 2>/dev/null) 2>/dev/null || echo "none")

    print_info "Current driver for $device: $current_driver"

    if [ "$current_driver" != "none" ] && [ "$current_driver" != "$DRIVER" ]; then
        print_info "Unbinding $device from $current_driver..."
        echo $device > /sys/bus/pci/drivers/$current_driver/unbind 2>/dev/null || {
            print_error "Failed to unbind device from $current_driver"
            print_error "Device may be in use. Check: lsof | grep nvme"
            return 1
        }
        sleep 1

        # Verify unbind
        current_driver=$(basename $(readlink /sys/bus/pci/devices/$device/driver 2>/dev/null) 2>/dev/null || echo "none")
        if [ "$current_driver" != "none" ]; then
            print_error "Device still bound to $current_driver after unbind"
            return 1
        fi
        print_info "Successfully unbound device"
    fi
}

# Function to bind device to userspace driver
bind_device() {
    local device=$1

    print_info "Binding $device to $DRIVER..."

    # Get vendor and device ID
    VENDOR=$(cat /sys/bus/pci/devices/$device/vendor)
    DEVICE_ID=$(cat /sys/bus/pci/devices/$device/device)

    print_info "Device: $device (Vendor: $VENDOR, Device: $DEVICE_ID)"

    # Unbind from current driver first
    unbind_device $device

    # Use driver_override method (more reliable)
    print_info "Setting driver_override to $DRIVER..."
    echo "$DRIVER" > /sys/bus/pci/devices/$device/driver_override 2>/dev/null || {
        print_error "Failed to set driver_override"
        return 1
    }

    # Try to probe the device
    print_info "Probing device..."
    echo "$device" > /sys/bus/pci/drivers_probe 2>/dev/null || {
        # If probe fails, try the new_id method as fallback
        print_warn "Probe failed, trying new_id method..."

        if [ "$DRIVER" = "uio_pci_generic" ]; then
            echo "$VENDOR $DEVICE_ID" > /sys/bus/pci/drivers/uio_pci_generic/new_id 2>/dev/null || true
            sleep 1
        elif [ "$DRIVER" = "vfio-pci" ]; then
            echo "$VENDOR $DEVICE_ID" > /sys/bus/pci/drivers/vfio-pci/new_id 2>/dev/null || true
            sleep 1
        fi
    }

    # Give it a moment to bind
    sleep 2

    # Verify binding by checking the driver symlink
    local new_driver=$(basename $(readlink /sys/bus/pci/devices/$device/driver 2>/dev/null) 2>/dev/null || echo "none")

    if [ "$new_driver" = "$DRIVER" ]; then
        print_info "Successfully bound $device to $DRIVER"

        # Additional verification - check for UIO device
        if [ "$DRIVER" = "uio_pci_generic" ]; then
            UIO_DEV=$(ls -d /sys/bus/pci/devices/$device/uio/uio* 2>/dev/null | head -1)
            if [ -n "$UIO_DEV" ]; then
                UIO_NAME=$(basename $UIO_DEV)
                print_info "UIO device created: /dev/$UIO_NAME"
            fi
        fi

        return 0
    else
        print_error "Failed to bind $device to $DRIVER (current driver: $new_driver)"
        print_error "The device may require additional steps or a different driver"

        # Show troubleshooting info
        print_info "Checking device status..."
        if [ -f "/sys/bus/pci/devices/$device/driver_override" ]; then
            OVERRIDE=$(cat /sys/bus/pci/devices/$device/driver_override 2>/dev/null)
            print_info "Driver override is set to: $OVERRIDE"
        fi

        return 1
    fi
}

# Function to create SPDK configuration files
create_spdk_config() {
    local index_path=$1
    local spdk_conf_path="${index_path}/spdk_config.json"
    local spdk_mapping_path="${index_path}/spdk_mapping.txt"
    local ssd_info_path="${index_path}/ssd_info.txt"

    print_info "Creating SPDK configuration files in $index_path..."

    mkdir -p "$index_path"

    # Create minimal SPDK JSON config
    cat > "$spdk_conf_path" <<EOF
{
  "subsystems": [
    {
      "subsystem": "bdev",
      "config": [
        {
          "method": "bdev_nvme_attach_controller",
          "params": {
            "name": "Nvme0",
            "trtype": "PCIe",
            "traddr": "${NVME_DEVICE}"
          }
        }
      ]
    }
  ]
}
EOF

    # Create SPDK mapping file
    cat > "$spdk_mapping_path" <<EOF
# SPDK Device Mapping
# Format: logical_name pci_address
Nvme0 ${NVME_DEVICE}
EOF

    # Create SSD info file
    cat > "$ssd_info_path" <<EOF
# SSD Information
# Format: device_name total_blocks block_size
Nvme0n1 17179869184 4096
EOF

    print_info "Created SPDK configuration files:"
    print_info "  - $spdk_conf_path"
    print_info "  - $spdk_mapping_path"
    print_info "  - $ssd_info_path"
}

# Function to print environment variables
print_env_vars() {
    local index_path=$1
    local spdk_conf_path="${index_path}/spdk_config.json"

    echo ""
    print_info "================================================"
    print_info "SPDK Setup Complete!"
    print_info "================================================"
    echo ""
    print_info "To run spfresh_stress_test_uint8, use these environment variables:"
    echo ""
    echo "export SPFRESH_SPDK_USE_SSD_IMPL=1"
    echo "export SPFRESH_SPDK_CONF=\"$spdk_conf_path\""
    echo "export SPFRESH_SPDK_BDEV=\"Nvme0n1\""
    echo "export SPFRESH_SPDK_IO_DEPTH=1024"
    echo ""
    print_info "Example command:"
    echo ""
    echo "sudo SPFRESH_SPDK_USE_SSD_IMPL=1 \\"
    echo "     SPFRESH_SPDK_CONF=\"$spdk_conf_path\" \\"
    echo "     SPFRESH_SPDK_BDEV=\"Nvme0n1\" \\"
    echo "     SPFRESH_SPDK_IO_DEPTH=1024 \\"
    echo "     ./build/spfresh_stress_test_uint8 \\"
    echo "         ./build/vectors_320M_uint8.bin \\"
    echo "         ./output.log \\"
    echo "         ./stats.log \\"
    echo "         10 \\"
    echo "         128 \\"
    echo "         1000 \\"
    echo "         $index_path \\"
    echo "         ${index_path}/spdk_mapping.txt \\"
    echo "         ${index_path}/ssd_info.txt \\"
    echo "         128 \\"
    echo "         16"
    echo ""
    print_info "Or use the wrapper script: ./script/run_spfresh_stress_test.sh"
    echo ""
}

# Main execution
main() {
    echo ""
    print_info "================================================"
    print_info "SPFresh SPDK Setup Script"
    print_info "================================================"
    echo ""

    # Parse command line arguments
    if [ $# -gt 0 ]; then
        NVME_DEVICE=$1
    fi

    if [ $# -gt 1 ]; then
        INDEX_PATH=$2
    else
        INDEX_PATH="./build/stress_test_index"
    fi

    print_info "Configuration:"
    print_info "  NVMe Device: $NVME_DEVICE"
    print_info "  Driver: $DRIVER"
    print_info "  Hugepages: $HUGEPAGES_NUM (2MB each)"
    print_info "  Index Path: $INDEX_PATH"
    echo ""

    # Ask for confirmation
    read -p "Continue with this configuration? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Setup cancelled"
        exit 0
    fi

    # Setup steps
    setup_hugepages
    load_kernel_modules
    check_device_compatibility $NVME_DEVICE || exit 1
    bind_device $NVME_DEVICE || {
        print_error ""
        print_error "Device binding failed. Troubleshooting steps:"
        print_error "1. Try using vfio-pci driver: sudo DRIVER=vfio-pci $0 $NVME_DEVICE"
        print_error "2. Check if device is in use: lsof | grep nvme"
        print_error "3. Check dmesg for errors: dmesg | tail -20"
        print_error "4. Verify device exists: ls -la /sys/bus/pci/devices/$NVME_DEVICE"
        exit 1
    }
    create_spdk_config $INDEX_PATH
    print_env_vars $INDEX_PATH

    print_info "Setup completed successfully!"
}

# Run main function
main "$@"
