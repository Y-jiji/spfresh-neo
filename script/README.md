# SPFresh SPDK Setup Scripts

This directory contains scripts to configure SPDK and run the SPFresh stress test.

## Overview

SPFresh uses SPDK (Storage Performance Development Kit) for high-performance direct NVMe access. These scripts automate the setup process and provide a convenient way to run the stress test.

## Prerequisites

- Root/sudo access
- NVMe SSD
- SPDK installed (binaries in `/usr/local/bin/`)
- Built SPFresh project (`./build/spfresh_stress_test_uint8`)

## Scripts

### 1. setup_spdk.sh

Configures the SPDK environment including:
- Hugepages allocation
- Kernel module loading (uio_pci_generic or vfio-pci)
- NVMe device binding to userspace driver
- SPDK configuration files creation

**Usage:**

```bash
# Use default settings (Samsung NVMe at 0000:b4:00.0)
sudo ./script/setup_spdk.sh

# Specify custom NVMe device and index path
sudo ./script/setup_spdk.sh 0000:b3:00.0 ./build/my_index
```

**Environment Variables:**

- `HUGEPAGES_NUM` - Number of 2MB hugepages (default: 1024 = 2GB)
- `NVME_DEVICE` - PCI address of NVMe device (default: 0000:b4:00.0)
- `DRIVER` - Userspace driver to use (default: uio_pci_generic)

**Example:**

```bash
# Setup with custom hugepages
sudo HUGEPAGES_NUM=2048 ./script/setup_spdk.sh

# Use different driver
sudo DRIVER=vfio-pci ./script/setup_spdk.sh 0000:b3:00.0
```

**What it does:**

1. Checks available NVMe devices
2. Allocates hugepages for DPDK/SPDK memory management
3. Loads required kernel modules
4. Unbinds NVMe device from kernel driver
5. Binds NVMe device to userspace driver (uio_pci_generic or vfio-pci)
6. Creates SPDK configuration files:
   - `spdk_config.json` - SPDK subsystem configuration
   - `spdk_mapping.txt` - Device mapping
   - `ssd_info.txt` - SSD specifications

### 2. run_spfresh_stress_test.sh

Wrapper script that sets up environment variables and runs the stress test.

**Usage:**

```bash
# Run with default settings
sudo ./script/run_spfresh_stress_test.sh

# Customize parameters
sudo ./script/run_spfresh_stress_test.sh \
    --data ./build/vectors_320M_uint8.bin \
    --k 10 \
    --dimension 128 \
    --threads 16 \
    --index ./build/stress_test_index
```

**Options:**

- `-h, --help` - Show help message
- `-d, --data FILE` - Data file (default: ./build/vectors_320M_uint8.bin)
- `-o, --output FILE` - Output log file (default: ./output.log)
- `-s, --stats FILE` - Statistics log file (default: ./stats.log)
- `-k, --k NUM` - Number of nearest neighbors (default: 10)
- `-D, --dimension NUM` - Vector dimension (default: 128)
- `-H, --head NUM` - Head vector count (default: 1000)
- `-i, --index PATH` - Index directory (default: ./build/stress_test_index)
- `-b, --batch NUM` - SPDK batch size (default: 128)
- `-t, --threads NUM` - Number of worker threads (default: 16)

**Environment Variables:**

You can also set parameters via environment variables:

```bash
export DATA_FILE="./build/vectors_320M_uint8.bin"
export K=10
export DIMENSION=128
export NUM_THREADS=16
export SPFRESH_SPDK_IO_DEPTH=1024

sudo -E ./script/run_spfresh_stress_test.sh
```

## Quick Start Guide

### Step 1: Identify Your NVMe Device

```bash
# List all NVMe devices
lspci | grep -i "Non-Volatile memory controller"

# Example output:
# b3:00.0 Non-Volatile memory controller: Micron Technology Inc 2300 NVMe SSD
# b4:00.0 Non-Volatile memory controller: Samsung Electronics Co Ltd NVMe SSD Controller
```

### Step 2: Setup SPDK

```bash
# For Samsung NVMe (0000:b4:00.0)
sudo ./script/setup_spdk.sh 0000:b4:00.0 ./build/stress_test_index

# For Micron NVMe (0000:b3:00.0)
sudo ./script/setup_spdk.sh 0000:b3:00.0 ./build/stress_test_index
```

### Step 3: Run Stress Test

```bash
# Run with default settings
sudo ./script/run_spfresh_stress_test.sh

# Or customize
sudo ./script/run_spfresh_stress_test.sh \
    --data ./build/vectors_320M_uint8.bin \
    --k 10 \
    --threads 16
```

## Manual Execution

If you prefer to run the stress test manually:

```bash
# 1. Set environment variables
export SPFRESH_SPDK_USE_SSD_IMPL=1
export SPFRESH_SPDK_CONF="./build/stress_test_index/spdk_config.json"
export SPFRESH_SPDK_BDEV="Nvme0n1"
export SPFRESH_SPDK_IO_DEPTH=1024

# 2. Run the stress test
sudo ./build/spfresh_stress_test_uint8 \
    ./build/vectors_320M_uint8.bin \
    ./output.log \
    ./stats.log \
    10 \
    128 \
    1000 \
    ./build/stress_test_index \
    ./build/stress_test_index/spdk_mapping.txt \
    ./build/stress_test_index/ssd_info.txt \
    128 \
    16
```

## Checking NVMe Device Drivers

```bash
# Check which driver is bound to each NVMe device
for dev in /sys/class/nvme/nvme*/device/driver; do
    echo "$(basename $(dirname $(dirname $dev))): $(basename $(readlink $dev))"
done

# Example output:
# nvme0: nvme           (kernel driver - not ready for SPDK)
# nvme1: uio_pci_generic (userspace driver - ready for SPDK)
```

## Unbinding Devices (Restore to Kernel Driver)

If you need to restore NVMe devices to the kernel driver:

```bash
# Unbind from userspace driver
sudo echo "0000:b4:00.0" > /sys/bus/pci/drivers/uio_pci_generic/unbind

# Rescan PCI devices to rebind to kernel driver
sudo echo "0000:b4:00.0" > /sys/bus/pci/drivers/nvme/bind

# Or simply reboot
sudo reboot
```

## Troubleshooting

### Problem: "Failed to bind device to uio_pci_generic"

**Solution:** Try using vfio-pci driver instead:

```bash
sudo DRIVER=vfio-pci ./script/setup_spdk.sh
```

### Problem: "Could only allocate X hugepages"

**Solution:** Free up memory or reboot and run setup early:

```bash
# Drop caches to free memory
sudo sync
sudo echo 3 > /proc/sys/vm/drop_caches

# Then try again
sudo ./script/setup_spdk.sh
```

### Problem: "spdk_bdev_open_ext failed"

**Solution:** Verify SPDK configuration and device binding:

```bash
# Check if device is bound to userspace driver
lspci -k -s 0000:b4:00.0

# Check SPDK config file
cat ./build/stress_test_index/spdk_config.json

# Verify hugepages
cat /proc/meminfo | grep -i huge
```

### Problem: Device is mounted/in use

**Solution:** Unmount the device before binding:

```bash
# Find mount points
mount | grep nvme

# Unmount
sudo umount /dev/nvme0n1p1

# Then run setup
sudo ./script/setup_spdk.sh
```

## Performance Tuning

### Hugepages

More hugepages = better performance (up to a point):

```bash
# 4GB of hugepages
sudo HUGEPAGES_NUM=2048 ./script/setup_spdk.sh
```

### SPDK IO Depth

Higher IO depth = better throughput (but more memory):

```bash
# Run with higher IO depth
sudo SPFRESH_SPDK_IO_DEPTH=2048 ./script/run_spfresh_stress_test.sh
```

### Worker Threads

Adjust based on CPU cores:

```bash
# Use all available cores
sudo ./script/run_spfresh_stress_test.sh --threads $(nproc)

# Or specific number
sudo ./script/run_spfresh_stress_test.sh --threads 32
```

## Important Notes

1. **Root Access Required**: SPDK requires root/sudo for direct hardware access
2. **Device Exclusive Access**: NVMe device must not be mounted or in use
3. **Hugepages**: Required for SPDK/DPDK memory management
4. **Persistence**: Device bindings are lost on reboot - re-run setup script
5. **Data Safety**: Ensure you're binding the correct device - wrong device binding can cause data loss

## Architecture

### SPDK Environment Variables

The application uses these environment variables (set by the wrapper script):

- `SPFRESH_SPDK_USE_SSD_IMPL=1` - Enable SPDK backend
- `SPFRESH_SPDK_CONF` - Path to SPDK JSON config
- `SPFRESH_SPDK_BDEV` - SPDK block device name (e.g., "Nvme0n1")
- `SPFRESH_SPDK_IO_DEPTH` - IO queue depth (default: 1024)

### File Structure

After running setup, your index directory will contain:

```
build/stress_test_index/
├── spdk_config.json      # SPDK subsystem configuration
├── spdk_mapping.txt      # Device mapping
├── ssd_info.txt          # SSD specifications
└── head/                 # Head vectors directory
```

## See Also

- [SPFreshInterface_README.md](../SPFreshInterface_README.md) - SPFresh API documentation
- [SPDK Documentation](https://spdk.io/doc/) - Official SPDK documentation
