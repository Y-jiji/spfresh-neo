# SPDK Setup Complete!

## Current Status

✅ **SPDK environment is now configured and ready to use!**

### What's Been Done:

1. ✅ **Hugepages**: 1024 x 2MB pages allocated (2GB total)
2. ✅ **Kernel Modules**: uio_pci_generic and uio loaded
3. ✅ **NVMe Device**: 0000:b4:00.0 (Samsung NVMe) bound to uio_pci_generic
4. ✅ **UIO Device**: /dev/uio8 created for the NVMe device
5. ✅ **Config Files**: SPDK configuration files created

### Device Details:

- **PCI Address**: 0000:b4:00.0
- **Model**: Samsung NVMe SSD Controller S4LV008[Pascal]
- **Driver**: uio_pci_generic (userspace)
- **UIO Device**: /dev/uio8
- **Status**: Ready for SPDK

## Running the Stress Test

You have two options:

### Option 1: Using the Wrapper Script (Recommended)

```bash
sudo ./script/run_spfresh_stress_test.sh
```

This will automatically:
- Set all required environment variables
- Validate the setup
- Run the stress test with optimal settings

### Option 2: Manual Execution

```bash
# Set environment variables
export SPFRESH_SPDK_USE_SSD_IMPL=1
export SPFRESH_SPDK_CONF="./build/stress_test_index/spdk_config.json"
export SPFRESH_SPDK_BDEV="Nvme0n1"
export SPFRESH_SPDK_IO_DEPTH=1024

# Run the stress test
sudo ./build/spfresh_stress_test_uint8 \\
    ./build/vectors_320M_uint8.bin \\
    ./output.log \\
    ./stats.log \\
    10 \\
    128 \\
    1000 \\
    ./build/stress_test_index \\
    ./build/stress_test_index/spdk_mapping.txt \\
    ./build/stress_test_index/ssd_info.txt \\
    128 \\
    16
```

### Customizing Parameters

```bash
# Run with custom settings
sudo ./script/run_spfresh_stress_test.sh \\
    --data ./build/vectors_320M_uint8.bin \\
    --k 20 \\
    --dimension 128 \\
    --threads 32 \\
    --batch 256
```

## Checking Status

To verify the SPDK setup at any time:

```bash
./script/check_spdk_status.sh
```

## Configuration Files

The following files have been created in `./build/stress_test_index/`:

1. **spdk_config.json** - SPDK subsystem configuration with NVMe controller
2. **spdk_mapping.txt** - Maps logical name "Nvme0" to PCI address
3. **ssd_info.txt** - SSD capacity and block size information

## Important Notes

### Persistence

- The device binding to uio_pci_generic is **NOT persistent** across reboots
- After reboot, you'll need to run the setup script again:
  ```bash
  sudo ./script/setup_spdk.sh 0000:b4:00.0 ./build/stress_test_index
  ```

### Device Access

- The NVMe device (0000:b4:00.0) is now bound to userspace
- It will NOT appear in `/dev/nvme*` or be accessible via normal file operations
- ONLY SPDK applications can access it
- To restore normal access, see "Restoring Kernel Driver" below

### Root Access

- SPDK requires root/sudo privileges for direct hardware access
- Always run the stress test with `sudo`

## Troubleshooting

### Problem: "spdk_bdev_open_ext failed"

**Solution**: Verify the device is still bound:
```bash
ls -la /sys/bus/pci/devices/0000:b4:00.0/driver
# Should point to: ../../../../bus/pci/drivers/uio_pci_generic
```

### Problem: "Failed to allocate hugepages"

**Solution**: Free memory and retry:
```bash
sudo sync
sudo echo 3 > /proc/sys/vm/drop_caches
sudo ./script/setup_spdk.sh
```

### Problem: Low performance

**Solution**: Tune SPDK parameters:
```bash
# Increase IO depth
sudo SPFRESH_SPDK_IO_DEPTH=2048 ./script/run_spfresh_stress_test.sh

# Use more threads
sudo ./script/run_spfresh_stress_test.sh --threads $(nproc)
```

## Restoring Kernel Driver

If you need to restore the NVMe device to normal kernel access:

```bash
# Clear driver override
sudo echo "" > /sys/bus/pci/devices/0000:b4:00.0/driver_override

# Unbind from uio_pci_generic
sudo echo "0000:b4:00.0" > /sys/bus/pci/drivers/uio_pci_generic/unbind

# Rescan to rebind to nvme driver
sudo echo "1" > /sys/bus/pci/rescan
```

Or simply reboot the system.

## Performance Tips

1. **Hugepages**: More is better (up to 4GB)
   ```bash
   sudo HUGEPAGES_NUM=2048 ./script/setup_spdk.sh
   ```

2. **IO Depth**: Higher = more throughput
   ```bash
   export SPFRESH_SPDK_IO_DEPTH=2048
   ```

3. **Worker Threads**: Match CPU cores
   ```bash
   sudo ./script/run_spfresh_stress_test.sh --threads $(nproc)
   ```

4. **CPU Isolation**: For maximum performance, isolate CPUs
   ```bash
   # Add to kernel boot parameters: isolcpus=1-15
   # Then pin SPDK to isolated cores
   ```

## Next Steps

1. **Run the stress test** to verify everything works
2. **Monitor performance** using the stats log
3. **Tune parameters** based on your workload
4. **Check documentation** in [script/README.md](README.md) for more details

## Quick Commands Reference

```bash
# Check status
./script/check_spdk_status.sh

# Run stress test
sudo ./script/run_spfresh_stress_test.sh

# View live statistics
tail -f stats.log

# Restore normal NVMe access
sudo echo "" > /sys/bus/pci/devices/0000:b4:00.0/driver_override
sudo echo "0000:b4:00.0" > /sys/bus/pci/drivers/uio_pci_generic/unbind
sudo echo "1" > /sys/bus/pci/rescan

# Re-setup after reboot
sudo ./script/setup_spdk.sh 0000:b4:00.0 ./build/stress_test_index
```

## Support

For issues or questions:
- Check [script/README.md](README.md) for detailed documentation
- Review [SPFreshInterface_README.md](../SPFreshInterface_README.md) for API details
- Check SPDK logs and dmesg for error messages

---

**Your SPDK environment is ready! You can now run the stress test on 320M vectors with direct NVMe access for maximum performance.**
