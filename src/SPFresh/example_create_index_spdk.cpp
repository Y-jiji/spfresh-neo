// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * @file example_create_index_spdk.cpp
 * @brief Example demonstrating how to create a new SPFresh index with SPDK backend
 *
 * SPDK (Storage Performance Development Kit) provides direct NVMe access for
 * maximum I/O performance, bypassing the kernel.
 *
 * REQUIREMENTS:
 * - NVMe SSD hardware
 * - SPDK properly installed and configured
 * - Devices bound to SPDK userspace drivers (use setup.sh from SPDK)
 * - Root/sudo access for device access
 * - SPDK device mapping file with NVMe device information
 *
 * This example shows:
 * 1. Creating a new empty SPFresh index with SPDK backend
 * 2. Configuring SPDK-specific parameters
 * 3. Inserting vectors using SPDK
 * 4. Performing searches with SPDK-backed storage
 */

#include "SPFresh/SPFreshInterface.h"
#include "Helper/Logging.h"
#include <iostream>
#include <vector>
#include <random>
#include <fstream>

using namespace SPTAG;
using namespace SPTAG::SSDServing::SPFresh;

void createSPDKMappingFile(const std::string& filename) {
    // Create a sample SPDK mapping file
    // In production, this should contain actual NVMe device information
    std::ofstream file(filename);
    if (file.is_open()) {
        // Format: device_name,namespace_id,transport_address
        // Example entries (adjust based on your hardware):
        file << "# SPDK Device Mapping File\n";
        file << "# Format: device_name,namespace_id,transport_address\n";
        file << "nvme0,1,0000:01:00.0\n";
        file << "nvme1,1,0000:02:00.0\n";
        file.close();
        std::cout << "Created SPDK mapping file: " << filename << std::endl;
        std::cout << "NOTE: Please update this file with your actual NVMe device information!" << std::endl;
    }
}

void createSSDInfoFile(const std::string& filename) {
    // Create a sample SSD info file
    std::ofstream file(filename);
    if (file.is_open()) {
        // Format: total_size_gb,block_size_bytes,num_blocks
        file << "# SSD Info File\n";
        file << "# Format: total_size_gb,block_size_bytes,num_blocks\n";
        file << "1024,4096,268435456\n";  // 1TB SSD, 4KB blocks
        file.close();
        std::cout << "Created SSD info file: " << filename << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <index_output_path>" << std::endl;
        std::cerr << "\nREQUIREMENTS:" << std::endl;
        std::cerr << "  - NVMe SSD hardware" << std::endl;
        std::cerr << "  - SPDK installed and configured" << std::endl;
        std::cerr << "  - Devices bound to SPDK drivers" << std::endl;
        std::cerr << "  - Root/sudo access" << std::endl;
        return 1;
    }

    const char* indexPath = argv[1];

    std::cout << "\n=== SPFresh Index Creation with SPDK Backend ===" << std::endl;
    std::cout << "\nWARNING: SPDK requires:" << std::endl;
    std::cout << "  1. NVMe hardware" << std::endl;
    std::cout << "  2. SPDK installation" << std::endl;
    std::cout << "  3. Proper device configuration" << std::endl;
    std::cout << "  4. This example will create template config files" << std::endl;
    std::cout << "     that you must update with your actual hardware info\n" << std::endl;

    // ============================================================================
    // Step 1: Configure SPDK index
    // ============================================================================
    std::cout << "=== Step 1: Configuring SPDK Index ===" << std::endl;

    IndexConfig config;
    config.dimension = 128;
    config.distanceMethod = DistCalcMethod::L2;
    config.indexPath = indexPath;
    config.headVectorCount = 1000;

    // SPDK configuration (SPDK is always used)
    config.spdkMappingPath = std::string(indexPath) + "/spdk_mapping.txt";
    config.ssdInfoFile = std::string(indexPath) + "/ssd_info.txt";
    config.spdkBatchSize = 128;  // Batch size for SPDK operations

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Dimension: " << config.dimension << std::endl;
    std::cout << "  Distance Method: L2" << std::endl;
    std::cout << "  Index Path: " << config.indexPath << std::endl;
    std::cout << "  Storage Backend: SPDK (direct NVMe access)" << std::endl;
    std::cout << "  SPDK Mapping File: " << config.spdkMappingPath << std::endl;
    std::cout << "  SSD Info File: " << config.ssdInfoFile << std::endl;
    std::cout << "  SPDK Batch Size: " << config.spdkBatchSize << std::endl;

    // ============================================================================
    // Step 2: Create SPDK configuration files
    // ============================================================================
    std::cout << "\n=== Step 2: Creating SPDK Configuration Files ===" << std::endl;

    // Create directory if it doesn't exist
    system(("mkdir -p " + std::string(indexPath)).c_str());

    createSPDKMappingFile(config.spdkMappingPath);
    createSSDInfoFile(config.ssdInfoFile);

    std::cout << "\nIMPORTANT: Before proceeding, you must:" << std::endl;
    std::cout << "  1. Edit " << config.spdkMappingPath << " with your NVMe device info" << std::endl;
    std::cout << "  2. Edit " << config.ssdInfoFile << " with your SSD specifications" << std::endl;
    std::cout << "  3. Ensure SPDK is properly initialized" << std::endl;
    std::cout << "  4. Run this program with sudo/root privileges" << std::endl;

    std::cout << "\nPress Enter to continue (or Ctrl+C to abort and configure files)...";
    std::cin.get();

    // ============================================================================
    // Step 3: Create the SPDK-backed index
    // ============================================================================
    std::cout << "\n=== Step 3: Creating SPDK-Backed Index ===" << std::endl;

    auto interface = SPFreshInterface<float>::createEmptyIndex(config);
    if (!interface) {
        std::cerr << "\nFailed to create SPDK index!" << std::endl;
        std::cerr << "Common issues:" << std::endl;
        std::cerr << "  - SPDK not properly initialized" << std::endl;
        std::cerr << "  - NVMe devices not bound to SPDK drivers" << std::endl;
        std::cerr << "  - Insufficient permissions (need root/sudo)" << std::endl;
        std::cerr << "  - Invalid device mapping file" << std::endl;
        return 1;
    }

    std::cout << "Successfully created SPDK-backed index!" << std::endl;
    std::cout << "Initial vector count: " << interface->getVectorCount() << std::endl;
    std::cout << "Dimension: " << interface->getDimension() << std::endl;

    // Initialize
    if (!interface->initialize()) {
        std::cerr << "Failed to initialize SPDK interface" << std::endl;
        return 1;
    }

    // ============================================================================
    // Step 4: Insert vectors (stored on NVMe via SPDK)
    // ============================================================================
    std::cout << "\n=== Step 4: Inserting Vectors via SPDK ===" << std::endl;

    int dimension = interface->getDimension();
    int numVectors = 10000;

    std::cout << "Inserting " << numVectors << " vectors using SPDK..." << std::endl;
    std::cout << "Data will be written directly to NVMe, bypassing kernel I/O" << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);

    std::vector<float> vectors(numVectors * dimension);
    for (int i = 0; i < numVectors * dimension; ++i) {
        vectors[i] = dis(gen);
    }

    // Insert in batches
    int batchSize = 1000;
    std::vector<int> allVectorIDs;

    for (int batch = 0; batch < numVectors / batchSize; ++batch) {
        auto batchIDs = interface->batchInsertVectors(
            vectors.data() + (batch * batchSize * dimension),
            batchSize
        );
        allVectorIDs.insert(allVectorIDs.end(), batchIDs.begin(), batchIDs.end());
        std::cout << "  Batch " << (batch + 1) << "/" << (numVectors / batchSize)
                  << " completed" << std::endl;
    }

    std::cout << "Successfully inserted " << allVectorIDs.size() << " vectors" << std::endl;
    std::cout << "All data stored on NVMe via SPDK" << std::endl;

    // ============================================================================
    // Step 5: Perform SPDK-backed searches
    // ============================================================================
    std::cout << "\n=== Step 5: Searching with SPDK Backend ===" << std::endl;

    std::vector<float> query(dimension);
    for (int i = 0; i < dimension; ++i) {
        query[i] = dis(gen);
    }

    int k = 10;
    auto results = interface->knnSearch(query.data(), k);

    std::cout << "Search completed (NVMe access via SPDK)" << std::endl;
    std::cout << "Found " << results.size() << " neighbors:" << std::endl;
    for (size_t i = 0; i < std::min(size_t(5), results.size()); ++i) {
        std::cout << "  " << (i + 1) << ". Vector ID: " << results[i].vectorID
                  << ", Distance: " << results[i].distance << std::endl;
    }

    // ============================================================================
    // Step 6: Performance comparison info
    // ============================================================================
    std::cout << "\n=== SPDK vs RocksDB Performance ===" << std::endl;
    std::cout << "SPDK advantages:" << std::endl;
    std::cout << "  - Direct NVMe access (no kernel overhead)" << std::endl;
    std::cout << "  - Lower latency for I/O operations" << std::endl;
    std::cout << "  - Higher throughput for random access" << std::endl;
    std::cout << "  - Reduced CPU usage for I/O" << std::endl;
    std::cout << "\nRocksDB advantages:" << std::endl;
    std::cout << "  - Easier setup (no special hardware config)" << std::endl;
    std::cout << "  - Works with any storage device" << std::endl;
    std::cout << "  - Better for development and testing" << std::endl;

    // ============================================================================
    // Step 7: Save the index
    // ============================================================================
    std::cout << "\n=== Step 7: Saving SPDK Index ===" << std::endl;

    if (interface->saveIndex(indexPath)) {
        std::cout << "Index saved successfully!" << std::endl;
        std::cout << "SPDK configuration preserved" << std::endl;
    } else {
        std::cerr << "Failed to save index" << std::endl;
        return 1;
    }

    // ============================================================================
    // Summary
    // ============================================================================
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Successfully created SPDK-backed SPFresh index:" << std::endl;
    std::cout << "  - " << numVectors << " vectors inserted" << std::endl;
    std::cout << "  - " << dimension << " dimensions" << std::endl;
    std::cout << "  - Direct NVMe access via SPDK" << std::endl;
    std::cout << "  - Optimized for high-performance workloads" << std::endl;
    std::cout << "\nThe index is ready for production use with SPDK!" << std::endl;

    return 0;
}
