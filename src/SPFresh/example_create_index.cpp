// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * @file example_create_index.cpp
 * @brief Example demonstrating how to create a new empty SPFresh index with SPDK
 *
 * This example shows:
 * 1. Creating a new empty SPFresh index from scratch with SPDK backend
 * 2. Inserting vectors into the new index
 * 3. Performing searches on the new index
 * 4. Saving the index to disk
 *
 * REQUIREMENTS:
 * - NVMe SSD hardware
 * - SPDK properly installed and configured
 * - Root/sudo access for device access
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
    std::ofstream file(filename);
    if (file.is_open()) {
        file << "# SPDK Device Mapping File\n";
        file << "# Format: device_name,namespace_id,transport_address\n";
        file << "nvme0,1,0000:01:00.0\n";
        file << "nvme1,1,0000:02:00.0\n";
        file.close();
        std::cout << "Created SPDK mapping file: " << filename << std::endl;
        std::cout << "NOTE: Update this file with your actual NVMe device information!" << std::endl;
    }
}

void createSSDInfoFile(const std::string& filename) {
    std::ofstream file(filename);
    if (file.is_open()) {
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
        std::cerr << "  - Root/sudo access" << std::endl;
        return 1;
    }

    const char* indexPath = argv[1];

    std::cout << "\n=== SPFresh Index Creation with SPDK Backend ===" << std::endl;
    std::cout << "\nWARNING: This requires SPDK with NVMe hardware!" << std::endl;
    std::cout << "The example will create template config files that you must update.\n" << std::endl;

    // ============================================================================
    // Step 1: Configure and create a new empty index
    // ============================================================================
    std::cout << "\n=== Step 1: Creating Empty Index ===" << std::endl;

    IndexConfig config;
    config.dimension = 128;                          // 128-dimensional vectors
    config.distanceMethod = DistCalcMethod::L2;      // L2 distance (can also use Cosine)
    config.indexPath = indexPath;
    config.headVectorCount = 1000;                   // Number of cluster centers
    config.spdkMappingPath = std::string(indexPath) + "/spdk_mapping.txt";
    config.ssdInfoFile = std::string(indexPath) + "/ssd_info.txt";
    config.spdkBatchSize = 128;

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Dimension: " << config.dimension << std::endl;
    std::cout << "  Distance Method: " << (config.distanceMethod == DistCalcMethod::L2 ? "L2" : "Cosine") << std::endl;
    std::cout << "  Index Path: " << config.indexPath << std::endl;
    std::cout << "  Head Vector Count: " << config.headVectorCount << std::endl;
    std::cout << "  Storage Backend: SPDK (direct NVMe access)" << std::endl;
    std::cout << "  SPDK Mapping: " << config.spdkMappingPath << std::endl;
    std::cout << "  SSD Info: " << config.ssdInfoFile << std::endl;
    std::cout << "  SPDK Batch Size: " << config.spdkBatchSize << std::endl;

    // Create directory and SPDK config files
    system(("mkdir -p " + std::string(indexPath)).c_str());
    createSPDKMappingFile(config.spdkMappingPath);
    createSSDInfoFile(config.ssdInfoFile);

    std::cout << "\nIMPORTANT: Edit the config files above with your actual NVMe device info" << std::endl;
    std::cout << "Press Enter to continue...";
    std::cin.get();

    // Create the empty index
    auto interface = SPFreshInterface<float>::createEmptyIndex(config);
    if (!interface) {
        std::cerr << "Failed to create empty index" << std::endl;
        return 1;
    }

    std::cout << "Successfully created empty index!" << std::endl;
    std::cout << "Initial vector count: " << interface->getVectorCount() << std::endl;
    std::cout << "Dimension: " << interface->getDimension() << std::endl;

    // Initialize for multi-threaded operations
    if (!interface->initialize()) {
        std::cerr << "Failed to initialize interface" << std::endl;
        return 1;
    }

    // ============================================================================
    // Step 2: Insert vectors into the new index
    // ============================================================================
    std::cout << "\n=== Step 2: Inserting Vectors ===" << std::endl;

    int dimension = interface->getDimension();
    int numVectorsToInsert = 10000;

    std::cout << "Inserting " << numVectorsToInsert << " vectors..." << std::endl;

    // Create random vectors
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);

    std::vector<float> vectors(numVectorsToInsert * dimension);
    for (int i = 0; i < numVectorsToInsert * dimension; ++i) {
        vectors[i] = dis(gen);
    }

    // Insert vectors in batches
    int batchSize = 1000;
    std::vector<int> allVectorIDs;
    allVectorIDs.reserve(numVectorsToInsert);

    for (int batch = 0; batch < numVectorsToInsert / batchSize; ++batch) {
        std::cout << "Inserting batch " << (batch + 1) << "/" << (numVectorsToInsert / batchSize) << "..." << std::endl;

        auto batchIDs = interface->batchInsertVectors(
            vectors.data() + (batch * batchSize * dimension),
            batchSize
        );

        allVectorIDs.insert(allVectorIDs.end(), batchIDs.begin(), batchIDs.end());
    }

    std::cout << "Successfully inserted " << allVectorIDs.size() << " vectors" << std::endl;
    std::cout << "New vector count: " << interface->getVectorCount() << std::endl;

    // Show first few IDs
    std::cout << "First 10 vector IDs: ";
    for (int i = 0; i < std::min(10, (int)allVectorIDs.size()); ++i) {
        std::cout << allVectorIDs[i] << " ";
    }
    std::cout << std::endl;

    // ============================================================================
    // Step 3: Perform searches on the new index
    // ============================================================================
    std::cout << "\n=== Step 3: Searching the Index ===" << std::endl;

    // Create a query vector
    std::vector<float> queryVector(dimension);
    for (int i = 0; i < dimension; ++i) {
        queryVector[i] = dis(gen);
    }

    // Search for top 10 nearest neighbors
    int k = 10;
    std::cout << "Searching for top " << k << " nearest neighbors..." << std::endl;

    auto results = interface->knnSearch(queryVector.data(), k);

    std::cout << "Found " << results.size() << " neighbors:" << std::endl;
    for (size_t i = 0; i < results.size(); ++i) {
        std::cout << "  " << (i + 1) << ". Vector ID: " << results[i].vectorID
                  << ", Distance: " << results[i].distance << std::endl;
    }

    // ============================================================================
    // Step 4: Perform batch searches
    // ============================================================================
    std::cout << "\n=== Step 4: Batch Searches ===" << std::endl;

    int numQueries = 100;
    std::vector<float> queries(numQueries * dimension);
    for (int i = 0; i < numQueries * dimension; ++i) {
        queries[i] = dis(gen);
    }

    std::cout << "Performing " << numQueries << " searches..." << std::endl;
    auto batchResults = interface->batchKnnSearch(queries.data(), numQueries, k);

    std::cout << "Completed " << batchResults.size() << " searches" << std::endl;
    std::cout << "Sample results from first 3 queries:" << std::endl;
    for (int q = 0; q < std::min(3, numQueries); ++q) {
        std::cout << "  Query " << (q + 1) << ": Found " << batchResults[q].size() << " neighbors" << std::endl;
        for (size_t i = 0; i < std::min(size_t(3), batchResults[q].size()); ++i) {
            std::cout << "    " << (i + 1) << ". VID: " << batchResults[q][i].vectorID
                      << ", Dist: " << batchResults[q][i].distance << std::endl;
        }
    }

    // ============================================================================
    // Step 5: Save the index to disk
    // ============================================================================
    std::cout << "\n=== Step 5: Saving Index ===" << std::endl;

    std::cout << "Saving index to: " << indexPath << std::endl;
    if (interface->saveIndex(indexPath)) {
        std::cout << "Index saved successfully!" << std::endl;
    } else {
        std::cerr << "Failed to save index" << std::endl;
        return 1;
    }

    // ============================================================================
    // Step 6: Verify by loading the index back
    // ============================================================================
    std::cout << "\n=== Step 6: Verifying Saved Index ===" << std::endl;

    std::cout << "Loading index from: " << indexPath << std::endl;
    auto loadedInterface = SPFreshInterface<float>::loadIndex(indexPath);

    if (!loadedInterface) {
        std::cerr << "Failed to load saved index" << std::endl;
        return 1;
    }

    std::cout << "Successfully loaded index!" << std::endl;
    std::cout << "Loaded vector count: " << loadedInterface->getVectorCount() << std::endl;
    std::cout << "Loaded dimension: " << loadedInterface->getDimension() << std::endl;

    // Verify search works on loaded index
    if (!loadedInterface->initialize()) {
        std::cerr << "Failed to initialize loaded interface" << std::endl;
        return 1;
    }

    auto verifyResults = loadedInterface->knnSearch(queryVector.data(), k);
    std::cout << "Verification search found " << verifyResults.size() << " neighbors" << std::endl;

    // ============================================================================
    // Summary
    // ============================================================================
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Created new SPFresh index with:" << std::endl;
    std::cout << "  - " << numVectorsToInsert << " vectors inserted" << std::endl;
    std::cout << "  - " << dimension << " dimensions" << std::endl;
    std::cout << "  - Saved to: " << indexPath << std::endl;
    std::cout << "  - Successfully verified by reloading" << std::endl;
    std::cout << "\nThe index is ready for use!" << std::endl;

    return 0;
}
