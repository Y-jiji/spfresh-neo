// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * @file example_usage.cpp
 * @brief Example demonstrating how to use the SPFreshInterface
 *
 * This example shows:
 * 1. Loading an existing SPFresh index
 * 2. Performing KNN searches (read operations)
 * 3. Inserting new vectors (write operations)
 * 4. Batch operations
 */

#include "SPFresh/SPFreshInterface.h"
#include "Core/VectorIndex.h"
#include "Helper/Logging.h"
#include <iostream>
#include <vector>

using namespace SPTAG;
using namespace SPTAG::SSDServing::SPFresh;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <index_path>" << std::endl;
        return 1;
    }

    const char* indexPath = argv[1];

    // Step 1: Load the existing SPFresh index
    std::shared_ptr<VectorIndex> baseIndex;
    ErrorCode loadResult = VectorIndex::LoadIndex(indexPath, baseIndex);

    if (loadResult != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Failed to load index from: %s\n", indexPath);
        return 1;
    }

    LOG(Helper::LogLevel::LL_Info, "Successfully loaded index from: %s\n", indexPath);

    // Cast to SPANN::Index<float> (adjust type based on your index)
    auto spannIndex = std::dynamic_pointer_cast<SPANN::Index<float>>(baseIndex);
    if (!spannIndex) {
        LOG(Helper::LogLevel::LL_Error, "Index is not a SPANN index\n");
        return 1;
    }

    // Step 2: Create the SPFreshInterface
    SPFreshInterface<float> interface(spannIndex);

    // Initialize (required for multi-threaded contexts)
    if (!interface.initialize()) {
        LOG(Helper::LogLevel::LL_Error, "Failed to initialize interface\n");
        return 1;
    }

    int dimension = interface.getDimension();
    int vectorCount = interface.getVectorCount();
    LOG(Helper::LogLevel::LL_Info, "Index info - Dimension: %d, Vector count: %d\n",
        dimension, vectorCount);

    // ============================================================================
    // Example 1: Single KNN Search (Read Operation)
    // ============================================================================
    std::cout << "\n=== Example 1: Single KNN Search ===" << std::endl;

    // Create a random query vector (in practice, this would be your actual query)
    std::vector<float> queryVector(dimension);
    for (int i = 0; i < dimension; ++i) {
        queryVector[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    // Perform KNN search for top 10 neighbors
    int k = 10;
    std::vector<SearchResult> results = interface.knnSearch(queryVector.data(), k);

    std::cout << "Found " << results.size() << " nearest neighbors:" << std::endl;
    for (size_t i = 0; i < results.size(); ++i) {
        std::cout << "  " << i + 1 << ". Vector ID: " << results[i].vectorID
                  << ", Distance: " << results[i].distance << std::endl;
    }

    // ============================================================================
    // Example 2: Batch KNN Search (Multiple Read Operations)
    // ============================================================================
    std::cout << "\n=== Example 2: Batch KNN Search ===" << std::endl;

    // Create multiple query vectors
    int numQueries = 5;
    std::vector<float> batchQueries(numQueries * dimension);
    for (int i = 0; i < numQueries * dimension; ++i) {
        batchQueries[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    // Perform batch search
    auto batchResults = interface.batchKnnSearch(
        batchQueries.data(),
        numQueries,
        k
    );

    std::cout << "Batch search results for " << numQueries << " queries:" << std::endl;
    for (size_t q = 0; q < batchResults.size(); ++q) {
        std::cout << "Query " << q + 1 << ": Found " << batchResults[q].size()
                  << " neighbors" << std::endl;
        for (size_t i = 0; i < std::min(size_t(3), batchResults[q].size()); ++i) {
            std::cout << "    " << i + 1 << ". VID: " << batchResults[q][i].vectorID
                      << ", Dist: " << batchResults[q][i].distance << std::endl;
        }
    }

    // ============================================================================
    // Example 3: Single Vector Insertion (Write Operation)
    // ============================================================================
    std::cout << "\n=== Example 3: Single Vector Insertion ===" << std::endl;

    // Create a new vector to insert
    std::vector<float> newVector(dimension);
    for (int i = 0; i < dimension; ++i) {
        newVector[i] = static_cast<float>(i) / dimension;
    }

    // Insert the vector
    int newVectorID = interface.insertVector(newVector.data());

    if (newVectorID >= 0) {
        std::cout << "Successfully inserted vector with ID: " << newVectorID << std::endl;
        std::cout << "New vector count: " << interface.getVectorCount() << std::endl;
    } else {
        std::cout << "Failed to insert vector" << std::endl;
    }

    // ============================================================================
    // Example 4: Batch Vector Insertion (Multiple Write Operations)
    // ============================================================================
    std::cout << "\n=== Example 4: Batch Vector Insertion ===" << std::endl;

    // Create multiple vectors to insert
    int numNewVectors = 100;
    std::vector<float> newVectors(numNewVectors * dimension);
    for (int i = 0; i < numNewVectors * dimension; ++i) {
        newVectors[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    // Insert the vectors
    std::vector<int> newVectorIDs = interface.batchInsertVectors(
        newVectors.data(),
        numNewVectors
    );

    std::cout << "Inserted " << newVectorIDs.size() << " vectors" << std::endl;
    std::cout << "First few IDs: ";
    for (size_t i = 0; i < std::min(size_t(5), newVectorIDs.size()); ++i) {
        std::cout << newVectorIDs[i] << " ";
    }
    std::cout << std::endl;
    std::cout << "New vector count: " << interface.getVectorCount() << std::endl;

    // ============================================================================
    // Example 5: Search for newly inserted vector
    // ============================================================================
    std::cout << "\n=== Example 5: Search for Newly Inserted Vector ===" << std::endl;

    if (!newVectorIDs.empty() && newVectorIDs[0] >= 0) {
        // Search using the first newly inserted vector as query
        const float* firstNewVector = newVectors.data();
        auto searchResults = interface.knnSearch(firstNewVector, k);

        std::cout << "Search results using first inserted vector as query:" << std::endl;
        for (size_t i = 0; i < std::min(size_t(5), searchResults.size()); ++i) {
            std::cout << "  " << i + 1 << ". Vector ID: " << searchResults[i].vectorID
                      << ", Distance: " << searchResults[i].distance;
            if (searchResults[i].vectorID == newVectorIDs[0]) {
                std::cout << " <- This is the inserted vector!";
            }
            std::cout << std::endl;
        }
    }

    // ============================================================================
    // Example 6: Delete a vector (optional - commented out by default)
    // ============================================================================
    std::cout << "\n=== Example 6: Delete Vector (demonstration) ===" << std::endl;

    // Uncomment to actually delete a vector:
    /*
    if (!newVectorIDs.empty() && newVectorIDs[0] >= 0) {
        bool deleted = interface.deleteVector(newVectorIDs[0]);
        if (deleted) {
            std::cout << "Successfully deleted vector ID: " << newVectorIDs[0] << std::endl;
            std::cout << "New vector count: " << interface.getVectorCount() << std::endl;
        } else {
            std::cout << "Failed to delete vector" << std::endl;
        }
    }
    */
    std::cout << "Delete functionality available via interface.deleteVector(vectorID)" << std::endl;

    std::cout << "\n=== Example completed successfully ===" << std::endl;

    return 0;
}
