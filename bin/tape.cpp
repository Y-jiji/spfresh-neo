// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/TracePlayer.h"

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>

// Simple deterministic hash function for demo
std::uint64_t SimpleHash(std::uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

// Helper to create a test vector file
void CreateTestFile(const std::string& path, std::uint32_t numVectors, std::uint32_t dim) {
    std::ofstream ofs(path, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(&numVectors), sizeof(numVectors));
    ofs.write(reinterpret_cast<const char*>(&dim), sizeof(dim));

    std::vector<float> vec(dim);
    for (std::uint32_t i = 0; i < numVectors; ++i) {
        for (std::uint32_t j = 0; j < dim; ++j) {
            vec[j] = static_cast<float>(i * dim + j);
        }
        ofs.write(reinterpret_cast<const char*>(vec.data()), dim * sizeof(float));
    }
}

int main() {
    const std::string testFile = "/tmp/test_vectors.bin";
    const std::uint32_t numVectors = 100;
    const std::uint32_t dim = 128;
    const std::size_t windowSize = 10;

    // Create test file
    CreateTestFile(testFile, numVectors, dim);
    std::cout << "Created test file with " << numVectors << " vectors of dim " << dim << "\n";

    // Create TracePlayer
    SPTAG::Helper::TracePlayer<float> player(testFile, windowSize, SimpleHash);

    std::cout << "TracePlayer initialized:\n"
              << "  Total vectors: " << player.GetTotalVectors() << "\n"
              << "  Dimension: " << player.GetDimension() << "\n"
              << "  Window size: " << player.GetWindowSize() << "\n\n";

    // Consume vectors
    std::size_t readCount = 0, writeCount = 0;
    while (auto guard = player.Next()) {
        // Dereference optional to get guard, then dereference guard to get record
        const auto& record = **guard;

        if (record.GetOperationKind() == SPTAG::Helper::OperationKind::Read) {
            ++readCount;
        } else {
            ++writeCount;
        }

        // Print first few for verification
        if (record.SequenceNumber() < 5) {
            std::cout << "Record " << record.SequenceNumber()
                      << " [" << (record.GetOperationKind() == SPTAG::Helper::OperationKind::Read ? "R" : "W") << "]"
                      << " first elem: " << record.Data()[0]
                      << " last elem: " << record.Data()[record.Dimension() - 1]
                      << "\n";
        }
    }

    std::cout << "\nProcessed " << (readCount + writeCount) << " vectors\n"
              << "  Reads: " << readCount << "\n"
              << "  Writes: " << writeCount << "\n";

    // Cleanup
    std::remove(testFile.c_str());

    return 0;
}
