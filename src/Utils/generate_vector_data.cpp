#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <chrono>
#include <cstdint>

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " <output_file> <num_vectors> <dimension>" << std::endl;
        std::cout << "Example: " << argv[0] << " vectors.bin 320000000 128" << std::endl;
        return 1;
    }

    std::string outputFile = argv[1];
    uint64_t numVectors = std::stoull(argv[2]);
    int dimension = std::stoi(argv[3]);

    std::cout << "Generating " << numVectors << " vectors with " << dimension << " dimensions each" << std::endl;
    std::cout << "Output file: " << outputFile << std::endl;
    
    // Calculate total size
    uint64_t totalSize = static_cast<uint64_t>(numVectors) * dimension;
    double sizeGB = (totalSize) / (1024.0 * 1024.0 * 1024.0);
    std::cout << "Total data size: " << sizeGB << " GB" << std::endl;

    // Create random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint8_t> dis(0, 255);

    // Open output file
    std::ofstream outFile(outputFile, std::ios::binary);
    if (!outFile) {
        std::cerr << "Error: Cannot open output file " << outputFile << std::endl;
        return 1;
    }

    // Buffer for writing chunks
    const size_t chunkSize = 1000000; // 1M vectors per chunk
    std::vector<uint8_t> buffer(chunkSize * dimension);

    auto startTime = std::chrono::steady_clock::now();

    // Generate vectors in chunks
    for (uint64_t chunkStart = 0; chunkStart < numVectors; chunkStart += chunkSize) {
        uint64_t chunkEnd = std::min(chunkStart + chunkSize, numVectors);
        uint64_t vectorsInChunk = chunkEnd - chunkStart;

        // Fill buffer with random uint8 values
        for (uint64_t i = 0; i < vectorsInChunk * dimension; ++i) {
            buffer[i] = dis(gen);
        }

        // Write chunk to file
        outFile.write(reinterpret_cast<const char*>(buffer.data()), vectorsInChunk * dimension);

        if (!outFile) {
            std::cerr << "Error: Failed to write to file at vector " << chunkStart << std::endl;
            return 1;
        }

        // Progress reporting
        if (chunkStart % (chunkSize * 10) == 0) {
            auto currentTime = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(currentTime - startTime).count();
            double progress = (static_cast<double>(chunkEnd) / numVectors) * 100.0;
            std::cout << "Progress: " << progress << "% (" << chunkEnd << "/" << numVectors 
                      << " vectors) - Elapsed: " << elapsed << "s" << std::endl;
        }
    }

    outFile.close();

    auto endTime = std::chrono::steady_clock::now();
    auto totalTime = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();

    std::cout << "Generation completed!" << std::endl;
    std::cout << "Total time: " << totalTime << " seconds" << std::endl;
    std::cout << "Throughput: " << (numVectors / std::max(1.0, static_cast<double>(totalTime))) << " vectors/second" << std::endl;

    return 0;
}