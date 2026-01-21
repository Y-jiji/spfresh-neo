#include "SPFresh/SPFreshInterface.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <cstring>

// Pure function to determine if a sequence number is for insert (true) or search (false)
// This is a dummy implementation that can be replaced
using namespace SPTAG::SSDServing::SPFresh;

inline bool isInsertOperation(uint64_t sequenceNumber) {
  // TODO: Replace this with your actual implementation
  // Current implementation: 70% inserts, 30% searches
  return (sequenceNumber % 10) < 7;
}

struct StressTestConfig {
    std::string dataFilePath;
    std::string outputLogPath;
    std::string statsLogPath;
    int k;  // k-nearest neighbors
    int dimension;
    int headVectorCount;
    std::string indexPath;
    std::string spdkMappingPath;
    std::string ssdInfoFile;
    int spdkBatchSize;
    int numThreads;
};

struct Statistics {
    std::atomic<uint64_t> numInserts{0};
    std::atomic<uint64_t> numSearches{0};
    std::atomic<uint64_t> numErrors{0};
    std::mutex logMutex;
};

struct VectorData {
    uint64_t sequenceNumber;
    std::vector<float> vector;
};

// Read all vectors from binary file
std::vector<VectorData> readVectorData(const std::string& filePath, int dimension) {
    std::ifstream inFile(filePath, std::ios::binary);
    if (!inFile) {
        throw std::runtime_error("Failed to open data file: " + filePath);
    }

    std::vector<VectorData> data;
    uint64_t seqNum = 0;

    std::vector<float> buffer(dimension);
    while (inFile.read(reinterpret_cast<char*>(buffer.data()), dimension * sizeof(float))) {
        VectorData vd;
        vd.sequenceNumber = seqNum++;
        vd.vector = buffer;
        data.push_back(vd);
    }

    inFile.close();
    std::cout << "Read " << data.size() << " vectors from " << filePath << std::endl;
    return data;
}

// Worker thread function
void workerThread(
    int threadId,
    SPFreshInterface<float>* index,
    const std::vector<VectorData>& data,
    size_t startIdx,
    size_t endIdx,
    int k,
    const std::string& outputLogPath,
    Statistics& stats
) {
    // Initialize thread-local index workspace
    index->initialize();

    // Open thread-specific log file with large buffer to minimize I/O overhead
    std::ofstream logFile(outputLogPath + ".thread" + std::to_string(threadId), std::ios::app);
    if (!logFile) {
        std::cerr << "Thread " << threadId << ": Failed to open log file" << std::endl;
        return;
    }

    // Set large buffer size (1MB) for efficient I/O
    const size_t bufferSize = 1024 * 1024;
    std::vector<char> buffer(bufferSize);
    logFile.rdbuf()->pubsetbuf(buffer.data(), bufferSize);

    // Use string stream for in-memory buffering to reduce file I/O
    std::ostringstream logBuffer;
    const size_t flushInterval = 1000; // Flush every 1000 operations
    size_t opsCount = 0;

    for (size_t i = startIdx; i < endIdx; ++i) {
        const VectorData& vd = data[i];
        bool isInsert = isInsertOperation(vd.sequenceNumber);

        if (isInsert) {
            // Insert operation
            int vectorId = index->insertVector(vd.vector.data(), "seq:" + std::to_string(vd.sequenceNumber));

            if (vectorId >= 0) {
                logBuffer << "INSERT," << vd.sequenceNumber << "," << vectorId << "\n";
                stats.numInserts.fetch_add(1, std::memory_order_relaxed);
            } else {
                logBuffer << "INSERT_ERROR," << vd.sequenceNumber << ",-1\n";
                stats.numErrors.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            // Search operation
            auto results = index->knnSearch(vd.vector.data(), k, false);

            logBuffer << "SEARCH," << vd.sequenceNumber << "," << k << ",";
            for (size_t j = 0; j < results.size(); ++j) {
                if (j > 0) logBuffer << ";";
                logBuffer << results[j].vectorID << ":" << results[j].distance;
            }
            logBuffer << "\n";
            stats.numSearches.fetch_add(1, std::memory_order_relaxed);
        }

        // Periodically flush buffer to file to avoid excessive memory usage
        ++opsCount;
        if (opsCount >= flushInterval) {
            logFile << logBuffer.str();
            logBuffer.str("");
            logBuffer.clear();
            opsCount = 0;
        }
    }

    // Flush any remaining buffered data
    if (opsCount > 0) {
        logFile << logBuffer.str();
    }

    logFile.close();
}

// Statistics logging thread
void statsLoggerThread(
    Statistics& stats,
    const std::string& statsLogPath,
    std::atomic<bool>& stopFlag
) {
    std::ofstream statsFile(statsLogPath, std::ios::app);
    if (!statsFile) {
        std::cerr << "Failed to open statistics log file: " << statsLogPath << std::endl;
        return;
    }

    statsFile << "Timestamp,Elapsed(s),TotalInserts,TotalSearches,InsertsPerSec,SearchesPerSec,Errors\n";

    auto startTime = std::chrono::steady_clock::now();
    uint64_t lastInserts = 0;
    uint64_t lastSearches = 0;

    while (!stopFlag) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto currentTime = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(currentTime - startTime).count();

        uint64_t currentInserts = stats.numInserts.load();
        uint64_t currentSearches = stats.numSearches.load();
        uint64_t currentErrors = stats.numErrors.load();

        uint64_t insertsPerSec = currentInserts - lastInserts;
        uint64_t searchesPerSec = currentSearches - lastSearches;

        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);

        statsFile << std::put_time(std::localtime(&now_c), "%Y-%m-%d %H:%M:%S") << ","
                  << elapsed << ","
                  << currentInserts << ","
                  << currentSearches << ","
                  << insertsPerSec << ","
                  << searchesPerSec << ","
                  << currentErrors << "\n";
        statsFile.flush();

        std::cout << "[" << elapsed << "s] Inserts: " << currentInserts
                  << " (" << insertsPerSec << "/s), Searches: " << currentSearches
                  << " (" << searchesPerSec << "/s), Errors: " << currentErrors << std::endl;

        lastInserts = currentInserts;
        lastSearches = currentSearches;
    }

    statsFile.close();
}

void printUsage(const char* programName) {
    std::cout << "Usage: " << programName << " <data_file> <output_log> <stats_log> <k> <dimension> "
              << "<head_vector_count> <index_path> <spdk_mapping_path> <ssd_info_file> <spdk_batch_size> [num_threads]\n"
              << "\nArguments:\n"
              << "  data_file          : Binary file containing vectors (float32)\n"
              << "  output_log         : Output log file for operations and results\n"
              << "  stats_log          : Statistics log file (per-second metrics)\n"
              << "  k                  : Number of nearest neighbors to search\n"
              << "  dimension          : Vector dimension\n"
              << "  head_vector_count  : Number of head vectors (cluster centers)\n"
              << "  index_path         : Directory for index storage\n"
              << "  spdk_mapping_path  : SPDK device mapping file\n"
              << "  ssd_info_file      : SSD information file\n"
              << "  spdk_batch_size    : SPDK batch size (typically 128)\n"
              << "  num_threads        : Number of worker threads (default: hardware concurrency)\n";
}

int main(int argc, char* argv[]) {
    if (argc < 11 || argc > 12) {
        printUsage(argv[0]);
        return 1;
    }

    // Parse command-line arguments
    StressTestConfig config;
    config.dataFilePath = argv[1];
    config.outputLogPath = argv[2];
    config.statsLogPath = argv[3];
    config.k = std::atoi(argv[4]);
    config.dimension = std::atoi(argv[5]);
    config.headVectorCount = std::atoi(argv[6]);
    config.indexPath = argv[7];
    config.spdkMappingPath = argv[8];
    config.ssdInfoFile = argv[9];
    config.spdkBatchSize = std::atoi(argv[10]);
    config.numThreads = (argc == 12) ? std::atoi(argv[11]) : std::thread::hardware_concurrency();

    if (config.numThreads <= 0) {
        config.numThreads = std::thread::hardware_concurrency();
    }

    std::cout << "=== SPFresh Stress Test Configuration ===" << std::endl;
    std::cout << "Data file: " << config.dataFilePath << std::endl;
    std::cout << "Output log: " << config.outputLogPath << std::endl;
    std::cout << "Statistics log: " << config.statsLogPath << std::endl;
    std::cout << "K (nearest neighbors): " << config.k << std::endl;
    std::cout << "Dimension: " << config.dimension << std::endl;
    std::cout << "Head vector count: " << config.headVectorCount << std::endl;
    std::cout << "Index path: " << config.indexPath << std::endl;
    std::cout << "SPDK mapping: " << config.spdkMappingPath << std::endl;
    std::cout << "SSD info: " << config.ssdInfoFile << std::endl;
    std::cout << "SPDK batch size: " << config.spdkBatchSize << std::endl;
    std::cout << "Worker threads: " << config.numThreads << std::endl;
    std::cout << "==========================================" << std::endl;

    try {
        // Read vector data
        std::cout << "\nReading vector data..." << std::endl;
        auto vectorData = readVectorData(config.dataFilePath, config.dimension);

        if (vectorData.empty()) {
            std::cerr << "No vectors found in data file!" << std::endl;
            return 1;
        }

        // Create SPDK-based index
        std::cout << "\nCreating SPDK-based index..." << std::endl;
        IndexConfig indexConfig;
        indexConfig.dimension = config.dimension;
        indexConfig.distanceMethod = SPTAG::DistCalcMethod::L2;
        indexConfig.indexPath = config.indexPath;
        indexConfig.headVectorCount = config.headVectorCount;
        indexConfig.spdkMappingPath = config.spdkMappingPath;
        indexConfig.ssdInfoFile = config.ssdInfoFile;
        indexConfig.spdkBatchSize = config.spdkBatchSize;

        auto index = SPFreshInterface<float>::createEmptyIndex(indexConfig);
        std::cout << "Index created successfully!" << std::endl;

        // Initialize statistics
        Statistics stats;
        std::atomic<bool> stopStatsLogger{false};

        // Start statistics logging thread
        std::cout << "\nStarting statistics logger..." << std::endl;
        std::thread statsThread(statsLoggerThread, std::ref(stats), config.statsLogPath, std::ref(stopStatsLogger));

        // Calculate work distribution
        size_t totalVectors = vectorData.size();
        size_t vectorsPerThread = (totalVectors + config.numThreads - 1) / config.numThreads;

        // Launch worker threads
        std::cout << "Launching " << config.numThreads << " worker threads..." << std::endl;
        std::vector<std::thread> workers;
        auto overallStart = std::chrono::steady_clock::now();

        for (int i = 0; i < config.numThreads; ++i) {
            size_t startIdx = i * vectorsPerThread;
            size_t endIdx = std::min(startIdx + vectorsPerThread, totalVectors);

            if (startIdx >= totalVectors) break;

            workers.emplace_back(
                workerThread,
                i,
                index.get(),
                std::cref(vectorData),
                startIdx,
                endIdx,
                config.k,
                config.outputLogPath,
                std::ref(stats)
            );
        }

        // Wait for all workers to complete
        for (auto& worker : workers) {
            worker.join();
        }

        auto overallEnd = std::chrono::steady_clock::now();
        auto totalSeconds = std::chrono::duration_cast<std::chrono::seconds>(overallEnd - overallStart).count();

        // Stop statistics logger
        stopStatsLogger = true;
        statsThread.join();

        // Print final statistics
        std::cout << "\n=== Stress Test Complete ===" << std::endl;
        std::cout << "Total time: " << totalSeconds << " seconds" << std::endl;
        std::cout << "Total inserts: " << stats.numInserts.load() << std::endl;
        std::cout << "Total searches: " << stats.numSearches.load() << std::endl;
        std::cout << "Total errors: " << stats.numErrors.load() << std::endl;
        std::cout << "Average insert rate: " << (stats.numInserts.load() / std::max(1L, totalSeconds)) << " ops/sec" << std::endl;
        std::cout << "Average search rate: " << (stats.numSearches.load() / std::max(1L, totalSeconds)) << " ops/sec" << std::endl;
        std::cout << "Final vector count in index: " << index->getVectorCount() << std::endl;
        std::cout << "============================" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
