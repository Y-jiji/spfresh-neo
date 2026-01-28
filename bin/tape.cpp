// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/SPANN/Index.h"
#include "Core/Common/QueryResultSet.h"
#include "Helper/TracePlayer.h"
#include "Helper/ResultWriter.h"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace SPTAG;

// Configuration structure for the test
struct TapeConfig {
    std::string initFile;       // Initial vectors file for building index
    std::string traceFile;      // Trace file for replay
    std::string logFile;        // Output log file
    std::string indexDir;       // Index directory
    std::string spdkMappingPath;// SPDK mapping file path
    int numThreads;             // Number of worker threads
    std::size_t windowSize;     // TracePlayer window size
    int insertQueries;          // Number of consecutive insert queries
    int searchQueries;          // Number of consecutive search queries
    int k;                      // Number of nearest neighbors for search
    int dimension;              // Vector dimension
};

// Hash function that creates alternating pattern of insertQueries inserts and searchQueries searches
class AlternatingPatternHash {
public:
    AlternatingPatternHash(int insertQueries, int searchQueries)
        : m_insertQueries(insertQueries), m_searchQueries(searchQueries),
          m_cycleLength(insertQueries + searchQueries) {}

    std::uint64_t operator()(std::uint64_t seqNum) const {
        // Position within the cycle
        std::uint64_t posInCycle = seqNum % m_cycleLength;

        // First insertQueries positions are Write (insert), rest are Read (search)
        // Return odd for Write, even for Read (based on TracePlayer::DetermineOp logic)
        if (posInCycle < static_cast<std::uint64_t>(m_insertQueries)) {
            return 1;  // Write (odd & 1 == 1)
        } else {
            return 0;  // Read (even & 1 == 0)
        }
    }

private:
    int m_insertQueries;
    int m_searchQueries;
    int m_cycleLength;
};

template <typename T>
bool RunTapeTest(const TapeConfig& config) {
    std::cout << "=== Tape Test ===" << std::endl;
    std::cout << "Init file: " << config.initFile << std::endl;
    std::cout << "Trace file: " << config.traceFile << std::endl;
    std::cout << "Log file: " << config.logFile << std::endl;
    std::cout << "Threads: " << config.numThreads << std::endl;
    std::cout << "Window size: " << config.windowSize << std::endl;
    std::cout << "Insert queries per cycle: " << config.insertQueries << std::endl;
    std::cout << "Search queries per cycle: " << config.searchQueries << std::endl;
    std::cout << "K: " << config.k << std::endl;

    // ========================================
    // Step 1: Initialize and build SPANN::Index
    // ========================================
    std::cout << "\n[1] Building SPANN Index from " << config.initFile << "..." << std::endl;

    auto index = std::make_shared<SPANN::Index<T>>();

    // Configure index parameters
    index->SetParameter("ValueType", Helper::Convert::ConvertToString(GetEnumValueType<T>()).c_str(), "Base");
    index->SetParameter("Dim", std::to_string(config.dimension).c_str(), "Base");
    index->SetParameter("VectorPath", config.initFile.c_str(), "Base");
    index->SetParameter("IndexDirectory", config.indexDir.c_str(), "Base");
    index->SetParameter("DistCalcMethod", "L2", "Base");

    // Select Head parameters
    index->SetParameter("isExecute", "true", "SelectHead");
    index->SetParameter("SelectHeadType", "BKT", "SelectHead");
    index->SetParameter("NumberOfThreads", std::to_string(config.numThreads).c_str(), "SelectHead");
    index->SetParameter("Ratio", "0.1", "SelectHead");
    index->SetParameter("TreeNumber", "1", "SelectHead");
    index->SetParameter("BKTKmeansK", "8", "SelectHead");
    index->SetParameter("BKTLeafSize", "4", "SelectHead");

    // Build Head parameters
    index->SetParameter("isExecute", "true", "BuildHead");

    // Build SSD Index parameters
    index->SetParameter("isExecute", "true", "BuildSSDIndex");
    index->SetParameter("BuildSsdIndex", "true", "BuildSSDIndex");
    index->SetParameter("NumberOfThreads", std::to_string(config.numThreads).c_str(), "BuildSSDIndex");
    index->SetParameter("ExcludeHead", "true", "BuildSSDIndex");
    index->SetParameter("UseDirectIO", "false", "BuildSSDIndex");
    index->SetParameter("SpdkMappingPath", config.spdkMappingPath.c_str(), "BuildSSDIndex");
    index->SetParameter("PostingPageLimit", "12", "BuildSSDIndex");
    index->SetParameter("SpdkCapacity", "1000000", "BuildSSDIndex");

    // Enable update mode for AddIndexSPFresh
    index->SetParameter("Update", "true", "BuildSSDIndex");
    index->SetParameter("AppendThreadNum", std::to_string(config.numThreads).c_str(), "BuildSSDIndex");
    index->SetParameter("ReassignThreadNum", std::to_string(config.numThreads).c_str(), "BuildSSDIndex");

    // Build the index
    ErrorCode ret = index->BuildIndex(false);
    if (ret != ErrorCode::Success) {
        std::cerr << "Failed to build index: " << static_cast<int>(ret) << std::endl;
        return false;
    }

    std::cout << "Index built successfully:" << std::endl;
    std::cout << "  Total vectors: " << index->GetNumSamples() << std::endl;
    std::cout << "  Dimension: " << index->GetFeatureDim() << std::endl;

    // ========================================
    // Step 2: Create TracePlayer with alternating pattern
    // ========================================
    std::cout << "\n[2] Creating TracePlayer from " << config.traceFile << "..." << std::endl;

    AlternatingPatternHash hashFn(config.insertQueries, config.searchQueries);
    Helper::TracePlayer<T> player(config.traceFile, config.windowSize, hashFn);

    std::cout << "TracePlayer initialized:" << std::endl;
    std::cout << "  Total vectors: " << player.GetTotalVectors() << std::endl;
    std::cout << "  Dimension: " << player.GetDimension() << std::endl;
    std::cout << "  Window size: " << player.GetWindowSize() << std::endl;

    // Verify dimension matches
    if (player.GetDimension() != static_cast<std::size_t>(index->GetFeatureDim())) {
        std::cerr << "Dimension mismatch: trace=" << player.GetDimension()
                  << ", index=" << index->GetFeatureDim() << std::endl;
        return false;
    }

    // ========================================
    // Step 3: Create ResultWriter
    // ========================================
    std::cout << "\n[3] Creating ResultWriter to " << config.logFile << "..." << std::endl;

    Helper::ResultWriter writer(config.logFile, config.k);

    std::cout << "ResultWriter initialized:" << std::endl;
    std::cout << "  K: " << writer.GetK() << std::endl;
    std::cout << "  Num slots: " << writer.GetNumSlots() << std::endl;

    // ========================================
    // Step 4: Launch worker threads
    // ========================================
    std::cout << "\n[4] Launching " << config.numThreads << " worker threads..." << std::endl;

    std::atomic<std::size_t> insertCount{0};
    std::atomic<std::size_t> searchCount{0};
    std::atomic<bool> hasError{false};

    auto workerFunc = [&]() {
        // Per-thread SPDK initialization (required!)
        index->Initialize();

        while (true) {
            // Get next trace record
            auto guard = player.Next();
            if (!guard) {
                // Trace exhausted
                break;
            }

            const auto& record = **guard;
            std::size_t seqNum = record.SequenceNumber();
            const T* data = record.Data();
            std::size_t dim = record.Dimension();

            if (record.GetOperationKind() == Helper::OperationKind::Write) {
                // Insert operation
                SizeType vid;
                ErrorCode err = index->AddIndexSPFresh(data, 1, static_cast<DimensionType>(dim), &vid);
                if (err != ErrorCode::Success) {
                    std::cerr << "Insert failed for seqNum " << seqNum << ": " << static_cast<int>(err) << std::endl;
                    hasError.store(true);
                    continue;
                }

                // Write insert result
                writer.WriteInsertRecord(static_cast<std::uint64_t>(seqNum),
                                        static_cast<std::uint64_t>(vid));
                insertCount.fetch_add(1, std::memory_order_relaxed);
            } else {
                // Search operation
                COMMON::QueryResultSet<T> query(data, config.k);
                query.Reset();

                ErrorCode err = index->SearchIndex(query);
                if (err != ErrorCode::Success) {
                    std::cerr << "Search failed for seqNum " << seqNum << ": " << static_cast<int>(err) << std::endl;
                    hasError.store(true);
                    continue;
                }

                // Collect result IDs
                std::vector<std::uint64_t> resultIds(config.k);
                for (int i = 0; i < config.k; ++i) {
                    BasicResult* result = query.GetResult(i);
                    resultIds[i] = (result && result->VID >= 0)
                                   ? static_cast<std::uint64_t>(result->VID)
                                   : static_cast<std::uint64_t>(-1);
                }

                // Write search result
                writer.WriteSearchRecord(static_cast<std::uint64_t>(seqNum), resultIds.data());
                searchCount.fetch_add(1, std::memory_order_relaxed);
            }
        }

        // Signal thread completion to SPDK
        index->ExitBlockController();
    };

    // Launch threads
    std::vector<std::thread> threads;
    threads.reserve(config.numThreads);
    for (int i = 0; i < config.numThreads; ++i) {
        threads.emplace_back(workerFunc);
    }

    // Join threads
    for (auto& t : threads) {
        t.join();
    }

    // Wait for background operations to complete
    std::cout << "\n[5] Waiting for background operations..." << std::endl;
    while (!index->AllFinished()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // ========================================
    // Step 5: Flush and close
    // ========================================
    std::cout << "\n[6] Flushing results..." << std::endl;
    writer.Close();

    // ========================================
    // Summary
    // ========================================
    std::cout << "\n=== Test Complete ===" << std::endl;
    std::cout << "Insert operations: " << insertCount.load() << std::endl;
    std::cout << "Search operations: " << searchCount.load() << std::endl;
    std::cout << "Total operations: " << (insertCount.load() + searchCount.load()) << std::endl;
    std::cout << "Errors: " << (hasError.load() ? "Yes" : "No") << std::endl;

    return !hasError.load();
}

void PrintUsage(const char* progName) {
    std::cerr << "Usage: " << progName << " [options]" << std::endl;
    std::cerr << "Options:" << std::endl;
    std::cerr << "  --init <file>        Initial vectors file for building index" << std::endl;
    std::cerr << "  --trace <file>       Trace file for replay" << std::endl;
    std::cerr << "  --log <file>         Output log file" << std::endl;
    std::cerr << "  --index-dir <dir>    Index directory (default: ./tape_index)" << std::endl;
    std::cerr << "  --spdk-map <file>    SPDK mapping file path" << std::endl;
    std::cerr << "  --threads <n>        Number of worker threads (default: 4)" << std::endl;
    std::cerr << "  --window <n>         TracePlayer window size (default: 64)" << std::endl;
    std::cerr << "  --insert <n>         Consecutive insert queries per cycle (default: 1)" << std::endl;
    std::cerr << "  --search <n>         Consecutive search queries per cycle (default: 1)" << std::endl;
    std::cerr << "  --k <n>              Number of nearest neighbors (default: 10)" << std::endl;
    std::cerr << "  --dim <n>            Vector dimension (default: 128)" << std::endl;
}

int main(int argc, char* argv[]) {
    TapeConfig config;
    config.indexDir = "./tape_index";
    config.numThreads = 4;
    config.windowSize = 64;
    config.insertQueries = 1;
    config.searchQueries = 1;
    config.k = 10;
    config.dimension = 128;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--init" && i + 1 < argc) {
            config.initFile = argv[++i];
        } else if (arg == "--trace" && i + 1 < argc) {
            config.traceFile = argv[++i];
        } else if (arg == "--log" && i + 1 < argc) {
            config.logFile = argv[++i];
        } else if (arg == "--index-dir" && i + 1 < argc) {
            config.indexDir = argv[++i];
        } else if (arg == "--spdk-map" && i + 1 < argc) {
            config.spdkMappingPath = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            config.numThreads = std::stoi(argv[++i]);
        } else if (arg == "--window" && i + 1 < argc) {
            config.windowSize = std::stoull(argv[++i]);
        } else if (arg == "--insert" && i + 1 < argc) {
            config.insertQueries = std::stoi(argv[++i]);
        } else if (arg == "--search" && i + 1 < argc) {
            config.searchQueries = std::stoi(argv[++i]);
        } else if (arg == "--k" && i + 1 < argc) {
            config.k = std::stoi(argv[++i]);
        } else if (arg == "--dim" && i + 1 < argc) {
            config.dimension = std::stoi(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            PrintUsage(argv[0]);
            return 1;
        }
    }

    // Validate required arguments
    if (config.initFile.empty() || config.traceFile.empty() ||
        config.logFile.empty() || config.spdkMappingPath.empty()) {
        std::cerr << "Error: --init, --trace, --log, and --spdk-map are required" << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }

    // Run the test
    bool success = RunTapeTest<float>(config);

    return success ? 0 : 1;
}
