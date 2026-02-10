#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "Core/Common/QueryResultSet.h"
#include "Core/SPANN/Index.h"

using namespace SPTAG;

struct Args {
    int dim = 0;
    int count = 0;
    int batches = 1;
    std::string dbVectors;
    std::string queryVectors;
    int queryCount = 0;
    int k = 10;
    int threads = 4;
    std::string indexDir = "./experiment_index";
    std::string spdkMap;
    std::string mappingOutput;
    std::string queryOutput;
    std::string valueType = "Float";
    unsigned seed = 42;
};

static void PrintUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "  --dim <n>              Vector dimension (required)\n"
              << "  --count <n>            Vectors per batch (required unless --db-vectors)\n"
              << "  --batches <n>          Number of batches (default: 1; first builds index, rest add)\n"
              << "  --db-vectors <file>    Database vector file (raw binary, no header)\n"
              << "  --query-vectors <file> Query vector file (raw binary, no header)\n"
              << "  --query-count <n>      Number of query vectors (random generation only)\n"
              << "  --k <n>               Number of nearest neighbors (default: 10)\n"
              << "  --threads <n>         Number of worker threads (default: 4)\n"
              << "  --index-dir <dir>     Index directory (default: ./experiment_index)\n"
              << "  --spdk-map <file>     SPDK mapping file path (required)\n"
              << "  --mapping-output <file> Output file for VID-to-SeqNum mapping (default: stdout)\n"
              << "  --query-output <file>   Output file for query results (default: stdout)\n"
              << "  --value-type <type>   Float, Int8, Int16, UInt8 (default: Float)\n"
              << "  --seed <n>            Random seed (default: 42)\n";
}

static bool ParseArgs(int argc, char* argv[], Args& args) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--dim" && i + 1 < argc) {
            args.dim = std::stoi(argv[++i]);
        } else if (arg == "--count" && i + 1 < argc) {
            args.count = std::stoi(argv[++i]);
        } else if (arg == "--batches" && i + 1 < argc) {
            args.batches = std::stoi(argv[++i]);
        } else if (arg == "--db-vectors" && i + 1 < argc) {
            args.dbVectors = argv[++i];
        } else if (arg == "--query-vectors" && i + 1 < argc) {
            args.queryVectors = argv[++i];
        } else if (arg == "--query-count" && i + 1 < argc) {
            args.queryCount = std::stoi(argv[++i]);
        } else if (arg == "--k" && i + 1 < argc) {
            args.k = std::stoi(argv[++i]);
        } else if (arg == "--threads" && i + 1 < argc) {
            args.threads = std::stoi(argv[++i]);
        } else if (arg == "--index-dir" && i + 1 < argc) {
            args.indexDir = argv[++i];
        } else if (arg == "--spdk-map" && i + 1 < argc) {
            args.spdkMap = argv[++i];
        } else if (arg == "--mapping-output" && i + 1 < argc) {
            args.mappingOutput = argv[++i];
        } else if (arg == "--query-output" && i + 1 < argc) {
            args.queryOutput = argv[++i];
        } else if (arg == "--value-type" && i + 1 < argc) {
            args.valueType = argv[++i];
        } else if (arg == "--seed" && i + 1 < argc) {
            args.seed = static_cast<unsigned>(std::stoul(argv[++i]));
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            std::exit(0);
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            PrintUsage(argv[0]);
            return false;
        }
    }

    if (args.dim <= 0) {
        std::cerr << "Error: --dim is required and must be > 0\n";
        return false;
    }
    if (args.spdkMap.empty()) {
        std::cerr << "Error: --spdk-map is required\n";
        return false;
    }
    if (args.count <= 0 && args.dbVectors.empty()) {
        std::cerr << "Error: --count is required when not using --db-vectors\n";
        return false;
    }
    return true;
}

template <typename T>
static void GenerateRandomVectors(std::vector<T>& data, int count, int dim, unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    data.resize(static_cast<size_t>(count) * dim);
    for (size_t i = 0; i < data.size(); i++) {
        data[i] = static_cast<T>(dist(rng));
    }
}

template <typename T>
static bool LoadVectorsFromFile(const std::string& path, std::vector<T>& data, int expectedCount, int dim) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in.is_open()) {
        std::cerr << "Error: cannot open file: " << path << "\n";
        return false;
    }
    auto fileSize = in.tellg();
    in.seekg(0);

    size_t needed = static_cast<size_t>(expectedCount) * dim * sizeof(T);
    if (static_cast<size_t>(fileSize) < needed) {
        std::cerr << "Error: file " << path << " too small: need " << needed
                  << " bytes, got " << fileSize << "\n";
        return false;
    }

    data.resize(static_cast<size_t>(expectedCount) * dim);
    in.read(reinterpret_cast<char*>(data.data()), needed);
    return true;
}

template <typename T>
static bool LoadQueryVectorsFromFile(const std::string& path, std::vector<T>& data, int dim, int& queryCount) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in.is_open()) {
        std::cerr << "Error: cannot open file: " << path << "\n";
        return false;
    }
    auto fileSize = static_cast<size_t>(in.tellg());
    in.seekg(0);

    size_t elemSize = static_cast<size_t>(dim) * sizeof(T);
    if (fileSize % elemSize != 0) {
        std::cerr << "Error: query file size (" << fileSize
                  << ") not divisible by vector size (" << elemSize << ")\n";
        return false;
    }

    queryCount = static_cast<int>(fileSize / elemSize);
    data.resize(static_cast<size_t>(queryCount) * dim);
    in.read(reinterpret_cast<char*>(data.data()), fileSize);
    return true;
}

template <typename T>
static bool SaveVectorsToBinaryFile(const T* data, int count, int dim, const std::string& path) {
    std::ofstream out(path, std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Error: cannot write to file: " << path << "\n";
        return false;
    }
    out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(count) * dim * sizeof(T));
    return out.good();
}

template <typename T>
static int Run(const Args& args) {
    const int dim = args.dim;
    const int count = args.count;
    const int numBatches = args.batches;
    const int totalVectors = count * numBatches;
    const int k = args.k;
    const int numThreads = args.threads;

    // --- Load or generate all database vectors ---
    std::vector<T> allVectors;

    if (!args.dbVectors.empty()) {
        std::cerr << "Loading " << totalVectors << " database vectors from " << args.dbVectors << "...\n";
        if (!LoadVectorsFromFile<T>(args.dbVectors, allVectors, totalVectors, dim))
            return 1;
    } else {
        std::cerr << "Generating " << totalVectors << " random vectors (dim=" << dim
                  << ", " << numBatches << " batches of " << count << ")...\n";
        GenerateRandomVectors<T>(allVectors, totalVectors, dim, args.seed);
    }

    // --- Load or generate query vectors ---
    std::vector<T> queryVectors;
    int queryCount = args.queryCount;

    if (!args.queryVectors.empty()) {
        std::cerr << "Loading query vectors from " << args.queryVectors << "...\n";
        if (!LoadQueryVectorsFromFile<T>(args.queryVectors, queryVectors, dim, queryCount))
            return 1;
        std::cerr << "Loaded " << queryCount << " query vectors.\n";
    } else if (queryCount > 0) {
        std::cerr << "Generating " << queryCount << " random query vectors...\n";
        GenerateRandomVectors<T>(queryVectors, queryCount, dim, args.seed + 2);
    }

    // --- Prepare index directory ---
    std::filesystem::create_directories(args.indexDir);

    // --- Write first batch to temp file for BuildIndex ---
    std::string tempVectorFile = args.indexDir + "/init_vectors.bin";
    std::cerr << "Writing batch 1 vectors to " << tempVectorFile << "...\n";
    if (!SaveVectorsToBinaryFile<T>(allVectors.data(), count, dim, tempVectorFile))
        return 1;

    // --- Create and configure SPANN index ---
    std::cerr << "Creating SPANN index...\n";
    auto index = std::make_shared<SPANN::Index<T>>();

    index->SetParameter("ValueType", Helper::Convert::ConvertToString(GetEnumValueType<T>()).c_str(), "Base");
    index->SetParameter("Dim", std::to_string(dim).c_str(), "Base");
    index->SetParameter("VectorPath", tempVectorFile.c_str(), "Base");
    index->SetParameter("IndexDirectory", args.indexDir.c_str(), "Base");
    index->SetParameter("DistCalcMethod", "L2", "Base");

    index->SetParameter("isExecute", "true", "SelectHead");
    index->SetParameter("SelectHeadType", "BKT", "SelectHead");
    index->SetParameter("NumberOfThreads", std::to_string(numThreads).c_str(), "SelectHead");
    index->SetParameter("Ratio", "0.1", "SelectHead");
    index->SetParameter("TreeNumber", "1", "SelectHead");
    index->SetParameter("BKTKmeansK", "8", "SelectHead");
    index->SetParameter("BKTLeafSize", "4", "SelectHead");

    index->SetParameter("isExecute", "true", "BuildHead");

    index->SetParameter("isExecute", "true", "BuildSSDIndex");
    index->SetParameter("BuildSsdIndex", "true", "BuildSSDIndex");
    index->SetParameter("NumberOfThreads", std::to_string(numThreads).c_str(), "BuildSSDIndex");
    index->SetParameter("ExcludeHead", "true", "BuildSSDIndex");
    index->SetParameter("UseDirectIO", "false", "BuildSSDIndex");
    index->SetParameter("SpdkMappingPath", args.spdkMap.c_str(), "BuildSSDIndex");
    index->SetParameter("PostingPageLimit", "1", "BuildSSDIndex");
    index->SetParameter("SpdkCapacity", "10000", "BuildSSDIndex");

    index->SetParameter("Update", "true", "BuildSSDIndex");
    index->SetParameter("AppendThreadNum", "1", "BuildSSDIndex");
    index->SetParameter("ReassignThreadNum", "1", "BuildSSDIndex");

    // --- Build index from first batch ---
    std::cerr << "Building index with batch 1 (" << count << " vectors)...\n";
    ErrorCode ret = index->BuildIndex(false);
    if (ret != ErrorCode::Success) {
        std::cerr << "Error: BuildIndex failed with code " << static_cast<int>(ret) << "\n";
        return 1;
    }
    std::cerr << "Index built: " << index->GetNumSamples() << " vectors, dim=" << index->GetFeatureDim() << "\n";

    // --- Helpers ---
    struct VIDMapping {
        int seqNum;
        SizeType vid;
    };
    struct QueryHit {
        SizeType vid;
        float dist;
    };
    struct QueryResult_ {
        int queryIdx;
        std::vector<QueryHit> hits;
    };

    auto runQueries = [&](std::vector<QueryResult_>& results) {
        std::vector<std::vector<QueryResult_>> threadResults(numThreads);
        std::atomic<int> nextQuery(0);
        std::vector<std::thread> queryThreads;

        for (int t = 0; t < numThreads; t++) {
            queryThreads.emplace_back([&, t]() {
                index->Initialize();

                while (true) {
                    int qi = nextQuery.fetch_add(1);
                    if (qi >= queryCount) break;

                    COMMON::QueryResultSet<T> query(
                        queryVectors.data() + static_cast<size_t>(qi) * dim, k);
                    query.Reset();

                    ErrorCode err = index->SearchIndex(query);
                    if (err != ErrorCode::Success) {
                        std::cerr << "Error: SearchIndex failed for query " << qi
                                  << " with code " << static_cast<int>(err) << "\n";
                        continue;
                    }

                    QueryResult_ qr;
                    qr.queryIdx = qi;
                    for (int j = 0; j < k; j++) {
                        BasicResult* r = query.GetResult(j);
                        if (r && r->VID >= 0) {
                            qr.hits.push_back({r->VID, r->Dist});
                        }
                    }
                    threadResults[t].push_back(std::move(qr));
                }

                index->ExitBlockController();
            });
        }

        for (auto& th : queryThreads) th.join();

        for (auto& tr : threadResults) {
            results.insert(results.end(),
                           std::make_move_iterator(tr.begin()),
                           std::make_move_iterator(tr.end()));
        }
        std::sort(results.begin(), results.end(),
                  [](const QueryResult_& a, const QueryResult_& b) { return a.queryIdx < b.queryIdx; });
    };

    auto writeQueryResults = [&](FILE* qout, const char* label, const std::vector<QueryResult_>& results) {
        fprintf(qout, "# %s (queryCount=%d, k=%d)\n", label, queryCount, k);
        for (auto& qr : results) {
            fprintf(qout, "Query %d:", qr.queryIdx);
            for (auto& h : qr.hits) {
                fprintf(qout, " [VID=%d Dist=%.6f]", h.vid, h.dist);
            }
            fprintf(qout, "\n");
        }
        fprintf(qout, "\n");
    };

    // --- Open query output file ---
    FILE* qout = stdout;
    if (!args.queryOutput.empty()) {
        qout = fopen(args.queryOutput.c_str(), "w");
        if (!qout) {
            std::cerr << "Error: cannot open query output file: " << args.queryOutput << "\n";
            return 1;
        }
    }

    // --- Query after initial build (before any adds) ---
    if (queryCount > 0) {
        std::cerr << "Querying after initial build...\n";
        std::vector<QueryResult_> results;
        runQueries(results);
        char label[64];
        snprintf(label, sizeof(label), "After batch 1/%d (%d total vectors)", numBatches, count);
        writeQueryResults(qout, label, results);
        std::cerr << "Batch 1 queries complete.\n";
    }

    // --- Add remaining batches with queries after each ---
    std::vector<std::vector<VIDMapping>> threadMappings(numThreads);

    for (int b = 1; b < numBatches; b++) {
        int batchStart = b * count;
        int batchEnd = batchStart + count;

        std::cerr << "Batch " << (b + 1) << "/" << numBatches
                  << ": adding " << count << " vectors (index " << batchStart
                  << ".." << (batchEnd - 1) << ") with " << numThreads << " threads...\n";

        std::atomic<int> nextIdx(batchStart);
        std::vector<std::thread> addThreads;

        for (int t = 0; t < numThreads; t++) {
            addThreads.emplace_back([&, t, batchEnd]() {
                index->Initialize();

                while (true) {
                    int i = nextIdx.fetch_add(1);
                    if (i >= batchEnd) break;

                    SizeType vid;
                    ErrorCode err = index->AddIndexSPFresh(
                        allVectors.data() + static_cast<size_t>(i) * dim,
                        1, dim, &vid);
                    if (err != ErrorCode::Success) {
                        std::cerr << "Error: AddIndexSPFresh failed for vector " << i
                                  << " with code " << static_cast<int>(err) << "\n";
                    } else {
                        threadMappings[t].push_back({i, vid});
                    }
                }

                index->ExitBlockController();
            });
        }

        for (auto& th : addThreads) th.join();

        std::cerr << "Waiting for batch " << (b + 1) << " background operations...\n";
        while (!index->AllFinished()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        std::cerr << "Batch " << (b + 1) << " complete.\n";

        // Query after this batch
        if (queryCount > 0) {
            std::cerr << "Querying after batch " << (b + 1) << "...\n";
            std::vector<QueryResult_> results;
            runQueries(results);
            char label[64];
            snprintf(label, sizeof(label), "After batch %d/%d (%d total vectors)", b + 1, numBatches, batchEnd);
            writeQueryResults(qout, label, results);
            std::cerr << "Batch " << (b + 1) << " queries complete.\n";
        }
    }
    if (numBatches > 1) {
        std::cerr << "All " << numBatches << " batches complete. Total vectors: " << totalVectors << "\n";
    }

    if (qout != stdout) fclose(qout);

    // --- Collect and sort VID mappings ---
    std::vector<VIDMapping> allMappings;
    for (auto& tm : threadMappings) {
        allMappings.insert(allMappings.end(), tm.begin(), tm.end());
    }
    std::sort(allMappings.begin(), allMappings.end(),
              [](const VIDMapping& a, const VIDMapping& b) { return a.seqNum < b.seqNum; });

    // --- Output VID-to-SeqNum mapping ---
    {
        FILE* mout = stdout;
        if (!args.mappingOutput.empty()) {
            mout = fopen(args.mappingOutput.c_str(), "w");
            if (!mout) {
                std::cerr << "Error: cannot open mapping output file: " << args.mappingOutput << "\n";
                return 1;
            }
        }
        fprintf(mout, "# VID-to-SeqNum Mapping (count=%d, batches=%d)\n", count, numBatches);
        fprintf(mout, "# Index VID\n");
        for (auto& m : allMappings) {
            fprintf(mout, "%d %d\n", m.seqNum, m.vid);
        }
        if (mout != stdout) fclose(mout);
    }

    // Wait for all background merge/reassign operations before index destruction
    std::cerr << "Waiting for background operations to finish...\n";
    while (!index->AllFinished()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // Cleanup temp file
    std::filesystem::remove(tempVectorFile);

    std::cerr << "Done.\n";
    return 0;
}

int main(int argc, char* argv[]) {
    Args args;
    if (!ParseArgs(argc, argv, args))
        return 1;

    if (args.valueType == "Float") {
        return Run<float>(args);
    } else if (args.valueType == "Int8") {
        return Run<std::int8_t>(args);
    } else if (args.valueType == "Int16") {
        return Run<std::int16_t>(args);
    } else if (args.valueType == "UInt8") {
        return Run<std::uint8_t>(args);
    } else {
        std::cerr << "Error: unknown value type: " << args.valueType << "\n";
        std::cerr << "Valid types: Float, Int8, Int16, UInt8\n";
        return 1;
    }
}
