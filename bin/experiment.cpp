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
    // SPDK parameters
    int spdkBatchSize = 256;
    int spdkCapacity = 10000000;
    // SelectHead parameters
    std::string distCalcMethod = "L2";
    double ratio = 0.1;
    int treeNumber = 1;
    int bktKmeansK = 32;
    int bktLeafSize = 8;
    int selectThreshold = 12;
    int splitFactor = 9;
    int splitThreshold = 18;
    // BuildSSDIndex parameters
    int internalResultNum = 64;
    int replicaCount = 8;
    int postingPageLimit = 3;
    bool excludeHead = false;
    bool searchDuringUpdate = true;
    int insertThreadNum = 4;
    int appendThreadNum = 2;
    int reassignThreadNum = 0;
    bool disableReassign = false;
    int reassignK = 64;
    int mergeThreshold = 10;
    int bufferLength = 1;
    int resultNum = 10;
    int searchInternalResultNum = 64;
    int maxDistRatio = 1000000;
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
              << "  --seed <n>            Random seed (default: 42)\n"
              << "  SPDK:\n"
              << "  --spdk-batch-size <n> SPDK batch size (default: 256)\n"
              << "  --spdk-capacity <n>   SPDK posting slot capacity (default: 10000000)\n"
              << "  SelectHead:\n"
              << "  --dist-calc-method <s> Distance method (default: L2)\n"
              << "  --ratio <f>           Head selection ratio (default: 0.1)\n"
              << "  --tree-number <n>     BKT tree count (default: 1)\n"
              << "  --bkt-kmeans-k <n>    BKT k-means K (default: 32)\n"
              << "  --bkt-leaf-size <n>   BKT leaf size (default: 8)\n"
              << "  --select-threshold <n> Head select threshold (default: 12)\n"
              << "  --split-factor <n>    Split factor (default: 9)\n"
              << "  --split-threshold <n> Split threshold (default: 18)\n"
              << "  BuildSSDIndex:\n"
              << "  --internal-result-num <n> Internal result count (default: 64)\n"
              << "  --replica-count <n>   Replica count (default: 8)\n"
              << "  --posting-page-limit <n> Posting page limit (default: 3)\n"
              << "  --exclude-head        Exclude head vectors from postings (default: false)\n"
              << "  --no-search-during-update Disable search during update (default: enabled)\n"
              << "  --insert-threads <n>  Frontend insert threads (default: 4)\n"
              << "  --append-threads <n>  Background append threads (default: 2)\n"
              << "  --reassign-threads <n> Background reassign threads (default: 0)\n"
              << "  --disable-reassign    Disable reassignment (default: false)\n"
              << "  --reassign-k <n>      Reassign K (default: 64)\n"
              << "  --merge-threshold <n> Merge threshold (default: 10)\n"
              << "  --buffer-length <n>   Buffer length (default: 1)\n"
              << "  --result-num <n>      Search result count (default: 10)\n"
              << "  --search-internal-result-num <n> Search internal result count (default: 64)\n"
              << "  --max-dist-ratio <n>  Max distance ratio (default: 1000000)\n";
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
        } else if (arg == "--spdk-batch-size" && i + 1 < argc) {
            args.spdkBatchSize = std::stoi(argv[++i]);
        } else if (arg == "--spdk-capacity" && i + 1 < argc) {
            args.spdkCapacity = std::stoi(argv[++i]);
        } else if (arg == "--dist-calc-method" && i + 1 < argc) {
            args.distCalcMethod = argv[++i];
        } else if (arg == "--ratio" && i + 1 < argc) {
            args.ratio = std::stod(argv[++i]);
        } else if (arg == "--tree-number" && i + 1 < argc) {
            args.treeNumber = std::stoi(argv[++i]);
        } else if (arg == "--bkt-kmeans-k" && i + 1 < argc) {
            args.bktKmeansK = std::stoi(argv[++i]);
        } else if (arg == "--bkt-leaf-size" && i + 1 < argc) {
            args.bktLeafSize = std::stoi(argv[++i]);
        } else if (arg == "--select-threshold" && i + 1 < argc) {
            args.selectThreshold = std::stoi(argv[++i]);
        } else if (arg == "--split-factor" && i + 1 < argc) {
            args.splitFactor = std::stoi(argv[++i]);
        } else if (arg == "--split-threshold" && i + 1 < argc) {
            args.splitThreshold = std::stoi(argv[++i]);
        } else if (arg == "--internal-result-num" && i + 1 < argc) {
            args.internalResultNum = std::stoi(argv[++i]);
        } else if (arg == "--replica-count" && i + 1 < argc) {
            args.replicaCount = std::stoi(argv[++i]);
        } else if (arg == "--posting-page-limit" && i + 1 < argc) {
            args.postingPageLimit = std::stoi(argv[++i]);
        } else if (arg == "--exclude-head") {
            args.excludeHead = true;
        } else if (arg == "--no-search-during-update") {
            args.searchDuringUpdate = false;
        } else if (arg == "--insert-threads" && i + 1 < argc) {
            args.insertThreadNum = std::stoi(argv[++i]);
        } else if (arg == "--append-threads" && i + 1 < argc) {
            args.appendThreadNum = std::stoi(argv[++i]);
        } else if (arg == "--reassign-threads" && i + 1 < argc) {
            args.reassignThreadNum = std::stoi(argv[++i]);
        } else if (arg == "--disable-reassign") {
            args.disableReassign = true;
        } else if (arg == "--reassign-k" && i + 1 < argc) {
            args.reassignK = std::stoi(argv[++i]);
        } else if (arg == "--merge-threshold" && i + 1 < argc) {
            args.mergeThreshold = std::stoi(argv[++i]);
        } else if (arg == "--buffer-length" && i + 1 < argc) {
            args.bufferLength = std::stoi(argv[++i]);
        } else if (arg == "--result-num" && i + 1 < argc) {
            args.resultNum = std::stoi(argv[++i]);
        } else if (arg == "--search-internal-result-num" && i + 1 < argc) {
            args.searchInternalResultNum = std::stoi(argv[++i]);
        } else if (arg == "--max-dist-ratio" && i + 1 < argc) {
            args.maxDistRatio = std::stoi(argv[++i]);
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
    index->SetParameter("DistCalcMethod", args.distCalcMethod.c_str(), "Base");

    index->SetParameter("isExecute", "true", "SelectHead");
    index->SetParameter("SelectHeadType", "BKT", "SelectHead");
    index->SetParameter("NumberOfThreads", std::to_string(numThreads).c_str(), "SelectHead");
    index->SetParameter("Ratio", std::to_string(args.ratio).c_str(), "SelectHead");
    index->SetParameter("TreeNumber", std::to_string(args.treeNumber).c_str(), "SelectHead");
    index->SetParameter("BKTKmeansK", std::to_string(args.bktKmeansK).c_str(), "SelectHead");
    index->SetParameter("BKTLeafSize", std::to_string(args.bktLeafSize).c_str(), "SelectHead");
    index->SetParameter("SelectThreshold", std::to_string(args.selectThreshold).c_str(), "SelectHead");
    index->SetParameter("SplitFactor", std::to_string(args.splitFactor).c_str(), "SelectHead");
    index->SetParameter("SplitThreshold", std::to_string(args.splitThreshold).c_str(), "SelectHead");

    index->SetParameter("isExecute", "true", "BuildHead");

    index->SetParameter("isExecute", "true", "BuildSSDIndex");
    index->SetParameter("BuildSsdIndex", "true", "BuildSSDIndex");
    index->SetParameter("NumberOfThreads", std::to_string(numThreads).c_str(), "BuildSSDIndex");
    index->SetParameter("InternalResultNum", std::to_string(args.internalResultNum).c_str(), "BuildSSDIndex");
    index->SetParameter("ReplicaCount", std::to_string(args.replicaCount).c_str(), "BuildSSDIndex");
    index->SetParameter("PostingPageLimit", std::to_string(args.postingPageLimit).c_str(), "BuildSSDIndex");
    index->SetParameter("ExcludeHead", args.excludeHead ? "true" : "false", "BuildSSDIndex");
    index->SetParameter("SpdkMappingPath", args.spdkMap.c_str(), "BuildSSDIndex");
    index->SetParameter("SpdkBatchSize", std::to_string(args.spdkBatchSize).c_str(), "BuildSSDIndex");
    index->SetParameter("SpdkCapacity", std::to_string(args.spdkCapacity).c_str(), "BuildSSDIndex");

    index->SetParameter("Update", "true", "BuildSSDIndex");
    index->SetParameter("SearchDuringUpdate", args.searchDuringUpdate ? "true" : "false", "BuildSSDIndex");
    index->SetParameter("InsertThreadNum", std::to_string(args.insertThreadNum).c_str(), "BuildSSDIndex");
    index->SetParameter("AppendThreadNum", std::to_string(args.appendThreadNum).c_str(), "BuildSSDIndex");
    index->SetParameter("ReassignThreadNum", std::to_string(args.reassignThreadNum).c_str(), "BuildSSDIndex");
    index->SetParameter("DisableReassign", args.disableReassign ? "true" : "false", "BuildSSDIndex");
    index->SetParameter("ReassignK", std::to_string(args.reassignK).c_str(), "BuildSSDIndex");
    index->SetParameter("MergeThreshold", std::to_string(args.mergeThreshold).c_str(), "BuildSSDIndex");
    index->SetParameter("BufferLength", std::to_string(args.bufferLength).c_str(), "BuildSSDIndex");

    index->SetParameter("ResultNum", std::to_string(args.resultNum).c_str(), "BuildSSDIndex");
    index->SetParameter("SearchInternalResultNum", std::to_string(args.searchInternalResultNum).c_str(), "BuildSSDIndex");
    index->SetParameter("SearchPostingPageLimit", std::to_string(args.postingPageLimit).c_str(), "BuildSSDIndex");
    index->SetParameter("MaxDistRatio", std::to_string(args.maxDistRatio).c_str(), "BuildSSDIndex");

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
