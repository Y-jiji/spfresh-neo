#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "Core/Common/QueryResultSet.h"
#include "Core/SPANN/Index.h"

using namespace SPTAG;

static long get_rss_mb() {
    FILE* f = fopen("/proc/self/status", "r");
    if (!f) return -1;
    long rss_kb = -1;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%ld", &rss_kb);
            break;
        }
    }
    fclose(f);
    return rss_kb > 0 ? rss_kb / 1024 : -1;
}

struct MmapFile {
    void* addr = MAP_FAILED;
    size_t len = 0;
    int fd = -1;

    bool open(const std::string& path, size_t minBytes) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error: cannot open file for mmap: " << path << "\n";
            return false;
        }
        struct stat st;
        if (fstat(fd, &st) != 0) {
            std::cerr << "Error: fstat failed: " << path << "\n";
            ::close(fd); fd = -1;
            return false;
        }
        len = static_cast<size_t>(st.st_size);
        if (len < minBytes) {
            std::cerr << "Error: file " << path << " too small: need " << minBytes
                      << " bytes, got " << len << "\n";
            ::close(fd); fd = -1;
            return false;
        }
        addr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED) {
            std::cerr << "Error: mmap failed: " << path << "\n";
            ::close(fd); fd = -1;
            return false;
        }
        return true;
    }

    void close() {
        if (addr != MAP_FAILED) {
            munmap(addr, len);
            addr = MAP_FAILED;
            len = 0;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }

    MmapFile() = default;
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;

    template<typename U> const U* as() const { return static_cast<const U*>(addr); }
};

struct Args {
    int dim = 0;
    int count = 0;
    int batches = -1;
    std::string dbVectors;
    std::string queryVectors;
    int queryCount = 0;
    std::vector<int> kValues = {10};
    int threads = 4;
    std::string indexDir = "./experiment_index";
    std::string spdkMap;
    std::string queryOutput;
    std::string statsOutput;
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
    std::vector<int> searchInternalResultNums; // per-K; expanded to match kValues
    int maxDistRatio = 1000000;
};

static void PrintUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "  --dim <n>              Vector dimension (required)\n"
              << "  --count <n>            Vectors per batch (required)\n"
              << "  --batches <n>          Number of batches (auto-computed from --db-vectors if omitted)\n"
              << "  --db-vectors <file>    Database vector file (raw binary, no header)\n"
              << "  --query-vectors <file> Query vector file (raw binary, no header)\n"
              << "  --query-count <n>      Number of query vectors (random generation only)\n"
              << "  --k <n>[,<n>...]      Comma-separated K values (default: 10)\n"
              << "  --threads <n>         Number of worker threads (default: 4)\n"
              << "  --index-dir <dir>     Index directory (default: ./experiment_index)\n"
              << "  --spdk-map <file>     SPDK mapping file path (required)\n"
              << "  --query-output <prefix> Output prefix for per-K query results (files: <prefix>_K<k>.txt)\n"
              << "  --stats-output <file>   TSV file for per-batch throughput/latency stats\n"
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
              << "  --search-internal-result-num <n>[,<n>,...] Search internal result count per K (default: 64)\n"
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
            args.kValues.clear();
            std::string kstr = argv[++i];
            size_t pos = 0;
            while (pos < kstr.size()) {
                size_t comma = kstr.find(',', pos);
                if (comma == std::string::npos) comma = kstr.size();
                args.kValues.push_back(std::stoi(kstr.substr(pos, comma - pos)));
                pos = comma + 1;
            }
        } else if (arg == "--threads" && i + 1 < argc) {
            args.threads = std::stoi(argv[++i]);
        } else if (arg == "--index-dir" && i + 1 < argc) {
            args.indexDir = argv[++i];
        } else if (arg == "--spdk-map" && i + 1 < argc) {
            args.spdkMap = argv[++i];
        } else if (arg == "--query-output" && i + 1 < argc) {
            args.queryOutput = argv[++i];
        } else if (arg == "--stats-output" && i + 1 < argc) {
            args.statsOutput = argv[++i];
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
            args.searchInternalResultNums.clear();
            std::string sstr = argv[++i];
            size_t pos = 0;
            while (pos < sstr.size()) {
                size_t comma = sstr.find(',', pos);
                if (comma == std::string::npos) comma = sstr.size();
                args.searchInternalResultNums.push_back(std::stoi(sstr.substr(pos, comma - pos)));
                pos = comma + 1;
            }
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
    if (args.count <= 0) {
        std::cerr << "Error: --count is required and must be > 0\n";
        return false;
    }
    if (args.batches < 0 && args.dbVectors.empty()) {
        std::cerr << "Error: --batches is required when not using --db-vectors\n";
        return false;
    }

    // Expand searchInternalResultNums to match kValues
    if (args.searchInternalResultNums.empty()) {
        args.searchInternalResultNums.assign(args.kValues.size(), 64);
    } else if (args.searchInternalResultNums.size() == 1) {
        args.searchInternalResultNums.assign(args.kValues.size(), args.searchInternalResultNums[0]);
    } else if (args.searchInternalResultNums.size() != args.kValues.size()) {
        std::cerr << "Error: --search-internal-result-num count (" << args.searchInternalResultNums.size()
                  << ") must match --k count (" << args.kValues.size() << ")\n";
        return false;
    }

    return true;
}

template <typename T>
static bool GenerateRandomVectorsToFile(const std::string& path, int count, int dim, unsigned seed) {
    std::ofstream out(path, std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Error: cannot write to file: " << path << "\n";
        return false;
    }
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    const int chunkVecs = 10000;
    std::vector<T> buf(static_cast<size_t>(chunkVecs) * dim);
    int remaining = count;
    while (remaining > 0) {
        int n = std::min(remaining, chunkVecs);
        size_t elems = static_cast<size_t>(n) * dim;
        for (size_t i = 0; i < elems; i++) {
            buf[i] = static_cast<T>(dist(rng));
        }
        out.write(reinterpret_cast<const char*>(buf.data()),
                  static_cast<std::streamsize>(elems) * sizeof(T));
        if (!out.good()) {
            std::cerr << "Error: write failed: " << path << "\n";
            return false;
        }
        remaining -= n;
    }
    return true;
}

template <typename T>
static int Run(const Args& args) {
    const int dim = args.dim;
    const int count = args.count;
    const std::vector<int>& kValues = args.kValues;
    const std::vector<int>& searchInternalResultNums = args.searchInternalResultNums;
    const int numThreads = args.threads;

    // --- mmap database vectors file, or prepare for random generation ---
    MmapFile dbMmap;
    bool useDbFile = !args.dbVectors.empty();

    // Determine numBatches and totalVectors
    int numBatches;
    int totalVectors;

    if (useDbFile) {
        struct stat dbst;
        if (stat(args.dbVectors.c_str(), &dbst) != 0) {
            std::cerr << "Error: cannot stat db file: " << args.dbVectors << "\n";
            return 1;
        }
        size_t dbFileSize = static_cast<size_t>(dbst.st_size);
        size_t vectorBytes = static_cast<size_t>(dim) * sizeof(T);
        int fileVectors = static_cast<int>(dbFileSize / vectorBytes);

        if (args.batches > 0) {
            numBatches = args.batches;
            totalVectors = count * numBatches;
        } else {
            numBatches = (fileVectors + count - 1) / count;
            totalVectors = fileVectors;
            std::cerr << "Auto-computed: " << fileVectors << " vectors in file -> "
                      << numBatches << " batches of " << count << "\n";
        }

        size_t needed = static_cast<size_t>(totalVectors) * dim * sizeof(T);
        std::cerr << "Memory-mapping " << totalVectors << " database vectors from " << args.dbVectors << "...\n";
        if (!dbMmap.open(args.dbVectors, needed))
            return 1;
    } else {
        numBatches = args.batches;
        totalVectors = count * numBatches;
    }

    // --- mmap or generate query vectors ---
    MmapFile queryMmap;
    std::vector<T> queryVectorsOwned;
    const T* queryData = nullptr;
    int queryCount = args.queryCount;

    if (!args.queryVectors.empty()) {
        std::cerr << "Memory-mapping query vectors from " << args.queryVectors << "...\n";
        // Determine query count from file size
        struct stat qst;
        if (stat(args.queryVectors.c_str(), &qst) != 0) {
            std::cerr << "Error: cannot stat query file: " << args.queryVectors << "\n";
            return 1;
        }
        size_t qFileSize = static_cast<size_t>(qst.st_size);
        size_t elemSize = static_cast<size_t>(dim) * sizeof(T);
        if (qFileSize % elemSize != 0) {
            std::cerr << "Error: query file size (" << qFileSize
                      << ") not divisible by vector size (" << elemSize << ")\n";
            return 1;
        }
        queryCount = static_cast<int>(qFileSize / elemSize);
        if (!queryMmap.open(args.queryVectors, qFileSize))
            return 1;
        queryData = queryMmap.as<T>();
        std::cerr << "Mapped " << queryCount << " query vectors.\n";
    } else if (queryCount > 0) {
        std::cerr << "Generating " << queryCount << " random query vectors...\n";
        std::mt19937 rng(args.seed + 2);
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        queryVectorsOwned.resize(static_cast<size_t>(queryCount) * dim);
        for (size_t i = 0; i < queryVectorsOwned.size(); i++) {
            queryVectorsOwned[i] = static_cast<T>(dist(rng));
        }
        queryData = queryVectorsOwned.data();
    }

    // --- Prepare index directory ---
    std::filesystem::create_directories(args.indexDir);

    // --- Write first batch to temp file for BuildIndex ---
    std::string tempVectorFile = args.indexDir + "/init_vectors.bin";
    std::cerr << "Writing batch 1 vectors to " << tempVectorFile << "...\n";
    if (useDbFile) {
        std::ofstream out(tempVectorFile, std::ios::binary);
        if (!out.is_open()) {
            std::cerr << "Error: cannot write to file: " << tempVectorFile << "\n";
            return 1;
        }
        size_t bytes = static_cast<size_t>(count) * dim * sizeof(T);
        out.write(reinterpret_cast<const char*>(dbMmap.as<T>()), static_cast<std::streamsize>(bytes));
        if (!out.good()) {
            std::cerr << "Error: write failed: " << tempVectorFile << "\n";
            return 1;
        }
    } else {
        if (!GenerateRandomVectorsToFile<T>(tempVectorFile, count, dim, args.seed))
            return 1;
    }

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
    index->SetParameter("NumberOfThreads", std::to_string(numThreads).c_str(), "BuildHead");

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
    int maxSearchInternalResultNum = *std::max_element(args.searchInternalResultNums.begin(), args.searchInternalResultNums.end());
    index->SetParameter("SearchInternalResultNum", std::to_string(maxSearchInternalResultNum).c_str(), "BuildSSDIndex");
    index->SetParameter("SearchPostingPageLimit", std::to_string(args.postingPageLimit).c_str(), "BuildSSDIndex");
    index->SetParameter("MaxDistRatio", std::to_string(args.maxDistRatio).c_str(), "BuildSSDIndex");

    // --- Build index from first batch ---
    std::cerr << "Building index with batch 1 (" << count << " vectors)...\n";
    auto buildStart = std::chrono::high_resolution_clock::now();
    ErrorCode ret = index->BuildIndex(false);
    auto buildEnd = std::chrono::high_resolution_clock::now();
    double buildMs = std::chrono::duration<double, std::milli>(buildEnd - buildStart).count();
    if (ret != ErrorCode::Success) {
        std::cerr << "Error: BuildIndex failed with code " << static_cast<int>(ret) << "\n";
        return 1;
    }
    std::cerr << "Index built: " << index->GetNumSamples() << " vectors, dim=" << index->GetFeatureDim()
              << " (" << buildMs << " ms)\n";

    // VID-to-SeqNum mapping: BuildIndex assigns VID = seqNum for the initial batch
    // Use a flat vector indexed by VID (VIDs are dense integers starting from 0)
    std::vector<int> vidToSeqNum(count);
    for (int i = 0; i < count; i++) vidToSeqNum[i] = i;

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
        double latencyUs; // per-query latency in microseconds
        int distCmps;     // number of distance computations (posting list phase)
    };

    auto runQueries = [&](std::vector<QueryResult_>& results, int kVal, int searchInternal) {
        // Set per-K search parameter
        index->SetParameter("SearchInternalResultNum", std::to_string(searchInternal).c_str(), "BuildSSDIndex");

        std::vector<std::vector<QueryResult_>> threadResults(numThreads);
        std::atomic<int> nextQuery(0);
        std::vector<std::thread> queryThreads;

        for (int t = 0; t < numThreads; t++) {
            queryThreads.emplace_back([&, t, kVal]() {
                index->Initialize();

                while (true) {
                    int qi = nextQuery.fetch_add(1);
                    if (qi >= queryCount) break;

                    COMMON::QueryResultSet<T> query(
                        queryData + static_cast<size_t>(qi) * dim, kVal);
                    query.Reset();

                    SPANN::SearchStats stats;
                    auto t0 = std::chrono::high_resolution_clock::now();
                    ErrorCode err = index->SearchIndex(query, false, &stats);
                    auto t1 = std::chrono::high_resolution_clock::now();
                    double latUs = std::chrono::duration<double, std::micro>(t1 - t0).count();

                    if (err != ErrorCode::Success) {
                        std::cerr << "Error: SearchIndex failed for query " << qi
                                  << " with code " << static_cast<int>(err) << "\n";
                        continue;
                    }

                    QueryResult_ qr;
                    qr.queryIdx = qi;
                    qr.latencyUs = latUs;
                    qr.distCmps = stats.m_totalListElementsCount;
                    for (int j = 0; j < kVal; j++) {
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

    struct LatencyStats {
        double qps;
        double meanUs;
        double p95Us;
        double p99Us;
        double p999Us;
        double meanDistCmps;
    };

    auto computePercentile = [](std::vector<double>& sorted, double pct) -> double {
        if (sorted.empty()) return 0.0;
        double idx = pct / 100.0 * (sorted.size() - 1);
        size_t lo = static_cast<size_t>(idx);
        size_t hi = lo + 1;
        if (hi >= sorted.size()) return sorted.back();
        double frac = idx - lo;
        return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
    };

    auto computeLatencyStats = [&](const std::vector<QueryResult_>& results,
                                    double wallSec) -> LatencyStats {
        LatencyStats stats{};
        std::vector<double> latencies;
        latencies.reserve(results.size());
        for (auto& qr : results)
            latencies.push_back(qr.latencyUs);
        std::sort(latencies.begin(), latencies.end());

        double sum = 0;
        for (double l : latencies) sum += l;
        stats.meanUs = latencies.empty() ? 0 : sum / latencies.size();
        stats.p95Us = computePercentile(latencies, 95.0);
        stats.p99Us = computePercentile(latencies, 99.0);
        stats.p999Us = computePercentile(latencies, 99.9);
        stats.qps = wallSec > 0 ? results.size() / wallSec : 0;

        double cmpsSum = 0;
        for (auto& qr : results) cmpsSum += qr.distCmps;
        stats.meanDistCmps = results.empty() ? 0 : cmpsSum / results.size();

        return stats;
    };

    auto writeQueryResults = [&](FILE* f, int batchIdx, int batchOffset, int kVal,
                                 const std::vector<QueryResult_>& results) {
        fprintf(f, "# after batch %d (offset %d)\n", batchIdx, batchOffset);
        for (auto& qr : results) {
            fprintf(f, "query %d:", qr.queryIdx);
            int limit = std::min(kVal, static_cast<int>(qr.hits.size()));
            for (int j = 0; j < limit; j++) {
                    fprintf(f, " (%d: id=%d, dist=%g)", j, vidToSeqNum[qr.hits[j].vid], qr.hits[j].dist);
            }
            fprintf(f, "\n");
        }
    };

    auto printSearchSummary = [&](int /*batch*/, const LatencyStats& stats, int kVal) {
        fprintf(stderr, "  K=%-4d  QPS=%10.1f  mean=%8.1fus  P95=%8.1fus  P99=%8.1fus  P99.9=%8.1fus\n",
                kVal, stats.qps, stats.meanUs, stats.p95Us, stats.p99Us, stats.p999Us);
    };

    // --- Open stats output file ---
    FILE* statsFile = nullptr;
    if (!args.statsOutput.empty()) {
        statsFile = fopen(args.statsOutput.c_str(), "w");
        if (!statsFile) {
            std::cerr << "Error: cannot open stats output file: " << args.statsOutput << "\n";
            return 1;
        }
        fprintf(statsFile, "batch\tphase\tK\tnum_points\tpoints_inserted\t"
                "insert_throughput_pts_sec\tinsert_time_ms\tmerge_time_ms\tbatch_total_ms\t"
                "search_qps\tmean_lat_ms\tp95_lat_ms\tp99_lat_ms\tp999_lat_ms\trss_mb\tmean_dist_cmps\tmean_hops\n");
    }

    // Print stats header to stdout
    printf("batch\tphase\tK\tnum_points\tpoints_inserted\t"
           "insert_throughput_pts_sec\tinsert_time_ms\tmerge_time_ms\tbatch_total_ms\t"
           "search_qps\tmean_lat_ms\tp95_lat_ms\tp99_lat_ms\tp999_lat_ms\trss_mb\tmean_dist_cmps\tmean_hops\n");
    fflush(stdout);

    // --- Open per-K query result files ---
    std::vector<std::pair<int, FILE*>> queryFiles;
    if (!args.queryOutput.empty()) {
        for (int kVal : kValues) {
            std::string path = args.queryOutput + "_K" + std::to_string(kVal) + ".txt";
            FILE* f = fopen(path.c_str(), "w");
            if (!f) {
                std::cerr << "Error: cannot open query output file: " << path << "\n";
                return 1;
            }
            queryFiles.push_back({kVal, f});
        }
    }

    auto getQueryFile = [&](int kVal) -> FILE* {
        for (auto& p : queryFiles)
            if (p.first == kVal) return p.second;
        return nullptr;
    };

    // --- Write build stats row ---
    {
        long rss = get_rss_mb();
        const char* fmt = "%d\tbuild\t\t%d\t%d\t\t\t\t%.2f\t\t\t\t\t\t%.2f\t\t\n";
        printf(fmt, 0, count, count, buildMs, (double)rss);
        fflush(stdout);
        if (statsFile) {
            fprintf(statsFile, fmt, 0, count, count, buildMs, (double)rss);
            fflush(statsFile);
        }
    }

    // --- Query after initial build (before any adds) ---
    if (queryCount > 0) {
        fprintf(stderr, "--- Batch 0/%d search results (%d points) ---\n", numBatches, count);
        for (size_t ki = 0; ki < kValues.size(); ki++) {
            int kVal = kValues[ki];
            std::vector<QueryResult_> results;
            auto wallStart = std::chrono::high_resolution_clock::now();
            runQueries(results, kVal, searchInternalResultNums[ki]);
            auto wallEnd = std::chrono::high_resolution_clock::now();
            double wallSec = std::chrono::duration<double>(wallEnd - wallStart).count();

            LatencyStats stats = computeLatencyStats(results, wallSec);
            printSearchSummary(0, stats, kVal);

            {
                const char* fmt = "%d\tsearch\t%d\t\t\t\t\t\t\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t\t%.1f\t\n";
                double meanMs = stats.meanUs / 1000.0, p95Ms = stats.p95Us / 1000.0,
                       p99Ms = stats.p99Us / 1000.0, p999Ms = stats.p999Us / 1000.0;
                printf(fmt, 0, kVal, stats.qps, meanMs, p95Ms, p99Ms, p999Ms, stats.meanDistCmps);
                fflush(stdout);
                if (statsFile) {
                    fprintf(statsFile, fmt, 0, kVal, stats.qps, meanMs, p95Ms, p99Ms, p999Ms, stats.meanDistCmps);
                    fflush(statsFile);
                }
            }

            FILE* qf = getQueryFile(kVal);
            if (qf) writeQueryResults(qf, 0, 0, kVal, results);
        }
        std::cerr << "Batch 0 queries complete.\n";
    }

    // --- Add remaining batches with queries after each ---
    std::vector<std::vector<VIDMapping>> threadMappings(numThreads);

    for (int b = 1; b < numBatches; b++) {
        int batchStart = b * count;
        int batchEnd = std::min(batchStart + count, totalVectors);
        int batchCount = batchEnd - batchStart;

        std::cerr << "Batch " << (b + 1) << "/" << numBatches
                  << ": adding " << batchCount << " vectors (index " << batchStart
                  << ".." << (batchEnd - 1) << ") with " << numThreads << " threads...\n";

        auto batchTotalStart = std::chrono::high_resolution_clock::now();
        auto insertStart = std::chrono::high_resolution_clock::now();

        std::atomic<int> nextIdx(batchStart);
        std::vector<std::thread> addThreads;

        for (int t = 0; t < numThreads; t++) {
            addThreads.emplace_back([&, t, batchStart, batchEnd]() {
                index->Initialize();

                while (true) {
                    int i = nextIdx.fetch_add(1);
                    if (i >= batchEnd) break;

                    const T* vecPtr;
                    std::vector<T> vecBuf;
                    if (useDbFile) {
                        vecPtr = dbMmap.as<T>() + static_cast<size_t>(i) * dim;
                    } else {
                        vecBuf.resize(dim);
                        std::mt19937 rng(args.seed + static_cast<unsigned>(i));
                        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
                        for (int d = 0; d < dim; d++)
                            vecBuf[d] = static_cast<T>(dist(rng));
                        vecPtr = vecBuf.data();
                    }

                    SizeType vid;
                    ErrorCode err = index->AddIndexSPFresh(
                        vecPtr, 1, dim, &vid);
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
        auto insertEnd = std::chrono::high_resolution_clock::now();
        double insertMs = std::chrono::duration<double, std::milli>(insertEnd - insertStart).count();

        // Update VID-to-SeqNum mapping with newly inserted vectors (not timed)
        if (static_cast<size_t>(batchEnd) > vidToSeqNum.size())
            vidToSeqNum.resize(batchEnd);
        for (int t = 0; t < numThreads; t++) {
            for (auto& m : threadMappings[t]) {
                if (m.vid >= 0 && static_cast<size_t>(m.vid) < vidToSeqNum.size())
                    vidToSeqNum[m.vid] = m.seqNum;
            }
            threadMappings[t].clear();
        }
        double insertThroughputPts = (insertMs > 0) ? batchCount / (insertMs / 1000.0) : 0;

        std::cerr << "Waiting for batch " << (b + 1) << " background operations...\n";
        auto waitStart = std::chrono::high_resolution_clock::now();
        while (!index->AllFinished()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        auto waitEnd = std::chrono::high_resolution_clock::now();
        double waitMs = std::chrono::duration<double, std::milli>(waitEnd - waitStart).count();
        auto batchTotalEnd = std::chrono::high_resolution_clock::now();
        double batchTotalMs = std::chrono::duration<double, std::milli>(batchTotalEnd - batchTotalStart).count();

        std::cerr << "Batch " << b << " complete: insert=" << insertMs
                  << "ms, wait=" << waitMs << "ms, total=" << batchTotalMs << "ms"
                  << ", throughput=" << insertThroughputPts << " pts/s\n";

        // Write insert stats row
        {
            long rss = get_rss_mb();
            const char* fmt = "%d\tinsert\t\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t\t\t\t\t\t%.2f\t\t\n";
            printf(fmt, b, batchCount, batchEnd,
                   insertThroughputPts, insertMs, waitMs, batchTotalMs, (double)rss);
            fflush(stdout);
            if (statsFile) {
                fprintf(statsFile, fmt, b, batchCount, batchEnd,
                        insertThroughputPts, insertMs, waitMs, batchTotalMs, (double)rss);
                fflush(statsFile);
            }
        }

        // Query after this batch
        if (queryCount > 0) {
            fprintf(stderr, "--- Batch %d/%d search results (%d points) ---\n", b, numBatches, batchEnd);
            for (size_t ki = 0; ki < kValues.size(); ki++) {
                int kVal = kValues[ki];
                std::vector<QueryResult_> results;
                auto wallStart = std::chrono::high_resolution_clock::now();
                runQueries(results, kVal, searchInternalResultNums[ki]);
                auto wallEnd = std::chrono::high_resolution_clock::now();
                double wallSec = std::chrono::duration<double>(wallEnd - wallStart).count();

                LatencyStats stats = computeLatencyStats(results, wallSec);
                printSearchSummary(b, stats, kVal);

                {
                    const char* fmt = "%d\tsearch\t%d\t\t\t\t\t\t\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t\t%.1f\t\n";
                    double meanMs = stats.meanUs / 1000.0, p95Ms = stats.p95Us / 1000.0,
                           p99Ms = stats.p99Us / 1000.0, p999Ms = stats.p999Us / 1000.0;
                    printf(fmt, b, kVal, stats.qps, meanMs, p95Ms, p99Ms, p999Ms, stats.meanDistCmps);
                    fflush(stdout);
                    if (statsFile) {
                        fprintf(statsFile, fmt, b, kVal, stats.qps, meanMs, p95Ms, p99Ms, p999Ms, stats.meanDistCmps);
                        fflush(statsFile);
                    }
                }

                FILE* qf = getQueryFile(kVal);
                if (qf) writeQueryResults(qf, b, batchStart, kVal, results);
            }
            std::cerr << "Batch " << (b + 1) << " queries complete.\n";
        }
    }
    if (numBatches > 1) {
        std::cerr << "All " << numBatches << " batches complete. Total vectors: " << totalVectors << "\n";
    }

    // Close output files
    if (statsFile) fclose(statsFile);
    for (auto& p : queryFiles) fclose(p.second);

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
