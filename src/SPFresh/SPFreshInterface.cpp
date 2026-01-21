// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "SPFresh/SPFreshInterface.h"
#include "Core/Common/QueryResultSet.h"
#include "Core/VectorIndex.h"
#include "Core/MetadataSet.h"
#include "Helper/Logging.h"
#include "Helper/CommonHelper.h"
#include <stdexcept>
#include <fstream>
#include <cstdio>

namespace SPTAG {
namespace SSDServing {
namespace SPFresh {

template<typename T>
SPFreshInterface<T>::SPFreshInterface(std::shared_ptr<SPANN::Index<T>> index)
    : m_index(index)
{
    if (!m_index) {
        throw std::invalid_argument("Index pointer cannot be null");
    }
    m_dimension = m_index->GetFeatureDim();
}

template<typename T>
SPFreshInterface<T>::~SPFreshInterface() {
    // Cleanup handled by shared_ptr
}

template<typename T>
std::vector<SearchResult> SPFreshInterface<T>::knnSearch(const T* query, int k, bool withMetadata) {
    if (!query) {
        LOG(Helper::LogLevel::LL_Error, "Query vector cannot be null\n");
        return std::vector<SearchResult>();
    }

    if (k <= 0) {
        LOG(Helper::LogLevel::LL_Error, "k must be positive\n");
        return std::vector<SearchResult>();
    }

    // Create query result object
    // Note: QueryResultSet always has withMeta=false, metadata retrieval happens separately
    COMMON::QueryResultSet<T> queryResult(query, k);
    queryResult.Reset();

    // Perform search on memory index (head)
    ErrorCode ret = m_index->GetMemoryIndex()->SearchIndex(queryResult);
    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Head search failed with error code: %d\n", static_cast<int>(ret));
        return std::vector<SearchResult>();
    }

    // Perform search on disk index (tail)
    ret = m_index->SearchDiskIndex(queryResult, nullptr);
    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Disk search failed with error code: %d\n", static_cast<int>(ret));
        return std::vector<SearchResult>();
    }

    // Sort and extract results
    queryResult.SortResult();

    std::vector<SearchResult> results;
    results.reserve(k);

    for (int i = 0; i < k; ++i) {
        const BasicResult* result = queryResult.GetResult(i);
        if (result && result->VID >= 0) {
            SearchResult sr;
            sr.vectorID = result->VID;
            sr.distance = result->Dist;

            // Get metadata if requested (retrieve directly from index)
            if (withMetadata) {
                sr.metadata = getMetadata(result->VID);
            }

            results.push_back(sr);
        }
    }

    return results;
}

template<typename T>
std::vector<std::vector<SearchResult>> SPFreshInterface<T>::batchKnnSearch(
    const T* queries,
    int numQueries,
    int k,
    bool withMetadata
) {
    if (!queries) {
        LOG(Helper::LogLevel::LL_Error, "Queries array cannot be null\n");
        return std::vector<std::vector<SearchResult>>();
    }

    if (numQueries <= 0 || k <= 0) {
        LOG(Helper::LogLevel::LL_Error, "numQueries and k must be positive\n");
        return std::vector<std::vector<SearchResult>>();
    }

    std::vector<std::vector<SearchResult>> allResults;
    allResults.reserve(numQueries);

    for (int i = 0; i < numQueries; ++i) {
        const T* queryPtr = queries + (i * m_dimension);
        std::vector<SearchResult> results = knnSearch(queryPtr, k, withMetadata);
        allResults.push_back(std::move(results));
    }

    return allResults;
}

template<typename T>
int SPFreshInterface<T>::insertVector(const T* vector, const std::string& metadata) {
    if (!vector) {
        LOG(Helper::LogLevel::LL_Error, "Vector cannot be null\n");
        return -1;
    }

    // If metadata is provided, use AddIndex with metadata
    if (!metadata.empty()) {
        // Create a simple in-memory metadata set with single entry
        auto metadataSet = std::make_shared<MemMetadataSet>(1024, 1000000, 10);

        // Create a mutable copy of metadata for ByteArray (which requires non-const pointer)
        std::vector<std::uint8_t> metadataCopy(metadata.begin(), metadata.end());
        ByteArray metaBytes(metadataCopy.data(), metadataCopy.size(), false);
        metadataSet->Add(metaBytes);

        ErrorCode ret = m_index->AddIndex(vector, 1, m_dimension, metadataSet, false, false);
        if (ret != ErrorCode::Success) {
            LOG(Helper::LogLevel::LL_Error, "Insert with metadata failed with error code: %d\n", static_cast<int>(ret));
            return -1;
        }

        // Return the newly added vector ID (last inserted)
        return static_cast<int>(m_index->GetNumSamples() - 1);
    }

    // No metadata - use SPFresh-specific insert
    SizeType vectorID = -1;
    ErrorCode ret = m_index->AddIndexSPFresh(vector, 1, m_dimension, &vectorID);

    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Insert failed with error code: %d\n", static_cast<int>(ret));
        return -1;
    }

    return static_cast<int>(vectorID);
}

template<typename T>
std::vector<int> SPFreshInterface<T>::batchInsertVectors(
    const T* vectors,
    int numVectors,
    const std::vector<std::string>& metadataList
) {
    if (!vectors) {
        LOG(Helper::LogLevel::LL_Error, "Vectors array cannot be null\n");
        return std::vector<int>();
    }

    if (numVectors <= 0) {
        LOG(Helper::LogLevel::LL_Error, "numVectors must be positive\n");
        return std::vector<int>();
    }

    // Check metadata list size if provided
    if (!metadataList.empty() && metadataList.size() != static_cast<size_t>(numVectors)) {
        LOG(Helper::LogLevel::LL_Error, "Metadata list size (%zu) must match number of vectors (%d)\n",
            metadataList.size(), numVectors);
        return std::vector<int>();
    }

    std::vector<int> vectorIDs;
    vectorIDs.reserve(numVectors);

    for (int i = 0; i < numVectors; ++i) {
        const T* vectorPtr = vectors + (i * m_dimension);
        const std::string& metadata = metadataList.empty() ? "" : metadataList[i];
        int vid = insertVector(vectorPtr, metadata);
        vectorIDs.push_back(vid);
    }

    return vectorIDs;
}

template<typename T>
std::string SPFreshInterface<T>::getMetadata(int vectorID) const {
    if (vectorID < 0 || vectorID >= static_cast<int>(m_index->GetNumSamples())) {
        return "";
    }

    ByteArray meta = m_index->GetMetadata(static_cast<SizeType>(vectorID));
    if (meta.Length() == 0) {
        return "";
    }

    return std::string(reinterpret_cast<const char*>(meta.Data()), meta.Length());
}

template<typename T>
bool SPFreshInterface<T>::deleteVector(int vectorID) {
    if (vectorID < 0) {
        LOG(Helper::LogLevel::LL_Error, "Invalid vector ID: %d\n", vectorID);
        return false;
    }

    ErrorCode ret = m_index->DeleteIndex(static_cast<SizeType>(vectorID));

    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Delete failed with error code: %d\n", static_cast<int>(ret));
        return false;
    }

    return true;
}

template<typename T>
int SPFreshInterface<T>::getVectorCount() const {
    return static_cast<int>(m_index->GetNumSamples());
}

template<typename T>
int SPFreshInterface<T>::getDimension() const {
    return m_dimension;
}

template<typename T>
bool SPFreshInterface<T>::initialize() {
    return m_index->Initialize();
}

template<typename T>
std::shared_ptr<SPFreshInterface<T>> SPFreshInterface<T>::createEmptyIndex(const IndexConfig& config) {
    if (config.dimension <= 0) {
        LOG(Helper::LogLevel::LL_Error, "Invalid dimension: %d\n", config.dimension);
        return nullptr;
    }

    if (config.indexPath.empty()) {
        LOG(Helper::LogLevel::LL_Error, "Index path cannot be empty\n");
        return nullptr;
    }

    // Create index instance
    auto baseIndex = VectorIndex::CreateInstance(IndexAlgoType::SPANN, GetEnumValueType<T>());
    if (!baseIndex) {
        LOG(Helper::LogLevel::LL_Error, "Failed to create SPANN index instance\n");
        return nullptr;
    }

    auto spannIndex = std::dynamic_pointer_cast<SPANN::Index<T>>(baseIndex);
    if (!spannIndex) {
        LOG(Helper::LogLevel::LL_Error, "Failed to cast to SPANN::Index\n");
        return nullptr;
    }

    // Configure the index
    SPANN::Options* opts = spannIndex->GetOptions();

    // Basic parameters
    opts->m_dim = config.dimension;
    opts->m_distCalcMethod = config.distanceMethod;
    opts->m_valueType = GetEnumValueType<T>();
    opts->m_indexAlgoType = IndexAlgoType::BKT;  // Head index type
    opts->m_indexDirectory = config.indexPath;
    opts->m_headIndexFolder = config.indexPath + "/head";

    // Head parameters
    opts->m_headVectorCount = config.headVectorCount;
    opts->m_iTreeNumber = 1;
    opts->m_iBKTKmeansK = 32;
    opts->m_iBKTLeafSize = 8;
    opts->m_replicaCount = 8;

    // Storage backend parameters - SPDK only
    opts->m_useSPDK = true;
    opts->m_useKV = false;

    // SPDK configuration
    opts->m_spdkMappingPath = config.spdkMappingPath.empty()
        ? config.indexPath + "/spdk_mapping.txt"
        : config.spdkMappingPath;
    opts->m_ssdInfoFile = config.ssdInfoFile.empty()
        ? config.indexPath + "/ssd_info.txt"
        : config.ssdInfoFile;
    opts->m_spdkBatchSize = config.spdkBatchSize;

    LOG(Helper::LogLevel::LL_Info, "SPDK enabled with mapping: %s, info: %s, batch size: %d\n",
        opts->m_spdkMappingPath.c_str(), opts->m_ssdInfoFile.c_str(), opts->m_spdkBatchSize);

    // SSD parameters
    opts->m_enableSSD = true;
    opts->m_iSSDNumberOfThreads = 32;
    opts->m_postingPageLimit = 3;
    opts->m_searchPostingPageLimit = 3;

    // Ensure directories exist
    if (!direxists(config.indexPath.c_str())) {
        mkdir(config.indexPath.c_str());
    }
    if (!direxists(opts->m_headIndexFolder.c_str())) {
        mkdir(opts->m_headIndexFolder.c_str());
    }

    // Build an empty index with a single dummy vector to initialize structures
    std::vector<T> dummyVector(config.dimension, 0);
    ErrorCode ret = spannIndex->BuildIndex(
        dummyVector.data(),
        1,
        config.dimension,
        config.distanceMethod == DistCalcMethod::Cosine,
        false
    );

    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Failed to build initial index structure: %d\n", static_cast<int>(ret));
        return nullptr;
    }

    // Mark the index as ready
    spannIndex->SetReady(true);

    // Create and return the interface
    try {
        auto interface = std::make_shared<SPFreshInterface<T>>(spannIndex);
        LOG(Helper::LogLevel::LL_Info, "Successfully created empty SPFresh index at: %s\n", config.indexPath.c_str());
        return interface;
    } catch (const std::exception& e) {
        LOG(Helper::LogLevel::LL_Error, "Failed to create interface: %s\n", e.what());
        return nullptr;
    }
}

template<typename T>
bool SPFreshInterface<T>::saveIndex(const std::string& path) {
    if (path.empty()) {
        LOG(Helper::LogLevel::LL_Error, "Save path cannot be empty\n");
        return false;
    }

    ErrorCode ret = m_index->SaveIndex(path);
    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Failed to save index to %s: %d\n", path.c_str(), static_cast<int>(ret));
        return false;
    }

    LOG(Helper::LogLevel::LL_Info, "Successfully saved index to: %s\n", path.c_str());
    return true;
}

template<typename T>
std::shared_ptr<SPFreshInterface<T>> SPFreshInterface<T>::loadIndex(const std::string& path) {
    if (path.empty()) {
        LOG(Helper::LogLevel::LL_Error, "Load path cannot be empty\n");
        return nullptr;
    }

    std::shared_ptr<VectorIndex> baseIndex;
    ErrorCode ret = VectorIndex::LoadIndex(path, baseIndex);

    if (ret != ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Failed to load index from %s: %d\n", path.c_str(), static_cast<int>(ret));
        return nullptr;
    }

    auto spannIndex = std::dynamic_pointer_cast<SPANN::Index<T>>(baseIndex);
    if (!spannIndex) {
        LOG(Helper::LogLevel::LL_Error, "Loaded index is not a SPANN::Index<T>\n");
        return nullptr;
    }

    try {
        auto interface = std::make_shared<SPFreshInterface<T>>(spannIndex);
        LOG(Helper::LogLevel::LL_Info, "Successfully loaded index from: %s\n", path.c_str());
        return interface;
    } catch (const std::exception& e) {
        LOG(Helper::LogLevel::LL_Error, "Failed to create interface: %s\n", e.what());
        return nullptr;
    }
}

// Explicit template instantiations for supported types (matching SPANN::Index types)
template class SPFreshInterface<float>;
template class SPFreshInterface<std::int8_t>;
template class SPFreshInterface<std::uint8_t>;
template class SPFreshInterface<std::int16_t>;

} // namespace SPFresh
} // namespace SSDServing
} // namespace SPTAG
