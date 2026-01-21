// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPFRESH_INTERFACE_H_
#define _SPFRESH_INTERFACE_H_

#include "Core/SPANN/Index.h"
#include <memory>
#include <vector>

namespace SPTAG {
namespace SSDServing {
namespace SPFresh {

/**
 * @brief Result structure for KNN search
 */
struct SearchResult {
    int vectorID;      // Vector ID
    float distance;    // Distance to query vector
    std::string metadata;  // Optional metadata (empty if not requested)
};

/**
 * @brief Configuration for creating a new empty SPFresh index
 */
struct IndexConfig {
    int dimension;                          // Vector dimension
    SPTAG::DistCalcMethod distanceMethod;   // Distance calculation method (Cosine or L2)
    std::string indexPath;                  // Path where the index will be stored
    int headVectorCount;                    // Number of head vectors (cluster centers)

    // SPDK storage backend configuration
    std::string spdkMappingPath;            // SPDK device mapping file path
    std::string ssdInfoFile;                // SPDK SSD info file path
    int spdkBatchSize;                      // SPDK batch size for operations

    IndexConfig()
        : dimension(0)
        , distanceMethod(SPTAG::DistCalcMethod::L2)
        , indexPath("")
        , headVectorCount(1000)
        , spdkMappingPath("")
        , ssdInfoFile("")
        , spdkBatchSize(128)
    {}
};

/**
 * @brief SPFresh Interface - Provides read (KNN search) and write (insert) operations
 *
 * This interface provides a simplified API over the SPFresh index for:
 * - Creating new empty indexes
 * - KNN search queries (read operations)
 * - Vector insertion (write operations)
 */
template<typename T>
class SPFreshInterface {
public:
    /**
     * @brief Construct a new SPFreshInterface object from an existing index
     * @param index Shared pointer to the SPANN index
     */
    explicit SPFreshInterface(std::shared_ptr<SPANN::Index<T>> index);

    /**
     * @brief Create a new empty SPFresh index
     *
     * @param config Configuration for the new index
     * @return std::shared_ptr<SPFreshInterface<T>> Interface to the newly created index, or nullptr on failure
     */
    static std::shared_ptr<SPFreshInterface<T>> createEmptyIndex(const IndexConfig& config);

    /**
     * @brief Destroy the SPFreshInterface object
     */
    ~SPFreshInterface();

    /**
     * @brief Perform KNN search for a single query vector
     *
     * @param query Pointer to the query vector data
     * @param k Number of nearest neighbors to return
     * @param withMetadata If true, retrieve metadata for results (default: false)
     * @return std::vector<SearchResult> Vector of k nearest neighbors with IDs and distances
     */
    std::vector<SearchResult> knnSearch(const T* query, int k, bool withMetadata = false);

    /**
     * @brief Perform batch KNN search for multiple query vectors
     *
     * @param queries Pointer to array of query vectors (dimension inferred from index)
     * @param numQueries Number of query vectors
     * @param k Number of nearest neighbors to return per query
     * @param withMetadata If true, retrieve metadata for results (default: false)
     * @return std::vector<std::vector<SearchResult>> Results for each query
     */
    std::vector<std::vector<SearchResult>> batchKnnSearch(
        const T* queries,
        int numQueries,
        int k,
        bool withMetadata = false
    );

    /**
     * @brief Insert a single new vector into the index
     *
     * @param vector Pointer to the vector data (dimension inferred from index)
     * @param metadata Optional metadata string to store with the vector
     * @return int The assigned vector ID, or -1 on failure
     */
    int insertVector(const T* vector, const std::string& metadata = "");

    /**
     * @brief Insert multiple vectors into the index
     *
     * @param vectors Pointer to array of vectors (dimension inferred from index)
     * @param numVectors Number of vectors to insert
     * @param metadataList Optional vector of metadata strings (must be size numVectors if provided)
     * @return std::vector<int> Vector of assigned IDs for each inserted vector
     */
    std::vector<int> batchInsertVectors(
        const T* vectors,
        int numVectors,
        const std::vector<std::string>& metadataList = std::vector<std::string>()
    );

    /**
     * @brief Get metadata for a specific vector
     *
     * @param vectorID ID of the vector
     * @return std::string Metadata string, or empty string if not found or no metadata
     */
    std::string getMetadata(int vectorID) const;

    /**
     * @brief Delete a vector from the index by ID
     *
     * @param vectorID ID of the vector to delete
     * @return bool True if deletion was successful, false otherwise
     */
    bool deleteVector(int vectorID);

    /**
     * @brief Get the number of vectors currently in the index
     *
     * @return int Number of vectors
     */
    int getVectorCount() const;

    /**
     * @brief Get the dimension of vectors in the index
     *
     * @return int Dimension
     */
    int getDimension() const;

    /**
     * @brief Initialize the interface (must be called before use in multi-threaded contexts)
     *
     * @return bool True if initialization was successful
     */
    bool initialize();

    /**
     * @brief Save the index to disk
     *
     * @param path Directory path where the index will be saved
     * @return bool True if save was successful
     */
    bool saveIndex(const std::string& path);

    /**
     * @brief Load an existing index from disk
     *
     * @param path Directory path where the index is stored
     * @return std::shared_ptr<SPFreshInterface<T>> Interface to the loaded index, or nullptr on failure
     */
    static std::shared_ptr<SPFreshInterface<T>> loadIndex(const std::string& path);

private:
    std::shared_ptr<SPANN::Index<T>> m_index;
    int m_dimension;
};

} // namespace SPFresh
} // namespace SSDServing
} // namespace SPTAG

#endif // _SPFRESH_INTERFACE_H_
