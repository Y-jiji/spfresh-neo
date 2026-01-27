// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <cstring>
#include <algorithm>
#include "Core/BKT/Index.h"
#include "Helper/VectorSetReader.h"

using namespace SPTAG;

template <typename T>
void GenerateRandomVectors(std::vector<T>& data, int numVectors, int dimension) {
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

    data.resize(numVectors * dimension);
    for (int i = 0; i < numVectors * dimension; ++i) {
        data[i] = static_cast<T>(dist(rng));
    }
}

template <typename T>
bool CompareDatasets(const BKT::Index<T>* index1, const BKT::Index<T>* index2) {
    if (index1->GetNumSamples() != index2->GetNumSamples()) {
        std::cerr << "Sample counts differ: " << index1->GetNumSamples() << " vs " << index2->GetNumSamples() << std::endl;
        return false;
    }

    if (index1->GetFeatureDim() != index2->GetFeatureDim()) {
        std::cerr << "Feature dimensions differ: " << index1->GetFeatureDim() << " vs " << index2->GetFeatureDim() << std::endl;
        return false;
    }

    const float epsilon = 1e-6f;
    for (SizeType i = 0; i < index1->GetNumSamples(); ++i) {
        const T* sample1 = static_cast<const T*>(index1->GetSample(i));
        const T* sample2 = static_cast<const T*>(index2->GetSample(i));

        for (DimensionType j = 0; j < index1->GetFeatureDim(); ++j) {
            float v1 = static_cast<float>(sample1[j]);
            float v2 = static_cast<float>(sample2[j]);
            if (std::fabs(v1 - v2) > epsilon) {
                std::cerr << "Data differs at [" << i << "][" << j << "]: " << v1 << " vs " << v2 << std::endl;
                return false;
            }
        }
    }

    return true;
}

template <typename T>
bool TestBKTSerialization() {
    std::cout << "Testing BKT Serialization for type " << typeid(T).name() << std::endl;

    const int numVectors = 1000;
    const int dimension = 128;

    std::cout << "  Generating " << numVectors << " random vectors of dimension " << dimension << "..." << std::endl;
    std::vector<T> data;
    GenerateRandomVectors(data, numVectors, dimension);

    std::cout << "  Building original BKT index..." << std::endl;
    std::shared_ptr<BKT::Index<T>> originalIndex = std::make_shared<BKT::Index<T>>();
    originalIndex->SetParameter("NumberOfThreads", "4");
    originalIndex->SetParameter("DistCalcMethod", "L2");

    ErrorCode ret = originalIndex->BuildIndex(data.data(), numVectors, dimension);
    if (ret != ErrorCode::Success) {
        std::cerr << "Failed to build original index, error code: " << static_cast<int>(ret) << std::endl;
        return false;
    }

    std::cout << "  Original index built successfully" << std::endl;
    std::cout << "    Number of samples: " << originalIndex->GetNumSamples() << std::endl;
    std::cout << "    Feature dimension: " << originalIndex->GetFeatureDim() << std::endl;

    std::string testDir = "test_bkt_temp";
    std::cout << "  Saving index to " << testDir << "..." << std::endl;
    ret = originalIndex->SaveIndex(testDir);
    if (ret != ErrorCode::Success) {
        std::cerr << "Failed to save index, error code: " << static_cast<int>(ret) << std::endl;
        return false;
    }

    std::cout << "  Index saved successfully" << std::endl;

    std::cout << "  Loading index from " << testDir << "..." << std::endl;
    std::shared_ptr<BKT::Index<T>> loadedIndex;
    ret = BKT::Index<T>::LoadIndex(testDir, loadedIndex);
    if (ret != ErrorCode::Success) {
        std::cerr << "Failed to load index, error code: " << static_cast<int>(ret) << std::endl;
        return false;
    }

    std::cout << "  Index loaded successfully" << std::endl;
    std::cout << "    Number of samples: " << loadedIndex->GetNumSamples() << std::endl;
    std::cout << "    Feature dimension: " << loadedIndex->GetFeatureDim() << std::endl;

    if (!loadedIndex) {
        std::cerr << "Failed to load index" << std::endl;
        return false;
    }

    std::cout << "  Verifying loaded data matches original..." << std::endl;

    bool allMatch = true;

    if (!CompareDatasets<T>(originalIndex.get(), loadedIndex.get())) {
        std::cerr << "  FAILED: Dataset comparison failed" << std::endl;
        allMatch = false;
    } else {
        std::cout << "  PASSED: Dataset comparison successful" << std::endl;
    }

    if (originalIndex->GetNumSamples() != loadedIndex->GetNumSamples()) {
        std::cerr << "  FAILED: Sample counts differ" << std::endl;
        allMatch = false;
    } else {
        std::cout << "  PASSED: Sample counts match" << std::endl;
    }

    if (originalIndex->GetFeatureDim() != loadedIndex->GetFeatureDim()) {
        std::cerr << "  FAILED: Feature dimensions differ" << std::endl;
        allMatch = false;
    } else {
        std::cout << "  PASSED: Feature dimensions match" << std::endl;
    }

    if (originalIndex->GetDistCalcMethod() != loadedIndex->GetDistCalcMethod()) {
        std::cerr << "  FAILED: Distance calculation methods differ" << std::endl;
        allMatch = false;
    } else {
        std::cout << "  PASSED: Distance calculation methods match" << std::endl;
    }

    std::cout << "  Testing search functionality..." << std::endl;

    const int numQueries = 20;
    const int k = 5;

    std::vector<T> queryData;
    GenerateRandomVectors(queryData, numQueries, dimension);

    for (int q = 0; q < numQueries; ++q) {
        COMMON::QueryResultSet<float> originalResults(queryData.data() + q * dimension, k);
        ret = originalIndex->SearchIndex(originalResults);
        if (ret != ErrorCode::Success) {
            std::cerr << "  FAILED: Original index search failed for query " << q << std::endl;
            allMatch = false;
            break;
        }

        COMMON::QueryResultSet<float> loadedResults(queryData.data() + q * dimension, k);
        ret = loadedIndex->SearchIndex(loadedResults);
        if (ret != ErrorCode::Success) {
            std::cerr << "  FAILED: Loaded index search failed for query " << q << std::endl;
            allMatch = false;
            break;
        }

        for (int i = 0; i < k; ++i) {
            SizeType origVID = originalResults.GetResult(i)->VID;
            SizeType loadedVID = loadedResults.GetResult(i)->VID;

            if (origVID != loadedVID) {
                std::cerr << "  FAILED: Search results differ for query " << q << " at position " << i << std::endl;
                std::cerr << "    Original VID: " << origVID << ", Loaded VID: " << loadedVID << std::endl;
                std::cerr << "    Original Dist: " << originalResults.GetResult(i)->Dist << ", Loaded Dist: " << loadedResults.GetResult(i)->Dist << std::endl;
                allMatch = false;
                break;
            }

            float origDist = originalResults.GetResult(i)->Dist;
            float loadedDist = loadedResults.GetResult(i)->Dist;
            if (std::fabs(origDist - loadedDist) > 1e-6f) {
                std::cerr << "  FAILED: Search distances differ for query " << q << " at position " << i << std::endl;
                std::cerr << "    Original Dist: " << origDist << ", Loaded Dist: " << loadedDist << std::endl;
                allMatch = false;
                break;
            }
        }

        if (!allMatch) {
            break;
        }
    }

    if (allMatch) {
        std::cout << "  PASSED: Search functionality verified for " << numQueries << " queries" << std::endl;
    }

    return allMatch;
}

int main(int argc, char* argv[]) {
    std::cout << "======================================" << std::endl;
    std::cout << "BKT Serialization Test" << std::endl;
    std::cout << "======================================" << std::endl;

    bool allPassed = true;

    if (!TestBKTSerialization<float>()) {
        allPassed = false;
        std::cerr << "\nFAILED: BKT Serialization test for float" << std::endl;
    } else {
        std::cout << "\nPASSED: BKT Serialization test for float" << std::endl;
    }

    std::cout << "\n======================================" << std::endl;
    if (allPassed) {
        std::cout << "ALL TESTS PASSED" << std::endl;
        std::cout << "======================================" << std::endl;
        return 0;
    } else {
        std::cout << "SOME TESTS FAILED" << std::endl;
        std::cout << "======================================" << std::endl;
        return 1;
    }
}
