// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_BKT_INDEX_H_
#define _SPTAG_BKT_INDEX_H_

#include "Core/Common.h"
#include "Core/VectorIndex.h"

#include "Utils/CommonUtils.h"
#include "Utils/DistanceUtils.h"
#include "Core/Common/QueryResultSet.h"
#include "Core/Common/Dataset.h"
#include "Core/Common/WorkSpace.h"
#include "Core/Common/WorkSpacePool.h"
#include "Core/Common/RelativeNeighborhoodGraph.h"
#include "Core/Common/BKTree.h"
#include "Core/Common/Labelset.h"
#include "Helper/SimpleIniReader.h"
#include "Helper/StringConvert.h"
#include "Helper/ThreadPool.h"

#include <functional>
#include <shared_mutex>

namespace SPTAG::Helper {
class IniReader;
}

namespace SPTAG::BKT {

template <typename T>
class Index : public VectorIndex {
    class RebuildJob : public Helper::ThreadPool::Job {
       public:
        RebuildJob(COMMON::Dataset<T>* p_data, COMMON::BKTree* p_tree, COMMON::RelativeNeighborhoodGraph* p_graph, DistCalcMethod p_distMethod) : m_data(p_data), m_tree(p_tree), p_graph(p_graph), m_distMethod(p_distMethod) {}
        void exec(IAbortOperation* p_abort) {
            m_tree->Rebuild<T>(*m_data, m_distMethod, p_abort);
        }

       private:
        COMMON::Dataset<T>* m_data;
        COMMON::BKTree* m_tree;
        COMMON::RelativeNeighborhoodGraph* p_graph;
        DistCalcMethod m_distMethod;
    };

   private:
    // data points
    COMMON::Dataset<T> m_pSamples;

    // BKT structures.
    COMMON::BKTree m_pTrees;

    // Graph structure
    COMMON::RelativeNeighborhoodGraph m_pGraph;

    std::string m_sBKTFilename;
    std::string m_sGraphFilename;
    std::string m_sDataPointsFilename;
    std::string m_sDeleteDataPointsFilename;

    int m_addCountForRebuild;
    float m_fDeletePercentageForRefine;
    std::mutex m_dataAddLock;  // protect data and graph
    std::shared_timed_mutex m_dataDeleteLock;
    COMMON::Labelset m_deletedID;

    Helper::ThreadPool m_threadPool;
    int m_iNumberOfThreads;

    DistCalcMethod m_iDistCalcMethod;
    std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
    int m_iBaseSquare;

    int m_iMaxCheck;
    int m_iThresholdOfNumberOfContinuousNoBetterPropagation;
    int m_iNumberOfInitialDynamicPivots;
    int m_iNumberOfOtherDynamicPivots;
    int m_iHashTableExp;

   public:
    static thread_local std::shared_ptr<COMMON::WorkSpace> m_workspace;

   public:
    Index() {
#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    VarName = DefaultValue;

#include "Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

        m_pSamples.SetName("Vector");
        m_fComputeDistance = std::function<float(const T*, const T*, DimensionType)>(COMMON::DistanceCalcSelector<T>(m_iDistCalcMethod));
        m_iBaseSquare = (m_iDistCalcMethod == DistCalcMethod::Cosine) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() : 1;
    }

    ~Index() {}

    inline SizeType GetNumSamples() const {
        return m_pSamples.R();
    }
    inline SizeType GetNumDeleted() const {
        return (SizeType)m_deletedID.Count();
    }
    inline DimensionType GetFeatureDim() const {
        return m_pSamples.C();
    }

    inline int GetCurrMaxCheck() const {
        return m_iMaxCheck;
    }
    inline int GetNumThreads() const {
        return m_iNumberOfThreads;
    }
    inline DistCalcMethod GetDistCalcMethod() const {
        return m_iDistCalcMethod;
    }
    inline IndexAlgoType GetIndexAlgoType() const {
        return IndexAlgoType::BKT;
    }
    inline VectorValueType GetVectorValueType() const {
        return GetEnumValueType<T>();
    }

    inline float AccurateDistance(const void* pX, const void* pY) const {
        if (m_iDistCalcMethod == DistCalcMethod::L2)
            return m_fComputeDistance((const T*)pX, (const T*)pY, m_pSamples.C());

        float xy = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pY, m_pSamples.C());
        float xx = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pX, m_pSamples.C());
        float yy = m_iBaseSquare - m_fComputeDistance((const T*)pY, (const T*)pY, m_pSamples.C());
        return 1.0f - xy / (sqrt(xx) * sqrt(yy));
    }
    inline float ComputeDistance(const void* pX, const void* pY) const {
        return m_fComputeDistance((const T*)pX, (const T*)pY, m_pSamples.C());
    }
    inline const void* GetSample(const SizeType idx) const {
        return (void*)m_pSamples[idx];
    }
    inline bool ContainSample(const SizeType idx) const {
        return idx >= 0 && idx < m_deletedID.R() && !m_deletedID.Contains(idx);
    }
    inline bool NeedRefine() const {
        return m_deletedID.Count() > (size_t)(GetNumSamples() * m_fDeletePercentageForRefine);
    }
    std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const {
        std::shared_ptr<std::vector<std::uint64_t>> buffersize(new std::vector<std::uint64_t>);
        buffersize->push_back(m_pSamples.BufferSize());
        buffersize->push_back(m_pTrees.BufferSize());
        buffersize->push_back(m_pGraph.BufferSize());
        buffersize->push_back(m_deletedID.BufferSize());
        return std::move(buffersize);
    }

    std::shared_ptr<std::vector<std::string>> GetIndexFiles() const {
        std::shared_ptr<std::vector<std::string>> files(new std::vector<std::string>);
        files->push_back(m_sDataPointsFilename);
        files->push_back(m_sBKTFilename);
        files->push_back(m_sGraphFilename);
        files->push_back(m_sDeleteDataPointsFilename);
        return std::move(files);
    }

    ErrorCode SaveConfig(std::shared_ptr<Helper::DiskIO> p_configout);
    ErrorCode SaveIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);

    ErrorCode LoadConfig(Helper::IniReader& p_reader);
    ErrorCode LoadIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
    ErrorCode LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs);

    ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, bool p_normalized = false, bool p_shareOwnership = false);
    ErrorCode SearchIndex(QueryResult& p_query, bool p_searchDeleted = false) const;
    ErrorCode RefineSearchIndex(QueryResult& p_query, bool p_searchDeleted = false) const;
    ErrorCode SearchTree(QueryResult& p_query) const;
    ErrorCode AddIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex = false, bool p_normalized = false);
    ErrorCode AddIndexIdx(SizeType begin, SizeType end);
    ErrorCode AddIndexId(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, int& beginHead, int& endHead);

    ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum);
    ErrorCode DeleteIndex(const SizeType& p_id);

    ErrorCode SetParameter(const char* p_param, const char* p_value, const char* p_section = nullptr);
    std::string GetParameter(const char* p_param, const char* p_section = nullptr) const;
    ErrorCode UpdateIndex();

    ErrorCode RefineIndex(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams, IAbortOperation* p_abort);
    ErrorCode RefineIndex(std::shared_ptr<VectorIndex>& p_newIndex);

   private:
    void SearchIndex(COMMON::QueryResultSet<T>& p_query, COMMON::WorkSpace& p_space, bool p_searchDeleted, bool p_searchDuplicated, std::function<bool(const ByteArray&)> filterFunc = nullptr) const;

    template <bool (*notDeleted)(const COMMON::Labelset&, SizeType), bool (*isDup)(COMMON::QueryResultSet<T>&, SizeType, float), bool (*checkFilter)(const std::shared_ptr<MetadataSet>&, SizeType, std::function<bool(const ByteArray&)>)>
    void Search(COMMON::QueryResultSet<T>& p_query, COMMON::WorkSpace& p_space, std::function<bool(const ByteArray&)> filterFunc) const;
};

// Estimation methods (BKT-specific)
std::uint64_t EstimatedVectorCount(std::uint64_t p_memory, DimensionType p_dimension, VectorValueType p_valuetype, SizeType p_vectorsInBlock, SizeType p_maxmeta, int p_treeNumber, int p_neighborhoodSize);

std::uint64_t EstimatedMemoryUsage(std::uint64_t p_vectorCount, DimensionType p_dimension, VectorValueType p_valuetype, SizeType p_vectorsInBlock, SizeType p_maxmeta, int p_treeNumber, int p_neighborhoodSize);

}  // namespace SPTAG::BKT

#endif  // _SPTAG_BKT_INDEX_H_
