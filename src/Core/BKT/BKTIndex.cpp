// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/BKT/Index.h"
#include <chrono>

#pragma warning(disable : 4242)  // '=' : conversion from 'int' to 'short', possible loss of data
#pragma warning(disable : 4244)  // '=' : conversion from 'int' to 'short', possible loss of data
#pragma warning(disable : 4127)  // conditional expression is constant

namespace SPTAG::BKT {
template <typename T>
thread_local std::shared_ptr<COMMON::WorkSpace> Index<T>::m_workspace;

template <typename T>
ErrorCode Index<T>::LoadConfig(Helper::IniReader& p_reader) {
#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    SetParameter(RepresentStr, p_reader.GetParameter("Index", RepresentStr, std::string(#DefaultValue)).c_str());

#include "Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs) {
    if (p_indexBlobs.size() < 3)
        return ErrorCode::LackOfInputs;

    if (m_pSamples.Load((char*)p_indexBlobs[0].Data(), m_iDataBlockSize, m_iDataCapacity) != ErrorCode::Success)
        return ErrorCode::FailedParseValue;
    if (m_pTrees.LoadTrees((char*)p_indexBlobs[1].Data()) != ErrorCode::Success)
        return ErrorCode::FailedParseValue;
    if (m_pGraph.LoadGraph((char*)p_indexBlobs[2].Data(), m_iDataBlockSize, m_iDataCapacity) != ErrorCode::Success)
        return ErrorCode::FailedParseValue;
    if (p_indexBlobs.size() <= 3)
        m_deletedID.Initialize(m_pSamples.R(), m_iDataBlockSize, m_iDataCapacity);
    else if (m_deletedID.Load((char*)p_indexBlobs[3].Data(), m_iDataBlockSize, m_iDataCapacity) != ErrorCode::Success)
        return ErrorCode::FailedParseValue;

    omp_set_num_threads(m_iNumberOfThreads);
    m_threadPool.init();
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::LoadIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams) {
    if (p_indexStreams.size() < 4)
        return ErrorCode::LackOfInputs;

    ErrorCode ret = ErrorCode::Success;
    if (p_indexStreams[0] == nullptr || (ret = m_pSamples.Load(p_indexStreams[0], m_iDataBlockSize, m_iDataCapacity)) != ErrorCode::Success)
        return ret;
    if (p_indexStreams[1] == nullptr || (ret = m_pTrees.LoadTrees(p_indexStreams[1])) != ErrorCode::Success)
        return ret;
    if (p_indexStreams[2] == nullptr || (ret = m_pGraph.LoadGraph(p_indexStreams[2], m_iDataBlockSize, m_iDataCapacity)) != ErrorCode::Success)
        return ret;
    if (p_indexStreams[3] == nullptr)
        m_deletedID.Initialize(m_pSamples.R(), m_iDataBlockSize, m_iDataCapacity);
    else if ((ret = m_deletedID.Load(p_indexStreams[3], m_iDataBlockSize, m_iDataCapacity)) != ErrorCode::Success)
        return ret;

    omp_set_num_threads(m_iNumberOfThreads);
    m_threadPool.init();
    return ret;
}

template <typename T>
ErrorCode Index<T>::SaveConfig(std::shared_ptr<Helper::DiskIO> p_configOut) {
    if (m_workspace.get() != nullptr) {
        m_iHashTableExp = m_workspace->HashTableExponent();
    }

#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    IOSTRING(p_configOut, WriteString, (RepresentStr + std::string("=") + GetParameter(RepresentStr) + std::string("\n")).c_str());

#include "Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

    IOSTRING(p_configOut, WriteString, "\n");
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SaveIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams) {
    if (p_indexStreams.size() < 4)
        return ErrorCode::LackOfInputs;

    std::lock_guard<std::mutex> lock(m_dataAddLock);
    std::unique_lock<std::shared_timed_mutex> uniquelock(m_dataDeleteLock);

    ErrorCode ret = ErrorCode::Success;
    if ((ret = m_pSamples.Save(p_indexStreams[0])) != ErrorCode::Success)
        return ret;
    if ((ret = m_pTrees.SaveTrees(p_indexStreams[1])) != ErrorCode::Success)
        return ret;
    if ((ret = m_pGraph.SaveGraph(p_indexStreams[2])) != ErrorCode::Success)
        return ret;
    if ((ret = m_deletedID.Save(p_indexStreams[3])) != ErrorCode::Success)
        return ret;
    return ret;
}

#pragma region K-NN search
/*
#define Search(CheckDeleted, CheckDuplicated) \
        std::shared_lock<std::shared_timed_mutex> lock(*(m_pTrees.m_lock)); \
        m_pTrees.InitSearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space); \
        m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space, m_iNumberOfInitialDynamicPivots); \
        const DimensionType checkPos = m_pGraph.m_iNeighborhoodSize - 1; \
        while (!p_space.m_NGQueue.empty()) { \
            NodeDistPair gnode = p_space.m_NGQueue.pop(); \
            SizeType tmpNode = gnode.node; \
            const SizeType *node = m_pGraph[tmpNode]; \
            _mm_prefetch((const char *)node, _MM_HINT_T0); \
            for (DimensionType i = 0; i <= checkPos; i++) { \
                _mm_prefetch((const char *)(m_pSamples)[node[i]], _MM_HINT_T0); \
            } \
            if (gnode.distance <= p_query.worstDist()) { \
                SizeType checkNode = node[checkPos]; \
                if (checkNode < -1) { \
                    const COMMON::BKTNode& tnode = m_pTrees[-2 - checkNode]; \
                    SizeType i = -tnode.childStart; \
                    do { \
                        CheckDeleted \
                        { \
                            p_space.m_iNumOfContinuousNoBetterPropagation = 0; \
                            CheckDuplicated \
                            break; \
                        } \
                        tmpNode = m_pTrees[i].centerid; \
                    } while (i++ < tnode.childEnd); \
                } else { \
                    CheckDeleted \
                    { \
                        p_space.m_iNumOfContinuousNoBetterPropagation = 0; \
                        p_query.AddPoint(tmpNode, gnode.distance); \
                    } \
                } \
            } else { \
                p_space.m_iNumOfContinuousNoBetterPropagation++; \
                if (p_space.m_iNumOfContinuousNoBetterPropagation > p_space.m_iContinuousLimit || p_space.m_iNumberOfCheckedLeaves > p_space.m_iMaxCheck) { \
                    p_query.SortResult(); return; \
                } \
            } \
            for (DimensionType i = 0; i <= checkPos; i++) { \
                SizeType nn_index = node[i]; \
                if (nn_index < 0) break; \
                if (p_space.CheckAndSet(nn_index)) continue; \
                float distance2leaf = m_fComputeDistance(p_query.GetTarget(), (m_pSamples)[nn_index], GetFeatureDim()); \
                p_space.m_iNumberOfCheckedLeaves++; \
                p_space.m_NGQueue.insert(NodeDistPair(nn_index, distance2leaf)); \
            } \
            if (p_space.m_NGQueue.Top().distance > p_space.m_SPTQueue.Top().distance) { \
                m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space, m_iNumberOfOtherDynamicPivots + p_space.m_iNumberOfCheckedLeaves); \
            } \
        } \
        p_query.SortResult(); \
*/

// #define Search(CheckDeleted, CheckDuplicated) \
//         std::shared_lock<std::shared_timed_mutex> lock(*(m_pTrees.m_lock)); \
//         m_pTrees.InitSearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space); \
//         m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space, m_iNumberOfInitialDynamicPivots); \
//         const DimensionType checkPos = m_pGraph.m_iNeighborhoodSize - 1; \
//         while (!p_space.m_NGQueue.empty()) { \
//             NodeDistPair gnode = p_space.m_NGQueue.pop(); \
//             SizeType tmpNode = gnode.node; \
//             const SizeType *node = m_pGraph[tmpNode]; \
//             _mm_prefetch((const char *)node, _MM_HINT_T0); \
//             for (DimensionType i = 0; i <= checkPos; i++) { \
//                 if (node[i] < 0 || node[i] >= m_pSamples.R()) break; \
//                 _mm_prefetch((const char *)(m_pSamples)[node[i]], _MM_HINT_T0); \
//             } \
//             if (gnode.distance <= p_query.worstDist()) { \
//                 SizeType checkNode = node[checkPos]; \
//                 if (checkNode < -1) { \
//                     const COMMON::BKTNode& tnode = m_pTrees[-2 - checkNode]; \
//                     SizeType i = -tnode.childStart; \
//                     do { \
//                         CheckDeleted \
//                         { \
//                             CheckDuplicated \
//                             break; \
//                         } \
//                         tmpNode = m_pTrees[i].centerid; \
//                     } while (i++ < tnode.childEnd); \
//                } else { \
//                    CheckDeleted \
//                    { \
//                        p_query.AddPoint(tmpNode, gnode.distance); \
//                    } \
//                } \
//             } else { \
//                 CheckDeleted \
//                 { \
//                     if (gnode.distance > p_space.m_Results.worst() || p_space.m_iNumberOfCheckedLeaves > p_space.m_iMaxCheck) { \
//                         p_query.SortResult(); return; \
//                     } \
//                 } \
//             } \
//             for (DimensionType i = 0; i <= checkPos; i++) { \
//                 SizeType nn_index = node[i]; \
//                 if (nn_index < 0) break; \
//                 if (nn_index >= m_pSamples.R()) continue; \
//                 if (p_space.CheckAndSet(nn_index)) continue; \
//                 float distance2leaf = m_fComputeDistance(p_query.GetTarget(), (m_pSamples)[nn_index], GetFeatureDim()); \
//                 p_space.m_iNumberOfCheckedLeaves++; \
//                 if (p_space.m_Results.insert(distance2leaf)) { \
//                     p_space.m_NGQueue.insert(NodeDistPair(nn_index, distance2leaf)); \
//                 } \
//             } \
//             if (p_space.m_NGQueue.Top().distance > p_space.m_SPTQueue.Top().distance) { \
//                 m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space, m_iNumberOfOtherDynamicPivots + p_space.m_iNumberOfCheckedLeaves); \
//             } \
//         } \
//         p_query.SortResult(); \


template <typename T>
template <bool (*notDeleted)(const COMMON::Labelset&, SizeType), bool (*isDup)(COMMON::QueryResultSet<T>&, SizeType, float), bool (*checkFilter)(const std::shared_ptr<MetadataSet>&, SizeType, std::function<bool(const ByteArray&)>)>
void Index<T>::Search(COMMON::QueryResultSet<T>& p_query, COMMON::WorkSpace& p_space, std::function<bool(const ByteArray&)> filterFunc) const {
    std::shared_lock<std::shared_timed_mutex> lock(*(m_pTrees.m_lock));
    m_pTrees.InitSearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space);
    m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space, m_iNumberOfInitialDynamicPivots);
    const DimensionType checkPos = m_pGraph.m_iNeighborhoodSize - 1;

    while (!p_space.m_NGQueue.empty()) {
        NodeDistPair gnode = p_space.m_NGQueue.pop();
        SizeType tmpNode = gnode.node;
        const SizeType* node = m_pGraph[tmpNode];
        _mm_prefetch((const char*)node, _MM_HINT_T0);
        for (DimensionType i = 0; i <= checkPos; i++) {
            auto futureNode = node[i];
            if (futureNode < 0 || futureNode >= m_pSamples.R())
                break;
            _mm_prefetch((const char*)(m_pSamples)[futureNode], _MM_HINT_T0);
        }

        if (gnode.distance <= p_query.worstDist()) {
            SizeType checkNode = node[checkPos];
            if (checkNode < -1) {
                const COMMON::BKTNode& tnode = m_pTrees[-2 - checkNode];
                SizeType i = -tnode.childStart;
                do {
                    if (notDeleted(m_deletedID, tmpNode)) {
                        if (checkFilter(m_pMetadata, tmpNode, filterFunc)) {
                            if (isDup(p_query, tmpNode, gnode.distance))
                                break;
                        }
                    }
                    tmpNode = m_pTrees[i].centerid;
                } while (i++ < tnode.childEnd);
            } else {
                if (notDeleted(m_deletedID, tmpNode)) {
                    if (checkFilter(m_pMetadata, tmpNode, filterFunc)) {
                        p_query.AddPoint(tmpNode, gnode.distance);
                    }
                }
            }
        } else {
            if (notDeleted(m_deletedID, tmpNode)) {
                if (gnode.distance > p_space.m_Results.worst() || p_space.m_iNumberOfCheckedLeaves > p_space.m_iMaxCheck) {
                    p_query.SortResult();
                    return;
                }
            }
        }
        for (DimensionType i = 0; i <= checkPos; i++) {
            SizeType nn_index = node[i];
            if (nn_index < 0)
                break;
            // IF_NDEBUG(if (nn_index >= m_pSamples.R()) continue; )
            if (p_space.CheckAndSet(nn_index))
                continue;
            float distance2leaf = m_fComputeDistance(p_query.GetTarget(), (m_pSamples)[nn_index], GetFeatureDim());
            p_space.m_iNumberOfCheckedLeaves++;
            if (p_space.m_Results.insert(distance2leaf)) {
                p_space.m_NGQueue.insert(NodeDistPair(nn_index, distance2leaf));
            }
        }
        if (p_space.m_NGQueue.Top().distance > p_space.m_SPTQueue.Top().distance) {
            m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, p_query, p_space, m_iNumberOfOtherDynamicPivots + p_space.m_iNumberOfCheckedLeaves);
        }
    }
    p_query.SortResult();
}

namespace StaticDispatch {
template <typename... Args>
bool AlwaysTrue(Args...) {
    return true;
}

bool CheckIfNotDeleted(const COMMON::Labelset& deletedIDs, SizeType node) {
    return !deletedIDs.Contains(node);
}

template <typename T>
bool CheckDup(COMMON::QueryResultSet<T>& query, SizeType node, float score) {
    return !query.AddPoint(node, score);
}

template <typename T>
bool NeverDup(COMMON::QueryResultSet<T>& query, SizeType node, float score) {
    query.AddPoint(node, score);
    return false;
}

bool CheckFilter(const std::shared_ptr<MetadataSet>& metadata, SizeType node, std::function<bool(const ByteArray&)> filterFunc) {
    return filterFunc(metadata->GetMetadata(node));
}

};  // namespace StaticDispatch

template <typename T>
void Index<T>::SearchIndex(COMMON::QueryResultSet<T>& p_query, COMMON::WorkSpace& p_space, bool p_searchDeleted, bool p_searchDuplicated, std::function<bool(const ByteArray&)> filterFunc) const {
    // bitflags for which dispatch to take
    uint8_t flags = 0;
    flags += (m_deletedID.Count() == 0 || p_searchDeleted) << 2;
    flags += p_searchDuplicated << 1;
    flags += (filterFunc == nullptr);

    switch (flags) {
        case 0b000:
            Search<StaticDispatch::CheckIfNotDeleted, StaticDispatch::NeverDup, StaticDispatch::CheckFilter>(p_query, p_space, filterFunc);
            break;
        case 0b001:
            Search<StaticDispatch::CheckIfNotDeleted, StaticDispatch::NeverDup, StaticDispatch::AlwaysTrue>(p_query, p_space, filterFunc);
            break;
        case 0b010:
            Search<StaticDispatch::CheckIfNotDeleted, StaticDispatch::CheckDup, StaticDispatch::CheckFilter>(p_query, p_space, filterFunc);
            break;
        case 0b011:
            Search<StaticDispatch::CheckIfNotDeleted, StaticDispatch::CheckDup, StaticDispatch::AlwaysTrue>(p_query, p_space, filterFunc);
            break;
        case 0b100:
            Search<StaticDispatch::AlwaysTrue, StaticDispatch::NeverDup, StaticDispatch::CheckFilter>(p_query, p_space, filterFunc);
            break;
        case 0b101:
            Search<StaticDispatch::AlwaysTrue, StaticDispatch::NeverDup, StaticDispatch::AlwaysTrue>(p_query, p_space, filterFunc);
            break;
        case 0b110:
            Search<StaticDispatch::AlwaysTrue, StaticDispatch::CheckDup, StaticDispatch::CheckFilter>(p_query, p_space, filterFunc);
            break;
        case 0b111:
            Search<StaticDispatch::AlwaysTrue, StaticDispatch::CheckDup, StaticDispatch::AlwaysTrue>(p_query, p_space, filterFunc);
            break;
        default:
            std::ostringstream oss;
            oss << "Invalid flags in BKT SearchIndex dispatch: " << flags;
            throw std::logic_error(oss.str());
    }
}

template <typename T>
ErrorCode Index<T>::SearchIndex(QueryResult& p_query, bool p_searchDeleted) const {
    if (!m_bReady)
        return ErrorCode::EmptyIndex;

    if (m_workspace.get() == nullptr) {
        m_workspace.reset(new COMMON::WorkSpace());
        m_workspace->Initialize(max(m_iMaxCheck, m_pGraph.m_iMaxCheckForRefineGraph), m_iHashTableExp);
    }
    m_workspace->Reset(m_iMaxCheck, p_query.GetResultNum());
    SearchIndex(*((COMMON::QueryResultSet<T>*)&p_query), *m_workspace, p_searchDeleted, true);

    if (p_query.WithMeta() && nullptr != m_pMetadata) {
        for (int i = 0; i < p_query.GetResultNum(); ++i) {
            SizeType result = p_query.GetResult(i)->VID;
            p_query.SetMetadata(i, (result < 0) ? ByteArray::c_empty : m_pMetadata->GetMetadataCopy(result));
        }
    }
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::RefineSearchIndex(QueryResult& p_query, bool p_searchDeleted) const {
    if (m_workspace.get() == nullptr) {
        m_workspace.reset(new COMMON::WorkSpace());
        m_workspace->Initialize(max(m_iMaxCheck, m_pGraph.m_iMaxCheckForRefineGraph), m_iHashTableExp);
    }
    m_workspace->Reset(m_pGraph.m_iMaxCheckForRefineGraph, p_query.GetResultNum());
    SearchIndex(*((COMMON::QueryResultSet<T>*)&p_query), *m_workspace, p_searchDeleted, false);

    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SearchTree(QueryResult& p_query) const {
    if (m_workspace.get() == nullptr) {
        m_workspace.reset(new COMMON::WorkSpace());
        m_workspace->Initialize(max(m_iMaxCheck, m_pGraph.m_iMaxCheckForRefineGraph), m_iHashTableExp);
    }
    m_workspace->Reset(m_pGraph.m_iMaxCheckForRefineGraph, p_query.GetResultNum());

    COMMON::QueryResultSet<T>* p_results = (COMMON::QueryResultSet<T>*)&p_query;
    m_pTrees.InitSearchTrees(m_pSamples, m_fComputeDistance, *p_results, *m_workspace);
    m_pTrees.SearchTrees(m_pSamples, m_fComputeDistance, *p_results, *m_workspace, m_iNumberOfInitialDynamicPivots);
    BasicResult* res = p_query.GetResults();
    for (int i = 0; i < p_query.GetResultNum(); i++) {
        auto& cell = m_workspace->m_NGQueue.pop();
        res[i].VID = cell.node;
        res[i].Dist = cell.distance;
    }
    return ErrorCode::Success;
}
#pragma endregion

template <typename T>
ErrorCode Index<T>::BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, bool p_normalized, bool p_shareOwnership) {
    if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0)
        return ErrorCode::EmptyData;

    omp_set_num_threads(m_iNumberOfThreads);

    m_pSamples.Initialize(p_vectorNum, p_dimension, m_iDataBlockSize, m_iDataCapacity, p_data, p_shareOwnership);
    m_deletedID.Initialize(p_vectorNum, m_iDataBlockSize, m_iDataCapacity);

    if (DistCalcMethod::Cosine == m_iDistCalcMethod && !p_normalized) {
        int base = COMMON::Utils::GetBase<T>();
#pragma omp parallel for
        for (SizeType i = 0; i < GetNumSamples(); i++) {
            COMMON::Utils::Normalize(m_pSamples[i], GetFeatureDim(), base);
        }
    }

    m_threadPool.init();

    auto t1 = std::chrono::high_resolution_clock::now();
    m_pTrees.BuildTrees<T>(m_pSamples, m_iDistCalcMethod, m_iNumberOfThreads);
    auto t2 = std::chrono::high_resolution_clock::now();
    LOG(Helper::LogLevel::LL_Info, "Build Tree time (s): %lld\n", std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count());

    m_pGraph.BuildGraph<T>(this, &(m_pTrees.GetSampleMap()));

    auto t3 = std::chrono::high_resolution_clock::now();
    LOG(Helper::LogLevel::LL_Info, "Build Graph time (s): %lld\n", std::chrono::duration_cast<std::chrono::seconds>(t3 - t2).count());

    m_bReady = true;
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::RefineIndex(std::shared_ptr<Index<T>>& p_newIndex) {
    p_newIndex.reset(new Index<T>());
    Index<T>* ptr = p_newIndex.get();

#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    ptr->VarName = VarName;

#include "Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

    std::lock_guard<std::mutex> lock(m_dataAddLock);
    std::unique_lock<std::shared_timed_mutex> uniquelock(m_dataDeleteLock);

    SizeType newR = GetNumSamples();

    std::vector<SizeType> indices;
    std::vector<SizeType> reverseIndices(newR);
    for (SizeType i = 0; i < newR; i++) {
        if (!m_deletedID.Contains(i)) {
            indices.push_back(i);
            reverseIndices[i] = i;
        } else {
            while (m_deletedID.Contains(newR - 1) && newR > i)
                newR--;
            if (newR == i)
                break;
            indices.push_back(newR - 1);
            reverseIndices[newR - 1] = i;
            newR--;
        }
    }

    LOG(Helper::LogLevel::LL_Info, "Refine... from %d -> %d\n", GetNumSamples(), newR);
    if (newR == 0)
        return ErrorCode::EmptyIndex;

    ptr->m_threadPool.init();

    ErrorCode ret = ErrorCode::Success;
    if ((ret = m_pSamples.Refine(indices, ptr->m_pSamples)) != ErrorCode::Success)
        return ret;
    if (nullptr != m_pMetadata && (ret = m_pMetadata->RefineMetadata(indices, ptr->m_pMetadata, m_iDataBlockSize, m_iDataCapacity, m_iMetaRecordSize)) != ErrorCode::Success)
        return ret;

    ptr->m_deletedID.Initialize(newR, m_iDataBlockSize, m_iDataCapacity);
    COMMON::BKTree* newtree = &(ptr->m_pTrees);
    (*newtree).BuildTrees<T>(ptr->m_pSamples, ptr->m_iDistCalcMethod, omp_get_num_threads());
    m_pGraph.RefineGraph<T>(this, indices, reverseIndices, nullptr, &(ptr->m_pGraph), &(ptr->m_pTrees.GetSampleMap()));
    if (HasMetaMapping())
        ptr->BuildMetaMapping(false);
    ptr->m_bReady = true;
    return ret;
}

template <typename T>
ErrorCode Index<T>::RefineIndex(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams, IAbortOperation* p_abort) {
    std::lock_guard<std::mutex> lock(m_dataAddLock);
    std::unique_lock<std::shared_timed_mutex> uniquelock(m_dataDeleteLock);

    SizeType newR = GetNumSamples();

    std::vector<SizeType> indices;
    std::vector<SizeType> reverseIndices(newR);
    for (SizeType i = 0; i < newR; i++) {
        if (!m_deletedID.Contains(i)) {
            indices.push_back(i);
            reverseIndices[i] = i;
        } else {
            while (m_deletedID.Contains(newR - 1) && newR > i)
                newR--;
            if (newR == i)
                break;
            indices.push_back(newR - 1);
            reverseIndices[newR - 1] = i;
            newR--;
        }
    }

    LOG(Helper::LogLevel::LL_Info, "Refine... from %d -> %d\n", GetNumSamples(), newR);
    if (newR == 0)
        return ErrorCode::EmptyIndex;

    ErrorCode ret = ErrorCode::Success;
    if ((ret = m_pSamples.Refine(indices, p_indexStreams[0])) != ErrorCode::Success)
        return ret;

    if (p_abort != nullptr && p_abort->ShouldAbort())
        return ErrorCode::ExternalAbort;

    COMMON::BKTree newTrees(m_pTrees);
    newTrees.BuildTrees<T>(m_pSamples, m_iDistCalcMethod, omp_get_num_threads(), &indices, &reverseIndices);
    if ((ret = newTrees.SaveTrees(p_indexStreams[1])) != ErrorCode::Success)
        return ret;

    if (p_abort != nullptr && p_abort->ShouldAbort())
        return ErrorCode::ExternalAbort;

    if ((ret = m_pGraph.RefineGraph<T>(this, indices, reverseIndices, p_indexStreams[2], nullptr, &(newTrees.GetSampleMap()))) != ErrorCode::Success)
        return ret;

    COMMON::Labelset newDeletedID;
    newDeletedID.Initialize(newR, m_iDataBlockSize, m_iDataCapacity);
    if ((ret = newDeletedID.Save(p_indexStreams[3])) != ErrorCode::Success)
        return ret;
    if (nullptr != m_pMetadata) {
        if (p_indexStreams.size() < 6)
            return ErrorCode::LackOfInputs;
        if ((ret = m_pMetadata->RefineMetadata(indices, p_indexStreams[4], p_indexStreams[5])) != ErrorCode::Success)
            return ret;
    }
    return ret;
}

template <typename T>
ErrorCode Index<T>::DeleteIndex(const void* p_vectors, SizeType p_vectorNum) {
    const T* ptr_v = (const T*)p_vectors;
#pragma omp parallel for schedule(dynamic)
    for (SizeType i = 0; i < p_vectorNum; i++) {
        COMMON::QueryResultSet<T> query(ptr_v + i * GetFeatureDim(), m_pGraph.m_iCEF);
        SearchIndex(query);

        for (int i = 0; i < m_pGraph.m_iCEF; i++) {
            if (query.GetResult(i)->Dist < 1e-6) {
                DeleteIndex(query.GetResult(i)->VID);
            }
        }
    }
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::DeleteIndex(const SizeType& p_id) {
    if (!m_bReady)
        return ErrorCode::EmptyIndex;

    std::shared_lock<std::shared_timed_mutex> sharedlock(m_dataDeleteLock);
    if (m_deletedID.Insert(p_id))
        return ErrorCode::Success;
    return ErrorCode::VectorNotFound;
}

template <typename T>
ErrorCode Index<T>::AddIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex, bool p_normalized) {
    if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0)
        return ErrorCode::EmptyData;

    SizeType begin, end;
    ErrorCode ret;
    {
        std::lock_guard<std::mutex> lock(m_dataAddLock);

        begin = GetNumSamples();
        end = begin + p_vectorNum;

        if (begin == 0) {
            if (p_metadataSet != nullptr) {
                m_pMetadata.reset(new MemMetadataSet(m_iDataBlockSize, m_iDataCapacity, m_iMetaRecordSize));
                m_pMetadata->AddBatch(*p_metadataSet);
                if (p_withMetaIndex)
                    BuildMetaMapping(false);
            }
            if ((ret = BuildIndex(p_data, p_vectorNum, p_dimension, p_normalized)) != ErrorCode::Success)
                return ret;
            return ErrorCode::Success;
        }

        if (p_dimension != GetFeatureDim())
            return ErrorCode::DimensionSizeMismatch;

        if (m_pSamples.AddBatch(p_vectorNum, (const T*)p_data) != ErrorCode::Success ||
            m_pGraph.AddBatch(p_vectorNum) != ErrorCode::Success ||
            m_deletedID.AddBatch(p_vectorNum) != ErrorCode::Success) {
            LOG(Helper::LogLevel::LL_Error, "Memory Error: Cannot alloc space for vectors!\n");
            m_pSamples.SetR(begin);
            m_pGraph.SetR(begin);
            m_deletedID.SetR(begin);
            return ErrorCode::MemoryOverFlow;
        }

        if (m_pMetadata != nullptr) {
            if (p_metadataSet != nullptr) {
                m_pMetadata->AddBatch(*p_metadataSet);
                if (HasMetaMapping()) {
                    for (SizeType i = begin; i < end; i++) {
                        ByteArray meta = m_pMetadata->GetMetadata(i);
                        std::string metastr((char*)meta.Data(), meta.Length());
                        UpdateMetaMapping(metastr, i);
                    }
                }
            } else {
                for (SizeType i = begin; i < end; i++)
                    m_pMetadata->Add(ByteArray::c_empty);
            }
        }
    }

    if (DistCalcMethod::Cosine == m_iDistCalcMethod && !p_normalized) {
        int base = COMMON::Utils::GetBase<T>();
        for (SizeType i = begin; i < end; i++) {
            COMMON::Utils::Normalize((T*)m_pSamples[i], GetFeatureDim(), base);
        }
    }

    if (end - m_pTrees.sizePerTree() >= m_addCountForRebuild && m_threadPool.jobsize() == 0) {
        m_threadPool.add(new RebuildJob(&m_pSamples, &m_pTrees, &m_pGraph, m_iDistCalcMethod));
    }

    for (SizeType node = begin; node < end; node++) {
        m_pGraph.RefineNode<T>(this, node, true, true, m_pGraph.m_iAddCEF);
    }
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::AddIndexId(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, int& beginHead, int& endHead) {
    if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0)
        return ErrorCode::EmptyData;

    SizeType begin, end;
    {
        std::lock_guard<std::mutex> lock(m_dataAddLock);

        begin = GetNumSamples();
        end = begin + p_vectorNum;

        if (begin == 0) {
            LOG(Helper::LogLevel::LL_Error, "Index Error: No vector in Index!\n");
            return ErrorCode::EmptyIndex;
        }

        if (p_dimension != GetFeatureDim())
            return ErrorCode::DimensionSizeMismatch;

        if (m_pSamples.AddBatch(p_vectorNum, (const T*)p_data) != ErrorCode::Success ||
            m_pGraph.AddBatch(p_vectorNum) != ErrorCode::Success ||
            m_deletedID.AddBatch(p_vectorNum) != ErrorCode::Success) {
            LOG(Helper::LogLevel::LL_Error, "Memory Error: Cannot alloc space for vectors!\n");
            m_pSamples.SetR(begin);
            m_pGraph.SetR(begin);
            m_deletedID.SetR(begin);
            return ErrorCode::MemoryOverFlow;
        }
    }
    beginHead = begin;
    endHead = end;
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::AddIndexIdx(SizeType begin, SizeType end) {
    // if (end - m_pTrees.sizePerTree() >= m_addCountForRebuild && m_threadPool.jobsize() == 0) {
    //     m_threadPool.add(new RebuildJob(&m_pSamples, &m_pTrees, &m_pGraph, m_iDistCalcMethod));
    // }

    for (SizeType node = begin; node < end; node++) {
        m_pGraph.RefineNode<T>(this, node, true, true, m_pGraph.m_iAddCEF);
    }
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::MergeIndex(Index<T>* p_addindex, int p_threadnum, IAbortOperation* p_abort) {
    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    if (p_addindex->m_pMetadata != nullptr) {
#pragma omp parallel for num_threads(p_threadnum) schedule(dynamic, 128)
        for (SPTAG::SizeType i = 0; i < p_addindex->GetNumSamples(); i++) {
            if (ret == SPTAG::ErrorCode::ExternalAbort)
                continue;

            if (p_addindex->ContainSample(i)) {
                SPTAG::ByteArray meta = p_addindex->GetMetadata(i);
                std::uint64_t offsets[2] = {0, meta.Length()};
                std::shared_ptr<SPTAG::MetadataSet> p_metaSet(new SPTAG::MemMetadataSet(meta, SPTAG::ByteArray((std::uint8_t*)offsets, sizeof(offsets), false), 1));
                AddIndex(p_addindex->GetSample(i), 1, p_addindex->GetFeatureDim(), p_metaSet);
            }

            if (p_abort != nullptr && p_abort->ShouldAbort()) {
                ret = SPTAG::ErrorCode::ExternalAbort;
            }
        }
    } else {
#pragma omp parallel for num_threads(p_threadnum) schedule(dynamic, 128)
        for (SPTAG::SizeType i = 0; i < p_addindex->GetNumSamples(); i++) {
            if (ret == SPTAG::ErrorCode::ExternalAbort)
                continue;

            if (p_addindex->ContainSample(i)) {
                AddIndex(p_addindex->GetSample(i), 1, p_addindex->GetFeatureDim(), nullptr);
            }

            if (p_abort != nullptr && p_abort->ShouldAbort()) {
                ret = SPTAG::ErrorCode::ExternalAbort;
            }
        }
    }
    return ret;
}

template <typename T>
ErrorCode
Index<T>::UpdateIndex() {
    // m_pTrees.Rebuild(m_pSamples, m_iDistCalcMethod, nullptr);
    // temporarily reuse this api to rebuild bkt tree
    omp_set_num_threads(m_iNumberOfThreads);
    return ErrorCode::Success;
}

template <typename T>
ErrorCode
Index<T>::SetParameter(const char* p_param, const char* p_value, const char* p_section) {
    if (nullptr == p_param || nullptr == p_value)
        return ErrorCode::Fail;

#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr)                     \
    else if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {           \
        LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n", RepresentStr, p_value); \
        VarType tmp;                                                                         \
        if (SPTAG::Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {                \
            VarName = tmp;                                                                   \
        }                                                                                    \
    }

#include "Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

    if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, "DistCalcMethod")) {
        m_fComputeDistance = COMMON::DistanceCalcSelector<T>(m_iDistCalcMethod);
        m_iBaseSquare = (m_iDistCalcMethod == DistCalcMethod::Cosine) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() : 1;
    }
    return ErrorCode::Success;
}

template <typename T>
std::string
Index<T>::GetParameter(const char* p_param, const char* p_section) const {
    if (nullptr == p_param)
        return std::string();

#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr)           \
    else if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) { \
        return SPTAG::Helper::Convert::ConvertToString(VarName);                   \
    }

#include "Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

    return std::string();
}

template <typename T>
SPTAG::ErrorCode
Index<T>::LoadIndex(const std::string& p_loaderFilePath, std::shared_ptr<Index<T>>& p_index) {
    std::string folderPath(p_loaderFilePath);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep)
        folderPath += FolderSep;

    Helper::IniReader iniReader;
    {
        auto fp = f_createIO();
        if (fp == nullptr || !fp->Initialize((folderPath + "indexloader.ini").c_str(), std::ios::in))
            return ErrorCode::FailedOpenFile;
        if (ErrorCode::Success != iniReader.LoadIni(fp))
            return ErrorCode::FailedParseValue;
    }

    p_index.reset(new Index<T>());

    ErrorCode ret = ErrorCode::Success;
    if ((ret = p_index->LoadIndexConfig(iniReader)) != ErrorCode::Success)
        return ret;

    std::shared_ptr<std::vector<std::string>> indexfiles = p_index->GetIndexFiles();
    if (iniReader.DoesSectionExist("MetaData")) {
        indexfiles->push_back(p_index->m_metadataManager.GetMetadataFile());
        indexfiles->push_back(p_index->m_metadataManager.GetMetadataIndexFile());
    }
    std::vector<std::shared_ptr<Helper::DiskIO>> handles;
    for (std::string& f : *indexfiles) {
        auto ptr = f_createIO();
        if (ptr == nullptr || !ptr->Initialize((folderPath + f).c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Cannot open file %s!\n", (folderPath + f).c_str());
            ptr = nullptr;
        }
        handles.push_back(std::move(ptr));
    }

    if ((ret = p_index->LoadIndexData(handles)) != ErrorCode::Success)
        return ret;

    size_t metaStart = p_index->GetIndexFiles()->size();
    if (iniReader.DoesSectionExist("MetaData")) {
        p_index->SetMetadata(new SPTAG::MemMetadataSet(handles[metaStart], handles[metaStart + 1], p_index->m_iDataBlockSize, p_index->m_iDataCapacity, p_index->m_iMetaRecordSize));

        if (!(p_index->GetMetadata()->Available())) {
            LOG(Helper::LogLevel::LL_Error, "Error: Failed to load metadata.\n");
            return ErrorCode::Fail;
        }

        if (iniReader.GetParameter("MetaData", "MetaDataToVectorIndex", std::string()) == "true") {
            p_index->BuildMetaMapping();
        }
    }
    p_index->m_bReady = true;
    return ErrorCode::Success;
}

std::uint64_t EstimatedVectorCount(std::uint64_t p_memory, DimensionType p_dimension, VectorValueType p_valuetype, SizeType p_vectorsInBlock, SizeType p_maxmeta, int p_treeNumber, int p_neighborhoodSize) {
    size_t treeNodeSize = sizeof(SizeType) * 3;
    std::uint64_t unit = GetValueTypeSize(p_valuetype) * p_dimension + p_maxmeta + sizeof(std::uint64_t) + sizeof(SizeType) * p_neighborhoodSize + 1 + treeNodeSize * p_treeNumber;
    return ((p_memory / unit) / p_vectorsInBlock) * p_vectorsInBlock;
}

std::uint64_t EstimatedMemoryUsage(std::uint64_t p_vectorCount, DimensionType p_dimension, VectorValueType p_valuetype, SizeType p_vectorsInBlock, SizeType p_maxmeta, int p_treeNumber, int p_neighborhoodSize) {
    p_vectorCount = ((p_vectorCount + p_vectorsInBlock - 1) / p_vectorsInBlock) * p_vectorsInBlock;
    size_t treeNodeSize = sizeof(SizeType) * 3;
    std::uint64_t ret = GetValueTypeSize(p_valuetype) * p_dimension * p_vectorCount;  // Vector Size
    ret += p_maxmeta * p_vectorCount;                                                 // MetaData Size
    ret += sizeof(std::uint64_t) * p_vectorCount;                                     // MetaIndex Size
    ret += sizeof(SizeType) * p_neighborhoodSize * p_vectorCount;                     // Graph Size
    ret += p_vectorCount;                                                             // DeletedFlag Size
    ret += treeNodeSize * p_treeNumber * p_vectorCount;                               // Tree Size
    return ret;
}

template <typename T>
std::string Index<T>::GetParameter(const std::string& p_param, const std::string& p_section) const {
    return GetParameter(p_param.c_str(), p_section.c_str());
}

template <typename T>
ErrorCode Index<T>::SetParameter(const std::string& p_param, const std::string& p_value, const std::string& p_section) {
    return SetParameter(p_param.c_str(), p_value.c_str(), p_section.c_str());
}

template <typename T>
void Index<T>::SetMetadata(MetadataSet* p_new) {
    m_pMetadata.reset(p_new);
}

template <typename T>
MetadataSet* Index<T>::GetMetadata() const {
    return m_pMetadata.get();
}

template <typename T>
ByteArray Index<T>::GetMetadata(SizeType p_vectorID) const {
    if (nullptr != m_pMetadata) {
        return m_pMetadata->GetMetadata(p_vectorID);
    }
    return ByteArray::c_empty;
}

template <typename T>
std::shared_ptr<std::vector<std::uint64_t>> Index<T>::CalculateBufferSize() const {
    std::shared_ptr<std::vector<std::uint64_t>> ret = BufferSize();

    if (m_pMetadata != nullptr) {
        auto metasize = m_pMetadata->BufferSize();
        ret->push_back(metasize.first);
        ret->push_back(metasize.second);
    }
    return std::move(ret);
}

template <typename T>
ErrorCode Index<T>::LoadIndexConfig(Helper::IniReader& p_reader) {
    std::string metadataSection("MetaData");
    if (p_reader.DoesSectionExist(metadataSection)) {
        m_metadataManager.SetMetadataFile(p_reader.GetParameter(metadataSection, "MetaDataFilePath", std::string()));
        m_metadataManager.SetMetadataIndexFile(p_reader.GetParameter(metadataSection, "MetaDataIndexPath", std::string()));
    }
    return LoadConfig(p_reader);
}

template <typename T>
ErrorCode Index<T>::SaveIndexConfig(std::shared_ptr<Helper::DiskIO> p_configOut) {
    if (nullptr != m_pMetadata) {
        IOSTRING(p_configOut, WriteString, "[MetaData]\n");
        IOSTRING(p_configOut, WriteString, ("MetaDataFilePath=" + m_metadataManager.GetMetadataFile() + "\n").c_str());
        IOSTRING(p_configOut, WriteString, ("MetaDataIndexPath=" + m_metadataManager.GetMetadataIndexFile() + "\n").c_str());
        if (m_metadataManager.HasMetaMapping())
            IOSTRING(p_configOut, WriteString, "MetaDataToVectorIndex=true\n");
        IOSTRING(p_configOut, WriteString, "\n");
    }

    IOSTRING(p_configOut, WriteString, "[Index]\n");
    IOSTRING(p_configOut, WriteString, ("IndexAlgoType=" + Helper::Convert::ConvertToString(GetIndexAlgoType()) + "\n").c_str());
    IOSTRING(p_configOut, WriteString, ("ValueType=" + Helper::Convert::ConvertToString(GetVectorValueType()) + "\n").c_str());
    IOSTRING(p_configOut, WriteString, "\n");

    return SaveConfig(p_configOut);
}

template <typename T>
SizeType Index<T>::GetMetaMapping(std::string& meta) const {
    return m_metadataManager.GetMetaMapping(meta);
}

template <typename T>
void Index<T>::UpdateMetaMapping(const std::string& meta, SizeType i) {
    SizeType existing = m_metadataManager.GetMetaMapping(const_cast<std::string&>(meta));
    if (existing >= 0)
        DeleteIndex(existing);
    m_metadataManager.UpdateMetaMapping(meta, i);
}

template <typename T>
void Index<T>::BuildMetaMapping(bool p_checkDeleted) {
    m_metadataManager.BuildMetaMapping(m_pMetadata.get(), GetNumSamples(), std::function<bool(SizeType)>([this](SizeType idx) -> bool {
                                           return this->ContainSample(idx);
                                       }),
                                       m_iDataBlockSize, p_checkDeleted);
}

template <typename T>
ErrorCode Index<T>::SaveIndex(std::string& p_config, const std::vector<ByteArray>& p_indexBlobs) {
    if (!m_bReady || GetNumSamples() - GetNumDeleted() == 0)
        return ErrorCode::EmptyIndex;

    ErrorCode ret = ErrorCode::Success;
    {
        std::shared_ptr<Helper::DiskIO> p_configStream(new Helper::SimpleBufferIO());
        if (p_configStream == nullptr || !p_configStream->Initialize(nullptr, std::ios::out))
            return ErrorCode::EmptyDiskIO;
        if ((ret = SaveIndexConfig(p_configStream)) != ErrorCode::Success)
            return ret;
        p_config.resize(p_configStream->TellP());
        IOBINARY(p_configStream, ReadBinary, p_config.size(), (char*)p_config.c_str(), 0);
    }

    std::vector<std::shared_ptr<Helper::DiskIO>> p_indexStreams;
    for (size_t i = 0; i < p_indexBlobs.size(); i++) {
        std::shared_ptr<Helper::DiskIO> ptr(new Helper::SimpleBufferIO());
        if (ptr == nullptr || !ptr->Initialize((char*)p_indexBlobs[i].Data(), std::ios::binary | std::ios::out, p_indexBlobs[i].Length()))
            return ErrorCode::EmptyDiskIO;
        p_indexStreams.push_back(std::move(ptr));
    }

    size_t metaStart = BufferSize()->size();
    if (NeedRefine()) {
        ret = RefineIndex(p_indexStreams, nullptr);
    } else {
        if (m_pMetadata != nullptr && p_indexStreams.size() >= metaStart + 2) {
            ret = m_pMetadata->SaveMetadata(p_indexStreams[metaStart], p_indexStreams[metaStart + 1]);
        }
        if (ErrorCode::Success == ret)
            ret = SaveIndexData(p_indexStreams);
    }
    return ret;
}

template <typename T>
ErrorCode Index<T>::SaveIndex(const std::string& p_folderPath) {
    if (!m_bReady || GetNumSamples() - GetNumDeleted() == 0)
        return ErrorCode::EmptyIndex;

    std::string folderPath(p_folderPath);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep) {
        folderPath += FolderSep;
    }
    if (!direxists(folderPath.c_str())) {
        mkdir(folderPath.c_str());
    }

    ErrorCode ret = ErrorCode::Success;
    {
        auto configFile = f_createIO();
        if (configFile == nullptr || !configFile->Initialize((folderPath + "indexloader.ini").c_str(), std::ios::out))
            return ErrorCode::FailedCreateFile;
        if ((ret = SaveIndexConfig(configFile)) != ErrorCode::Success)
            return ret;
    }

    std::shared_ptr<std::vector<std::string>> indexfiles = GetIndexFiles();
    if (nullptr != m_pMetadata) {
        indexfiles->push_back(m_metadataManager.GetMetadataFile());
        indexfiles->push_back(m_metadataManager.GetMetadataIndexFile());
    }
    std::vector<std::shared_ptr<Helper::DiskIO>> handles;
    for (std::string& f : *indexfiles) {
        std::string newfile = folderPath + f;
        if (!direxists(newfile.substr(0, newfile.find_last_of(FolderSep)).c_str()))
            mkdir(newfile.substr(0, newfile.find_last_of(FolderSep)).c_str());

        auto ptr = f_createIO();
        if (ptr == nullptr || !ptr->Initialize(newfile.c_str(), std::ios::binary | std::ios::out))
            return ErrorCode::FailedCreateFile;
        handles.push_back(std::move(ptr));
    }

    size_t metaStart = GetIndexFiles()->size();
    if (NeedRefine()) {
        ret = RefineIndex(handles, nullptr);
    } else {
        if (m_pMetadata != nullptr)
            ret = m_pMetadata->SaveMetadata(handles[metaStart], handles[metaStart + 1]);
        if (ErrorCode::Success == ret)
            ret = SaveIndexData(handles);
    }
    return ret;
}

template <typename T>
ErrorCode Index<T>::SaveIndexToFile(const std::string& p_file, IAbortOperation* p_abort) {
    if (!m_bReady || GetNumSamples() - GetNumDeleted() == 0)
        return ErrorCode::EmptyIndex;

    auto fp = f_createIO();
    if (fp == nullptr || !fp->Initialize(p_file.c_str(), std::ios::binary | std::ios::out))
        return ErrorCode::FailedCreateFile;

    auto mp = std::shared_ptr<Helper::DiskIO>(new Helper::SimpleBufferIO());
    if (mp == nullptr || !mp->Initialize(nullptr, std::ios::binary | std::ios::out))
        return ErrorCode::FailedCreateFile;
    ErrorCode ret = ErrorCode::Success;
    if ((ret = SaveIndexConfig(mp)) != ErrorCode::Success)
        return ret;

    std::uint64_t configSize = mp->TellP();
    mp->ShutDown();

    IOBINARY(fp, WriteBinary, sizeof(configSize), (char*)&configSize);
    if ((ret = SaveIndexConfig(fp)) != ErrorCode::Success)
        return ret;

    if (p_abort != nullptr && p_abort->ShouldAbort())
        ret = ErrorCode::ExternalAbort;
    else {
        std::uint64_t blobs = CalculateBufferSize()->size();
        IOBINARY(fp, WriteBinary, sizeof(blobs), (char*)&blobs);
        std::vector<std::shared_ptr<Helper::DiskIO>> p_indexStreams(blobs, fp);

        if (NeedRefine()) {
            ret = RefineIndex(p_indexStreams, p_abort);
        } else {
            ret = SaveIndexData(p_indexStreams);

            if (p_abort != nullptr && p_abort->ShouldAbort())
                ret = ErrorCode::ExternalAbort;

            if (ErrorCode::Success == ret && m_pMetadata != nullptr)
                ret = m_pMetadata->SaveMetadata(fp, fp);
        }
    }
    fp->ShutDown();

    if (ret != ErrorCode::Success)
        std::remove(p_file.c_str());
    return ret;
}

template <typename T>
ErrorCode Index<T>::BuildIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex, bool p_normalized, bool p_shareOwnership) {
    LOG(Helper::LogLevel::LL_Info, "Begin build index...\n");

    if (nullptr == p_vectorSet || p_vectorSet->GetValueType() != GetVectorValueType()) {
        return ErrorCode::Fail;
    }
    m_pMetadata = std::move(p_metadataSet);
    if (p_withMetaIndex && m_pMetadata != nullptr) {
        LOG(Helper::LogLevel::LL_Info, "Build meta mapping...\n");
        BuildMetaMapping(false);
    }
    BuildIndex(p_vectorSet->GetData(), p_vectorSet->Count(), p_vectorSet->Dimension(), p_normalized, p_shareOwnership);
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SearchIndex(const void* p_vector, int p_vectorCount, int p_neighborCount, bool p_withMeta, BasicResult* p_results) const {
    size_t vectorSize = GetValueTypeSize(GetVectorValueType()) * GetFeatureDim();
#pragma omp parallel for schedule(dynamic, 10)
    for (int i = 0; i < p_vectorCount; i++) {
        QueryResult res((char*)p_vector + i * vectorSize, p_neighborCount, p_withMeta, p_results + i * p_neighborCount);
        SearchIndex(res);
    }
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::AddIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex, bool p_normalized) {
    if (nullptr == p_vectorSet || p_vectorSet->GetValueType() != GetVectorValueType()) {
        return ErrorCode::Fail;
    }

    return AddIndex(p_vectorSet->GetData(), p_vectorSet->Count(), p_vectorSet->Dimension(), p_metadataSet, p_withMetaIndex, p_normalized);
}

template <typename T>
ErrorCode Index<T>::DeleteIndex(ByteArray p_meta) {
    if (!m_metadataManager.HasMetaMapping())
        return ErrorCode::VectorNotFound;

    std::string meta((char*)p_meta.Data(), p_meta.Length());
    SizeType vid = GetMetaMapping(meta);
    if (vid >= 0)
        return DeleteIndex(vid);
    return ErrorCode::VectorNotFound;
}

template <typename T>
const void* Index<T>::GetSample(ByteArray p_meta, bool& deleteFlag) {
    if (!m_metadataManager.HasMetaMapping())
        return nullptr;

    std::string meta((char*)p_meta.Data(), p_meta.Length());
    SizeType vid = GetMetaMapping(meta);
    if (vid >= 0 && vid < GetNumSamples()) {
        deleteFlag = !ContainSample(vid);
        return GetSample(vid);
    }
    return nullptr;
}

#if defined(GPU)

    #include "Core/Common/cuda/TailNeighbors.hxx"

template <typename T>
void Index<T>::SortSelections(std::vector<Edge>* selections) {
    LOG(Helper::LogLevel::LL_Debug, "Starting sort of final input on GPU\n");
    GPU_SortSelections(selections);
}

template <typename T>
void Index<T>::ApproximateRNG(std::shared_ptr<VectorSet>& fullVectors, std::unordered_set<SizeType>& exceptIDS, int candidateNum, Edge* selections, int replicaCount, int numThreads, int numTrees, int leafSize, float RNGFactor, int numGPUs) {
    LOG(Helper::LogLevel::LL_Info, "Starting GPU SSD Index build stage...\n");

    int metric = (GetDistCalcMethod() == DistCalcMethod::Cosine);

    if (GetVectorValueType() != VectorValueType::Float) {
        typedef int32_t SUMTYPE;
        switch (GetVectorValueType()) {
    #define DefineVectorValueType(Name, Type)                                                                                                                                                                              \
        case VectorValueType::Name:                                                                                                                                                                                        \
            getTailNeighborsTPT<Type, SUMTYPE>((Type*)fullVectors->GetData(), fullVectors->Count(), this, exceptIDS, fullVectors->Dimension(), replicaCount, numThreads, numTrees, leafSize, metric, numGPUs, selections); \
            break;

    #include "Core/DefinitionList.h"
    #undef DefineVectorValueType

            default:
                break;
        }
    } else {
        getTailNeighborsTPT<float, float>((float*)fullVectors->GetData(), fullVectors->Count(), this, exceptIDS, fullVectors->Dimension(), replicaCount, numThreads, numTrees, leafSize, metric, numGPUs, selections);
    }
}
#else

template <typename T>
void Index<T>::SortSelections(std::vector<Edge>* selections) {
    EdgeCompare edgeComparer;
    std::sort(selections->begin(), selections->end(), edgeComparer);
}

template <typename T>
void Index<T>::ApproximateRNG(std::shared_ptr<VectorSet>& fullVectors, std::unordered_set<SizeType>& exceptIDS, int candidateNum, Edge* selections, int replicaCount, int numThreads, int numTrees, int leafSize, float RNGFactor, int numGPUs) {
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    std::atomic_int nextFullID(0);
    std::atomic_size_t rngFailedCountTotal(0);

    for (int tid = 0; tid < numThreads; ++tid) {
        threads.emplace_back([&, tid]() {
            QueryResult resultSet(NULL, candidateNum, false);

            size_t rngFailedCount = 0;

            while (true) {
                int fullID = nextFullID.fetch_add(1);
                if (fullID >= fullVectors->Count()) {
                    break;
                }

                if (exceptIDS.count(fullID) > 0) {
                    continue;
                }

                resultSet.SetTarget(fullVectors->GetVector(fullID));
                resultSet.Reset();

                SearchIndex(resultSet);

                size_t selectionOffset = static_cast<size_t>(fullID) * replicaCount;

                BasicResult* queryResults = resultSet.GetResults();
                int currReplicaCount = 0;
                for (int i = 0; i < candidateNum && currReplicaCount < replicaCount; ++i) {
                    if (queryResults[i].VID == -1) {
                        break;
                    }

                    // RNG Check.
                    bool rngAccpeted = true;
                    for (int j = 0; j < currReplicaCount; ++j) {
                        float nnDist = ComputeDistance(GetSample(queryResults[i].VID), GetSample(selections[selectionOffset + j].node));

                        if (RNGFactor * nnDist <= queryResults[i].Dist) {
                            rngAccpeted = false;
                            break;
                        }
                    }

                    if (!rngAccpeted) {
                        ++rngFailedCount;
                        continue;
                    }

                    selections[selectionOffset + currReplicaCount].node = queryResults[i].VID;
                    selections[selectionOffset + currReplicaCount].distance = queryResults[i].Dist;
                    ++currReplicaCount;
                }
            }
            rngFailedCountTotal += rngFailedCount;
        });
    }

    for (int tid = 0; tid < numThreads; ++tid) {
        threads[tid].join();
    }
    LOG(Helper::LogLevel::LL_Info, "Searching replicas ended. RNG failed count: %llu\n", static_cast<uint64_t>(rngFailedCountTotal.load()));
}
#endif
}  // namespace SPTAG::BKT

#define DefineVectorValueType(Name, Type) \
    template class SPTAG::BKT::Index<Type>;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType
