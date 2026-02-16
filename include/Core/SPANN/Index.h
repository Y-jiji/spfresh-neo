// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_INDEX_H_
#define _SPTAG_SPANN_INDEX_H_

#include "Core/Common.h"
#include "Core/CommonDataStructure.h"
#include "Core/MetaDataManager.h"
#include "Core/BKT/Index.h"

#include "Utils/CommonUtils.h"
#include "Utils/DistanceUtils.h"
#include "Utils/SIMDUtils.h"
#include "Core/Common/QueryResultSet.h"
#include "Core/Common/BKTree.h"
#include "Core/Common/WorkSpacePool.h"

#include "Core/Common/Labelset.h"
#include "Helper/SimpleIniReader.h"
#include "Helper/StringConvert.h"
#include "Helper/ThreadPool.h"
#include "Helper/ConcurrentSet.h"
#include "Helper/VectorSetReader.h"

#include "Core/Common/VersionLabel.h"
#include "ExtraDynamicSearcher.h"
#include "Options.h"

#include <functional>
#include <shared_mutex>

namespace SPTAG::SPANN {
template <typename T>
class Index {
   private:
    std::shared_ptr<BKT::Index<T>> m_index;
    std::shared_ptr<std::uint64_t> m_vectorTranslateMap;
    std::unordered_map<std::string, std::string> m_headParameters;

    std::shared_ptr<ExtraDynamicSearcher<T>> m_extraSearcher;

    Options m_options;

    std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
    int m_iBaseSquare;

    std::mutex m_dataAddLock;
    COMMON::VersionLabel m_versionMap;

    bool m_bReady;
    std::shared_ptr<MetadataSet> m_pMetadata;
    MetaDataManager m_metadataManager;

   public:
    int m_iDataBlockSize;
    int m_iDataCapacity;
    int m_iMetaRecordSize;

   public:
    static thread_local std::shared_ptr<ExtraWorkSpace> m_workspace;

   public:
    Index() : m_bReady(false), m_iDataBlockSize(1024 * 1024), m_iDataCapacity(MaxSize), m_iMetaRecordSize(10) {
        m_fComputeDistance = std::function<float(const T*, const T*, DimensionType)>(COMMON::DistanceCalcSelector<T>(m_options.m_distCalcMethod));
        m_iBaseSquare = (m_options.m_distCalcMethod == DistCalcMethod::Cosine) ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>() : 1;
    }

    ~Index() {}

    inline std::shared_ptr<BKT::Index<T>> GetMemoryIndex() {
        return m_index;
    }
    inline std::shared_ptr<ExtraDynamicSearcher<T>> GetDiskIndex() {
        return m_extraSearcher;
    }
    inline Options* GetOptions() {
        return &m_options;
    }

    inline SizeType GetNumSamples() const {
        return m_versionMap.Count();
    }
    inline DimensionType GetFeatureDim() const {
        return m_index->GetFeatureDim();
    }
    inline SizeType GetValueSize() const {
        return m_options.m_dim * sizeof(T);
    }

    inline int GetCurrMaxCheck() const {
        return m_options.m_maxCheck;
    }
    inline int GetNumThreads() const {
        return m_options.m_iSSDNumberOfThreads;
    }
    inline DistCalcMethod GetDistCalcMethod() const {
        return m_options.m_distCalcMethod;
    }
    inline IndexAlgoType GetIndexAlgoType() const {
        return IndexAlgoType::SPANN;
    }
    inline VectorValueType GetVectorValueType() const {
        return GetEnumValueType<T>();
    }

    inline float AccurateDistance(const void* pX, const void* pY) const {
        if (m_options.m_distCalcMethod == DistCalcMethod::L2)
            return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);

        float xy = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);
        float xx = m_iBaseSquare - m_fComputeDistance((const T*)pX, (const T*)pX, m_options.m_dim);
        float yy = m_iBaseSquare - m_fComputeDistance((const T*)pY, (const T*)pY, m_options.m_dim);
        return 1.0f - xy / (sqrt(xx) * sqrt(yy));
    }
    inline float ComputeDistance(const void* pX, const void* pY) const {
        return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);
    }
    inline bool ContainSample(const SizeType idx) const {
        return idx < m_options.m_vectorSize;
    }

    std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const {
        std::shared_ptr<std::vector<std::uint64_t>> buffersize(new std::vector<std::uint64_t>);
        auto headIndexBufferSize = m_index->BufferSize();
        buffersize->insert(buffersize->end(), headIndexBufferSize->begin(), headIndexBufferSize->end());
        buffersize->push_back(sizeof(long long) * m_index->GetNumSamples());
        return std::move(buffersize);
    }

    std::shared_ptr<std::vector<std::string>> GetIndexFiles() const {
        std::shared_ptr<std::vector<std::string>> files(new std::vector<std::string>);
        auto headfiles = m_index->GetIndexFiles();
        for (auto file : *headfiles) {
            files->push_back(m_options.m_headIndexFolder + FolderSep + file);
        }
        if (m_options.m_excludehead)
            files->push_back(m_options.m_headIDFile);
        return std::move(files);
    }

    ErrorCode LoadIndexConfig(Helper::IniReader& p_reader);
    ErrorCode SaveIndexConfig(std::shared_ptr<Helper::DiskIO> p_configOut);
    ErrorCode SaveConfig(std::shared_ptr<Helper::DiskIO> p_configout);
    ErrorCode SaveIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);

    ErrorCode LoadConfig(Helper::IniReader& p_reader);
    ErrorCode LoadIndexData(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
    ErrorCode LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs);

    ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, bool p_normalized = false, bool p_shareOwnership = false);
    ErrorCode BuildIndex(bool p_normalized = false);
    ErrorCode SearchIndex(QueryResult& p_query, bool p_searchDeleted = false, SearchStats* p_stats = nullptr) const;
    ErrorCode SearchDiskIndex(QueryResult& p_query, SearchStats* p_stats = nullptr) const;
    ErrorCode DebugSearchDiskIndex(QueryResult& p_query, int p_subInternalResultNum, int p_internalResultNum, SearchStats* p_stats = nullptr, std::set<int>* truth = nullptr, std::map<int, std::set<int>>* found = nullptr) const;
    ErrorCode UpdateIndex();

    ErrorCode SetParameter(const char* p_param, const char* p_value, const char* p_section = nullptr);
    std::string GetParameter(const char* p_param, const char* p_section = nullptr) const;

    inline const void* GetSample(const SizeType idx) const {
        return nullptr;
    }
    inline bool IsReady() const {
        return m_bReady;
    }
    inline void SetReady(bool p_ready) {
        m_bReady = p_ready;
    }
    inline bool HasMetaMapping() const {
        return m_metadataManager.HasMetaMapping();
    }
    SizeType GetMetaMapping(std::string& meta) const;
    void UpdateMetaMapping(const std::string& meta, SizeType i);
    void BuildMetaMapping(bool p_checkDeleted = true);
    inline ByteArray GetMetadata(SizeType p_vectorID) const {
        if (nullptr != m_pMetadata) {
            return m_pMetadata->GetMetadata(p_vectorID);
        }
        return ByteArray::c_empty;
    }
    inline MetadataSet* GetMetadata() const {
        return m_pMetadata.get();
    }
    inline void SetMetadata(MetadataSet* p_new) {
        m_pMetadata.reset(p_new);
    }
    inline SizeType GetNumDeleted() const {
        return m_versionMap.GetDeleteCount();
    }
    inline bool NeedRefine() const {
        return false;
    }

    static ErrorCode LoadIndex(const std::string& p_loaderFilePath, std::shared_ptr<Index<T>>& p_index) {
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
    ErrorCode RefineSearchIndex(QueryResult& p_query, bool p_searchDeleted = false) const {
        return ErrorCode::Undefined;
    }
    ErrorCode SearchTree(QueryResult& p_query) const {
        return ErrorCode::Undefined;
    }
    ErrorCode AddIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, std::shared_ptr<MetadataSet> p_metadataSet, bool p_withMetaIndex = false, bool p_normalized = false);
    ErrorCode AddIndexId(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, int& beginHead, int& endHead);
    ErrorCode AddIndexIdx(SizeType begin, SizeType end);
    ErrorCode DeleteIndex(const SizeType& p_id);

    ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum);
    ErrorCode DeleteIndex(ByteArray p_meta);
    const void* GetSample(ByteArray p_meta, bool& deleteFlag);
    ErrorCode RefineIndex(const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams, IAbortOperation* p_abort) {
        return ErrorCode::Undefined;
    }
    ErrorCode RefineIndex(std::shared_ptr<SPANN::Index<T>>& p_newIndex) {
        return ErrorCode::Undefined;
    }

   private:
    bool CheckHeadIndexType();
    void SelectHeadAdjustOptions(int p_vectorCount);
    int SelectHeadDynamicallyInternal(const std::shared_ptr<COMMON::BKTree> p_tree, int p_nodeID, const Options& p_opts, std::vector<int>& p_selected);
    void SelectHeadDynamically(const std::shared_ptr<COMMON::BKTree> p_tree, int p_vectorCount, std::vector<int>& p_selected);

    template <typename InternalDataType>
    bool SelectHeadInternal(std::shared_ptr<Helper::VectorSetReader<T>>& p_reader);

    ErrorCode BuildIndexInternal(std::shared_ptr<Helper::VectorSetReader<T>>& p_reader);

   public:
    bool AllFinished() {
        return m_extraSearcher->AllFinished();
    }

    void GetDBStat() {
        m_extraSearcher->GetDBStats();
        LOG(Helper::LogLevel::LL_Info, "Current Vector Num: %d, Deleted: %d .\n", GetNumSamples(), GetNumDeleted());
    }

    void GetIndexStat(int finishedInsert, bool cost, bool reset) {
        m_extraSearcher->GetIndexStats(finishedInsert, cost, reset);
    }

    void StopMerge() {
        m_options.m_inPlace = true;
    }

    void OpenMerge() {
        m_options.m_inPlace = false;
    }

    void ForceGC() {
        m_extraSearcher->ForceGC(m_index.get());
    }

    bool Initialize() {
        return m_extraSearcher->Initialize();
    }

    bool ExitBlockController() {
        return m_extraSearcher->ExitBlockController();
    }

    ErrorCode AddIndexSPFresh(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension, SizeType* VID) {
        if (m_extraSearcher == nullptr) {
            LOG(Helper::LogLevel::LL_Error, "ExtraSearcher not initialized\n");
            return ErrorCode::Fail;
        }

        if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0)
            return ErrorCode::EmptyData;
        if (p_dimension != GetFeatureDim())
            return ErrorCode::DimensionSizeMismatch;

        SizeType begin, end;
        {
            std::lock_guard<std::mutex> lock(m_dataAddLock);

            begin = m_versionMap.GetVectorNum();
            end = begin + p_vectorNum;

            if (begin == 0) {
                return ErrorCode::EmptyIndex;
            }

            if (m_versionMap.AddBatch(p_vectorNum) != ErrorCode::Success) {
                LOG(Helper::LogLevel::LL_Info, "MemoryOverFlow: VID: %d, Map Size:%d\n", begin, m_versionMap.BufferSize());
                exit(1);
            }
        }
        for (int i = 0; i < p_vectorNum; i++)
            VID[i] = begin + i;

        std::shared_ptr<VectorSet> vectorSet;
        if (m_options.m_distCalcMethod == DistCalcMethod::Cosine) {
            ByteArray arr = ByteArray::Alloc(sizeof(T) * p_vectorNum * p_dimension);
            memcpy(arr.Data(), p_data, sizeof(T) * p_vectorNum * p_dimension);
            vectorSet.reset(new BasicVectorSet(arr, GetEnumValueType<T>(), p_dimension, p_vectorNum));
            int base = COMMON::Utils::GetBase<T>();
            for (SizeType i = 0; i < p_vectorNum; i++) {
                COMMON::Utils::Normalize((T*)(vectorSet->GetVector(i)), p_dimension, base);
            }
        } else {
            vectorSet.reset(new BasicVectorSet(ByteArray((std::uint8_t*)p_data, sizeof(T) * p_vectorNum * p_dimension, false), GetEnumValueType<T>(), p_dimension, p_vectorNum));
        }

        return m_extraSearcher->AddIndex(vectorSet, m_index, begin);
    }
};

}  // namespace SPTAG::SPANN

#endif  // _SPTAG_SPANN_INDEX_H_
