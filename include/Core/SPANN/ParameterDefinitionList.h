// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifdef DefineBasicParameter

// DefineBasicParameter(VarName, VarType, DefaultValue, RepresentStr)
DefineBasicParameter(m_valueType, SPTAG::VectorValueType, SPTAG::VectorValueType::Undefined, "ValueType")
BasicParameter(m_distCalcMethod, SPTAG::DistCalcMethod, SPTAG::DistCalcMethod::Undefined, "DistCalcMethod")
BasicParameter(m_indexAlgoType, SPTAG::IndexAlgoType, SPTAG::IndexAlgoType::BKT, "IndexAlgoType")
BasicParameter(m_dim, SPTAG::DimensionType, -1, "Dim")
BasicParameter(m_vectorPath, std::string, std::string(""), "VectorPath")
BasicParameter(m_vectorSize, SPTAG::SizeType, -1, "VectorSize")
BasicParameter(m_vectorDelimiter, std::string, std::string("|"), "VectorDelimiter")
BasicParameter(m_queryPath, std::string, std::string(""), "QueryPath")
BasicParameter(m_querySize, SPTAG::SizeType, -1, "QuerySize")
BasicParameter(m_queryDelimiter, std::string, std::string("|"), "QueryDelimiter")
BasicParameter(m_warmupPath, std::string, std::string(""), "WarmupPath")
BasicParameter(m_warmupSize, SPTAG::SizeType, -1, "WarmupSize")
BasicParameter(m_warmupDelimiter, std::string, std::string("|"), "WarmupDelimiter")
BasicParameter(m_truthPath, std::string, std::string(""), "TruthPath")
BasicParameter(m_truthType, SPTAG::TruthFileType, SPTAG::TruthFileType::Undefined, "TruthType")
BasicParameter(m_generateTruth, bool, false, "GenerateTruth")
BasicParameter(m_indexDirectory, std::string, std::string("SPANN"), "IndexDirectory")
BasicParameter(m_headIDFile, std::string, std::string("SPTAGHeadVectorIDs.bin"), "HeadVectorIDs")
BasicParameter(m_deleteIDFile, std::string, std::string("DeletedIDs.bin"), "DeletedIDs")
BasicParameter(m_headVectorFile, std::string, std::string("SPTAGHeadVectors.bin"), "HeadVectors")
BasicParameter(m_headIndexFolder, std::string, std::string("HeadIndex"), "HeadIndexFolder")
BasicParameter(m_ssdIndex, std::string, std::string("SPTAGFullList.bin"), "SSDIndex")
BasicParameter(m_deleteHeadVectors, bool, false, "DeleteHeadVectors")
BasicParameter(m_ssdIndexFileNum, int, 1, "SSDIndexFileNum")
BasicParameter(m_datasetRowsInBlock, int, 1024 * 1024, "DataBlockSize")
BasicParameter(m_datasetCapacity, int, SPTAG::MaxSize, "DataCapacity")
#endif

#ifdef DefineSelectHeadParameter

SelectHeadParameter(m_selectHead, bool, false, "isExecute")
SelectHeadParameter(m_iTreeNumber, int, 1, "TreeNumber")
SelectHeadParameter(m_iBKTKmeansK, int, 32, "BKTKmeansK")
SelectHeadParameter(m_iBKTLeafSize, int, 8, "BKTLeafSize")
SelectHeadParameter(m_iSamples, int, 1000, "SamplesNumber")
SelectHeadParameter(m_fBalanceFactor, float, -1.0F, "BKTLambdaFactor")

SelectHeadParameter(m_iSelectHeadNumberOfThreads, int, 4, "NumberOfThreads")
SelectHeadParameter(m_saveBKT, bool, false, "SaveBKT")

SelectHeadParameter(m_analyzeOnly, bool, false, "AnalyzeOnly")
SelectHeadParameter(m_calcStd, bool, false, "CalcStd")
SelectHeadParameter(m_selectDynamically, bool, true, "SelectDynamically")
SelectHeadParameter(m_noOutput, bool, false, "NoOutput")

SelectHeadParameter(m_selectThreshold, int, 6, "SelectThreshold")
SelectHeadParameter(m_splitFactor, int, 5, "SplitFactor")
SelectHeadParameter(m_splitThreshold, int, 25, "SplitThreshold")
SelectHeadParameter(m_maxRandomTryCount, int, 8, "SplitMaxTry")
SelectHeadParameter(m_ratio, double, 0.2, "Ratio")
SelectHeadParameter(m_headVectorCount, int, 0, "Count")
SelectHeadParameter(m_recursiveCheckSmallCluster, bool, true, "RecursiveCheckSmallCluster")
SelectHeadParameter(m_printSizeCount, bool, true, "PrintSizeCount")
SelectHeadParameter(m_selectType, std::string, "BKT", "SelectHeadType")
#endif

#ifdef DefineBuildHeadParameter

BuildHeadParameter(m_buildHead, bool, false, "isExecute")

#endif

#ifdef DefineSSDParameter
SSDParameter(m_enableSSD, bool, false, "isExecute")
SSDParameter(m_buildSsdIndex, bool, false, "BuildSsdIndex")
SSDParameter(m_iSSDNumberOfThreads, int, 16, "NumberOfThreads")
SSDParameter(m_enableDeltaEncoding, bool, false, "EnableDeltaEncoding")
SSDParameter(m_enablePostingListRearrange, bool, false, "EnablePostingListRearrange")
SSDParameter(m_enableDataCompression, bool, false, "EnableDataCompression")
SSDParameter(m_enableDictTraining, bool, true, "EnableDictTraining")
SSDParameter(m_minDictTraingBufferSize, int, 10240000, "MinDictTrainingBufferSize")
SSDParameter(m_dictBufferCapacity, int, 204800, "DictBufferCapacity")
SSDParameter(m_zstdCompressLevel, int, 0, "ZstdCompressLevel")

    // Building
SSDParameter(m_internalResultNum, int, 64, "InternalResultNum")
SSDParameter(m_postingPageLimit, int, 3, "PostingPageLimit")
SSDParameter(m_replicaCount, int, 8, "ReplicaCount")
SSDParameter(m_outputEmptyReplicaID, bool, false, "OutputEmptyReplicaID")
SSDParameter(m_batches, int, 1, "Batches")
SSDParameter(m_tmpdir, std::string, std::string("."), "TmpDir")
SSDParameter(m_rngFactor, float, 1.0f, "RNGFactor")
SSDParameter(m_samples, int, 100, "RecallTestSampleNumber")
SSDParameter(m_excludehead, bool, true, "ExcludeHead")
SSDParameter(m_spdkBatchSize, int, 64, "SpdkBatchSize")
SSDParameter(m_spdkCapacity, int, 10000000, "SpdkCapacity")
SSDParameter(m_spdkMappingPath, std::string, std::string(""), "SpdkMappingPath")
SSDParameter(m_ssdInfoFile, std::string, std::string(""), "SsdInfoFile")
SSDParameter(m_useDirectIO, bool, false, "UseDirectIO")
SSDParameter(m_preReassign, bool, false, "PreReassign")
SSDParameter(m_preReassignRatio, float, 0.7f, "PreReassignRatio")
SSDParameter(m_bufferLength, int, 3, "BufferLength")

    // GPU Building
SSDParameter(m_gpuSSDNumTrees, int, 100, "GPUSSDNumTrees")
SSDParameter(m_gpuSSDLeafSize, int, 200, "GPUSSDLeafSize")
SSDParameter(m_numGPUs, int, 1, "NumGPUs")

    // Searching
SSDParameter(m_searchResult, std::string, std::string(""), "SearchResult")
SSDParameter(m_logFile, std::string, std::string(""), "LogFile")
SSDParameter(m_qpsLimit, int, 0, "QpsLimit")
SSDParameter(m_resultNum, int, 5, "ResultNum")
SSDParameter(m_truthResultNum, int, -1, "TruthResultNum")
SSDParameter(m_maxCheck, int, 4096, "MaxCheck")
SSDParameter(m_hashExp, int, 4, "HashTableExponent")
SSDParameter(m_queryCountLimit, int, (std::numeric_limits<int>::max)(), "QueryCountLimit")
SSDParameter(m_maxDistRatio, float, 10000, "MaxDistRatio")
SSDParameter(m_ioThreads, int, 4, "IOThreadsPerHandler")
SSDParameter(m_searchInternalResultNum, int, 64, "SearchInternalResultNum")
SSDParameter(m_searchPostingPageLimit, int, (std::numeric_limits<int>::max)() - 1, "SearchPostingPageLimit")
SSDParameter(m_rerank, int, 0, "Rerank")
SSDParameter(m_enableADC, bool, false, "EnableADC")
SSDParameter(m_recall_analysis, bool, false, "RecallAnalysis")
SSDParameter(m_debugBuildInternalResultNum, int, 64, "DebugBuildInternalResultNum")
SSDParameter(m_iotimeout, int, 30, "IOTimeout")

    // Calculating
    // TruthFilePrefix
SSDParameter(m_truthFilePrefix, std::string, std::string(""), "TruthFilePrefix")
    // CalTruth
SSDParameter(m_calTruth, bool, true, "CalTruth")
SSDParameter(m_onlySearchFinalBatch, bool, false, "OnlySearchFinalBatch")
    // Search multiple times for stable result
SSDParameter(m_searchTimes, int, 1, "SearchTimes")
    // Frontend search threadnum
SSDParameter(m_searchThreadNum, int, 16, "SearchThreadNum")
    // Show tradeoff of latency and acurracy
SSDParameter(m_minInternalResultNum, int, -1, "MinInternalResultNum")
SSDParameter(m_stepInternalResultNum, int, -1, "StepInternalResultNum")
SSDParameter(m_maxInternalResultNum, int, -1, "MaxInternalResultNum")

    // Updating(SPFresh Update Test)
    // For update mode: current only update
SSDParameter(m_update, bool, false, "Update")
    // For Test Mode
SSDParameter(m_inPlace, bool, false, "InPlace")
SSDParameter(m_outOfPlace, bool, false, "OutOfPlace")
    // latency limit
SSDParameter(m_latencyLimit, float, 2.0, "LatencyLimit")
    // Update batch size
SSDParameter(m_step, int, 0, "Step")
    // Frontend update threadnum
SSDParameter(m_insertThreadNum, int, 16, "InsertThreadNum")
    // Update limit
SSDParameter(m_endVectorNum, int, -1, "EndVectorNum")
    // Persistent buffer path
SSDParameter(m_persistentBufferPath, std::string, std::string(""), "PersistentBufferPath")
    // Background append threadnum
SSDParameter(m_appendThreadNum, int, 16, "AppendThreadNum")
    // Background reassign threadnum
SSDParameter(m_reassignThreadNum, int, 16, "ReassignThreadNum")
    // Background process batch size
SSDParameter(m_batch, int, 1000, "Batch")
    // Total Vector Path
SSDParameter(m_fullVectorPath, std::string, std::string(""), "FullVectorPath")
    // Steady State: update trace
SSDParameter(m_updateFilePrefix, std::string, std::string(""), "UpdateFilePrefix")
    // Steady State: update mapping
SSDParameter(m_updateMappingPrefix, std::string, std::string(""), "UpdateMappingPrefix")
    // Steady State: days
SSDParameter(m_days, int, 0, "Days")
    // Steady State: deleteQPS
SSDParameter(m_deleteQPS, int, -1, "DeleteQPS")
    // Steady State: sampling
SSDParameter(m_sampling, int, -1, "Sampling")
    // Steady State: showUpdateProgress
SSDParameter(m_showUpdateProgress, bool, true, "ShowUpdateProgress")
    // Steady State: Merge Threshold
SSDParameter(m_mergeThreshold, int, 10, "MergeThreshold")
    // Steady State: showUpdateProgress
SSDParameter(m_loadAllVectors, bool, false, "LoadAllVectors")
    // Steady State: steady state
SSDParameter(m_steadyState, bool, false, "SteadyState")
    // Steady State: stress test
SSDParameter(m_stressTest, bool, false, "StressTest")

    // SPANN
SSDParameter(m_postingVectorLimit, int, 1000, "PostingSearchVectorLimit")
SSDParameter(m_disableReassign, bool, false, "DisableReassign")
SSDParameter(m_searchDuringUpdate, bool, false, "SearchDuringUpdate")
SSDParameter(m_reassignK, int, 0, "ReassignK")
SSDParameter(m_virtualHead, bool, false, "VirtualHead")
#endif
