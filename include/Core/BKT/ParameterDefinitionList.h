// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifdef DefineBKTParameter

// DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr)
DefineBKTParameter(m_sBKTFilename, std::string, std::string("tree.bin"), "TreeFilePath")
BKTParameter(m_sGraphFilename, std::string, std::string("graph.bin"), "GraphFilePath")
BKTParameter(m_sDataPointsFilename, std::string, std::string("vectors.bin"), "VectorFilePath")
BKTParameter(m_sDeleteDataPointsFilename, std::string, std::string("deletes.bin"), "DeleteVectorFilePath")

BKTParameter(m_pTrees.m_bfs, int, 0L, "EnableBfs")
BKTParameter(m_pTrees.m_iTreeNumber, int, 1L, "BKTNumber")
BKTParameter(m_pTrees.m_iBKTKmeansK, int, 32L, "BKTKmeansK")
BKTParameter(m_pTrees.m_iBKTLeafSize, int, 8L, "BKTLeafSize")
BKTParameter(m_pTrees.m_iSamples, int, 1000L, "Samples")
BKTParameter(m_pTrees.m_fBalanceFactor, float, 100.0F, "BKTLambdaFactor")

BKTParameter(m_pGraph.m_iTPTNumber, int, 32L, "TPTNumber")
BKTParameter(m_pGraph.m_iTPTLeafSize, int, 2000L, "TPTLeafSize")
BKTParameter(m_pGraph.m_numTopDimensionTPTSplit, int, 5L, "NumTopDimensionTpTreeSplit")

BKTParameter(m_pGraph.m_iNeighborhoodSize, DimensionType, 32L, "NeighborhoodSize")
BKTParameter(m_pGraph.m_fNeighborhoodScale, float, 2.0F, "GraphNeighborhoodScale")
BKTParameter(m_pGraph.m_fCEFScale, float, 2.0F, "GraphCEFScale")
BKTParameter(m_pGraph.m_iRefineIter, int, 2L, "RefineIterations")
BKTParameter(m_pGraph.m_rebuild, int, 0L, "EnableRebuild")
BKTParameter(m_pGraph.m_iCEF, int, 1000L, "CEF")
BKTParameter(m_pGraph.m_iAddCEF, int, 500L, "AddCEF")
BKTParameter(m_pGraph.m_iMaxCheckForRefineGraph, int, 8192L, "MaxCheckForRefineGraph")
BKTParameter(m_pGraph.m_fRNGFactor, float, 1.0f, "RNGFactor")

BKTParameter(m_pGraph.m_iGPUGraphType, int, 2, "GPUGraphType")  // Have GPU construct KNN,loose RNG or RNG
BKTParameter(m_pGraph.m_iGPURefineSteps, int, 0, "GPURefineSteps")                                                                                  // Steps of GPU neighbor-refinement
BKTParameter(m_pGraph.m_iGPURefineDepth, int, 30, "GPURefineDepth")                                                                                 // Depth of graph search for refinement
BKTParameter(m_pGraph.m_iGPULeafSize, int, 500, "GPULeafSize")
BKTParameter(m_pGraph.m_iheadNumGPUs, int, 1, "HeadNumGPUs")
BKTParameter(m_pGraph.m_iTPTBalanceFactor, int, 2, "TPTBalanceFactor")

BKTParameter(m_iNumberOfThreads, int, 1L, "NumberOfThreads")
BKTParameter(m_iDistCalcMethod, SPTAG::DistCalcMethod, SPTAG::DistCalcMethod::Cosine, "DistCalcMethod")

BKTParameter(m_fDeletePercentageForRefine, float, 0.4F, "DeletePercentageForRefine")
BKTParameter(m_addCountForRebuild, int, 1000, "AddCountForRebuild")
BKTParameter(m_iMaxCheck, int, 8192L, "MaxCheck")
BKTParameter(m_iThresholdOfNumberOfContinuousNoBetterPropagation, int, 3L, "ThresholdOfNumberOfContinuousNoBetterPropagation")
BKTParameter(m_iNumberOfInitialDynamicPivots, int, 50L, "NumberOfInitialDynamicPivots")
BKTParameter(m_iNumberOfOtherDynamicPivots, int, 4L, "NumberOfOtherDynamicPivots")
BKTParameter(m_iHashTableExp, int, 2L, "HashTableExponent")
BKTParameter(m_iDataBlockSize, int, 1024 * 1024, "DataBlockSize")
BKTParameter(m_iDataCapacity, int, MaxSize, "DataCapacity")
BKTParameter(m_iMetaRecordSize, int, 10, "MetaRecordSize")

#endif
