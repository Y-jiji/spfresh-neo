// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include <iostream>

#include "Core/Common.h"
#include "Core/VectorIndex.h"
#include "Core/SPANN/Index.h"
#include "Helper/SimpleIniReader.h"
#include "Helper/VectorSetReader.h"
#include "Helper/StringConvert.h"
#include "Core/Common/TruthSet.h"

#include "SSDServing/main.h"
#include "SSDServing/Utils.h"
#include "SSDServing/SSDIndex.h"

namespace SPTAG::SSDServing {

int BootProgram(std::map<std::string, std::map<std::string, std::string>>* config_map, const char* configurationPath) {
    SPTAG::VectorValueType valueType = SPTAG::VectorValueType::Undefined;
    SPTAG::DistCalcMethod distCalcMethod = SPTAG::DistCalcMethod::Undefined;

    bool searchSSD = false;
    std::string QuantizerFilePath = "";
    Helper::IniReader iniReader;
    iniReader.LoadIniFile(configurationPath);
    (*config_map)[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
    (*config_map)[SEC_SELECT_HEAD] = iniReader.GetParameters(SEC_SELECT_HEAD);
    (*config_map)[SEC_BUILD_HEAD] = iniReader.GetParameters(SEC_BUILD_HEAD);
    (*config_map)[SEC_BUILD_SSD_INDEX] = iniReader.GetParameters(SEC_BUILD_SSD_INDEX);

    valueType = iniReader.GetParameter(SEC_BASE, "ValueType", valueType);
    distCalcMethod = iniReader.GetParameter(SEC_BASE, "DistCalcMethod", distCalcMethod);
    bool buildSSD = iniReader.GetParameter(SEC_BUILD_SSD_INDEX, "isExecute", false);
    searchSSD = iniReader.GetParameter(SEC_SEARCH_SSD_INDEX, "isExecute", false);
    QuantizerFilePath = iniReader.GetParameter(SEC_BASE, "QuantizerFilePath", std::string(""));

    for (auto& KV : iniReader.GetParameters(SEC_SEARCH_SSD_INDEX)) {
        std::string param = KV.first, value = KV.second;
        if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "BuildSsdIndex"))
            continue;
        if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "isExecute"))
            continue;
        if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "PostingPageLimit"))
            param = "SearchPostingPageLimit";
        if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(), "InternalResultNum"))
            param = "SearchInternalResultNum";
        (*config_map)[SEC_BUILD_SSD_INDEX][param] = value;
    }

    LOG(Helper::LogLevel::LL_Info, "Set QuantizerFile = %s\n", QuantizerFilePath.c_str());

    std::shared_ptr<SPTAG::VectorIndex> index;
    switch (valueType) {
#define DefineVectorValueType(Name, Type)                                           \
    case SPTAG::VectorValueType::Name:                                              \
        index = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::SPANN::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType
        default:
            LOG(Helper::LogLevel::LL_Error, "Cannot create Index with ValueType %s!\n", (*config_map)[SEC_BASE]["ValueType"].c_str());
            return -1;
    }

    if (!QuantizerFilePath.empty() && index->LoadQuantizer(QuantizerFilePath) != SPTAG::ErrorCode::Success) {
        exit(1);
    }

    for (auto& sectionKV : *config_map) {
        for (auto& KV : sectionKV.second) {
            index->SetParameter(KV.first, KV.second, sectionKV.first);
        }
    }

    if (index->BuildIndex() != SPTAG::ErrorCode::Success) {
        LOG(Helper::LogLevel::LL_Error, "Failed to build index.\n");
        exit(1);
    }

    SPTAG::SPANN::Options* opts = nullptr;

#define DefineVectorValueType(Name, Type)                               \
    if (index->GetVectorValueType() == SPTAG::VectorValueType::Name) {  \
        opts = ((SPTAG::SPANN::Index<Type>*)index.get())->GetOptions(); \
    }

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

    if (opts == nullptr) {
        LOG(Helper::LogLevel::LL_Error, "Cannot get options.\n");
        exit(1);
    }

    if (opts->m_generateTruth) {
        LOG(Helper::LogLevel::LL_Info, "Start generating truth. It's maybe a long time.\n");
        SPTAG::SizeType dim = opts->m_dim;
        if (index->m_pQuantizer) {
            valueType = SPTAG::VectorValueType::UInt8;
            dim = index->m_pQuantizer->GetNumSubvectors();
        }
        std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(valueType, dim, opts->m_vectorType, opts->m_vectorDelimiter));
        auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
        if (SPTAG::ErrorCode::Success != vectorReader->LoadFile(opts->m_vectorPath)) {
            LOG(Helper::LogLevel::LL_Error, "Failed to read vector file.\n");
            exit(1);
        }
        std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(opts->m_valueType, opts->m_dim, opts->m_queryType, opts->m_queryDelimiter));
        auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
        if (SPTAG::ErrorCode::Success != queryReader->LoadFile(opts->m_queryPath)) {
            LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
            exit(1);
        }
        auto vectorSet = vectorReader->GetVectorSet();
        auto querySet = queryReader->GetVectorSet();
        if (distCalcMethod == SPTAG::DistCalcMethod::Cosine && !index->m_pQuantizer)
            vectorSet->Normalize(opts->m_iSSDNumberOfThreads);

        omp_set_num_threads(opts->m_iSSDNumberOfThreads);

#define DefineVectorValueType(Name, Type)                                                                                                                         \
    if (opts->m_valueType == SPTAG::VectorValueType::Name) {                                                                                                      \
        COMMON::TruthSet::GenerateTruth<Type>(querySet, vectorSet, opts->m_truthPath, distCalcMethod, opts->m_resultNum, opts->m_truthType, index->m_pQuantizer); \
    }

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

        LOG(Helper::LogLevel::LL_Info, "End generating truth.\n");
    }

    if (searchSSD) {
#define DefineVectorValueType(Name, Type)                            \
    if (opts->m_valueType == SPTAG::VectorValueType::Name) {         \
        SSDIndex::Search((SPTAG::SPANN::Index<Type>*)(index.get())); \
    }

#include "Core/DefinitionList.h"
#undef DefineVectorValueType
    }
    return 0;
}
}  // namespace SPTAG::SSDServing

// switch between exe and static library by _$(OutputType)
#ifdef _exe

int main(int argc, char* argv[]) {
    using SPTAG::g_pLogger;
    if (argc < 2) {
        LOG(SPTAG::Helper::LogLevel::LL_Error,
            "ssdserving configFilePath\n");
        exit(-1);
    }

    std::map<std::string, std::map<std::string, std::string>> my_map;
    auto ret = SPTAG::SSDServing::BootProgram(&my_map, argv[1]);
    return ret;
}

#endif
