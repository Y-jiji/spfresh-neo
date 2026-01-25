// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/VectorSetReader.h"
#include "Helper/CommonHelper.h"

SPTAG::Helper::ReaderOptions::ReaderOptions(SPTAG::VectorValueType p_valueType, SPTAG::DimensionType p_dimension, SPTAG::VectorFileType p_fileType, std::string p_vectorDelimiter, std::uint32_t p_threadNum, bool p_normalized)
    : m_inputValueType(p_valueType), m_dimension(p_dimension), m_inputFileType(p_fileType), m_vectorDelimiter(p_vectorDelimiter), m_threadNum(p_threadNum), m_normalized(p_normalized) {
    AddOptionalOption(m_threadNum, "-t", "--thread", "Thread Number.");
    AddOptionalOption(m_vectorDelimiter, "-dl", "--delimiter", "Vector delimiter.");
    AddOptionalOption(m_normalized, "-norm", "--normalized", "Vector is normalized.");
    AddRequiredOption(m_dimension, "-d", "--dimension", "Dimension of vector.");
    AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
    AddRequiredOption(m_inputFileType, "-f", "--filetype", "Input file type (DEFAULT). Default is DEFAULT.");
}

SPTAG::Helper::ReaderOptions::~ReaderOptions() {
}

SPTAG::Helper::VectorSetReader::VectorSetReader(std::shared_ptr<SPTAG::Helper::ReaderOptions> p_options)
    : m_options(p_options), m_vectorOutput(""), m_metadataConentOutput(""), m_metadataIndexOutput("") {
}

SPTAG::Helper::VectorSetReader::~VectorSetReader() {
}

std::shared_ptr<SPTAG::Helper::VectorSetReader>
SPTAG::Helper::VectorSetReader::CreateInstance(std::shared_ptr<SPTAG::Helper::ReaderOptions> p_options) {
    return std::make_shared<SPTAG::Helper::VectorSetReader>(p_options);
}

SPTAG::ErrorCode
SPTAG::Helper::VectorSetReader::LoadFile(const std::string& p_filePaths) {
    const auto& files = SPTAG::Helper::StrUtils::SplitString(p_filePaths, ",");
    m_vectorOutput = files[0];
    if (files.size() >= 3) {
        m_metadataConentOutput = files[1];
        m_metadataIndexOutput = files[2];
    }
    return SPTAG::ErrorCode::Success;
}

std::shared_ptr<SPTAG::VectorSet>
SPTAG::Helper::VectorSetReader::GetVectorSet(SPTAG::SizeType start, SPTAG::SizeType end) const {
    auto ptr = SPTAG::f_createIO();
    if (ptr == nullptr || !ptr->Initialize(m_vectorOutput.c_str(), std::ios::binary | std::ios::in)) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to read file %s.\n", m_vectorOutput.c_str());
        throw std::runtime_error("Failed read file");
    }

    SPTAG::SizeType row;
    SPTAG::DimensionType col;
    if (ptr->ReadBinary(sizeof(SPTAG::SizeType), (char*)&row) != sizeof(SPTAG::SizeType)) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
        throw std::runtime_error("Failed read file");
    }
    if (ptr->ReadBinary(sizeof(SPTAG::DimensionType), (char*)&col) != sizeof(SPTAG::DimensionType)) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
        throw std::runtime_error("Failed read file");
    }
    if (start > row)
        start = row;
    if (end < 0 || end > row)
        end = row;
    std::uint64_t totalRecordVectorBytes = ((std::uint64_t)SPTAG::GetValueTypeSize(m_options->m_inputValueType)) * (end - start) * col;
    SPTAG::ByteArray vectorSet;
    if (totalRecordVectorBytes > 0) {
        vectorSet = SPTAG::ByteArray::Alloc(totalRecordVectorBytes);
        char* vecBuf = reinterpret_cast<char*>(vectorSet.Data());
        std::uint64_t offset = ((std::uint64_t)SPTAG::GetValueTypeSize(m_options->m_inputValueType)) * start * col + sizeof(SPTAG::SizeType) + sizeof(SPTAG::DimensionType);
        if (ptr->ReadBinary(totalRecordVectorBytes, vecBuf, offset) != totalRecordVectorBytes) {
            LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
            throw std::runtime_error("Failed read file");
        }
    }

    LOG(SPTAG::Helper::LogLevel::LL_Info, "Load Vector(%d,%d)\n", end - start, col);
    return std::make_shared<SPTAG::BasicVectorSet>(vectorSet, m_options->m_inputValueType, col, end - start);
}

std::shared_ptr<SPTAG::MetadataSet>
SPTAG::Helper::VectorSetReader::GetMetadataSet() const {
    if (fileexists(m_metadataIndexOutput.c_str()) && fileexists(m_metadataConentOutput.c_str()))
        return std::shared_ptr<SPTAG::MetadataSet>(new SPTAG::FileMetadataSet(m_metadataConentOutput, m_metadataIndexOutput));
    return nullptr;
}
