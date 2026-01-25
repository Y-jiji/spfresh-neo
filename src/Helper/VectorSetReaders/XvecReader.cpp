// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/VectorSetReaders/XvecReader.h"
#include "Helper/CommonHelper.h"

#include <time.h>

namespace SPTAG::Helper {

using SPTAG::g_pLogger;

XvecVectorReader::XvecVectorReader(std::shared_ptr<ReaderOptions> p_options)
    : VectorSetReader(p_options) {
    std::string tempFolder("tempfolder");
    if (!direxists(tempFolder.c_str())) {
        mkdir(tempFolder.c_str());
    }
    std::srand(clock());
    m_vectorOutput = tempFolder + FolderSep + "vectorset.bin." + std::to_string(std::rand());
}

XvecVectorReader::~XvecVectorReader() {
    if (fileexists(m_vectorOutput.c_str())) {
        remove(m_vectorOutput.c_str());
    }
}

ErrorCode
XvecVectorReader::LoadFile(const std::string& p_filePaths) {
    const auto& files = StrUtils::SplitString(p_filePaths, ",");
    auto fp = SPTAG::f_createIO();
    if (fp == nullptr || !fp->Initialize(m_vectorOutput.c_str(), std::ios::binary | std::ios::out)) {
        LOG(LogLevel::LL_Error, "Failed to write file: %s \n", m_vectorOutput.c_str());
        return ErrorCode::FailedCreateFile;
    }
    SizeType vectorCount = 0;
    IOBINARY(fp, WriteBinary, sizeof(vectorCount), (char*)&vectorCount);
    IOBINARY(fp, WriteBinary, sizeof(m_options->m_dimension), (char*)&(m_options->m_dimension));

    size_t vectorDataSize = SPTAG::GetValueTypeSize(m_options->m_inputValueType) * m_options->m_dimension;
    std::unique_ptr<char[]> buffer(new char[vectorDataSize]);
    for (std::string file : files) {
        auto ptr = SPTAG::f_createIO();
        if (ptr == nullptr || !ptr->Initialize(file.c_str(), std::ios::binary | std::ios::in)) {
            LOG(LogLevel::LL_Error, "Failed to read file: %s \n", file.c_str());
            return ErrorCode::FailedOpenFile;
        }
        while (true) {
            DimensionType dim;
            if (ptr->ReadBinary(sizeof(DimensionType), (char*)&dim) == 0)
                break;

            if (dim != m_options->m_dimension) {
                LOG(LogLevel::LL_Error, "Xvec file %s has No.%d vector whose dims are not as many as expected. Expected: %d, Fact: %d\n", file.c_str(), vectorCount, m_options->m_dimension, dim);
                return ErrorCode::DimensionSizeMismatch;
            }
            IOBINARY(ptr, ReadBinary, vectorDataSize, buffer.get());
            IOBINARY(fp, WriteBinary, vectorDataSize, buffer.get());
            vectorCount++;
        }
    }
    IOBINARY(fp, WriteBinary, sizeof(vectorCount), (char*)&vectorCount, 0);
    return ErrorCode::Success;
}

std::shared_ptr<SPTAG::VectorSet>
XvecVectorReader::GetVectorSet(SPTAG::SizeType start, SPTAG::SizeType end) const {
    auto ptr = SPTAG::f_createIO();
    if (ptr == nullptr || !ptr->Initialize(m_vectorOutput.c_str(), std::ios::binary | std::ios::in)) {
        LOG(LogLevel::LL_Error, "Failed to read file %s.\n", m_vectorOutput.c_str());
        throw std::runtime_error("Failed read file");
    }

    SPTAG::SizeType row;
    SPTAG::DimensionType col;
    if (ptr->ReadBinary(sizeof(SPTAG::SizeType), (char*)&row) != sizeof(SPTAG::SizeType)) {
        LOG(LogLevel::LL_Error, "Failed to read VectorSet!\n");
        throw std::runtime_error("Failed read file");
    }
    if (ptr->ReadBinary(sizeof(SPTAG::DimensionType), (char*)&col) != sizeof(SPTAG::DimensionType)) {
        LOG(LogLevel::LL_Error, "Failed to read VectorSet!\n");
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
        std::uint64_t offset = ((std::uint64_t)SPTAG::GetValueTypeSize(m_options->m_inputValueType)) * start * col + +sizeof(SPTAG::SizeType) + sizeof(SPTAG::DimensionType);
        if (ptr->ReadBinary(totalRecordVectorBytes, vecBuf, offset) != totalRecordVectorBytes) {
            LOG(LogLevel::LL_Error, "Failed to read VectorSet!\n");
            throw std::runtime_error("Failed read file");
        }
    }
    return std::shared_ptr<SPTAG::VectorSet>(new SPTAG::BasicVectorSet(vectorSet, m_options->m_inputValueType, col, end - start));
}

std::shared_ptr<SPTAG::MetadataSet>
XvecVectorReader::GetMetadataSet() const {
    return nullptr;
}

}  // namespace SPTAG::Helper
