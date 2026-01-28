// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/VectorSetReader.h"
#include "Helper/CommonHelper.h"
#include <unistd.h>

SPTAG::Helper::ReaderOptions::ReaderOptions(SPTAG::VectorValueType p_valueType, SPTAG::DimensionType p_dimension, std::string p_vectorDelimiter, std::uint32_t p_threadNum, bool p_normalized)
    : m_inputValueType(p_valueType), m_dimension(p_dimension), m_vectorDelimiter(p_vectorDelimiter), m_threadNum(p_threadNum), m_normalized(p_normalized) {
    AddOptionalOption(m_threadNum, "-t", "--thread", "Thread Number.");
    AddOptionalOption(m_vectorDelimiter, "-dl", "--delimiter", "Vector delimiter.");
    AddOptionalOption(m_normalized, "-norm", "--normalized", "Vector is normalized.");
    AddRequiredOption(m_dimension, "-d", "--dimension", "Dimension of vector.");
    AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
}

SPTAG::Helper::ReaderOptions::~ReaderOptions() {
}

template <typename T>
SPTAG::Helper::VectorSetReader<T>::VectorSetReader(SPTAG::SizeType size, SPTAG::DimensionType dim, std::string p_vectorDelimiter, std::uint32_t p_threadNum, bool p_normalized)
    : m_size(size), m_dim(dim), m_vectorDelimiter(p_vectorDelimiter), m_threadNum(p_threadNum), m_normalized(p_normalized), m_vectorOutput(""), m_metadataConentOutput(""), m_metadataIndexOutput(""), m_mappedData(nullptr), m_fileSize(0), m_fd(-1) {
}

template <typename T>
SPTAG::Helper::VectorSetReader<T>::~VectorSetReader() {
    if (m_mappedData != nullptr && m_mappedData != MAP_FAILED) {
        munmap(m_mappedData, m_fileSize);
    }
    if (m_fd >= 0) {
        close(m_fd);
    }
}

template <typename T>
std::shared_ptr<SPTAG::Helper::VectorSetReader<T>>
SPTAG::Helper::VectorSetReader<T>::CreateInstance(SPTAG::SizeType size, SPTAG::DimensionType dim, std::string p_vectorDelimiter, std::uint32_t p_threadNum, bool p_normalized) {
    return std::make_shared<SPTAG::Helper::VectorSetReader<T>>(size, dim, p_vectorDelimiter, p_threadNum, p_normalized);
}

template <typename T>
SPTAG::ErrorCode
SPTAG::Helper::VectorSetReader<T>::LoadFile(const std::string& p_filePaths) {
    const auto& files = SPTAG::Helper::StrUtils::SplitString(p_filePaths, ",");
    m_vectorOutput = files[0];
    if (files.size() >= 3) {
        m_metadataConentOutput = files[1];
        m_metadataIndexOutput = files[2];
    }

    m_fd = open(m_vectorOutput.c_str(), O_RDONLY);
    if (m_fd < 0) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to open file %s.\n", m_vectorOutput.c_str());
        throw std::runtime_error("Failed to open file");
    }

    struct stat st;
    if (fstat(m_fd, &st) != 0) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to get file size %s.\n", m_vectorOutput.c_str());
        close(m_fd);
        m_fd = -1;
        throw std::runtime_error("Failed to get file size");
    }
    m_fileSize = st.st_size;

    m_mappedData = mmap(nullptr, m_fileSize, PROT_READ, MAP_PRIVATE, m_fd, 0);
    if (m_mappedData == MAP_FAILED) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to mmap file %s.\n", m_vectorOutput.c_str());
        close(m_fd);
        m_fd = -1;
        throw std::runtime_error("Failed to mmap file");
    }

    if (m_size <= 0 && m_fileSize >= sizeof(SPTAG::SizeType) + sizeof(SPTAG::DimensionType)) {
        m_size = *reinterpret_cast<const SPTAG::SizeType*>(m_mappedData);
        SPTAG::DimensionType fileDim = *reinterpret_cast<const SPTAG::DimensionType*>(
            static_cast<const std::uint8_t*>(m_mappedData) + sizeof(SPTAG::SizeType));
        if (m_dim <= 0) {
            m_dim = fileDim;
        }
    }

    return ErrorCode::Success;
}

template <typename T>
std::shared_ptr<SPTAG::VectorSet>
SPTAG::Helper::VectorSetReader<T>::GetVectorSet(SPTAG::SizeType start, SPTAG::SizeType end) const {
    if (start > m_size)
        start = m_size;
    if (end < 0 || end > m_size)
        end = m_size;

    std::uint64_t offset = ((std::uint64_t)sizeof(T)) * start * m_dim;
    std::uint64_t totalBytes = ((std::uint64_t)sizeof(T)) * (end - start) * m_dim;

    if (totalBytes == 0) {
        SPTAG::VectorValueType valueType = SPTAG::VectorValueType::Float;
        if constexpr (sizeof(T) == 1) {
            valueType = SPTAG::VectorValueType::Int8;
        } else if constexpr (sizeof(T) == 2) {
            valueType = SPTAG::VectorValueType::Int16;
        }
        return std::make_shared<SPTAG::BasicVectorSet>(SPTAG::ByteArray(), valueType, m_dim, 0);
    }

    void* sliceStart = static_cast<std::uint8_t*>(m_mappedData) + offset;

    auto self = this->shared_from_this();
    auto deleter = [self](std::uint8_t*) mutable {
    };
    std::shared_ptr<std::uint8_t> dataHolder(reinterpret_cast<std::uint8_t*>(sliceStart), deleter);

    SPTAG::ByteArray arr(reinterpret_cast<std::uint8_t*>(sliceStart), totalBytes, dataHolder);

    SPTAG::VectorValueType valueType = SPTAG::VectorValueType::Float;
    if constexpr (sizeof(T) == 1) {
        valueType = SPTAG::VectorValueType::Int8;
    } else if constexpr (sizeof(T) == 2) {
        valueType = SPTAG::VectorValueType::Int16;
    }

    LOG(SPTAG::Helper::LogLevel::LL_Info, "Load Vector(%d,%d)\n", end - start, m_dim);
    return std::make_shared<SPTAG::BasicVectorSet>(arr, valueType, m_dim, end - start);
}

template <typename T>
std::shared_ptr<SPTAG::MetadataSet>
SPTAG::Helper::VectorSetReader<T>::GetMetadataSet() const {
    if (fileexists(m_metadataIndexOutput.c_str()) && fileexists(m_metadataConentOutput.c_str()))
        return std::shared_ptr<SPTAG::MetadataSet>(new SPTAG::FileMetadataSet(m_metadataConentOutput, m_metadataIndexOutput));
    return nullptr;
}

template class SPTAG::Helper::VectorSetReader<std::int8_t>;
template class SPTAG::Helper::VectorSetReader<std::uint8_t>;
template class SPTAG::Helper::VectorSetReader<std::int16_t>;
template class SPTAG::Helper::VectorSetReader<float>;
