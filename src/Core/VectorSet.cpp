// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/VectorSet.h"
#include "Utils/CommonUtils.h"

SPTAG::VectorSet::VectorSet() {
}

SPTAG::VectorSet::~VectorSet() {
}

SPTAG::BasicVectorSet::BasicVectorSet(const SPTAG::ByteArray& p_bytesArray, SPTAG::VectorValueType p_valueType, SPTAG::DimensionType p_dimension, SPTAG::SizeType p_vectorCount)
    : m_data(p_bytesArray),
      m_valueType(p_valueType),
      m_dimension(p_dimension),
      m_vectorCount(p_vectorCount),
      m_perVectorDataSize(static_cast<SPTAG::SizeType>(p_dimension * SPTAG::GetValueTypeSize(p_valueType))) {
}

SPTAG::BasicVectorSet::~BasicVectorSet() {
}

SPTAG::VectorValueType
SPTAG::BasicVectorSet::GetValueType() const {
    return m_valueType;
}

void* SPTAG::BasicVectorSet::GetVector(SPTAG::SizeType p_vectorID) const {
    if (p_vectorID < 0 || p_vectorID >= m_vectorCount) {
        return nullptr;
    }

    return reinterpret_cast<void*>(m_data.Data() + ((size_t)p_vectorID) * m_perVectorDataSize);
}

void* SPTAG::BasicVectorSet::GetData() const {
    return reinterpret_cast<void*>(m_data.Data());
}

SPTAG::DimensionType
SPTAG::BasicVectorSet::Dimension() const {
    return m_dimension;
}

SPTAG::SizeType
SPTAG::BasicVectorSet::Count() const {
    return m_vectorCount;
}

bool SPTAG::BasicVectorSet::Available() const {
    return m_data.Data() != nullptr;
}

SPTAG::ErrorCode
SPTAG::BasicVectorSet::Save(const std::string& p_vectorFile) const {
    auto fp = SPTAG::f_createIO();
    if (fp == nullptr || !fp->Initialize(p_vectorFile.c_str(), std::ios::binary | std::ios::out))
        return SPTAG::ErrorCode::FailedOpenFile;

    IOBINARY(fp, WriteBinary, m_data.Length(), (char*)m_data.Data());
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::BasicVectorSet::AppendSave(const std::string& p_vectorFile) const {
    auto fp_append = SPTAG::f_createIO();
    if (fp_append == nullptr || !fp_append->Initialize(p_vectorFile.c_str(), std::ios::binary | std::ios::out | std::ios::app))
        return SPTAG::ErrorCode::FailedOpenFile;

    IOBINARY(fp_append, WriteBinary, m_data.Length(), (char*)m_data.Data());

    return SPTAG::ErrorCode::Success;
}

SPTAG::SizeType SPTAG::BasicVectorSet::PerVectorDataSize() const {
    return (SPTAG::SizeType)m_perVectorDataSize;
}

void SPTAG::BasicVectorSet::Normalize(int p_threads) {
    switch (m_valueType) {
#define DefineVectorValueType(Name, Type)                                                                                                                                 \
    case SPTAG::VectorValueType::Name:                                                                                                                                    \
        SPTAG::COMMON::Utils::BatchNormalize<Type>(reinterpret_cast<Type*>(m_data.Data()), m_vectorCount, m_dimension, SPTAG::COMMON::Utils::GetBase<Type>(), p_threads); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType
        default:
            break;
    }
}
