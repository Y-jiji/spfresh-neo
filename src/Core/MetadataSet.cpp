// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/MetadataSet.h"

#include <string.h>
#include <shared_mutex>

#include "Helper/LockFree.h"
typedef typename SPTAG::Helper::LockFree::LockFreeVector<std::uint64_t> MetadataOffsets;

SPTAG::ErrorCode
SPTAG::MetadataSet::RefineMetadata(std::vector<SPTAG::SizeType>& indices, std::shared_ptr<SPTAG::MetadataSet>& p_newMetadata,
    std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize) const
{
    p_newMetadata.reset(new SPTAG::MemMetadataSet(p_blockSize, p_capacity, p_metaSize));
    for (SPTAG::SizeType& t : indices) {
        p_newMetadata->Add(GetMetadata(t));
    }
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::MetadataSet::RefineMetadata(std::vector<SPTAG::SizeType>& indices, std::shared_ptr<SPTAG::Helper::DiskIO> p_metaOut, std::shared_ptr<SPTAG::Helper::DiskIO> p_metaIndexOut) const
{
    SPTAG::SizeType R = (SPTAG::SizeType)indices.size();
    IOBINARY(p_metaIndexOut, WriteBinary, sizeof(SPTAG::SizeType), (const char*)&R);
    std::uint64_t offset = 0;
    for (SPTAG::SizeType i = 0; i < R; i++) {
        IOBINARY(p_metaIndexOut, WriteBinary, sizeof(std::uint64_t), (const char*)&offset);
        SPTAG::ByteArray meta = GetMetadata(indices[i]);
        offset += meta.Length();
    }
    IOBINARY(p_metaIndexOut, WriteBinary, sizeof(std::uint64_t), (const char*)&offset);

    for (SPTAG::SizeType i = 0; i < R; i++) {
        SPTAG::ByteArray meta = GetMetadata(indices[i]);
        IOBINARY(p_metaOut, WriteBinary, sizeof(uint8_t) * meta.Length(), (const char*)meta.Data());
    }
    SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Info, "Save MetaIndex(%d) Meta(%llu)\n", R, offset);
    return SPTAG::ErrorCode::Success;
}


SPTAG::ErrorCode 
SPTAG::MetadataSet::RefineMetadata(std::vector<SPTAG::SizeType>& indices, const std::string& p_metaFile, const std::string& p_metaindexFile) const
{
    {
        std::shared_ptr<SPTAG::Helper::DiskIO> ptrMeta = SPTAG::f_createIO(), ptrMetaIndex = SPTAG::f_createIO();
        if (ptrMeta == nullptr || ptrMetaIndex == nullptr || !ptrMeta->Initialize((p_metaFile + "_tmp").c_str(), std::ios::binary | std::ios::out) || !ptrMetaIndex->Initialize((p_metaindexFile + "_tmp").c_str(), std::ios::binary | std::ios::out))
        return SPTAG::ErrorCode::FailedCreateFile;

        SPTAG::ErrorCode ret = RefineMetadata(indices, ptrMeta, ptrMetaIndex);
        if (ret != SPTAG::ErrorCode::Success) return ret;
    }

    if (fileexists(p_metaFile.c_str())) std::remove(p_metaFile.c_str());
    if (fileexists(p_metaindexFile.c_str())) std::remove(p_metaindexFile.c_str());
    std::rename((p_metaFile + "_tmp").c_str(), p_metaFile.c_str());
    std::rename((p_metaindexFile + "_tmp").c_str(), p_metaindexFile.c_str());
    return SPTAG::ErrorCode::Success;
}


void
SPTAG::MetadataSet::AddBatch(SPTAG::MetadataSet& data)
{
    for (SPTAG::SizeType i = 0; i < data.Count(); i++)
    {
        Add(data.GetMetadata(i));
    }
}


SPTAG::MetadataSet::MetadataSet()
{
}


SPTAG::MetadataSet::~MetadataSet()
{
}

bool SPTAG::MetadataSet::GetMetadataOffsets(const std::uint8_t* p_meta, const std::uint64_t p_metaLength, std::uint64_t* p_offsets, std::uint64_t p_offsetLength, char p_delimiter)
{
    std::uint64_t current = 0;
    p_offsets[current++] = 0;
    for (std::uint64_t i = 0; i < p_metaLength && current < p_offsetLength; i++) {
        if ((char)(p_meta[i]) == p_delimiter)
            p_offsets[current++] = (std::uint64_t)(i + 1);
    }
    if ((char)(p_meta[p_metaLength - 1]) != p_delimiter && current < p_offsetLength)
        p_offsets[current++] = p_metaLength;

    if (current < p_offsetLength) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "The metadata(%d) and vector(%d) numbers are not match! Check whether it is unicode encoding issue.\n", current-1, p_offsetLength-1);
        return false;
    }
    return true;
}


SPTAG::FileMetadataSet::FileMetadataSet(const std::string& p_metafile, const std::string& p_metaindexfile, std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize)
{
    m_fp = SPTAG::f_createIO();
    auto fpidx = SPTAG::f_createIO();
    if (m_fp == nullptr || fpidx == nullptr || !m_fp->Initialize(p_metafile.c_str(), std::ios::binary | std::ios::in) || !fpidx->Initialize(p_metaindexfile.c_str(), std::ios::binary | std::ios::in)) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "ERROR: Cannot open meta files %s or %s!\n", p_metafile.c_str(), p_metaindexfile.c_str());
        throw std::runtime_error("Cannot open meta files");
    }

    if (fpidx->ReadBinary(sizeof(m_count), (char*)&m_count) != sizeof(m_count)) {
        LOG(Helper::LogLevel::LL_Error, "ERROR: Cannot read FileMetadataSet!\n");
        throw std::runtime_error("Cannot read meta files");
    }

    m_offsets.reserve(p_blockSize);
    m_offsets.resize(m_count + 1);
    if (fpidx->ReadBinary(sizeof(std::uint64_t) * (m_count + 1), (char*)m_offsets.data()) != sizeof(std::uint64_t) * (m_count + 1)) {
        LOG(Helper::LogLevel::LL_Error, "ERROR: Cannot read FileMetadataSet!\n");
        throw std::runtime_error("Cannot read meta files");
    }
    m_newdata.reserve(p_blockSize * p_metaSize);
    m_lock.reset(new std::shared_timed_mutex, std::default_delete<std::shared_timed_mutex>());
    LOG(Helper::LogLevel::LL_Info, "Load MetaIndex(%d) Meta(%llu)\n", m_count, m_offsets[m_count]);
}


SPTAG::FileMetadataSet::~FileMetadataSet()
{
}


SPTAG::ByteArray
SPTAG::FileMetadataSet::GetMetadata(SPTAG::SizeType p_vectorID) const
{
    std::unique_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    std::uint64_t startoff = m_offsets[p_vectorID];
    std::uint64_t bytes = m_offsets[p_vectorID + 1] - startoff;
    if (p_vectorID < m_count) {
        SPTAG::ByteArray b = SPTAG::ByteArray::Alloc(bytes);
        m_fp->ReadBinary(bytes, (char*)b.Data(), startoff);
        return b;
    }
    else {
        return SPTAG::ByteArray((std::uint8_t*)m_newdata.data() + startoff - m_offsets[m_count], bytes, false);
    }
}


SPTAG::ByteArray
SPTAG::FileMetadataSet::GetMetadataCopy(SPTAG::SizeType p_vectorID) const
{
    std::unique_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    std::uint64_t startoff = m_offsets[p_vectorID];
    std::uint64_t bytes = m_offsets[p_vectorID + 1] - startoff;
    if (p_vectorID < m_count) {
        SPTAG::ByteArray b = SPTAG::ByteArray::Alloc(bytes);
        m_fp->ReadBinary(bytes, (char*)b.Data(), startoff);
        return b;
    }
    else {
        SPTAG::ByteArray b = SPTAG::ByteArray::Alloc(bytes);
        memcpy(b.Data(), m_newdata.data() + (startoff - m_offsets[m_count]), bytes);
        return b;
    }
}


SPTAG::SizeType
SPTAG::FileMetadataSet::Count() const
{
    std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    return static_cast<SPTAG::SizeType>(m_offsets.size() - 1);
}


bool
SPTAG::FileMetadataSet::Available() const
{
    std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    return m_fp != nullptr && m_offsets.size() > 1;
}


std::pair<std::uint64_t, std::uint64_t>
SPTAG::FileMetadataSet::BufferSize() const
{
    std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    return std::make_pair(m_offsets.back(),
        sizeof(SPTAG::SizeType) + sizeof(std::uint64_t) * m_offsets.size());
}


void
SPTAG::FileMetadataSet::Add(const SPTAG::ByteArray& data)
{
    std::unique_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    m_newdata.insert(m_newdata.end(), data.Data(), data.Data() + data.Length());
    m_offsets.push_back(m_offsets.back() + data.Length());
}


SPTAG::ErrorCode
SPTAG::FileMetadataSet::SaveMetadata(std::shared_ptr<SPTAG::Helper::DiskIO> p_metaOut, std::shared_ptr<SPTAG::Helper::DiskIO> p_metaIndexOut)
{
    std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    SPTAG::SizeType count = Count();
    IOBINARY(p_metaIndexOut, WriteBinary, sizeof(SPTAG::SizeType), (const char*)&count);
    IOBINARY(p_metaIndexOut, WriteBinary, sizeof(std::uint64_t) * m_offsets.size(), (const char*)m_offsets.data());

    std::uint64_t bufsize = 1000000;
    char* buf = new char[bufsize];
    auto readsize = m_fp->ReadBinary(bufsize, buf, 0);
    while (readsize > 0) {
        IOBINARY(p_metaOut, WriteBinary, readsize, buf);
        readsize = m_fp->ReadBinary(bufsize, buf);
    }
    delete[] buf;

    if (m_newdata.size() > 0) {
        IOBINARY(p_metaOut, WriteBinary, m_newdata.size(), (const char*)m_newdata.data());
    }
    SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Info, "Save MetaIndex(%llu) Meta(%llu)\n", m_offsets.size() - 1, m_offsets.back());
    return SPTAG::ErrorCode::Success;
}


SPTAG::ErrorCode
SPTAG::FileMetadataSet::SaveMetadata(const std::string& p_metaFile, const std::string& p_metaindexFile)
{
    {
        std::shared_ptr<SPTAG::Helper::DiskIO> metaOut = SPTAG::f_createIO(), metaIndexOut = SPTAG::f_createIO();
        if (metaOut == nullptr || metaIndexOut == nullptr || !metaOut->Initialize((p_metaFile + "_tmp").c_str(), std::ios::binary | std::ios::out) || !metaIndexOut->Initialize((p_metaindexFile + "_tmp").c_str(), std::ios::binary | std::ios::out))
            return SPTAG::ErrorCode::FailedCreateFile;

        SPTAG::ErrorCode ret = SaveMetadata(metaOut, metaIndexOut);
        if (ret != SPTAG::ErrorCode::Success) return ret;
    }
    {
        std::unique_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
        m_fp->ShutDown();
        if (fileexists(p_metaFile.c_str())) std::remove(p_metaFile.c_str());
        if (fileexists(p_metaindexFile.c_str())) std::remove(p_metaindexFile.c_str());
        std::rename((p_metaFile + "_tmp").c_str(), p_metaFile.c_str());
        std::rename((p_metaindexFile + "_tmp").c_str(), p_metaindexFile.c_str());
        if (!m_fp->Initialize(p_metaFile.c_str(), std::ios::binary | std::ios::in)) return SPTAG::ErrorCode::FailedOpenFile;
        m_count = static_cast<SPTAG::SizeType>(m_offsets.size() - 1);
        m_newdata.clear();
    }
    return SPTAG::ErrorCode::Success;
}


SPTAG::MemMetadataSet::MemMetadataSet(std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize): m_count(0), m_metadataHolder(SPTAG::ByteArray::c_empty)
{
    m_pOffsets.reset(new MetadataOffsets, std::default_delete<MetadataOffsets>());
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    m_offsets.reserve(p_blockSize, p_capacity);
    m_offsets.push_back(0);
    m_newdata.reserve(p_blockSize * p_metaSize);
    m_lock.reset(new std::shared_timed_mutex, std::default_delete<std::shared_timed_mutex>());
}


SPTAG::ErrorCode
SPTAG::MemMetadataSet::Init(std::shared_ptr<SPTAG::Helper::DiskIO> p_metain, std::shared_ptr<SPTAG::Helper::DiskIO> p_metaindexin,
    std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize)
{
    SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Info, "Reading m_count\n");
    IOBINARY(p_metaindexin, ReadBinary, sizeof(m_count), (char*)&m_count);
    m_pOffsets.reset(new MetadataOffsets, std::default_delete<MetadataOffsets>());
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    m_offsets.reserve(p_blockSize, p_capacity);
    {
        std::vector<std::uint64_t> tmp(m_count + 1, 0);
        IOBINARY(p_metaindexin, ReadBinary, sizeof(std::uint64_t) * (m_count + 1), (char*)tmp.data());
        m_offsets.assign(tmp.data(), tmp.data() + tmp.size());
    }
    m_metadataHolder = SPTAG::ByteArray::Alloc(m_offsets[m_count]);
    IOBINARY(p_metain, ReadBinary, m_metadataHolder.Length(), (char*)m_metadataHolder.Data());

    m_newdata.reserve(p_blockSize * p_metaSize);
    m_lock.reset(new std::shared_timed_mutex, std::default_delete<std::shared_timed_mutex>());
    SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Info, "Load MetaIndex(%d) Meta(%llu)\n", m_count, m_offsets[m_count]);
    return SPTAG::ErrorCode::Success;
}


SPTAG::MemMetadataSet::MemMetadataSet(std::shared_ptr<SPTAG::Helper::DiskIO> p_metain, std::shared_ptr<SPTAG::Helper::DiskIO> p_metaindexin,
    std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize)
{
    if (Init(p_metain, p_metaindexin, p_blockSize, p_capacity, p_metaSize) != SPTAG::ErrorCode::Success) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "ERROR: Cannot read MemMetadataSet!\n");
        throw std::runtime_error("Cannot read MemMetadataSet");
    }
}

SPTAG::MemMetadataSet::MemMetadataSet(const std::string& p_metafile, const std::string& p_metaindexfile,
    std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize)
{
    std::shared_ptr<SPTAG::Helper::DiskIO> ptrMeta = SPTAG::f_createIO(), ptrMetaIndex = SPTAG::f_createIO();
    if (ptrMeta == nullptr || ptrMetaIndex == nullptr || !ptrMeta->Initialize(p_metafile.c_str(), std::ios::binary | std::ios::in) || !ptrMetaIndex->Initialize(p_metaindexfile.c_str(), std::ios::binary | std::ios::in)) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "ERROR: Cannot open meta files %s or %s!\n", p_metafile.c_str(),  p_metaindexfile.c_str());
        throw std::runtime_error("Cannot open MemMetadataSet files");
    }
    if (Init(ptrMeta, ptrMetaIndex, p_blockSize, p_capacity, p_metaSize) != SPTAG::ErrorCode::Success) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "ERROR: Cannot read MemMetadataSet!\n");
        throw std::runtime_error("Cannot read MemMetadataSet");
    }
}


SPTAG::MemMetadataSet::MemMetadataSet(SPTAG::ByteArray p_metadata, SPTAG::ByteArray p_offsets, SPTAG::SizeType p_count)
    : m_count(p_count), m_metadataHolder(std::move(p_metadata))
{
    m_pOffsets.reset(new MetadataOffsets, std::default_delete<MetadataOffsets>());
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    m_offsets.reserve(p_count + 1, p_count + 1);
    const std::uint64_t* newdata = reinterpret_cast<const std::uint64_t*>(p_offsets.Data());
    m_offsets.assign(newdata, newdata + p_count + 1);
    m_lock.reset(new std::shared_timed_mutex, std::default_delete<std::shared_timed_mutex>());
}

SPTAG::MemMetadataSet::MemMetadataSet(SPTAG::ByteArray p_metadata, SPTAG::ByteArray p_offsets, SPTAG::SizeType p_count,
    std::uint64_t p_blockSize, std::uint64_t p_capacity, std::uint64_t p_metaSize)
    : m_count(p_count), m_metadataHolder(std::move(p_metadata))
{
    m_pOffsets.reset(new MetadataOffsets, std::default_delete<MetadataOffsets>());
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    m_offsets.reserve(p_blockSize, p_capacity);
    const std::uint64_t* newdata = reinterpret_cast<const std::uint64_t*>(p_offsets.Data());
    m_offsets.assign(newdata, newdata + p_count + 1);
    m_newdata.reserve(p_blockSize * p_metaSize);
    m_lock.reset(new std::shared_timed_mutex, std::default_delete<std::shared_timed_mutex>());
}


SPTAG::MemMetadataSet::~MemMetadataSet()
{
}


SPTAG::ByteArray
SPTAG::MemMetadataSet::GetMetadata(SPTAG::SizeType p_vectorID) const
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    std::uint64_t startoff = m_offsets[p_vectorID];
    std::uint64_t bytes = m_offsets[p_vectorID + 1] - startoff;
    if (p_vectorID < m_count) {
        return SPTAG::ByteArray(m_metadataHolder.Data() + startoff, bytes, false);
    } else {
        std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
        return SPTAG::ByteArray((std::uint8_t*)m_newdata.data() + startoff - m_offsets[m_count], bytes, false);
    }
}


SPTAG::ByteArray
SPTAG::MemMetadataSet::GetMetadataCopy(SPTAG::SizeType p_vectorID) const
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    std::uint64_t startoff = m_offsets[p_vectorID];
    std::uint64_t bytes = m_offsets[p_vectorID + 1] - startoff;
    if (p_vectorID < m_count) {
        SPTAG::ByteArray b = SPTAG::ByteArray::Alloc(bytes);
        memcpy(b.Data(), m_metadataHolder.Data() + startoff, bytes);
        return b;
    } else {
        SPTAG::ByteArray b = SPTAG::ByteArray::Alloc(bytes);
        std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
        memcpy(b.Data(), m_newdata.data() + (startoff - m_offsets[m_count]), bytes);
        return b;
    }
}


SPTAG::SizeType
SPTAG::MemMetadataSet::Count() const
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    return static_cast<SPTAG::SizeType>(m_offsets.size() - 1);
}


bool
SPTAG::MemMetadataSet::Available() const
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    return m_offsets.size() > 1;
}


std::pair<std::uint64_t, std::uint64_t>
SPTAG::MemMetadataSet::BufferSize() const
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    auto n = m_offsets.size();
    return std::make_pair(m_offsets[n-1],
        sizeof(SPTAG::SizeType) + sizeof(std::uint64_t) * n);
}


void
SPTAG::MemMetadataSet::Add(const SPTAG::ByteArray& data)
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    std::unique_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
    m_newdata.insert(m_newdata.end(), data.Data(), data.Data() + data.Length());
    if (!m_offsets.push_back(m_offsets.back() + data.Length())) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "Insert MetaIndex error! DataCapacity overflow!\n");
        m_newdata.resize(m_newdata.size() - data.Length());
    }
}


SPTAG::ErrorCode
SPTAG::MemMetadataSet::SaveMetadata(std::shared_ptr<SPTAG::Helper::DiskIO> p_metaOut, std::shared_ptr<SPTAG::Helper::DiskIO> p_metaIndexOut)
{
    auto& m_offsets = *static_cast<MetadataOffsets*>(m_pOffsets.get());
    SPTAG::SizeType count = Count();
    IOBINARY(p_metaIndexOut, WriteBinary, sizeof(SPTAG::SizeType), (const char*)&count);
    for (SPTAG::SizeType i = 0; i <= count; i++) {
        IOBINARY(p_metaIndexOut, WriteBinary, sizeof(std::uint64_t), (const char*)(&m_offsets[i]));
    }

    IOBINARY(p_metaOut, WriteBinary, m_metadataHolder.Length(), reinterpret_cast<const char*>(m_metadataHolder.Data()));
    if (m_newdata.size() > 0) {
        std::shared_lock<std::shared_timed_mutex> lock(*static_cast<std::shared_timed_mutex*>(m_lock.get()));
        IOBINARY(p_metaOut, WriteBinary, m_offsets[count] - m_offsets[m_count], (const char*)m_newdata.data());
    }
    SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Info, "Save MetaIndex(%llu) Meta(%llu)\n", m_offsets.size() - 1, m_offsets.back());
    return SPTAG::ErrorCode::Success;
}



SPTAG::ErrorCode
SPTAG::MemMetadataSet::SaveMetadata(const std::string& p_metaFile, const std::string& p_metaindexFile)
{
    {
        std::shared_ptr<SPTAG::Helper::DiskIO> metaOut = SPTAG::f_createIO(), metaIndexOut = SPTAG::f_createIO();
        if (metaOut == nullptr || metaIndexOut == nullptr || !metaOut->Initialize((p_metaFile + "_tmp").c_str(), std::ios::binary | std::ios::out) || !metaIndexOut->Initialize((p_metaindexFile + "_tmp").c_str(), std::ios::binary | std::ios::out))
            return SPTAG::ErrorCode::FailedCreateFile;

        SPTAG::ErrorCode ret = SaveMetadata(metaOut, metaIndexOut);
        if (ret != SPTAG::ErrorCode::Success) return ret;
    }
    if (fileexists(p_metaFile.c_str())) std::remove(p_metaFile.c_str());
    if (fileexists(p_metaindexFile.c_str())) std::remove(p_metaindexFile.c_str());
    std::rename((p_metaFile + "_tmp").c_str(), p_metaFile.c_str());
    std::rename((p_metaindexFile + "_tmp").c_str(), p_metaindexFile.c_str());
    return SPTAG::ErrorCode::Success;
}

