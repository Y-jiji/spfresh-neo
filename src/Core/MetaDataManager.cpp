// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/MetaDataManager.h"
#include "Helper/StringConvert.h"

typedef typename SPTAG::Helper::Concurrent::ConcurrentMap<std::string, SPTAG::SizeType> MetadataMap;



SPTAG::MetaDataManager::MetaDataManager()
    : m_sIndexName("")
    , m_sMetadataFile("metadata.bin")
    , m_sMetadataIndexFile("metadataIndex.bin")
    , m_sQuantizerFile("quantizer.bin")
    , m_pMetaToVec(nullptr)
{
}

SPTAG::MetaDataManager::~MetaDataManager()
{
}

std::string
SPTAG::MetaDataManager::GetIndexName() const
{
    if (m_sIndexName == "") return "";
    return m_sIndexName;
}

void
SPTAG::MetaDataManager::SetIndexName(const std::string& p_name)
{
    m_sIndexName = p_name;
}

std::string
SPTAG::MetaDataManager::GetMetadataFile() const
{
    return m_sMetadataFile;
}

void
SPTAG::MetaDataManager::SetMetadataFile(const std::string& p_file)
{
    m_sMetadataFile = p_file;
}

std::string
SPTAG::MetaDataManager::GetMetadataIndexFile() const
{
    return m_sMetadataIndexFile;
}

void
SPTAG::MetaDataManager::SetMetadataIndexFile(const std::string& p_file)
{
    m_sMetadataIndexFile = p_file;
}

std::string
SPTAG::MetaDataManager::GetQuantizerFile() const
{
    return m_sQuantizerFile;
}

void
SPTAG::MetaDataManager::SetQuantizerFile(const std::string& p_file)
{
    m_sQuantizerFile = p_file;
}

bool
SPTAG::MetaDataManager::HasMetaMapping() const
{
    return nullptr != m_pMetaToVec;
}

SPTAG::SizeType
SPTAG::MetaDataManager::GetMetaMapping(std::string& meta) const
{
    MetadataMap* ptr = static_cast<MetadataMap*>(m_pMetaToVec.get());
    auto iter = ptr->find(meta);
    if (iter != ptr->end()) return iter->second;
    return -1;
}

void
SPTAG::MetaDataManager::UpdateMetaMapping(const std::string& meta, SPTAG::SizeType i)
{
    MetadataMap* ptr = static_cast<MetadataMap*>(m_pMetaToVec.get());
    auto iter = ptr->find(meta);
    if (iter != ptr->end())
    {
    }
    (*ptr)[meta] = i;
}

template<typename ContainSampleFunc>
void
SPTAG::MetaDataManager::BuildMetaMapping(SPTAG::MetadataSet* p_metadata, SPTAG::SizeType p_vectorCount, ContainSampleFunc p_containSample, SPTAG::SizeType p_dataBlockSize, bool p_checkDeleted)
{
    MetadataMap* ptr = new MetadataMap(p_dataBlockSize);
    for (SPTAG::SizeType i = 0; i < p_metadata->Count(); i++) {
        if (!p_checkDeleted || p_containSample(i)) {
            SPTAG::ByteArray meta = p_metadata->GetMetadata(i);
            (*ptr)[std::string((char*)meta.Data(), meta.Length())] = i;
        }
    }
    m_pMetaToVec.reset(ptr, std::default_delete<MetadataMap>());
}

template void SPTAG::MetaDataManager::BuildMetaMapping<bool(*)(SPTAG::SizeType)>(SPTAG::MetadataSet*, SPTAG::SizeType, bool (*)(SPTAG::SizeType), SPTAG::SizeType, bool);
template void SPTAG::MetaDataManager::BuildMetaMapping<std::function<bool(SPTAG::SizeType)>>(SPTAG::MetadataSet*, SPTAG::SizeType, std::function<bool(SPTAG::SizeType)>, SPTAG::SizeType, bool);
