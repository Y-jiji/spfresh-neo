// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/MetaDataManager.h"
#include "Helper/StringConvert.h"

typedef typename SPTAG::Helper::Concurrent::ConcurrentMap<std::string, SPTAG::SizeType> MetadataMap;

using namespace SPTAG;

MetaDataManager::MetaDataManager()
    : m_sIndexName("")
    , m_sMetadataFile("metadata.bin")
    , m_sMetadataIndexFile("metadataIndex.bin")
    , m_sQuantizerFile("quantizer.bin")
    , m_pMetaToVec(nullptr)
{
}

MetaDataManager::~MetaDataManager()
{
}

std::string
MetaDataManager::GetIndexName() const
{
    if (m_sIndexName == "") return "";
    return m_sIndexName;
}

void
MetaDataManager::SetIndexName(const std::string& p_name)
{
    m_sIndexName = p_name;
}

std::string
MetaDataManager::GetMetadataFile() const
{
    return m_sMetadataFile;
}

void
MetaDataManager::SetMetadataFile(const std::string& p_file)
{
    m_sMetadataFile = p_file;
}

std::string
MetaDataManager::GetMetadataIndexFile() const
{
    return m_sMetadataIndexFile;
}

void
MetaDataManager::SetMetadataIndexFile(const std::string& p_file)
{
    m_sMetadataIndexFile = p_file;
}

std::string
MetaDataManager::GetQuantizerFile() const
{
    return m_sQuantizerFile;
}

void
MetaDataManager::SetQuantizerFile(const std::string& p_file)
{
    m_sQuantizerFile = p_file;
}

bool
MetaDataManager::HasMetaMapping() const
{
    return nullptr != m_pMetaToVec;
}

SizeType
MetaDataManager::GetMetaMapping(std::string& meta) const
{
    MetadataMap* ptr = static_cast<MetadataMap*>(m_pMetaToVec.get());
    auto iter = ptr->find(meta);
    if (iter != ptr->end()) return iter->second;
    return -1;
}

void
MetaDataManager::UpdateMetaMapping(const std::string& meta, SizeType i)
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
MetaDataManager::BuildMetaMapping(MetadataSet* p_metadata, SizeType p_vectorCount, ContainSampleFunc p_containSample, SizeType p_dataBlockSize, bool p_checkDeleted)
{
    MetadataMap* ptr = new MetadataMap(p_dataBlockSize);
    for (SizeType i = 0; i < p_metadata->Count(); i++) {
        if (!p_checkDeleted || p_containSample(i)) {
            ByteArray meta = p_metadata->GetMetadata(i);
            (*ptr)[std::string((char*)meta.Data(), meta.Length())] = i;
        }
    }
    m_pMetaToVec.reset(ptr, std::default_delete<MetadataMap>());
}

template void MetaDataManager::BuildMetaMapping<bool(*)(SizeType)>(MetadataSet*, SizeType, bool (*)(SizeType), SizeType, bool);
template void MetaDataManager::BuildMetaMapping<std::function<bool(SizeType)>>(MetadataSet*, SizeType, std::function<bool(SizeType)>, SizeType, bool);
