// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_METADATAMANAGER_H_
#define _SPTAG_METADATAMANAGER_H_

#include "Common.h"
#include "MetadataSet.h"
#include "Helper/ConcurrentSet.h"
#include <string>
#include <memory>
#include <functional>

namespace SPTAG
{

class MetaDataManager
{
public:
    MetaDataManager();
    ~MetaDataManager();

    std::string GetIndexName() const;
    void SetIndexName(const std::string& p_name);

    std::string GetMetadataFile() const;
    void SetMetadataFile(const std::string& p_file);

    std::string GetMetadataIndexFile() const;
    void SetMetadataIndexFile(const std::string& p_file);

    std::string GetQuantizerFile() const;
    void SetQuantizerFile(const std::string& p_file);

    bool HasMetaMapping() const;
    SizeType GetMetaMapping(std::string& meta) const;
    void UpdateMetaMapping(const std::string& meta, SizeType i);

    template<typename ContainSampleFunc>
    void BuildMetaMapping(MetadataSet* p_metadata, SizeType p_vectorCount, ContainSampleFunc p_containSample, SizeType p_dataBlockSize, bool p_checkDeleted = true);

private:
    std::string m_sIndexName;
    std::string m_sMetadataFile;
    std::string m_sMetadataIndexFile;
    std::string m_sQuantizerFile;
    std::shared_ptr<void> m_pMetaToVec;
};

} // namespace SPTAG

#endif // _SPTAG_METADATAMANAGER_H_
