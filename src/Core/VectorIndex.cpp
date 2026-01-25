// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/VectorIndex.h"
#include "Core/MetaDataManager.h"
#include "Helper/CommonHelper.h"
#include "Helper/StringConvert.h"
#include "Helper/SimpleIniReader.h"
#include "Helper/ConcurrentSet.h"

#include "Core/BKT/Index.h"
#include "Core/SPANN/Index.h"

typedef typename SPTAG::Helper::Concurrent::ConcurrentMap<std::string, SPTAG::SizeType> MetadataMap;

#ifdef DEBUG
std::shared_ptr<SPTAG::Helper::Logger> SPTAG::g_pLogger(new SPTAG::Helper::SimpleLogger(SPTAG::Helper::LogLevel::LL_Debug));
#else
std::shared_ptr<SPTAG::Helper::Logger> SPTAG::g_pLogger(new SPTAG::Helper::SimpleLogger(SPTAG::Helper::LogLevel::LL_Info));
#endif

std::mt19937 SPTAG::rg;

std::shared_ptr<SPTAG::Helper::DiskIO> (*SPTAG::f_createIO)() = []() -> std::shared_ptr<SPTAG::Helper::DiskIO> { return std::shared_ptr<SPTAG::Helper::DiskIO>(new SPTAG::Helper::SimpleFileIO()); };

namespace SPTAG {

bool copyfile(const char* oldpath, const char* newpath) {
    auto input = f_createIO(), output = f_createIO();
    if (input == nullptr || !input->Initialize(oldpath, std::ios::binary | std::ios::in) ||
        output == nullptr || !output->Initialize(newpath, std::ios::binary | std::ios::out)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to open files: %s %s\n", oldpath, newpath);
        return false;
    }

    const std::size_t bufferSize = 1 << 30;
    std::unique_ptr<char[]> bufferHolder(new char[bufferSize]);

    std::uint64_t readSize;
    while ((readSize = input->ReadBinary(bufferSize, bufferHolder.get()))) {
        if (output->WriteBinary(readSize, bufferHolder.get()) != readSize) {
            LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", newpath);
            return false;
        }
    }
    input->ShutDown();
    output->ShutDown();
    return true;
}

void listdir(std::string path, std::vector<std::string>& files) {
    if (auto dirptr = opendir(path.substr(0, path.length() - 1).c_str())) {
        while (auto f = readdir(dirptr)) {
            if (!f->d_name || f->d_name[0] == '.')
                continue;
            std::string tmp = path.substr(0, path.length() - 1);
            tmp += std::string(f->d_name);
            if (f->d_type == DT_DIR) {
                listdir(tmp + FolderSep + "*", files);
            } else {
                files.push_back(tmp);
            }
        }
        closedir(dirptr);
    }
}
}  // namespace SPTAG

SPTAG::VectorIndex::VectorIndex() {
}

SPTAG::VectorIndex::~VectorIndex() {
}

std::string
SPTAG::VectorIndex::GetParameter(const std::string& p_param, const std::string& p_section) const {
    return GetParameter(p_param.c_str(), p_section.c_str());
}

SPTAG::ErrorCode
SPTAG::VectorIndex::SetParameter(const std::string& p_param, const std::string& p_value, const std::string& p_section) {
    return SetParameter(p_param.c_str(), p_value.c_str(), p_section.c_str());
}

void SPTAG::VectorIndex::SetMetadata(SPTAG::MetadataSet* p_new) {
    m_pMetadata.reset(p_new);
}

SPTAG::MetadataSet*
SPTAG::VectorIndex::GetMetadata() const {
    return m_pMetadata.get();
}

SPTAG::ByteArray
SPTAG::VectorIndex::GetMetadata(SPTAG::SizeType p_vectorID) const {
    if (nullptr != m_pMetadata) {
        return m_pMetadata->GetMetadata(p_vectorID);
    }
    return SPTAG::ByteArray::c_empty;
}

std::shared_ptr<std::vector<std::uint64_t>> SPTAG::VectorIndex::CalculateBufferSize() const {
    std::shared_ptr<std::vector<std::uint64_t>> ret = BufferSize();

    if (m_pMetadata != nullptr) {
        auto metasize = m_pMetadata->BufferSize();
        ret->push_back(metasize.first);
        ret->push_back(metasize.second);
    }

    if (m_pQuantizer) {
        ret->push_back(m_pQuantizer->BufferSize());
    }
    return std::move(ret);
}

SPTAG::ErrorCode
SPTAG::VectorIndex::LoadIndexConfig(SPTAG::Helper::IniReader& p_reader) {
    std::string metadataSection("MetaData");
    if (p_reader.DoesSectionExist(metadataSection)) {
        m_metadataManager.SetMetadataFile(p_reader.GetParameter(metadataSection, "MetaDataFilePath", std::string()));
        m_metadataManager.SetMetadataIndexFile(p_reader.GetParameter(metadataSection, "MetaDataIndexPath", std::string()));
    }

    std::string quantizerSection("Quantizer");
    if (p_reader.DoesSectionExist(quantizerSection)) {
        m_metadataManager.SetQuantizerFile(p_reader.GetParameter(quantizerSection, "QuantizerFilePath", std::string()));
    }
    return LoadConfig(p_reader);
}

SPTAG::ErrorCode
SPTAG::VectorIndex::SaveIndexConfig(std::shared_ptr<SPTAG::Helper::DiskIO> p_configOut) {
    if (nullptr != m_pMetadata) {
        IOSTRING(p_configOut, WriteString, "[MetaData]\n");
        IOSTRING(p_configOut, WriteString, ("MetaDataFilePath=" + m_metadataManager.GetMetadataFile() + "\n").c_str());
        IOSTRING(p_configOut, WriteString, ("MetaDataIndexPath=" + m_metadataManager.GetMetadataIndexFile() + "\n").c_str());
        if (m_metadataManager.HasMetaMapping())
            IOSTRING(p_configOut, WriteString, "MetaDataToVectorIndex=true\n");
        IOSTRING(p_configOut, WriteString, "\n");
    }

    if (m_pQuantizer) {
        IOSTRING(p_configOut, WriteString, "[Quantizer]\n");
        IOSTRING(p_configOut, WriteString, ("QuantizerFilePath=" + m_metadataManager.GetQuantizerFile() + "\n").c_str());
        IOSTRING(p_configOut, WriteString, "\n");
    }

    IOSTRING(p_configOut, WriteString, "[Index]\n");
    IOSTRING(p_configOut, WriteString, ("IndexAlgoType=" + SPTAG::Helper::Convert::ConvertToString(GetIndexAlgoType()) + "\n").c_str());
    IOSTRING(p_configOut, WriteString, ("ValueType=" + SPTAG::Helper::Convert::ConvertToString(GetVectorValueType()) + "\n").c_str());
    IOSTRING(p_configOut, WriteString, "\n");

    return SaveConfig(p_configOut);
}

SPTAG::SizeType
SPTAG::VectorIndex::GetMetaMapping(std::string& meta) const {
    return m_metadataManager.GetMetaMapping(meta);
}

void SPTAG::VectorIndex::UpdateMetaMapping(const std::string& meta, SPTAG::SizeType i) {
    SPTAG::SizeType existing = m_metadataManager.GetMetaMapping(const_cast<std::string&>(meta));
    if (existing >= 0)
        DeleteIndex(existing);
    m_metadataManager.UpdateMetaMapping(meta, i);
}

void SPTAG::VectorIndex::BuildMetaMapping(bool p_checkDeleted) {
    m_metadataManager.BuildMetaMapping(m_pMetadata.get(), GetNumSamples(), std::function<bool(SPTAG::SizeType)>([this](SPTAG::SizeType idx) -> bool {
                                           return this->ContainSample(idx);
                                       }),
                                       m_iDataBlockSize, p_checkDeleted);
}

SPTAG::ErrorCode
SPTAG::VectorIndex::SaveIndex(std::string& p_config, const std::vector<SPTAG::ByteArray>& p_indexBlobs) {
    if (!m_bReady || GetNumSamples() - GetNumDeleted() == 0)
        return SPTAG::ErrorCode::EmptyIndex;

    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    {
        std::shared_ptr<SPTAG::Helper::DiskIO> p_configStream(new SPTAG::Helper::SimpleBufferIO());
        if (p_configStream == nullptr || !p_configStream->Initialize(nullptr, std::ios::out))
            return SPTAG::ErrorCode::EmptyDiskIO;
        if ((ret = SaveIndexConfig(p_configStream)) != SPTAG::ErrorCode::Success)
            return ret;
        p_config.resize(p_configStream->TellP());
        IOBINARY(p_configStream, ReadBinary, p_config.size(), (char*)p_config.c_str(), 0);
    }

    std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> p_indexStreams;
    for (size_t i = 0; i < p_indexBlobs.size(); i++) {
        std::shared_ptr<SPTAG::Helper::DiskIO> ptr(new SPTAG::Helper::SimpleBufferIO());
        if (ptr == nullptr || !ptr->Initialize((char*)p_indexBlobs[i].Data(), std::ios::binary | std::ios::out, p_indexBlobs[i].Length()))
            return SPTAG::ErrorCode::EmptyDiskIO;
        p_indexStreams.push_back(std::move(ptr));
    }

    size_t metaStart = BufferSize()->size();
    if (NeedRefine()) {
        ret = RefineIndex(p_indexStreams, nullptr);
    } else {
        if (m_pMetadata != nullptr && p_indexStreams.size() >= metaStart + 2) {
            ret = m_pMetadata->SaveMetadata(p_indexStreams[metaStart], p_indexStreams[metaStart + 1]);
        }
        if (SPTAG::ErrorCode::Success == ret)
            ret = SaveIndexData(p_indexStreams);
    }
    if (m_pMetadata != nullptr)
        metaStart += 2;

    if (SPTAG::ErrorCode::Success == ret && m_pQuantizer && p_indexStreams.size() > metaStart) {
        ret = m_pQuantizer->SaveQuantizer(p_indexStreams[metaStart]);
    }
    return ret;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::SaveIndex(const std::string& p_folderPath) {
    if (!m_bReady || GetNumSamples() - GetNumDeleted() == 0)
        return SPTAG::ErrorCode::EmptyIndex;

    std::string folderPath(p_folderPath);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep) {
        folderPath += FolderSep;
    }
    if (!direxists(folderPath.c_str())) {
        mkdir(folderPath.c_str());
    }

    if (GetIndexAlgoType() == SPTAG::IndexAlgoType::SPANN && GetParameter("IndexDirectory", "Base") != p_folderPath) {
        std::vector<std::string> files;
        std::string oldFolder = GetParameter("IndexDirectory", "Base");
        if (!oldFolder.empty() && *(oldFolder.rbegin()) != FolderSep)
            oldFolder += FolderSep;
        listdir((oldFolder + "*").c_str(), files);
        for (auto file : files) {
            size_t firstSep = oldFolder.length(), lastSep = file.find_last_of(FolderSep);
            std::string newFolder = folderPath + ((lastSep > firstSep) ? file.substr(firstSep, lastSep - firstSep) : ""), filename = file.substr(lastSep + 1);
            if (!direxists(newFolder.c_str()))
                mkdir(newFolder.c_str());
            LOG(SPTAG::Helper::LogLevel::LL_Info, "Copy file %s to %s...\n", file.c_str(), (newFolder + FolderSep + filename).c_str());
            if (!copyfile(file.c_str(), (newFolder + FolderSep + filename).c_str()))
                return SPTAG::ErrorCode::DiskIOFail;
        }
        SetParameter("IndexDirectory", p_folderPath, "Base");
    }

    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    {
        auto configFile = SPTAG::f_createIO();
        if (configFile == nullptr || !configFile->Initialize((folderPath + "indexloader.ini").c_str(), std::ios::out))
            return SPTAG::ErrorCode::FailedCreateFile;
        if ((ret = SaveIndexConfig(configFile)) != SPTAG::ErrorCode::Success)
            return ret;
    }

    std::shared_ptr<std::vector<std::string>> indexfiles = GetIndexFiles();
    if (nullptr != m_pMetadata) {
        indexfiles->push_back(m_metadataManager.GetMetadataFile());
        indexfiles->push_back(m_metadataManager.GetMetadataIndexFile());
    }
    if (m_pQuantizer) {
        indexfiles->push_back(m_metadataManager.GetQuantizerFile());
    }
    std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> handles;
    for (std::string& f : *indexfiles) {
        std::string newfile = folderPath + f;
        if (!direxists(newfile.substr(0, newfile.find_last_of(FolderSep)).c_str()))
            mkdir(newfile.substr(0, newfile.find_last_of(FolderSep)).c_str());

        auto ptr = SPTAG::f_createIO();
        if (ptr == nullptr || !ptr->Initialize(newfile.c_str(), std::ios::binary | std::ios::out))
            return SPTAG::ErrorCode::FailedCreateFile;
        handles.push_back(std::move(ptr));
    }

    size_t metaStart = GetIndexFiles()->size();
    if (NeedRefine()) {
        ret = RefineIndex(handles, nullptr);
    } else {
        if (m_pMetadata != nullptr)
            ret = m_pMetadata->SaveMetadata(handles[metaStart], handles[metaStart + 1]);
        if (SPTAG::ErrorCode::Success == ret)
            ret = SaveIndexData(handles);
    }
    if (m_pMetadata != nullptr)
        metaStart += 2;

    if (SPTAG::ErrorCode::Success == ret && m_pQuantizer) {
        ret = m_pQuantizer->SaveQuantizer(handles[metaStart]);
    }
    return ret;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::SaveIndexToFile(const std::string& p_file, SPTAG::IAbortOperation* p_abort) {
    if (!m_bReady || GetNumSamples() - GetNumDeleted() == 0)
        return SPTAG::ErrorCode::EmptyIndex;

    auto fp = SPTAG::f_createIO();
    if (fp == nullptr || !fp->Initialize(p_file.c_str(), std::ios::binary | std::ios::out))
        return SPTAG::ErrorCode::FailedCreateFile;

    auto mp = std::shared_ptr<SPTAG::Helper::DiskIO>(new SPTAG::Helper::SimpleBufferIO());
    if (mp == nullptr || !mp->Initialize(nullptr, std::ios::binary | std::ios::out))
        return SPTAG::ErrorCode::FailedCreateFile;
    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    if ((ret = SaveIndexConfig(mp)) != SPTAG::ErrorCode::Success)
        return ret;

    std::uint64_t configSize = mp->TellP();
    mp->ShutDown();

    IOBINARY(fp, WriteBinary, sizeof(configSize), (char*)&configSize);
    if ((ret = SaveIndexConfig(fp)) != SPTAG::ErrorCode::Success)
        return ret;

    if (p_abort != nullptr && p_abort->ShouldAbort())
        ret = SPTAG::ErrorCode::ExternalAbort;
    else {
        std::uint64_t blobs = CalculateBufferSize()->size();
        IOBINARY(fp, WriteBinary, sizeof(blobs), (char*)&blobs);
        std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> p_indexStreams(blobs, fp);

        if (NeedRefine()) {
            ret = RefineIndex(p_indexStreams, p_abort);
        } else {
            ret = SaveIndexData(p_indexStreams);

            if (p_abort != nullptr && p_abort->ShouldAbort())
                ret = SPTAG::ErrorCode::ExternalAbort;

            if (SPTAG::ErrorCode::Success == ret && m_pMetadata != nullptr)
                ret = m_pMetadata->SaveMetadata(fp, fp);
        }
        if (SPTAG::ErrorCode::Success == ret && m_pQuantizer) {
            ret = m_pQuantizer->SaveQuantizer(fp);
        }
    }
    fp->ShutDown();

    if (ret != SPTAG::ErrorCode::Success)
        std::remove(p_file.c_str());
    return ret;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::BuildIndex(std::shared_ptr<SPTAG::VectorSet> p_vectorSet, std::shared_ptr<SPTAG::MetadataSet> p_metadataSet, bool p_withMetaIndex, bool p_normalized, bool p_shareOwnership) {
    LOG(SPTAG::Helper::LogLevel::LL_Info, "Begin build index...\n");

    bool valueMatches = p_vectorSet->GetValueType() == GetVectorValueType();
    bool quantizerMatches = ((bool)m_pQuantizer) && (p_vectorSet->GetValueType() == SPTAG::VectorValueType::UInt8);
    if (nullptr == p_vectorSet || !(valueMatches || quantizerMatches)) {
        return SPTAG::ErrorCode::Fail;
    }
    m_pMetadata = std::move(p_metadataSet);
    if (p_withMetaIndex && m_pMetadata != nullptr) {
        LOG(SPTAG::Helper::LogLevel::LL_Info, "Build meta mapping...\n");
        BuildMetaMapping(false);
    }
    BuildIndex(p_vectorSet->GetData(), p_vectorSet->Count(), p_vectorSet->Dimension(), p_normalized, p_shareOwnership);
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::SearchIndex(const void* p_vector, int p_vectorCount, int p_neighborCount, bool p_withMeta, SPTAG::BasicResult* p_results) const {
    size_t vectorSize = GetValueTypeSize(GetVectorValueType()) * GetFeatureDim();
#pragma omp parallel for schedule(dynamic, 10)
    for (int i = 0; i < p_vectorCount; i++) {
        SPTAG::QueryResult res((char*)p_vector + i * vectorSize, p_neighborCount, p_withMeta, p_results + i * p_neighborCount);
        SearchIndex(res);
    }
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::AddIndex(std::shared_ptr<SPTAG::VectorSet> p_vectorSet, std::shared_ptr<SPTAG::MetadataSet> p_metadataSet, bool p_withMetaIndex, bool p_normalized) {
    if (nullptr == p_vectorSet || p_vectorSet->GetValueType() != GetVectorValueType()) {
        return SPTAG::ErrorCode::Fail;
    }

    return AddIndex(p_vectorSet->GetData(), p_vectorSet->Count(), p_vectorSet->Dimension(), p_metadataSet, p_withMetaIndex, p_normalized);
}

SPTAG::ErrorCode
SPTAG::VectorIndex::DeleteIndex(SPTAG::ByteArray p_meta) {
    if (!m_metadataManager.HasMetaMapping())
        return SPTAG::ErrorCode::VectorNotFound;

    std::string meta((char*)p_meta.Data(), p_meta.Length());
    SPTAG::SizeType vid = GetMetaMapping(meta);
    if (vid >= 0)
        return DeleteIndex(vid);
    return SPTAG::ErrorCode::VectorNotFound;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::MergeIndex(SPTAG::VectorIndex* p_addindex, int p_threadnum, SPTAG::IAbortOperation* p_abort) {
    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    if (p_addindex->m_pMetadata != nullptr) {
#pragma omp parallel for num_threads(p_threadnum) schedule(dynamic, 128)
        for (SPTAG::SizeType i = 0; i < p_addindex->GetNumSamples(); i++) {
            if (ret == SPTAG::ErrorCode::ExternalAbort)
                continue;

            if (p_addindex->ContainSample(i)) {
                SPTAG::ByteArray meta = p_addindex->GetMetadata(i);
                std::uint64_t offsets[2] = {0, meta.Length()};
                std::shared_ptr<SPTAG::MetadataSet> p_metaSet(new SPTAG::MemMetadataSet(meta, SPTAG::ByteArray((std::uint8_t*)offsets, sizeof(offsets), false), 1));
                AddIndex(p_addindex->GetSample(i), 1, p_addindex->GetFeatureDim(), p_metaSet);
            }

            if (p_abort != nullptr && p_abort->ShouldAbort()) {
                ret = SPTAG::ErrorCode::ExternalAbort;
            }
        }
    } else {
#pragma omp parallel for num_threads(p_threadnum) schedule(dynamic, 128)
        for (SPTAG::SizeType i = 0; i < p_addindex->GetNumSamples(); i++) {
            if (ret == SPTAG::ErrorCode::ExternalAbort)
                continue;

            if (p_addindex->ContainSample(i)) {
                AddIndex(p_addindex->GetSample(i), 1, p_addindex->GetFeatureDim(), nullptr);
            }

            if (p_abort != nullptr && p_abort->ShouldAbort()) {
                ret = SPTAG::ErrorCode::ExternalAbort;
            }
        }
    }
    return ret;
}

const void* SPTAG::VectorIndex::GetSample(SPTAG::ByteArray p_meta, bool& deleteFlag) {
    if (!m_metadataManager.HasMetaMapping())
        return nullptr;

    std::string meta((char*)p_meta.Data(), p_meta.Length());
    SPTAG::SizeType vid = GetMetaMapping(meta);
    if (vid >= 0 && vid < GetNumSamples()) {
        deleteFlag = !ContainSample(vid);
        return GetSample(vid);
    }
    return nullptr;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::LoadQuantizer(std::string p_quantizerFile) {
    auto ptr = SPTAG::f_createIO();
    if (!ptr->Initialize(p_quantizerFile.c_str(), std::ios::binary | std::ios::in)) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to read quantizer file.\n");
        return SPTAG::ErrorCode::FailedOpenFile;
    }
    SetQuantizer(SPTAG::COMMON::IQuantizer::LoadIQuantizer(ptr));
    if (!m_pQuantizer) {
        LOG(SPTAG::Helper::LogLevel::LL_Error, "Failed to load quantizer.\n");
        return SPTAG::ErrorCode::FailedParseValue;
    }
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::LoadIndex(const std::string& p_loaderFilePath, std::shared_ptr<SPTAG::VectorIndex>& p_vectorIndex) {
    std::string folderPath(p_loaderFilePath);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep)
        folderPath += FolderSep;

    SPTAG::Helper::IniReader iniReader;
    {
        auto fp = SPTAG::f_createIO();
        if (fp == nullptr || !fp->Initialize((folderPath + "indexloader.ini").c_str(), std::ios::in))
            return SPTAG::ErrorCode::FailedOpenFile;
        if (SPTAG::ErrorCode::Success != iniReader.LoadIni(fp))
            return SPTAG::ErrorCode::FailedParseValue;
    }
    SPTAG::IndexAlgoType algoType = iniReader.GetParameter("Index", "IndexAlgoType", SPTAG::IndexAlgoType::Undefined);
    SPTAG::VectorValueType valueType = iniReader.GetParameter("Index", "ValueType", SPTAG::VectorValueType::Undefined);

    if (SPTAG::IndexAlgoType::Undefined == algoType || SPTAG::VectorValueType::Undefined == valueType) {
        return SPTAG::ErrorCode::FailedParseValue;
    }

    if (algoType == SPTAG::IndexAlgoType::BKT) {
        switch (valueType) {
#define DefineVectorValueType(Name, Type)                                                 \
    case SPTAG::VectorValueType::Name:                                                    \
        p_vectorIndex = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::BKT::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                break;
        }
    } else if (algoType == SPTAG::IndexAlgoType::SPANN) {
        switch (valueType) {
#define DefineVectorValueType(Name, Type)                                                   \
    case SPTAG::VectorValueType::Name:                                                      \
        p_vectorIndex = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::SPANN::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                break;
        }
    }

    if (p_vectorIndex == nullptr)
        return SPTAG::ErrorCode::FailedParseValue;

    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    if ((ret = p_vectorIndex->LoadIndexConfig(iniReader)) != SPTAG::ErrorCode::Success)
        return ret;

    std::shared_ptr<std::vector<std::string>> indexfiles = p_vectorIndex->GetIndexFiles();
    if (iniReader.DoesSectionExist("MetaData")) {
        indexfiles->push_back(p_vectorIndex->m_metadataManager.GetMetadataFile());
        indexfiles->push_back(p_vectorIndex->m_metadataManager.GetMetadataIndexFile());
    }
    if (iniReader.DoesSectionExist("Quantizer")) {
        indexfiles->push_back(p_vectorIndex->m_metadataManager.GetQuantizerFile());
    }
    std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> handles;
    for (std::string& f : *indexfiles) {
        auto ptr = SPTAG::f_createIO();
        if (ptr == nullptr || !ptr->Initialize((folderPath + f).c_str(), std::ios::binary | std::ios::in)) {
            LOG(SPTAG::Helper::LogLevel::LL_Error, "Cannot open file %s!\n", (folderPath + f).c_str());
            ptr = nullptr;
        }
        handles.push_back(std::move(ptr));
    }

    if ((ret = p_vectorIndex->LoadIndexData(handles)) != SPTAG::ErrorCode::Success)
        return ret;

    size_t metaStart = p_vectorIndex->GetIndexFiles()->size();
    if (iniReader.DoesSectionExist("MetaData")) {
        p_vectorIndex->SetMetadata(new SPTAG::MemMetadataSet(handles[metaStart], handles[metaStart + 1], p_vectorIndex->m_iDataBlockSize, p_vectorIndex->m_iDataCapacity, p_vectorIndex->m_iMetaRecordSize));

        if (!(p_vectorIndex->GetMetadata()->Available())) {
            LOG(SPTAG::Helper::LogLevel::LL_Error, "Error: Failed to load metadata.\n");
            return SPTAG::ErrorCode::Fail;
        }

        if (iniReader.GetParameter("MetaData", "MetaDataToVectorIndex", std::string()) == "true") {
            p_vectorIndex->BuildMetaMapping();
        }
        metaStart += 2;
    }
    if (iniReader.DoesSectionExist("Quantizer")) {
        p_vectorIndex->SetQuantizer(SPTAG::COMMON::IQuantizer::LoadIQuantizer(handles[metaStart]));
        if (!p_vectorIndex->m_pQuantizer)
            return SPTAG::ErrorCode::FailedParseValue;
    }
    p_vectorIndex->m_bReady = true;
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::LoadIndexFromFile(const std::string& p_file, std::shared_ptr<SPTAG::VectorIndex>& p_vectorIndex) {
    auto fp = SPTAG::f_createIO();
    if (fp == nullptr || !fp->Initialize(p_file.c_str(), std::ios::binary | std::ios::in))
        return SPTAG::ErrorCode::FailedOpenFile;

    SPTAG::Helper::IniReader iniReader;
    {
        std::uint64_t configSize;
        IOBINARY(fp, ReadBinary, sizeof(configSize), (char*)&configSize);
        std::vector<char> config(configSize + 1, '\0');
        IOBINARY(fp, ReadBinary, configSize, config.data());

        std::shared_ptr<SPTAG::Helper::DiskIO> bufferhandle(new SPTAG::Helper::SimpleBufferIO());
        if (bufferhandle == nullptr || !bufferhandle->Initialize(config.data(), std::ios::in, configSize))
            return SPTAG::ErrorCode::EmptyDiskIO;
        if (SPTAG::ErrorCode::Success != iniReader.LoadIni(bufferhandle))
            return SPTAG::ErrorCode::FailedParseValue;
    }

    SPTAG::IndexAlgoType algoType = iniReader.GetParameter("Index", "IndexAlgoType", SPTAG::IndexAlgoType::Undefined);
    SPTAG::VectorValueType valueType = iniReader.GetParameter("Index", "ValueType", SPTAG::VectorValueType::Undefined);

    if (SPTAG::IndexAlgoType::Undefined == algoType || SPTAG::VectorValueType::Undefined == valueType) {
        return SPTAG::ErrorCode::FailedParseValue;
    }

    if (algoType == SPTAG::IndexAlgoType::BKT) {
        switch (valueType) {
#define DefineVectorValueType(Name, Type)                                                 \
    case SPTAG::VectorValueType::Name:                                                    \
        p_vectorIndex = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::BKT::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                break;
        }
    } else if (algoType == SPTAG::IndexAlgoType::SPANN) {
        switch (valueType) {
#define DefineVectorValueType(Name, Type)                                                   \
    case SPTAG::VectorValueType::Name:                                                      \
        p_vectorIndex = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::SPANN::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                break;
        }
    }

    if (p_vectorIndex == nullptr)
        return SPTAG::ErrorCode::FailedParseValue;

    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;

    if ((ret = p_vectorIndex->LoadIndexConfig(iniReader)) != SPTAG::ErrorCode::Success)
        return ret;

    std::uint64_t blobs;
    IOBINARY(fp, ReadBinary, sizeof(blobs), (char*)&blobs);

    std::vector<std::shared_ptr<SPTAG::Helper::DiskIO>> p_indexStreams(blobs, fp);
    if ((ret = p_vectorIndex->LoadIndexData(p_indexStreams)) != SPTAG::ErrorCode::Success)
        return ret;

    if (iniReader.DoesSectionExist("MetaData")) {
        p_vectorIndex->SetMetadata(new SPTAG::MemMetadataSet(fp, fp, p_vectorIndex->m_iDataBlockSize, p_vectorIndex->m_iDataCapacity, p_vectorIndex->m_iMetaRecordSize));

        if (!(p_vectorIndex->GetMetadata()->Available())) {
            LOG(SPTAG::Helper::LogLevel::LL_Error, "Error: Failed to load metadata.\n");
            return SPTAG::ErrorCode::Fail;
        }

        if (iniReader.GetParameter("MetaData", "MetaDataToVectorIndex", std::string()) == "true") {
            p_vectorIndex->BuildMetaMapping();
        }
    }

    if (iniReader.DoesSectionExist("Quantizer")) {
        p_vectorIndex->SetQuantizer(SPTAG::COMMON::IQuantizer::LoadIQuantizer(fp));
        if (!p_vectorIndex->m_pQuantizer)
            return SPTAG::ErrorCode::FailedParseValue;
    }

    p_vectorIndex->m_bReady = true;
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::VectorIndex::LoadIndex(const std::string& p_config, const std::vector<SPTAG::ByteArray>& p_indexBlobs, std::shared_ptr<SPTAG::VectorIndex>& p_vectorIndex) {
    SPTAG::Helper::IniReader iniReader;
    std::shared_ptr<SPTAG::Helper::DiskIO> fp(new SPTAG::Helper::SimpleBufferIO());
    if (fp == nullptr || !fp->Initialize(p_config.c_str(), std::ios::in, p_config.size()))
        return SPTAG::ErrorCode::EmptyDiskIO;
    if (SPTAG::ErrorCode::Success != iniReader.LoadIni(fp))
        return SPTAG::ErrorCode::FailedParseValue;

    SPTAG::IndexAlgoType algoType = iniReader.GetParameter("Index", "IndexAlgoType", SPTAG::IndexAlgoType::Undefined);
    SPTAG::VectorValueType valueType = iniReader.GetParameter("Index", "ValueType", SPTAG::VectorValueType::Undefined);

    if (SPTAG::IndexAlgoType::Undefined == algoType || SPTAG::VectorValueType::Undefined == valueType) {
        return SPTAG::ErrorCode::FailedParseValue;
    }

    if (algoType == SPTAG::IndexAlgoType::BKT) {
        switch (valueType) {
#define DefineVectorValueType(Name, Type)                                                 \
    case SPTAG::VectorValueType::Name:                                                    \
        p_vectorIndex = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::BKT::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                break;
        }
    } else if (algoType == SPTAG::IndexAlgoType::SPANN) {
        switch (valueType) {
#define DefineVectorValueType(Name, Type)                                                   \
    case SPTAG::VectorValueType::Name:                                                      \
        p_vectorIndex = std::shared_ptr<SPTAG::VectorIndex>(new SPTAG::SPANN::Index<Type>); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                break;
        }
    }

    if (p_vectorIndex == nullptr)
        return SPTAG::ErrorCode::FailedParseValue;

    SPTAG::ErrorCode ret = SPTAG::ErrorCode::Success;
    if (!iniReader.GetParameter<std::string>("Base", "QuantizerFilePath", std::string()).empty()) {
        p_vectorIndex->SetQuantizer(SPTAG::COMMON::IQuantizer::LoadIQuantizer(p_indexBlobs[4]));
        if (!p_vectorIndex->m_pQuantizer)
            return SPTAG::ErrorCode::FailedParseValue;
    }

    if ((p_vectorIndex->LoadIndexConfig(iniReader)) != SPTAG::ErrorCode::Success)
        return ret;

    if ((ret = p_vectorIndex->LoadIndexDataFromMemory(p_indexBlobs)) != SPTAG::ErrorCode::Success)
        return ret;

    size_t metaStart = p_vectorIndex->BufferSize()->size();
    if (iniReader.DoesSectionExist("MetaData") && p_indexBlobs.size() >= metaStart + 2) {
        SPTAG::ByteArray pMetaIndex = p_indexBlobs[metaStart + 1];
        p_vectorIndex->SetMetadata(new SPTAG::MemMetadataSet(p_indexBlobs[metaStart], SPTAG::ByteArray(pMetaIndex.Data() + sizeof(SPTAG::SizeType), pMetaIndex.Length() - sizeof(SPTAG::SizeType), false), *((SPTAG::SizeType*)pMetaIndex.Data()), p_vectorIndex->m_iDataBlockSize, p_vectorIndex->m_iDataCapacity, p_vectorIndex->m_iMetaRecordSize));

        if (!(p_vectorIndex->GetMetadata()->Available())) {
            LOG(SPTAG::Helper::LogLevel::LL_Error, "Error: Failed to load metadata.\n");
            return SPTAG::ErrorCode::Fail;
        }

        if (iniReader.GetParameter("MetaData", "MetaDataToVectorIndex", std::string()) == "true") {
            p_vectorIndex->BuildMetaMapping();
        }
        metaStart += 2;
    }

    p_vectorIndex->m_bReady = true;
    return SPTAG::ErrorCode::Success;
}

std::uint64_t SPTAG::VectorIndex::EstimatedVectorCount(std::uint64_t p_memory, SPTAG::DimensionType p_dimension, SPTAG::VectorValueType p_valuetype, SPTAG::SizeType p_vectorsInBlock, SPTAG::SizeType p_maxmeta, SPTAG::IndexAlgoType p_algo, int p_treeNumber, int p_neighborhoodSize) {
    size_t treeNodeSize;
    if (p_algo == SPTAG::IndexAlgoType::BKT) {
        treeNodeSize = sizeof(SPTAG::SizeType) * 3;
    } else {
        return 0;
    }
    std::uint64_t unit = GetValueTypeSize(p_valuetype) * p_dimension + p_maxmeta + sizeof(std::uint64_t) + sizeof(SPTAG::SizeType) * p_neighborhoodSize + 1 + treeNodeSize * p_treeNumber;
    return ((p_memory / unit) / p_vectorsInBlock) * p_vectorsInBlock;
}

std::uint64_t SPTAG::VectorIndex::EstimatedMemoryUsage(std::uint64_t p_vectorCount, SPTAG::DimensionType p_dimension, SPTAG::VectorValueType p_valuetype, SPTAG::SizeType p_vectorsInBlock, SPTAG::SizeType p_maxmeta, SPTAG::IndexAlgoType p_algo, int p_treeNumber, int p_neighborhoodSize) {
    p_vectorCount = ((p_vectorCount + p_vectorsInBlock - 1) / p_vectorsInBlock) * p_vectorsInBlock;
    size_t treeNodeSize;
    if (p_algo == IndexAlgoType::BKT) {
        treeNodeSize = sizeof(SizeType) * 3;
    } else {
        return 0;
    }
    std::uint64_t ret = GetValueTypeSize(p_valuetype) * p_dimension * p_vectorCount;  // Vector Size
    ret += p_maxmeta * p_vectorCount;                                                 // MetaData Size
    ret += sizeof(std::uint64_t) * p_vectorCount;                                     // MetaIndex Size
    ret += sizeof(SizeType) * p_neighborhoodSize * p_vectorCount;                     // Graph Size
    ret += p_vectorCount;                                                             // DeletedFlag Size
    ret += treeNodeSize * p_treeNumber * p_vectorCount;                               // Tree Size
    return ret;
}

#if defined(GPU)

    #include "Core/Common/cuda/TailNeighbors.hxx"

void SPTAG::VectorIndex::SortSelections(std::vector<SPTAG::Edge>* selections) {
    LOG(SPTAG::Helper::LogLevel::LL_Debug, "Starting sort of final input on GPU\n");
    GPU_SortSelections(selections);
}

void SPTAG::VectorIndex::ApproximateRNG(std::shared_ptr<SPTAG::VectorSet>& fullVectors, std::unordered_set<SPTAG::SizeType>& exceptIDS, int candidateNum, SPTAG::Edge* selections, int replicaCount, int numThreads, int numTrees, int leafSize, float RNGFactor, int numGPUs) {
    LOG(SPTAG::Helper::LogLevel::LL_Info, "Starting GPU SSD Index build stage...\n");

    int metric = (GetDistCalcMethod() == SPTAG::DistCalcMethod::Cosine);

    if (m_pQuantizer) {
        getTailNeighborsTPT<uint8_t, float>((uint8_t*)fullVectors->GetData(), fullVectors->Count(), this, exceptIDS, fullVectors->Dimension(), replicaCount, numThreads, numTrees, leafSize, metric, numGPUs, selections);
    } else if (GetVectorValueType() != SPTAG::VectorValueType::Float) {
        typedef int32_t SUMTYPE;
        switch (GetVectorValueType()) {
    #define DefineVectorValueType(Name, Type)                                                                                                                                                                              \
        case SPTAG::VectorValueType::Name:                                                                                                                                                                                 \
            getTailNeighborsTPT<Type, SUMTYPE>((Type*)fullVectors->GetData(), fullVectors->Count(), this, exceptIDS, fullVectors->Dimension(), replicaCount, numThreads, numTrees, leafSize, metric, numGPUs, selections); \
            break;

    #include "Core/DefinitionList.h"
    #undef DefineVectorValueType

            default:
                break;
        }
    } else {
        getTailNeighborsTPT<float, float>((float*)fullVectors->GetData(), fullVectors->Count(), this, exceptIDS, fullVectors->Dimension(), replicaCount, numThreads, numTrees, leafSize, metric, numGPUs, selections);
    }
}
#else

void SPTAG::VectorIndex::SortSelections(std::vector<SPTAG::Edge>* selections) {
    SPTAG::EdgeCompare edgeComparer;
    std::sort(selections->begin(), selections->end(), edgeComparer);
}

void SPTAG::VectorIndex::ApproximateRNG(std::shared_ptr<SPTAG::VectorSet>& fullVectors, std::unordered_set<SPTAG::SizeType>& exceptIDS, int candidateNum, SPTAG::Edge* selections, int replicaCount, int numThreads, int numTrees, int leafSize, float RNGFactor, int numGPUs) {
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    std::atomic_int nextFullID(0);
    std::atomic_size_t rngFailedCountTotal(0);

    for (int tid = 0; tid < numThreads; ++tid) {
        threads.emplace_back([&, tid]() {
            SPTAG::QueryResult resultSet(NULL, candidateNum, false);

            size_t rngFailedCount = 0;

            while (true) {
                int fullID = nextFullID.fetch_add(1);
                if (fullID >= fullVectors->Count()) {
                    break;
                }

                if (exceptIDS.count(fullID) > 0) {
                    continue;
                }

                void* reconstructed_vector = nullptr;
                if (m_pQuantizer) {
                    reconstructed_vector = ALIGN_ALLOC(m_pQuantizer->ReconstructSize());
                    m_pQuantizer->ReconstructVector((const uint8_t*)fullVectors->GetVector(fullID), reconstructed_vector);
                    switch (m_pQuantizer->GetReconstructType()) {
    #define DefineVectorValueType(Name, Type)                                                                                             \
        case SPTAG::VectorValueType::Name:                                                                                                \
            (*((SPTAG::COMMON::QueryResultSet<Type>*)&resultSet)).SetTarget(reinterpret_cast<Type*>(reconstructed_vector), m_pQuantizer); \
            break;
    #include "Core/DefinitionList.h"
    #undef DefineVectorValueType
                        default:
                            LOG(SPTAG::Helper::LogLevel::LL_Error, "Unable to get quantizer reconstruct type");
                    }
                } else {
                    resultSet.SetTarget(fullVectors->GetVector(fullID));
                }
                resultSet.Reset();

                SearchIndex(resultSet);

                size_t selectionOffset = static_cast<size_t>(fullID) * replicaCount;

                BasicResult* queryResults = resultSet.GetResults();
                int currReplicaCount = 0;
                for (int i = 0; i < candidateNum && currReplicaCount < replicaCount; ++i) {
                    if (queryResults[i].VID == -1) {
                        break;
                    }

                    // RNG Check.
                    bool rngAccpeted = true;
                    for (int j = 0; j < currReplicaCount; ++j) {
                        float nnDist = ComputeDistance(GetSample(queryResults[i].VID), GetSample(selections[selectionOffset + j].node));

                        if (RNGFactor * nnDist <= queryResults[i].Dist) {
                            rngAccpeted = false;
                            break;
                        }
                    }

                    if (!rngAccpeted) {
                        ++rngFailedCount;
                        continue;
                    }

                    selections[selectionOffset + currReplicaCount].node = queryResults[i].VID;
                    selections[selectionOffset + currReplicaCount].distance = queryResults[i].Dist;
                    ++currReplicaCount;
                }

                if (reconstructed_vector) {
                    ALIGN_FREE(reconstructed_vector);
                }
            }
            rngFailedCountTotal += rngFailedCount;
        });
    }

    for (int tid = 0; tid < numThreads; ++tid) {
        threads[tid].join();
    }
    LOG(SPTAG::Helper::LogLevel::LL_Info, "Searching replicas ended. RNG failed count: %llu\n", static_cast<uint64_t>(rngFailedCountTotal.load()));
}
#endif
