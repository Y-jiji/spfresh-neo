// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/VectorSetReaders/TxtReader.h"
#include "Helper/StringConvert.h"
#include "Helper/CommonHelper.h"

#include <omp.h>

SPTAG::Helper::TxtVectorReader::TxtVectorReader(std::shared_ptr<ReaderOptions> p_options)
    : SPTAG::Helper::VectorSetReader(p_options),
      m_subTaskBlocksize(0) {
    omp_set_num_threads(m_options->m_threadNum);

    std::string tempFolder("tempfolder");
    if (!direxists(tempFolder.c_str())) {
        mkdir(tempFolder.c_str());
    }

    tempFolder += FolderSep;
    std::srand(clock());
    std::string randstr = std::to_string(std::rand());
    m_vectorOutput = tempFolder + "vectorset.bin." + randstr;
    m_metadataConentOutput = tempFolder + "metadata.bin." + randstr;
    m_metadataIndexOutput = tempFolder + "metadataindex.bin." + randstr;
}

SPTAG::Helper::TxtVectorReader::~TxtVectorReader() {
    if (fileexists(m_vectorOutput.c_str())) {
        remove(m_vectorOutput.c_str());
    }

    if (fileexists(m_metadataIndexOutput.c_str())) {
        remove(m_metadataIndexOutput.c_str());
    }

    if (fileexists(m_metadataConentOutput.c_str())) {
        remove(m_metadataConentOutput.c_str());
    }
}

SPTAG::ErrorCode
SPTAG::Helper::TxtVectorReader::LoadFile(const std::string& p_filePaths) {
    const auto& files = GetFileSizes(p_filePaths);
    std::vector<std::function<SPTAG::ErrorCode()>> subWorks;
    subWorks.reserve(files.size() * m_options->m_threadNum);

    m_subTaskCount = 0;
    for (const auto& fileInfo : files) {
        if (fileInfo.second == (std::numeric_limits<std::size_t>::max)()) {
            LOG(Helper::LogLevel::LL_Error, "File %s not exists or can't access.\n", fileInfo.first.c_str());
            return SPTAG::ErrorCode::FailedOpenFile;
        }

        std::uint32_t fileTaskCount = 0;
        std::size_t blockSize = m_subTaskBlocksize;
        if (0 == blockSize) {
            fileTaskCount = m_options->m_threadNum;
            if (fileTaskCount == 0)
                fileTaskCount = 1;
            blockSize = (fileInfo.second + fileTaskCount - 1) / fileTaskCount;
        } else {
            fileTaskCount = static_cast<std::uint32_t>((fileInfo.second + blockSize - 1) / blockSize);
        }

        for (std::uint32_t i = 0; i < fileTaskCount; ++i) {
            subWorks.emplace_back(std::bind(&TxtVectorReader::LoadFileInternal, this, fileInfo.first, m_subTaskCount++, i, blockSize));
        }
    }

    m_totalRecordCount = 0;
    m_totalRecordVectorBytes = 0;
    m_subTaskRecordCount.clear();
    m_subTaskRecordCount.resize(m_subTaskCount, 0);

    m_waitSignal.Reset(m_subTaskCount);

#pragma omp parallel for schedule(dynamic)
    for (int64_t i = 0; i < (int64_t)subWorks.size(); i++) {
        SPTAG::ErrorCode code = subWorks[i]();
        if (SPTAG::ErrorCode::Success != code) {
            throw std::runtime_error("LoadFileInternal failed");
        }
    }

    m_waitSignal.Wait();

    return MergeData();
}

std::shared_ptr<SPTAG::VectorSet>
SPTAG::Helper::TxtVectorReader::GetVectorSet(SizeType start, SizeType end) const {
    auto ptr = SPTAG::f_createIO();
    if (ptr == nullptr || !ptr->Initialize(m_vectorOutput.c_str(), std::ios::binary | std::ios::in)) {
        LOG(Helper::LogLevel::LL_Error, "Failed to read file %s.\n", m_vectorOutput.c_str());
        throw std::runtime_error("Failed to read vectorset file");
    }

    SizeType row;
    DimensionType col;
    if (ptr->ReadBinary(sizeof(SizeType), (char*)&row) != sizeof(SizeType)) {
        LOG(Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
        throw std::runtime_error("Failed to read vectorset file");
    }
    if (ptr->ReadBinary(sizeof(DimensionType), (char*)&col) != sizeof(DimensionType)) {
        LOG(Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
        throw std::runtime_error("Failed to read vectorset file");
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
        std::uint64_t offset = ((std::uint64_t)SPTAG::GetValueTypeSize(m_options->m_inputValueType)) * start * col + +sizeof(SizeType) + sizeof(DimensionType);
        if (ptr->ReadBinary(totalRecordVectorBytes, vecBuf, offset) != totalRecordVectorBytes) {
            LOG(Helper::LogLevel::LL_Error, "Failed to read VectorSet!\n");
            throw std::runtime_error("Failed to read vectorset file");
        }
    }
    return std::shared_ptr<SPTAG::VectorSet>(new SPTAG::BasicVectorSet(vectorSet, m_options->m_inputValueType, col, end - start));
}

std::shared_ptr<SPTAG::MetadataSet>
SPTAG::Helper::TxtVectorReader::GetMetadataSet() const {
    if (fileexists(m_metadataIndexOutput.c_str()) && fileexists(m_metadataConentOutput.c_str()))
        return std::shared_ptr<SPTAG::MetadataSet>(new SPTAG::FileMetadataSet(m_metadataConentOutput, m_metadataIndexOutput));
    return nullptr;
}

SPTAG::ErrorCode
SPTAG::Helper::TxtVectorReader::LoadFileInternal(const std::string& p_filePath, std::uint32_t p_subTaskID, std::uint32_t p_fileBlockID, std::size_t p_fileBlockSize) {
    std::uint64_t lineBufferSize = 1 << 16;
    std::unique_ptr<char[]> currentLine(new char[lineBufferSize]);

    SizeType recordCount = 0;
    std::uint64_t metaOffset = 0;
    std::size_t totalRead = 0;
    std::streamoff startpos = p_fileBlockID * p_fileBlockSize;

    std::shared_ptr<Helper::DiskIO> input = SPTAG::f_createIO(), output = SPTAG::f_createIO(), meta = SPTAG::f_createIO(), metaIndex = SPTAG::f_createIO();
    if (input == nullptr || !input->Initialize(p_filePath.c_str(), std::ios::in | std::ios::binary)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to open file: %s\n", p_filePath.c_str());
        return SPTAG::ErrorCode::FailedOpenFile;
    }

    LOG(Helper::LogLevel::LL_Info, "Begin Subtask: %u, start offset position: %lld\n", p_subTaskID, startpos);

    std::string subFileSuffix("_");
    subFileSuffix += std::to_string(p_subTaskID);
    subFileSuffix += ".tmp";

    if (output == nullptr || !output->Initialize((m_vectorOutput + subFileSuffix).c_str(), std::ios::binary | std::ios::out) ||
        meta == nullptr || !meta->Initialize((m_metadataConentOutput + subFileSuffix).c_str(), std::ios::binary | std::ios::out) ||
        metaIndex == nullptr || !metaIndex->Initialize((m_metadataIndexOutput + subFileSuffix).c_str(), std::ios::binary | std::ios::out)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to create files: %s %s %s\n", (m_vectorOutput + subFileSuffix).c_str(), (m_metadataConentOutput + subFileSuffix).c_str(), (m_metadataIndexOutput + subFileSuffix).c_str());
        return SPTAG::ErrorCode::FailedCreateFile;
    }

    if (p_fileBlockID != 0) {
        totalRead += input->ReadString(lineBufferSize, currentLine, '\n', startpos);
    }

    std::size_t vectorByteSize = SPTAG::GetValueTypeSize(m_options->m_inputValueType) * m_options->m_dimension;
    std::unique_ptr<std::uint8_t[]> vector;
    vector.reset(new std::uint8_t[vectorByteSize]);

    while (totalRead <= p_fileBlockSize) {
        std::uint64_t lineLength = input->ReadString(lineBufferSize, currentLine);
        if (lineLength == 0)
            break;
        totalRead += lineLength;

        std::size_t tabIndex = lineLength - 1;
        while (tabIndex > 0 && currentLine[tabIndex] != '\t') {
            --tabIndex;
        }

        if (0 == tabIndex && currentLine[tabIndex] != '\t') {
            LOG(Helper::LogLevel::LL_Error, "Subtask: %u cannot parsing line:%s\n", p_subTaskID, currentLine.get());
            return SPTAG::ErrorCode::FailedParseValue;
        }

        bool parseSuccess = false;
        switch (m_options->m_inputValueType) {
#define DefineVectorValueType(Name, Type)                                                                        \
    case SPTAG::VectorValueType::Name:                                                                           \
        parseSuccess = TranslateVector(currentLine.get() + tabIndex + 1, reinterpret_cast<Type*>(vector.get())); \
        break;

#include "Core/DefinitionList.h"
#undef DefineVectorValueType

            default:
                parseSuccess = false;
                break;
        }

        if (!parseSuccess) {
            LOG(Helper::LogLevel::LL_Error, "Subtask: %u cannot parsing vector:%s\n", p_subTaskID, currentLine.get());
            return SPTAG::ErrorCode::FailedParseValue;
        }

        ++recordCount;
        if (output->WriteBinary(vectorByteSize, (char*)vector.get()) != vectorByteSize ||
            meta->WriteBinary(tabIndex, currentLine.get()) != tabIndex ||
            metaIndex->WriteBinary(sizeof(metaOffset), (const char*)&metaOffset) != sizeof(metaOffset)) {
            LOG(Helper::LogLevel::LL_Error, "Subtask: %u cannot write line:%s\n", p_subTaskID, currentLine.get());
            return SPTAG::ErrorCode::DiskIOFail;
        }
        metaOffset += tabIndex;
    }
    if (metaIndex->WriteBinary(sizeof(metaOffset), (const char*)&metaOffset) != sizeof(metaOffset)) {
        LOG(Helper::LogLevel::LL_Error, "Subtask: %u cannot write final offset!\n", p_subTaskID);
        return SPTAG::ErrorCode::DiskIOFail;
    }

    m_totalRecordCount += recordCount;
    m_subTaskRecordCount[p_subTaskID] = recordCount;
    m_totalRecordVectorBytes += recordCount * vectorByteSize;

    m_waitSignal.FinishOne();
    return SPTAG::ErrorCode::Success;
}

SPTAG::ErrorCode
SPTAG::Helper::TxtVectorReader::MergeData() {
    const std::size_t bufferSize = 1 << 30;
    const std::size_t bufferSizeTrim64 = (bufferSize / sizeof(std::uint64_t)) * sizeof(std::uint64_t);

    std::shared_ptr<Helper::DiskIO> input = SPTAG::f_createIO(), output = SPTAG::f_createIO(), meta = SPTAG::f_createIO(), metaIndex = SPTAG::f_createIO();

    if (output == nullptr || !output->Initialize(m_vectorOutput.c_str(), std::ios::binary | std::ios::out) ||
        meta == nullptr || !meta->Initialize(m_metadataConentOutput.c_str(), std::ios::binary | std::ios::out) ||
        metaIndex == nullptr || !metaIndex->Initialize(m_metadataIndexOutput.c_str(), std::ios::binary | std::ios::out)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to create files: %s %s %s\n", m_vectorOutput.c_str(), m_metadataConentOutput.c_str(), m_metadataIndexOutput.c_str());
        return SPTAG::ErrorCode::FailedCreateFile;
    }

    std::unique_ptr<char[]> bufferHolder(new char[bufferSize]);
    char* buf = bufferHolder.get();

    SizeType totalRecordCount = m_totalRecordCount;
    if (output->WriteBinary(sizeof(totalRecordCount), (char*)(&totalRecordCount)) != sizeof(totalRecordCount)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_vectorOutput.c_str());
        return SPTAG::ErrorCode::DiskIOFail;
    }
    if (output->WriteBinary(sizeof(m_options->m_dimension), (char*)&(m_options->m_dimension)) != sizeof(m_options->m_dimension)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_vectorOutput.c_str());
        return SPTAG::ErrorCode::DiskIOFail;
    }

    for (std::uint32_t i = 0; i < m_subTaskCount; ++i) {
        std::string file = m_vectorOutput;
        file += "_";
        file += std::to_string(i);
        file += ".tmp";

        if (input == nullptr || !input->Initialize(file.c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Unable to open file: %s\n", file.c_str());
            return SPTAG::ErrorCode::FailedOpenFile;
        }

        std::uint64_t readSize;
        while ((readSize = input->ReadBinary(bufferSize, bufferHolder.get()))) {
            if (output->WriteBinary(readSize, bufferHolder.get()) != readSize) {
                LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_vectorOutput.c_str());
                return SPTAG::ErrorCode::DiskIOFail;
            }
        }
        input->ShutDown();
        remove(file.c_str());
    }

    for (std::uint32_t i = 0; i < m_subTaskCount; ++i) {
        std::string file = m_metadataConentOutput;
        file += "_";
        file += std::to_string(i);
        file += ".tmp";

        if (input == nullptr || !input->Initialize(file.c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Unable to open file: %s\n", file.c_str());
            return SPTAG::ErrorCode::FailedOpenFile;
        }

        std::uint64_t readSize;
        while ((readSize = input->ReadBinary(bufferSize, bufferHolder.get()))) {
            if (meta->WriteBinary(readSize, bufferHolder.get()) != readSize) {
                LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_metadataConentOutput.c_str());
                return SPTAG::ErrorCode::DiskIOFail;
            }
        }
        input->ShutDown();
        remove(file.c_str());
    }

    if (metaIndex->WriteBinary(sizeof(totalRecordCount), (char*)(&totalRecordCount)) != sizeof(totalRecordCount)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_metadataIndexOutput.c_str());
        return SPTAG::ErrorCode::DiskIOFail;
    }

    std::uint64_t totalOffset = 0;
    for (std::uint32_t i = 0; i < m_subTaskCount; ++i) {
        std::string file = m_metadataIndexOutput;
        file += "_";
        file += std::to_string(i);
        file += ".tmp";

        if (input == nullptr || !input->Initialize(file.c_str(), std::ios::binary | std::ios::in)) {
            LOG(Helper::LogLevel::LL_Error, "Unable to open file: %s\n", file.c_str());
            return SPTAG::ErrorCode::FailedOpenFile;
        }

        for (SizeType remains = m_subTaskRecordCount[i]; remains > 0;) {
            std::size_t readBytesCount = min(remains * sizeof(std::uint64_t), bufferSizeTrim64);
            if (input->ReadBinary(readBytesCount, buf) != readBytesCount) {
                LOG(Helper::LogLevel::LL_Error, "Unable to read file: %s\n", file.c_str());
                return SPTAG::ErrorCode::DiskIOFail;
            }
            std::uint64_t* offset = reinterpret_cast<std::uint64_t*>(buf);
            for (std::uint64_t i = 0; i < readBytesCount / sizeof(std::uint64_t); ++i) {
                offset[i] += totalOffset;
            }

            if (metaIndex->WriteBinary(readBytesCount, buf) != readBytesCount) {
                LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_metadataIndexOutput.c_str());
                return SPTAG::ErrorCode::DiskIOFail;
            }
            remains -= static_cast<SizeType>(readBytesCount / sizeof(std::uint64_t));
        }
        if (input->ReadBinary(sizeof(std::uint64_t), buf) != sizeof(std::uint64_t)) {
            LOG(Helper::LogLevel::LL_Error, "Unable to read file: %s\n", file.c_str());
            return SPTAG::ErrorCode::DiskIOFail;
        }
        totalOffset += *(reinterpret_cast<std::uint64_t*>(buf));

        input->ShutDown();
        remove(file.c_str());
    }

    if (metaIndex->WriteBinary(sizeof(totalOffset), (char*)&totalOffset) != sizeof(totalOffset)) {
        LOG(Helper::LogLevel::LL_Error, "Unable to write file: %s\n", m_metadataIndexOutput.c_str());
        return SPTAG::ErrorCode::DiskIOFail;
    }
    return SPTAG::ErrorCode::Success;
}

std::vector<SPTAG::Helper::TxtVectorReader::FileInfoPair>
SPTAG::Helper::TxtVectorReader::GetFileSizes(const std::string& p_filePaths) {
    const auto& files = Helper::StrUtils::SplitString(p_filePaths, ",");
    std::vector<TxtVectorReader::FileInfoPair> res;
    res.reserve(files.size());

    for (const auto& filePath : files) {
        if (!fileexists(filePath.c_str())) {
            res.emplace_back(filePath, (std::numeric_limits<std::size_t>::max)());
            continue;
        }
        struct stat stat_buf;
        stat(filePath.c_str(), &stat_buf);
        std::size_t fileSize = stat_buf.st_size;
        res.emplace_back(filePath, static_cast<std::size_t>(fileSize));
    }

    return res;
}
