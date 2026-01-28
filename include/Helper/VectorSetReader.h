// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_VECTORSETREADER_H_
#define _SPTAG_HELPER_VECTORSETREADER_H_

#include "Core/Common.h"
#include "Core/VectorSet.h"
#include "Core/MetadataSet.h"
#include "Helper/ArgumentsParser.h"

#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace SPTAG::Helper {

class ReaderOptions : public ArgumentsParser {
   public:
    ReaderOptions(VectorValueType p_valueType, DimensionType p_dimension, std::string p_vectorDelimiter = "|", std::uint32_t p_threadNum = 32, bool p_normalized = false);

    ~ReaderOptions();

    SPTAG::VectorValueType m_inputValueType;

    DimensionType m_dimension;

    std::string m_vectorDelimiter;

    std::uint32_t m_threadNum;

    bool m_normalized;
};

template <typename T>
class VectorSetReader : public std::enable_shared_from_this<VectorSetReader<T>> {
   public:
    VectorSetReader(SizeType size, DimensionType dim, std::string p_vectorDelimiter = "|", std::uint32_t p_threadNum = 32, bool p_normalized = false);

    virtual ~VectorSetReader();

    virtual ErrorCode LoadFile(const std::string& p_filePath);

    virtual std::shared_ptr<VectorSet> GetVectorSet(SizeType start = 0, SizeType end = -1) const;

    virtual std::shared_ptr<MetadataSet> GetMetadataSet() const;

    virtual bool IsNormalized() const {
        return m_normalized;
    }

    static std::shared_ptr<VectorSetReader<T>> CreateInstance(SizeType size, DimensionType dim, std::string p_vectorDelimiter = "|", std::uint32_t p_threadNum = 32, bool p_normalized = false);

   protected:
    SizeType m_size;

    DimensionType m_dim;

    std::string m_vectorDelimiter;

    std::uint32_t m_threadNum;

    bool m_normalized;

    std::string m_vectorOutput;

    std::string m_metadataConentOutput;

    std::string m_metadataIndexOutput;

    void* m_mappedData;
    size_t m_fileSize;
    int m_fd;
};

}  // namespace SPTAG::Helper

#endif  // _SPTAG_HELPER_VECTORSETREADER_H_
