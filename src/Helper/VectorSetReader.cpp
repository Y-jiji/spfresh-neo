// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/VectorSetReader.h"
#include "Helper/VectorSetReaders/DefaultReader.h"
#include "Helper/VectorSetReaders/TxtReader.h"
#include "Helper/VectorSetReaders/XvecReader.h"

SPTAG::Helper::ReaderOptions::ReaderOptions(SPTAG::VectorValueType p_valueType, SPTAG::DimensionType p_dimension, SPTAG::VectorFileType p_fileType, std::string p_vectorDelimiter, std::uint32_t p_threadNum, bool p_normalized)
    : m_inputValueType(p_valueType), m_dimension(p_dimension), m_inputFileType(p_fileType), m_vectorDelimiter(p_vectorDelimiter), m_threadNum(p_threadNum), m_normalized(p_normalized) {
    AddOptionalOption(m_threadNum, "-t", "--thread", "Thread Number.");
    AddOptionalOption(m_vectorDelimiter, "-dl", "--delimiter", "Vector delimiter.");
    AddOptionalOption(m_normalized, "-norm", "--normalized", "Vector is normalized.");
    AddRequiredOption(m_dimension, "-d", "--dimension", "Dimension of vector.");
    AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
    AddRequiredOption(m_inputFileType, "-f", "--filetype", "Input file type (DEFAULT, TXT, XVEC). Default is DEFAULT.");
}

SPTAG::Helper::ReaderOptions::~ReaderOptions() {
}

SPTAG::Helper::VectorSetReader::VectorSetReader(std::shared_ptr<SPTAG::Helper::ReaderOptions> p_options)
    : m_options(p_options) {
}

SPTAG::Helper::VectorSetReader::~VectorSetReader() {
}

std::shared_ptr<SPTAG::Helper::VectorSetReader>
SPTAG::Helper::VectorSetReader::CreateInstance(std::shared_ptr<SPTAG::Helper::ReaderOptions> p_options) {
    if (p_options->m_inputFileType == SPTAG::VectorFileType::DEFAULT) {
        return std::make_shared<SPTAG::Helper::DefaultVectorReader>(p_options);
    } else if (p_options->m_inputFileType == SPTAG::VectorFileType::TXT) {
        return std::make_shared<SPTAG::Helper::TxtVectorReader>(p_options);
    } else if (p_options->m_inputFileType == SPTAG::VectorFileType::XVEC) {
        return std::make_shared<SPTAG::Helper::XvecVectorReader>(p_options);
    }
    return nullptr;
}
