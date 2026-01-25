// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/SimpleIniReader.h"
#include "Helper/CommonHelper.h"

#include <cctype>
#include <functional>

const SPTAG::Helper::IniReader::ParameterValueMap SPTAG::Helper::IniReader::c_emptyParameters;


SPTAG::Helper::IniReader::IniReader()
{
}


SPTAG::Helper::IniReader::~IniReader()
{
}


SPTAG::ErrorCode SPTAG::Helper::IniReader::LoadIni(std::shared_ptr<SPTAG::Helper::DiskIO> p_input)
{
    std::uint64_t c_bufferSize = 1 << 16;

    std::unique_ptr<char[]> line(new char[c_bufferSize]);

    std::string currSection;
    std::shared_ptr<ParameterValueMap> currParamMap(new ParameterValueMap);

    if (m_parameters.count(currSection) == 0)
    {
        m_parameters.emplace(currSection, currParamMap);
    }

    auto isSpace = [](char p_ch) -> bool
    {
        return std::isspace(p_ch) != 0;
    };

    while (true)
    {
        if (!p_input->ReadString(c_bufferSize, line)) break;

        std::uint64_t len = 0;
        while (len < c_bufferSize && line[len] != '\0')
        {
            ++len;
        }

        auto nonSpaceSeg = SPTAG::Helper::StrUtils::FindTrimmedSegment(line.get(), line.get() + len, isSpace);

        if (nonSpaceSeg.second <= nonSpaceSeg.first)
        {
            // Blank line.
            continue;
        }

        if (';' == *nonSpaceSeg.first)
        {
            // Comments.
            continue;
        }
        else if ('[' == *nonSpaceSeg.first)
        {
            // Parse Section
            if (']' != *(nonSpaceSeg.second - 1))
            {
                return SPTAG::ErrorCode::ReadIni_FailedParseSection;
            }

            auto sectionSeg = SPTAG::Helper::StrUtils::FindTrimmedSegment(nonSpaceSeg.first + 1, nonSpaceSeg.second - 1, isSpace);

            if (sectionSeg.second <= sectionSeg.first)
            {
                // Empty section name.
                return SPTAG::ErrorCode::ReadIni_FailedParseSection;
            }

            currSection.assign(sectionSeg.first, sectionSeg.second);
            SPTAG::Helper::StrUtils::ToLowerInPlace(currSection);

            if (m_parameters.count(currSection) == 0)
            {
                currParamMap.reset(new ParameterValueMap);
                m_parameters.emplace(currSection, currParamMap);
            }
            else
            {
                return SPTAG::ErrorCode::ReadIni_DuplicatedSection;
            }
        }
        else
        {
            // Parameter Value Pair.
            const char* equalSignLoc = nonSpaceSeg.first;
            while (equalSignLoc < nonSpaceSeg.second && '=' != *equalSignLoc)
            {
                ++equalSignLoc;
            }

            if (equalSignLoc >= nonSpaceSeg.second)
            {
                return SPTAG::ErrorCode::ReadIni_FailedParseParam;
            }

            auto paramSeg = SPTAG::Helper::StrUtils::FindTrimmedSegment(nonSpaceSeg.first, equalSignLoc, isSpace);

            if (paramSeg.second <= paramSeg.first)
            {
                // Empty parameter name.
                return SPTAG::ErrorCode::ReadIni_FailedParseParam;
            }

            std::string paramName(paramSeg.first, paramSeg.second);
            SPTAG::Helper::StrUtils::ToLowerInPlace(paramName);

            if (currParamMap->count(paramName) == 0)
            {
                currParamMap->emplace(std::move(paramName), std::string(equalSignLoc + 1, nonSpaceSeg.second));
            }
            else
            {
                return SPTAG::ErrorCode::ReadIni_DuplicatedParam;
            }
        }
    }
    return SPTAG::ErrorCode::Success;
}


SPTAG::ErrorCode
SPTAG::Helper::IniReader::LoadIniFile(const std::string& p_iniFilePath)
{
    auto ptr = SPTAG::f_createIO();
    if (ptr == nullptr || !ptr->Initialize(p_iniFilePath.c_str(), std::ios::in)) return SPTAG::ErrorCode::FailedOpenFile;
    return LoadIni(ptr);
}


bool
SPTAG::Helper::IniReader::DoesSectionExist(const std::string& p_section) const
{
    std::string section(p_section);
    SPTAG::Helper::StrUtils::ToLowerInPlace(section);
    return m_parameters.count(section) != 0;
}


bool
SPTAG::Helper::IniReader::DoesParameterExist(const std::string& p_section, const std::string& p_param) const
{
    std::string name(p_section);
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);
    auto iter = m_parameters.find(name);
    if (iter == m_parameters.cend())
    {
        return false;
    }

    const auto& paramMap = iter->second;
    if (paramMap == nullptr)
    {
        return false;
    }

    name = p_param;
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);
    return paramMap->count(name) != 0;
}


bool
SPTAG::Helper::IniReader::GetRawValue(const std::string& p_section, const std::string& p_param, std::string& p_value) const
{
    std::string name(p_section);
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);
    auto sectionIter = m_parameters.find(name);
    if (sectionIter == m_parameters.cend())
    {
        return false;
    }

    const auto& paramMap = sectionIter->second;
    if (paramMap == nullptr)
    {
        return false;
    }

    name = p_param;
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);
    auto paramIter = paramMap->find(name);
    if (paramIter == paramMap->cend())
    {
        return false;
    }

    p_value = paramIter->second;
    return true;
}


const SPTAG::Helper::IniReader::ParameterValueMap&
SPTAG::Helper::IniReader::GetParameters(const std::string& p_section) const
{
    std::string name(p_section);
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);
    auto sectionIter = m_parameters.find(name);
    if (sectionIter == m_parameters.cend() || nullptr == sectionIter->second)
    {
        return c_emptyParameters;
    }

    return *(sectionIter->second);
}

void
SPTAG::Helper::IniReader::SetParameter(const std::string& p_section, const std::string& p_param, const std::string& p_val)
{
    std::string name(p_section);
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);
    auto sectionIter = m_parameters.find(name);
    if (sectionIter == m_parameters.cend() || sectionIter->second == nullptr) 
    {
        m_parameters[name] = std::shared_ptr<ParameterValueMap>(new ParameterValueMap);
    }

    std::string param(p_param);
    SPTAG::Helper::StrUtils::ToLowerInPlace(param);
    (*m_parameters[name])[param] = p_val;
}
