// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include <string>
#include <map>
#include "Core/Common.h"

namespace SPTAG::SSDServing {

int BootProgram(std::map<std::string, std::map<std::string, std::string>>* config_map, const char* configurationPath = nullptr);

const std::string SEC_BASE = "Base";
const std::string SEC_SELECT_HEAD = "SelectHead";
const std::string SEC_BUILD_HEAD = "BuildHead";
const std::string SEC_BUILD_SSD_INDEX = "BuildSSDIndex";
const std::string SEC_SEARCH_SSD_INDEX = "SearchSSDIndex";
}  // namespace SPTAG::SSDServing