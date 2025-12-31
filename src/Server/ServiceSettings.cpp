// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Server/ServiceSettings.h"

using namespace SPTAG;
using namespace SPTAG::Service;


ServiceSettings::ServiceSettings()
    : m_defaultMaxResultNumber(10),
      m_threadNum(12)
{
}
