// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include <iostream>

#include "Core/Common.h"
#include "Core/VectorIndex.h"
#include "Core/SPANN/Index.h"
#include "Helper/SimpleIniReader.h"
#include "Helper/VectorSetReader.h"
#include "Helper/StringConvert.h"
#include "Core/Common/TruthSet.h"

#include "SPFresh/SPFresh.h"

// switch between exe and static library by _$(OutputType)
#ifdef _exe

int main(int argc, char* argv[]) {
    if (argc < 2) {
        SPTAG::LOG(SPTAG::Helper::LogLevel::LL_Error, "spfresh storePath\n");
        exit(-1);
    }

    auto ret = SPTAG::SSDServing::SPFresh::UpdateTest(argv[1]);
    return ret;
}

#endif
