// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/Common/TruthSet.h"
#include "Core/VectorIndex.h"
#include "Core/Common/QueryResultSet.h"

#if defined(GPU)
    #include <cuda.h>
    #include <cuda_runtime.h>
    #include <device_launch_parameters.h>
    #include <typeinfo>
    #include <cuda_fp16.h>

    #include "Core/Common/cuda/KNN.hxx"
    #include "Core/Common/cuda/params.h"
#endif

namespace SPTAG::COMMON {
}  // namespace SPTAG::COMMON
