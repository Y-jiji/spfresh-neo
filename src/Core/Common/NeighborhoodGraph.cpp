// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Core/Common/NeighborhoodGraph.h"
#include "Core/Common/KNearestNeighborhoodGraph.h"
#include "Core/Common/RelativeNeighborhoodGraph.h"

std::shared_ptr<SPTAG::COMMON::NeighborhoodGraph> SPTAG::COMMON::NeighborhoodGraph::CreateInstance(std::string type)
{
    std::shared_ptr<SPTAG::COMMON::NeighborhoodGraph> res;
    if (type == "RNG")
    {
        res.reset(new SPTAG::COMMON::RelativeNeighborhoodGraph);
    }
    else if (type == "NNG") 
    {
        res.reset(new SPTAG::COMMON::KNearestNeighborhoodGraph);
    }
    return res;
}