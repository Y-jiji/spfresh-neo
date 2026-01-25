#include "Utils/CommonUtils.h"
#include "Utils/DistanceUtils.h"

#define DefineVectorValueType(Name, Type) template int SPTAG::COMMON::Utils::GetBase<Type>();
#include "Core/DefinitionList.h"
#undef DefineVectorValueType

template <typename T>
void SPTAG::COMMON::Utils::BatchNormalize(T* data, SPTAG::SizeType row, SPTAG::DimensionType col, int base, int threads) {
#pragma omp parallel for num_threads(threads)
    for (SPTAG::SizeType i = 0; i < row; i++) {
        SPTAG::COMMON::Utils::Normalize(data + i * (size_t)col, col, base);
    }
}

#define DefineVectorValueType(Name, Type) template void SPTAG::COMMON::Utils::BatchNormalize<Type>(Type * data, SPTAG::SizeType row, SPTAG::DimensionType col, int base, int threads);
#include "Core/DefinitionList.h"
#undef DefineVectorValueType