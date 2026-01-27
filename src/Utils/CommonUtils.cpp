#include "Utils/CommonUtils.h"
#include "Utils/DistanceUtils.h"
#include "Helper/DiskIO.h"
#include "Helper/Logging.h"

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

namespace SPTAG {
std::shared_ptr<Helper::Logger> g_pLogger = std::make_shared<Helper::SimpleLogger>(Helper::LogLevel::LL_Info);

std::shared_ptr<Helper::DiskIO> (*f_createIO)() = []() -> std::shared_ptr<Helper::DiskIO> {
    return std::make_shared<Helper::SimpleFileIO>();
};

std::mt19937 rg(std::random_device{}());
}  // namespace SPTAG