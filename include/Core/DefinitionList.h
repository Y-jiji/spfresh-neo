// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifdef DefineVectorValueType

DefineVectorValueType(Int8, std::int8_t)
VectorValueType(UInt8, std::uint8_t)
VectorValueType(Int16, std::int16_t)
VectorValueType(Float, float)

#endif  // DefineVectorValueType

#ifdef DefineVectorValueType2

VectorValueType2(Int8, Int8, std::int8_t, std::int8_t)
VectorValueType2(Int8, UInt8, std::int8_t, std::uint8_t)
VectorValueType2(Int8, Int16, std::int8_t, std::int16_t)
VectorValueType2(Int8, Float, std::int8_t, float)
VectorValueType2(UInt8, Int8, std::uint8_t, std::int8_t)
VectorValueType2(UInt8, UInt8, std::uint8_t, std::uint8_t)
VectorValueType2(UInt8, Int16, std::uint8_t, std::int16_t)
VectorValueType2(UInt8, Float, std::uint8_t, float)
VectorValueType2(Int16, Int8, std::int16_t, std::int8_t)
VectorValueType2(Int16, UInt8, std::int16_t, std::uint8_t)
VectorValueType2(Int16, Int16, std::int16_t, std::int16_t)
VectorValueType2(Int16, Float, std::int16_t, float)
VectorValueType2(Float, Int8, float, std::int8_t)
VectorValueType2(Float, UInt8, float, std::uint8_t)
VectorValueType2(Float, Int16, float, std::int16_t)
VectorValueType2(Float, Float, float, float)

#endif  // DefineVectorValueType2

#ifdef DefineDistCalcMethod

DistCalcMethod(L2)
DistCalcMethod(Cosine)
DistCalcMethod(InnerProduct)

#endif  // DefineDistCalcMethod

#ifdef DefineErrorCode

    // 0x0000 ~ 0x0FFF  General Status
ErrorCode(Success, 0x0000)
ErrorCode(Fail, 0x0001)
ErrorCode(FailedOpenFile, 0x0002)
ErrorCode(FailedCreateFile, 0x0003)
ErrorCode(ParamNotFound, 0x0010)
ErrorCode(FailedParseValue, 0x0011)
ErrorCode(MemoryOverFlow, 0x0012)
ErrorCode(LackOfInputs, 0x0013)
ErrorCode(VectorNotFound, 0x0014)
ErrorCode(EmptyIndex, 0x0015)
ErrorCode(EmptyData, 0x0016)
ErrorCode(DimensionSizeMismatch, 0x0017)
ErrorCode(ExternalAbort, 0x0018)
ErrorCode(EmptyDiskIO, 0x0019)
ErrorCode(DiskIOFail, 0x0020)

    // 0x1000 ~ 0x1FFF  Index Build Status

    // 0x2000 ~ 0x2FFF  Index Serve Status

    // 0x3000 ~ 0x3FFF  Helper Function Status
ErrorCode(ReadIni_FailedParseSection, 0x3000)
ErrorCode(ReadIni_FailedParseParam, 0x3001)
ErrorCode(ReadIni_DuplicatedSection, 0x3002)
ErrorCode(ReadIni_DuplicatedParam, 0x3003)

    // 0x4000 ~ 0x4FFF Socket Library Status
ErrorCode(Socket_FailedResolveEndPoint, 0x4000)
ErrorCode(Socket_FailedConnectToEndPoint, 0x4001)

#endif  // DefineErrorCode

#ifdef DefineIndexAlgo

IndexAlgo(BKT)
IndexAlgo(SPANN)

#endif  // DefineIndexAlgo

#ifdef DefineTruthFileType

    // 1st nn id(int32_t), SPACE, 2nd nn id, SPACE, 3rd nn id,...
    // 1st nn id, SPACE, 2nd nn id, SPACE, 3rd nn id,...
    // ...
TruthFileType(TXT)
    // K of 1st vector(int32_t), 1st nn id(int32_t), SPACE, 2nd nn id, SPACE, 3rd nn id,...
    // K of 2nd vector(int32_t), 1st nn id, SPACE, 2nd nn id, SPACE, 3rd nn id,...
    // ...
TruthFileType(XVEC)
    // row(int32_t), column(int32_t), data...
TruthFileType(DEFAULT)

#endif  // DefineTruthFileType