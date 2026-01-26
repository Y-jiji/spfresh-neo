cmake_minimum_required(VERSION 3.29)
project(SPFresh LANGUAGES CXX)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
enable_testing()
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(PkgConfig REQUIRED)
find_package(Boost REQUIRED CONFIG)
find_package(MPI REQUIRED)
find_package(TBB REQUIRED CONFIG)
find_package(OpenMP REQUIRED)

set(ENV{PKG_CONFIG_PATH} "/usr/local/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(SPDK_NVME     REQUIRED IMPORTED_TARGET spdk_nvme)
pkg_check_modules(SPDK_ENV_DPDK REQUIRED IMPORTED_TARGET spdk_env_dpdk)
pkg_check_modules(SPDK_BDEV     REQUIRED IMPORTED_TARGET spdk_bdev)
pkg_check_modules(SPDK_EVENT    REQUIRED IMPORTED_TARGET spdk_event)
pkg_check_modules(ZSTD          REQUIRED IMPORTED_TARGET libzstd)

file(GLOB UTIL_HDR_FILES include/Utils/*.h)
file(GLOB UTIL_FILES src/Utils/*.cpp)
add_library(DistanceUtils STATIC ${UTIL_FILES})
target_include_directories(DistanceUtils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_options(DistanceUtils PRIVATE -mavx2 -mavx -msse -msse2 -mavx512f -mavx512bw -mavx512dq -fPIC)

file(GLOB_RECURSE SPTAG_HDR_FILES include/Helper/*.h include/Core/*.h)
file(GLOB_RECURSE SPTAG_FILES src/Helper/*.cpp src/Core/*.cpp)
add_library(SPTAGLib STATIC ${SPTAG_FILES})
target_include_directories(SPTAGLib PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(SPTAGLib PUBLIC
    DistanceUtils tbb
    PkgConfig::SPDK_NVME
    PkgConfig::SPDK_ENV_DPDK
    PkgConfig::SPDK_BDEV
    PkgConfig::SPDK_EVENT
    PkgConfig::ZSTD
    uring isal isal_crypto numa OpenMP::OpenMP_CXX
)

file(GLOB SPFRESH_HDR_FILES include/SPFresh/*.h)
file(GLOB SPFRESH_FILES src/SPFresh/*.cpp)
add_executable(spfresh ${SPFRESH_FILES})
target_compile_definitions(spfresh PRIVATE _exe)
target_link_libraries(spfresh PRIVATE Boost::headers SPTAGLib)
target_include_directories(spfresh PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB SSDSERVING_HDR_FILES include/SSDServing/*.h)
file(GLOB SSDSERVING_FILES src/SSDServing/*.cpp)
add_executable(ssdserving ${SSDSERVING_FILES})
target_compile_definitions(ssdserving PRIVATE _exe)
target_link_libraries(ssdserving PRIVATE Boost::headers SPTAGLib)
target_include_directories(ssdserving PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_executable(BKTSerializationTest unittest/BKTSerializationTest.cpp)
target_link_libraries(BKTSerializationTest PRIVATE SPTAGLib)
target_include_directories(BKTSerializationTest PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
add_test(NAME BKTSerializationTest COMMAND BKTSerializationTest)
set_tests_properties(BKTSerializationTest PROPERTIES ENVIRONMENT "LD_LIBRARY_PATH=/usr/local/lib")

add_executable(ExtraSPDKControllerTest unittest/ExtraSPDKControllerTest.cpp)
target_link_libraries(ExtraSPDKControllerTest PRIVATE SPTAGLib)
target_include_directories(ExtraSPDKControllerTest PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
add_test(NAME ExtraSPDKControllerTest COMMAND ExtraSPDKControllerTest)
set_tests_properties(ExtraSPDKControllerTest PROPERTIES ENVIRONMENT "LD_LIBRARY_PATH=/usr/local/lib;SPFRESH_SPDK_CONF=${CMAKE_CURRENT_SOURCE_DIR}/script/bdev-user.json;SPFRESH_SPDK_BDEV=Uring0")
