cmake_minimum_required(VERSION 3.29)
project(SPFresh LANGUAGES CXX)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(GitProject)
find_package(PkgConfig REQUIRED)
find_package(Boost REQUIRED CONFIG)
find_package(MPI REQUIRED)
find_package(TBB REQUIRED CONFIG)
find_package(OpenMP REQUIRED)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

pkg_check_modules(SPDK REQUIRED IMPORTED_TARGET spdk_nvme spdk_event spdk_env_dpdk spdk_bdev spdk_accel spdk_sock_uring)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)
find_library(ISAL_LIB isal REQUIRED)
find_library(ISAL_CRYPTO_LIB isal_crypto REQUIRED)
find_library(URING_LIB uring REQUIRED)

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
target_include_directories(SPTAGLib PRIVATE
    ${SPDK_INCLUDE_DIRS}
)
target_link_libraries(SPTAGLib PUBLIC
    DistanceUtils tbb
    ${SPDK_LIBRARIES}
    ${ZSTD_LIBRARIES}
    ${ISAL_LIB}
    ${ISAL_CRYPTO_LIB}
    ${URING_LIB}
    OpenMP::OpenMP_CXX
)

file(GLOB SERVER_HDR_FILES include/Server/*.h include/Socket/*.h)
file(GLOB SERVER_FILES src/Server/*.cpp src/Socket/*.cpp)
add_executable(server ${SERVER_FILES})
target_link_libraries(server Boost::headers SPTAGLib)
target_include_directories(server PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB CLIENT_HDR_FILES include/Client/*.h include/Socket/*.h)
file(GLOB CLIENT_FILES src/Client/*.cpp src/Socket/*.cpp)
add_executable(client ${CLIENT_FILES})
target_link_libraries(client PRIVATE Boost::headers SPTAGLib)
target_include_directories(client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB AGG_HDR_FILES include/Aggregator/*.h include/Socket/*.h include/Server/QueryParser.h)
file(GLOB AGG_FILES src/Aggregator/*.cpp src/Socket/*.cpp src/Server/QueryParser.cpp)
add_executable(aggregator ${AGG_FILES})
target_link_libraries(aggregator PRIVATE Boost::headers SPTAGLib)
target_include_directories(aggregator PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB BUILDER_FILES src/IndexBuilder/*.cpp)
add_executable(indexbuilder ${BUILDER_FILES})
target_link_libraries(indexbuilder PRIVATE Boost::headers SPTAGLib)
target_include_directories(indexbuilder PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB SEARCHER_FILES src/IndexSearcher/*.cpp)
add_executable(indexsearcher ${SEARCHER_FILES})
target_link_libraries(indexsearcher PRIVATE Boost::headers SPTAGLib)
target_include_directories(indexsearcher PRIVATE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB QUANTIZER_HDR_FILES include/Quantizer/*.h)
file(GLOB QUANTIZER_FILES src/Quantizer/*.cpp)
add_executable(quantizer ${QUANTIZER_FILES})
target_link_libraries(quantizer PRIVATE Boost::headers SPTAGLib)
target_include_directories(quantizer PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
