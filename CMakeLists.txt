cmake_minimum_required(VERSION 3.29)
project(SPFresh LANGUAGES CXX)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(GitProject)
find_package(PkgConfig REQUIRED)
find_package(Boost REQUIRED CONFIG)
find_package(MPI REQUIRED)
find_package(TBB REQUIRED CONFIG)

GitProject(spdk
    GIT_REPOSITORY      https://github.com/spdk/spdk
    GIT_TAG             v25.09
    SOURCE_DIR          ${CMAKE_BINARY_DIR}/3rd_party/spdk
    BINARY_DIR          ${CMAKE_BINARY_DIR}/3rd_party/spdk/build
    BUILD_COMMAND       sudo ./scripts/pkgdep.sh && ./configure --with-shared && make -j
)

GitProject(zstd
    GIT_REPOSITORY      https://github.com/facebook/zstd
    GIT_TAG             v1.5.7
    SOURCE_DIR          ${CMAKE_BINARY_DIR}/3rd_party/zstd
    BINARY_DIR          ${CMAKE_BINARY_DIR}/3rd_party/zstd-install
    BUILD_COMMAND       make install PREFIX=${CMAKE_BINARY_DIR}/3rd_party/zstd-install -j
)

set(ENV{PKG_CONFIG_PATH} "${CMAKE_BINARY_DIR}/3rd_party/spdk/build/lib/pkgconfig:${CMAKE_BINARY_DIR}/3rd_party/zstd-install/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(SPDK_NVME     REQUIRED IMPORTED_TARGET spdk_nvme)
pkg_check_modules(SPDK_ENV_DPDK REQUIRED IMPORTED_TARGET spdk_env_dpdk)
pkg_check_modules(ZSTD          REQUIRED IMPORTED_TARGET libzstd)

file(GLOB UTIL_HDR_FILES include/Utils/*.h)
file(GLOB UTIL_FILES src/Utils/*.cpp)
add_library(DistanceUtils STATIC ${UTIL_FILES})
target_include_directories(DistanceUtils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_options(DistanceUtils PRIVATE -mavx2 -mavx -msse -msse2 -mavx512f -mavx512bw -mavx512dq -fPIC)

file(GLOB SPTAG_HDR_FILES include/Helper/*.h include/Core/*.h)
file(GLOB SPTAG_FILES src/Helper/*.cpp src/Core/*.cpp)
add_library(SPTAGLib STATIC ${SPTAG_FILES})
target_include_directories(SPTAGLib PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(SPTAGLib PUBLIC
    DistanceUtils tbb
    PkgConfig::SPDK_NVME
    PkgConfig::SPDK_ENV_DPDK
    PkgConfig::ZSTD
)

file(GLOB SERVER_HDR_FILES include/Server/*.h include/Socket/*.h)
file(GLOB SERVER_FILES src/Server/*.cpp src/Socket/*.cpp)
add_executable (server ${SERVER_FILES})
target_link_libraries(server Boost::headers SPTAGLib)
target_include_directories(server PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB CLIENT_HDR_FILES include/Client/*.h include/Socket/*.h)
file(GLOB CLIENT_FILES src/Client/*.cpp src/Socket/*.cpp)
add_executable (client ${CLIENT_FILES})
target_link_libraries(client PRIVATE Boost::headers SPTAGLib)
target_include_directories(client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB AGG_HDR_FILES include/Aggregator/*.h include/Socket/*.h include/Server/QueryParser.h)
file(GLOB AGG_FILES src/Aggregator/*.cpp src/Socket/*.cpp src/Server/QueryParser.cpp)
add_executable (aggregator ${AGG_FILES})
target_link_libraries(aggregator PRIVATE Boost::headers SPTAGLib)
target_include_directories(aggregator PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB BUILDER_FILES src/IndexBuilder/*.cpp)
add_executable (indexbuilder ${BUILDER_FILES})
target_link_libraries(indexbuilder PRIVATE Boost::headers SPTAGLib)
target_include_directories(indexbuilder PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB SEARCHER_FILES src/IndexSearcher/*.cpp)
add_executable (indexsearcher ${SEARCHER_FILES})
target_link_libraries(indexsearcher PRIVATE Boost::headers SPTAGLib)
target_include_directories(indexsearcher PRIVATE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB QUANTIZER_HDR_FILES include/Quantizer/*.h)
file(GLOB QUANTIZER_FILES src/Quantizer/*.cpp)
add_executable (quantizer ${QUANTIZER_FILES})
target_link_libraries(quantizer PRIVATE Boost::headers SPTAGLib)
target_include_directories(quantizer PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
