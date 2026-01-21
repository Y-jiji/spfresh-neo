cmake_minimum_required(VERSION 3.28)
project(SPFresh LANGUAGES CXX)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(GitProject)
find_package(PkgConfig REQUIRED)
find_package(Boost REQUIRED CONFIG)
find_package(MPI REQUIRED)
find_package(TBB REQUIRED CONFIG)
find_package(OpenMP REQUIRED)
find_package(rocksdb REQUIRED)
find_package(OpenSSL REQUIRED)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

pkg_check_modules(SPDK REQUIRED IMPORTED_TARGET spdk_nvme spdk_event spdk_env_dpdk spdk_bdev spdk_accel)

# CRITICAL FIX: DPDK PMD plugins cause "tailq already registered" crash
# The SPDK pkg-config incorrectly includes PMD plugins as direct link dependencies
# PMDs (bus drivers, mempools, power) must ONLY be loaded dynamically, not linked
# We keep the filter commented for now - the real fix is upstream in SPDK/DPDK build

pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)
find_library(ISAL_LIB isal REQUIRED)
find_library(ISAL_CRYPTO_LIB isal_crypto REQUIRED)
find_library(URING_LIB uring REQUIRED)
find_library(UUID_LIB uuid REQUIRED)

file(GLOB UTIL_HDR_FILES include/Utils/*.h)
file(GLOB UTIL_FILES src/Utils/*.cpp)
add_library(DistanceUtils STATIC ${UTIL_FILES})
target_include_directories(DistanceUtils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_include_directories(DistanceUtils PRIVATE
    OpenMP::OpenMP_CXX
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
    OpenSSL::SSL
    OpenSSL::Crypto
    ${UUID_LIB}
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

file(GLOB_RECURSE SSD_SERVING_HDR_FILES include/SSDServing/*.h)
file(GLOB_RECURSE SSD_SERVING_FILES src/SSDServing/*.cpp)
add_executable(ssdserving ${SSD_SERVING_HDR_FILES} ${SSD_SERVING_FILES})
target_link_libraries(ssdserving SPTAGLib Boost::headers rocksdb)
target_compile_definitions(ssdserving PRIVATE _exe)
target_include_directories(ssdserving PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

file(GLOB_RECURSE SPFRESH_HDR_FILES include/SPFresh/*.h)
file(GLOB SPFRESH_FILES src/SPFresh/main.cpp)
add_executable(spfresh ${SPFRESH_HDR_FILES} ${SPFRESH_FILES})
target_link_libraries(spfresh SPTAGLib Boost::headers rocksdb)
target_compile_definitions(spfresh PRIVATE _exe)
target_include_directories(spfresh PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# SPFresh Interface library
add_library(SPFreshInterface STATIC src/SPFresh/SPFreshInterface.cpp)
target_link_libraries(SPFreshInterface PUBLIC SPTAGLib)
target_include_directories(SPFreshInterface PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# SPFresh Example executable (using existing index)
add_executable(spfresh_example src/SPFresh/example_usage.cpp)
target_link_libraries(spfresh_example PRIVATE SPFreshInterface Boost::headers rocksdb)
target_include_directories(spfresh_example PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# SPFresh Create Index Example (creating new empty index with RocksDB)
add_executable(spfresh_create_index src/SPFresh/example_create_index.cpp)
target_link_libraries(spfresh_create_index PRIVATE SPFreshInterface Boost::headers rocksdb)
target_include_directories(spfresh_create_index PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# SPFresh Create Index with SPDK Example (creating new empty index with SPDK backend)
add_executable(spfresh_create_index_spdk src/SPFresh/example_create_index_spdk.cpp)
target_link_libraries(spfresh_create_index_spdk PRIVATE SPFreshInterface Boost::headers rocksdb)
target_include_directories(spfresh_create_index_spdk PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# SPFresh Stress Test (multi-threaded testing with memory-mapped datasets)
add_executable(spfresh_stress_test src/SPFresh/spfresh_stress_test.cpp)
target_link_libraries(spfresh_stress_test PRIVATE SPFreshInterface Boost::headers rocksdb)
target_include_directories(spfresh_stress_test PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# SPFresh Stress Test for uint8 vectors
add_executable(spfresh_stress_test_uint8 src/SPFresh/spfresh_stress_test_uint8.cpp)
target_link_libraries(spfresh_stress_test_uint8 PRIVATE SPFreshInterface Boost::headers rocksdb)
target_include_directories(spfresh_stress_test_uint8 PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Vector Data Generation Utility
add_executable(generate_vector_data src/Utils/generate_vector_data.cpp)
target_link_libraries(generate_vector_data PRIVATE)
target_include_directories(generate_vector_data PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)


file(GLOB_RECURSE USEFULTOOL_FILES src/UsefulTool/*.cpp)
add_executable(usefultool ${USEFULTOOL_FILES})
target_link_libraries(usefultool SPTAGLib Boost::headers rocksdb)
target_compile_definitions(usefultool PRIVATE _exe)
target_include_directories(usefultool PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

include_directories (${MPI_CXX_INCLUDE_PATH})
file(GLOB PARTITION_FILES src/BalancedDataPartition/*.cpp)
add_executable(balanceddatapartition ${PARTITION_FILES})
target_link_libraries(balanceddatapartition SPTAGLib Boost::headers ${MPI_CXX_LIBRARIES})
target_include_directories(balanceddatapartition PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
