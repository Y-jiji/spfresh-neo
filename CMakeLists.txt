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
pkg_check_modules(SPDK REQUIRED IMPORTED_TARGET spdk_nvme spdk_env_dpdk spdk_bdev spdk_event spdk_bdev_uring spdk_bdev_nvme spdk_event_bdev spdk_event_accel spdk_event_sock spdk_event_iobuf)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)

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
    PkgConfig::SPDK
    PkgConfig::ZSTD
    uring isal isal_crypto numa OpenMP::OpenMP_CXX
)

add_executable(spfresh bin/spfresh.cpp)
target_link_libraries(spfresh PRIVATE Boost::headers SPTAGLib)
target_include_directories(spfresh PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_executable(ssdserving bin/ssdserving.cpp)
target_link_libraries(ssdserving PRIVATE Boost::headers SPTAGLib)
target_include_directories(ssdserving PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_executable(tape bin/tape.cpp)
target_link_libraries(tape PRIVATE Boost::headers SPTAGLib)
target_include_directories(tape PRIVATE
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
set_property(TEST ExtraSPDKControllerTest PROPERTY ENVIRONMENT_MODIFICATION
    "LD_LIBRARY_PATH=set:/usr/local/lib"
    "SPFRESH_SPDK_CONF=set:${CMAKE_CURRENT_SOURCE_DIR}/script/bdev-uring.json"
    "SPFRESH_SPDK_BDEV=set:Uring0"
    "DPDK_IOVA_MODE=set:va"
)

add_executable(SPANNIndexBuildTest unittest/SPANNIndexBuildTest.cpp)
target_link_libraries(SPANNIndexBuildTest PRIVATE SPTAGLib)
target_include_directories(SPANNIndexBuildTest PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
add_test(NAME SPANNIndexBuildTest COMMAND SPANNIndexBuildTest)
set_property(TEST SPANNIndexBuildTest PROPERTY ENVIRONMENT_MODIFICATION
    "LD_LIBRARY_PATH=set:/usr/local/lib"
    "SPFRESH_SPDK_CONF=set:${CMAKE_CURRENT_SOURCE_DIR}/script/bdev-uring.json"
    "SPFRESH_SPDK_BDEV=set:Uring0"
    "DPDK_IOVA_MODE=set:va"
)

add_executable(TracePlayerParallelTest unittest/TracePlayerParallelTest.cpp)
target_link_libraries(TracePlayerParallelTest PRIVATE SPTAGLib)
target_include_directories(TracePlayerParallelTest PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
add_test(NAME TracePlayerParallelTest COMMAND TracePlayerParallelTest)
set_tests_properties(TracePlayerParallelTest PROPERTIES ENVIRONMENT "LD_LIBRARY_PATH=/usr/local/lib")

add_executable(ResultWriterParallelTest unittest/ResultWriterParallelTest.cpp)
target_link_libraries(ResultWriterParallelTest PRIVATE SPTAGLib)
target_include_directories(ResultWriterParallelTest PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
add_test(NAME ResultWriterParallelTest COMMAND ResultWriterParallelTest)
set_tests_properties(ResultWriterParallelTest PROPERTIES ENVIRONMENT "LD_LIBRARY_PATH=/usr/local/lib")