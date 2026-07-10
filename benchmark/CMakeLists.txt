set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-parentheses -Wno-error")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DFAULT_INJECTOR")

option(WITH_ROCKSDB "With rocksdb" OFF)
if (WITH_ROCKSDB)
    find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
    if (NOT ROCKSDB_INCLUDE_PATH)
    	message(FATAL_ERROR "Fail to find RocksDB include path")
    endif ()
    include_directories(${ROCKSDB_INCLUDE_PATH})

    find_library(ROCKSDB_LIB NAMES rocksdb)
    if (NOT ROCKSDB_LIB)
        message(FATAL_ERROR "Fail to find RocksDB lib path")
    endif()
    set(DYNAMIC_LIB ${DYNAMIC_LIB} ${ROCKSDB_LIB})

    add_compile_definitions(WITH_ROCKSDB)
endif()

# brpc (eloqdata fork with io_uring support), built in-tree instead of a
# system-wide install (pinned in cmake/dependencies.cmake). Its plain
# find_path/find_library discovery of glog and liburing is pre-seeded with the
# in-tree builds from the top-level CMakeLists.
set(WITH_GLOG ON CACHE BOOL "brpc: log through glog")
set(IO_URING_ENABLED ON CACHE BOOL "brpc: io_uring based network")
set(BUILD_BRPC_TOOLS OFF CACHE BOOL "brpc: bundled tools")
set(GLOG_INCLUDE_PATH "${glog_SOURCE_DIR}/src;${glog_BINARY_DIR}"
    CACHE STRING "in-tree glog headers")
set(GLOG_LIB glog::glog CACHE STRING "in-tree glog target")
set(URING_INCLUDE_PATH ${URING_INCLUDE_PATH} CACHE PATH "in-tree liburing headers")
set(URING_LIB ${URING_LIB} CACHE FILEPATH "in-tree liburing archive")
FetchContent_MakeAvailable(brpc)
# Static brpc no longer carries its shared-library DT_NEEDED entries, so its
# private dependencies (brpc's pkg-config list) must be spelled out at the
# final link.
find_library(PROTOBUF_LIB protobuf REQUIRED)
find_library(PROTOC_LIB protoc REQUIRED)
find_library(LEVELDB_LIB leveldb REQUIRED)
set(BRPC_LIB brpc-static OpenSSL::SSL OpenSSL::Crypto
    ${PROTOBUF_LIB} ${PROTOC_LIB} ${LEVELDB_LIB} z dl)
include_directories(${brpc_SOURCE_DIR}/src ${brpc_BINARY_DIR}/output/include)
set(DYNAMIC_LIB ${DYNAMIC_LIB} ${BRPC_LIB})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif ()
include_directories(${GFLAGS_INCLUDE_PATH})


option(HAVE_RANDOM_R "Whether have random_r" ON)
if(HAVE_RANDOM_R)
    add_definitions(-DHAVE_RANDOM_R)
endif()

set(ELOQ_STORE_BM_SOURCES
    obj_gen.cc
    eloq_store_bm.cc)

if (WITH_ROCKSDB)
    set(ELOQ_STORE_BM_SOURCES
        ${ELOQ_STORE_BM_SOURCES}
        random.cc
        rocksdb_bm.cc)
endif()

add_executable(eloqstore_benchmark main.cpp ${ELOQ_STORE_BM_SOURCES})
target_link_libraries(eloqstore_benchmark ${DYNAMIC_LIB} eloqstore ${GFLAGS_LIBRARY})

add_executable(load_bench load_bench.cpp)
target_link_libraries(load_bench eloqstore ${BRPC_LIB} ${GFLAGS_LIBRARY})

add_executable(simple_bench simple_bench.cpp)
target_link_libraries(simple_bench eloqstore ${BRPC_LIB} ${GFLAGS_LIBRARY})

add_executable(simple_test simple_test.cpp)
target_link_libraries(simple_test eloqstore ${BRPC_LIB} ${GFLAGS_LIBRARY})

include(CTest)
enable_testing()

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)

install(TARGETS eloqstore_benchmark
        RUNTIME DESTINATION bin)
