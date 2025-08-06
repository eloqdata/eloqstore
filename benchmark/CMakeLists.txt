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
target_link_libraries(eloqstore_benchmark ${DYNAMIC_LIB} eloqstore)

add_executable(load_bench load_bench.cpp)
target_link_libraries(load_bench eloqstore)

add_executable(simple_test simple_test.cpp)
target_link_libraries(simple_test eloqstore)

include(CTest)
enable_testing()

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)

install(TARGETS eloqstore_benchmark
        RUNTIME DESTINATION bin)
