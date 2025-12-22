cmake_minimum_required(VERSION 3.20)
project(eloqkvstore)

include(CheckCXXSourceCompiles)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Coverage option
option(WITH_COVERAGE "Enable code coverage" OFF)
if (WITH_COVERAGE)
    message(STATUS "Building with coverage flags")
    add_compile_options(-O0 -g --coverage)
    add_link_options(--coverage)
endif ()

option(WITH_ASAN "build with ASAN" OFF)
if (WITH_ASAN)
    message(STATUS "build eloqstore with ASAN: ${WITH_ASAN}")

    add_compile_definitions(BOOST_USE_ASAN)
    add_compile_definitions(BOOST_USE_UCONTEXT)

    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)

    find_library(Boost_CONTEXT_ASAN_LIBRARY
            NAMES boost_context-asan
    )

    if (NOT Boost_CONTEXT_ASAN_LIBRARY)
        message(FATAL_ERROR
                "libboost_context-asan not found in system library paths")
    endif ()

    set(BOOST_CONTEXT_TARGET ${Boost_CONTEXT_ASAN_LIBRARY})

else ()
    find_library(Boost_CONTEXT_LIBRARY NAMES boost_context)

    set(BOOST_CONTEXT_TARGET ${Boost_CONTEXT_LIBRARY})
endif ()
message("BOOST_CONTEXT_TARGET: ${BOOST_CONTEXT_TARGET}")

option(ELOQ_MODULE_ENABLED "Enable EloqModule" OFF)
message("ELOQ_MODULE_ENABLED: " ${ELOQ_MODULE_ENABLED})
if (ELOQ_MODULE_ENABLED)
    add_compile_definitions(ELOQ_MODULE_ENABLED)
endif ()


find_package(Threads REQUIRED)
find_package(glog REQUIRED)


find_package(jsoncpp REQUIRED)
find_package(CURL REQUIRED)
find_library(ZSTD_LIBRARY zstd)
find_package(AWSSDK REQUIRED COMPONENTS s3)

find_path(URING_INCLUDE_PATH NAMES liburing.h)
find_library(URING_LIB NAMES uring)
if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
    message(FATAL_ERROR "Fail to find liburing")
endif ()

find_package(Git QUIET)
if (GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    # Update submodules as needed
    message(STATUS "Submodule update")
    execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE GIT_SUBMOD_RESULT)
    if (NOT GIT_SUBMOD_RESULT EQUAL "0")
        message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
    endif ()
endif ()

if (NOT EXISTS "${PROJECT_SOURCE_DIR}/external/concurrentqueue/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif ()
add_subdirectory(external/concurrentqueue)

add_subdirectory(external/abseil)

set(INI_SOURCES ${PROJECT_SOURCE_DIR}/external/inih/ini.c ${PROJECT_SOURCE_DIR}/external/inih/cpp/INIReader.cpp)

SET(ELOQ_STORE_INCLUDE
        ${URING_INCLUDE_PATH}
        ${Boost_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/external
        ${PROJECT_SOURCE_DIR}/external/abseil
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR})

set(ELOQ_STORE_SOURCES
        external/random.cc
        external/xxhash.c
        src/async_io_manager.cpp
        src/cloud_storage_service.cpp
        src/coding.cpp
        src/comparator.cpp
        src/compression.cpp
        src/eloq_store.cpp
        src/eloqstore_module.cpp
        src/file_gc.cpp
        src/kill_point.cpp
        src/kv_options.cpp
        src/replayer.cpp
        src/storage/data_page.cpp
        src/storage/data_page_builder.cpp
        src/storage/index_page_builder.cpp
        src/storage/index_page_manager.cpp
        src/storage/mem_index_page.cpp
        src/storage/object_store.cpp
        src/storage/page.cpp
        src/storage/page_mapper.cpp
        src/storage/root_meta.cpp
        src/storage/shard.cpp
        src/tasks/archive_crond.cpp
        src/tasks/background_write.cpp
        src/tasks/batch_write_task.cpp
        src/tasks/prewarm_task.cpp
        src/tasks/read_task.cpp
        src/tasks/scan_task.cpp
        src/tasks/task.cpp
        src/tasks/task_manager.cpp
        src/tasks/write_task.cpp
        src/types.cpp)

add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES} ${INI_SOURCES})

target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(eloqstore PRIVATE ${URING_LIB} glog::glog absl::flat_hash_map ${BOOST_CONTEXT_TARGET} ${CURL_LIBRARIES} jsoncpp_lib ${ZSTD_LIBRARY} ${AWSSDK_LINK_LIBRARIES})

add_executable(page_checksum_tool tools/page_checksum_tool.cpp)
target_include_directories(page_checksum_tool PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(page_checksum_tool PRIVATE eloqstore)


add_library(test_util STATIC src/test_utils.cpp)
target_link_libraries(test_util PUBLIC eloqstore absl::flat_hash_map)

option(WITH_UNIT_TESTS "build with unit tests" ON)
if (WITH_UNIT_TESTS)
    add_subdirectory(tests)
endif ()

option(WITH_EXAMPLE "build with example" ON)
if (WITH_EXAMPLE)
    add_subdirectory(examples)
endif ()

option(WITH_DB_STRESS "build with stress tool" ON)
if (WITH_DB_STRESS)
    add_subdirectory(db_stress)
endif ()

option(WITH_BENCHMARK "build with benchmark tool" ON)
if (WITH_BENCHMARK)
    add_subdirectory(benchmark)
endif ()
