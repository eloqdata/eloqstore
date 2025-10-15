cmake_minimum_required(VERSION 3.20)
project(eloqkvstore)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Coverage build type
if(CMAKE_BUILD_TYPE STREQUAL "Coverage")
    message(STATUS "Building with coverage flags")
    add_compile_options(-O0 -g --coverage)
    add_link_options(--coverage)
endif()

option(WITH_ASAN "build with ASAN" OFF)
if (WITH_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
endif ()

option(ELOQ_MODULE_ENABLED "Enable EloqModule" OFF)
message("ELOQ_MODULE_ENABLED: " ${ELOQ_MODULE_ENABLED})
if (ELOQ_MODULE_ENABLED)
    add_compile_definitions(ELOQ_MODULE_ENABLED)
endif ()


find_package(Threads REQUIRED)
find_package(glog REQUIRED)

find_package(Boost REQUIRED COMPONENTS context)

find_package(jsoncpp REQUIRED)
find_package(CURL REQUIRED)
if (Boost_FOUND)
    message(STATUS "Boost found at: ${Boost_INCLUDE_DIRS}")
else ()
    message(FATAL_ERROR "Boost.Context not found!")
endif ()

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

if (NOT EXISTS "${PROJECT_SOURCE_DIR}/concurrentqueue/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif ()
add_subdirectory(concurrentqueue)

add_subdirectory(abseil)

set(INI_SOURCES ${PROJECT_SOURCE_DIR}/inih/ini.c ${PROJECT_SOURCE_DIR}/inih/cpp/INIReader.cpp)

SET(ELOQ_STORE_INCLUDE
        ${URING_INCLUDE_PATH}
        ${Boost_INCLUDE_DIRS})

set(ELOQ_STORE_SOURCES
        coding.cpp
        data_page_builder.cpp
        comparator.cpp
        index_page_builder.cpp
        mem_index_page.cpp
        index_page_manager.cpp
        task.cpp
        read_task.cpp
        scan_task.cpp
        write_task.cpp
        batch_write_task.cpp
        background_write.cpp
        async_io_manager.cpp
        data_page.cpp
        page.cpp
        page_mapper.cpp
        task_manager.cpp
        eloq_store.cpp
        shard.cpp
        root_meta.cpp
        replayer.cpp
        kill_point.cpp
        file_gc.cpp
        archive_crond.cpp
        object_store.cpp
        types.cpp
        kv_options.cpp
        eloqstore_module.cpp
        external/random.cc
        external/xxhash.c)

add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES} ${INI_SOURCES})

target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(eloqstore PRIVATE ${URING_LIB} Boost::context glog::glog absl::flat_hash_map ${CURL_LIBRARIES} jsoncpp_lib)


add_library(test_util STATIC test_utils.cpp)
target_link_libraries(test_util PUBLIC eloqstore)

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


# Coverage target for generating coverage reports
find_program(LCOV_EXECUTABLE lcov)
find_program(GENHTML_EXECUTABLE genhtml)

if(LCOV_EXECUTABLE AND GENHTML_EXECUTABLE)
    # Unit test coverage target (for actual project code)
    add_custom_target(unit-test-coverage
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/unit_test_coverage_report
        COMMAND ${LCOV_EXECUTABLE} --capture --directory ${CMAKE_BINARY_DIR} --output-file ${CMAKE_BINARY_DIR}/unit_coverage.info
        COMMAND ${LCOV_EXECUTABLE} --remove ${CMAKE_BINARY_DIR}/unit_coverage.info 
            '/usr/*' 
            '*/abseil/*' 
            '*/concurrentqueue/*' 
            '*/inih/*' 
            '*/external/*' 
            '*/tests/*' 
            '*/benchmark/*' 
            '*/db_stress/*' 
            '*/examples/*'
            --output-file ${CMAKE_BINARY_DIR}/unit_coverage_filtered.info --ignore-errors unused
        COMMAND ${GENHTML_EXECUTABLE} ${CMAKE_BINARY_DIR}/unit_coverage_filtered.info --output-directory ${CMAKE_BINARY_DIR}/unit_test_coverage_report
        COMMAND ${CMAKE_COMMAND} -E echo "Unit test coverage report generated in ${CMAKE_BINARY_DIR}/unit_test_coverage_report/index.html"
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating unit test coverage report for project code"
    )
    
    add_custom_target(coverage-clean
        COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/unit_test_coverage_report
        COMMAND ${CMAKE_COMMAND} -E remove -f ${CMAKE_BINARY_DIR}/unit_coverage.info
        COMMAND ${CMAKE_COMMAND} -E remove -f ${CMAKE_BINARY_DIR}/unit_coverage_filtered.info
        COMMAND find ${CMAKE_BINARY_DIR} -name "*.gcda" -delete
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Cleaning coverage data"
    )
else()
    message(WARNING "lcov and/or genhtml not found. Coverage target will not be available.")
endif()