cmake_minimum_required(VERSION 3.20)
project(eloqstore_comprehensive_tests)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build options
option(WITH_COVERAGE "Enable coverage reporting" OFF)
option(WITH_ASAN "Build with Address Sanitizer" OFF)
option(WITH_TSAN "Build with Thread Sanitizer" OFF)
option(WITH_UBSAN "Build with Undefined Behavior Sanitizer" OFF)

# Coverage settings
if(WITH_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
endif()

# Sanitizer settings
if(WITH_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

if(WITH_TSAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()

if(WITH_UBSAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
endif()

# Find required packages
find_package(Threads REQUIRED)
find_package(glog REQUIRED)
find_package(Catch2 3 REQUIRED)
find_package(Boost REQUIRED COMPONENTS context filesystem)
find_package(absl REQUIRED)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/fixtures
    ${CMAKE_CURRENT_SOURCE_DIR}/mocks
)

# Link to parent project's eloqstore library and its dependencies
link_directories(${CMAKE_SOURCE_DIR}/../build)
link_directories(${CMAKE_SOURCE_DIR}/../Release)

# Find and link against the same abseil that parent project uses
set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/../build/abseil)
find_package(absl REQUIRED)

# Common test libraries
add_library(test_fixtures STATIC
    fixtures/test_fixtures.cpp
    fixtures/test_helpers.cpp
    fixtures/temp_directory.cpp
    fixtures/data_generator.cpp
)
target_link_libraries(test_fixtures PUBLIC
    eloqstore
    glog::glog
    Threads::Threads
    Boost::filesystem
    Boost::context
    uring
)

add_library(test_mocks STATIC
    mocks/mock_io_manager.cpp
    mocks/mock_shard.cpp
    mocks/mock_task_manager.cpp
)
target_link_libraries(test_mocks PUBLIC
    eloqstore
    test_fixtures
)

# Enable testing
enable_testing()
include(CTest)
include(Catch)

# Core tests
set(CORE_TESTS
    simple_test
    coding_simple_test
    async_simple_test
    #coding_test  # Disabled - needs more fixes
    #page_test
    #data_page_test
    #index_page_test
    #overflow_page_test
    #data_page_builder_test
    #comparator_test
    #types_test
    #page_mapper_test
)

foreach(test ${CORE_TESTS})
    add_executable(${test} tests/core/${test}.cpp)
    # IMPORTANT: Use whole-archive for eloqstore to ensure all symbols are available
    target_link_libraries(${test} PRIVATE
        Catch2::Catch2WithMain
        test_fixtures
        test_mocks
        -Wl,--whole-archive
        ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
        -Wl,--no-whole-archive
        Boost::context
        Boost::filesystem
        uring
        glog::glog
        absl::flat_hash_map
        absl::raw_hash_set
        absl::hash
        absl::strings
        absl::time
        absl::synchronization
        absl::cord
        pthread
        dl
    )
    catch_discover_tests(${test})
endforeach()

# I/O tests
set(IO_TESTS
    async_io_manager_test
)

foreach(test ${IO_TESTS})
    add_executable(${test} tests/io/${test}.cpp)
    target_link_libraries(${test} PRIVATE
        Catch2::Catch2WithMain
        test_fixtures
        test_mocks
        ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
        glog::glog
        Boost::context
        Boost::filesystem
        uring
        absl::flat_hash_map
        absl::raw_hash_set
        absl::hash
        absl::strings
        absl::time
        absl::synchronization
        absl::cord
        pthread
        dl
    )
    catch_discover_tests(${test})
endforeach()

# Storage tests
set(STORAGE_TESTS
    file_gc_test
)

foreach(test ${STORAGE_TESTS})
    add_executable(${test} tests/storage/${test}.cpp)
    target_link_libraries(${test} PRIVATE
        Catch2::Catch2WithMain
        test_fixtures
        test_mocks
        ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
        glog::glog
        Boost::context
        Boost::filesystem
        uring
        absl::flat_hash_map
        absl::raw_hash_set
        absl::hash
        absl::strings
        absl::time
        absl::synchronization
        absl::cord
        pthread
        dl
    )
    catch_discover_tests(${test})
endforeach()

# Task tests
set(TASK_TESTS
    task_base_test
    read_task_test
    scan_task_test
    batch_write_task_test
)

foreach(test ${TASK_TESTS})
    add_executable(${test} tests/task/${test}.cpp)
    target_link_libraries(${test} PRIVATE
        Catch2::Catch2WithMain
        test_fixtures
        test_mocks
        ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
        glog::glog
        Boost::context
        Boost::filesystem
        uring
        absl::flat_hash_map
        absl::raw_hash_set
        absl::hash
        absl::strings
        absl::time
        absl::synchronization
        absl::cord
        pthread
        dl
    )
    catch_discover_tests(${test})
endforeach()

# Sharding tests
set(SHARDING_TESTS
    shard_test
)

foreach(test ${SHARDING_TESTS})
    add_executable(${test} tests/shard/${test}.cpp)
    target_link_libraries(${test} PRIVATE
        Catch2::Catch2WithMain
        test_fixtures
        test_mocks
        ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
        glog::glog
        Boost::context
        Boost::filesystem
        uring
        absl::flat_hash_map
        absl::raw_hash_set
        absl::hash
        absl::strings
        absl::time
        absl::synchronization
        absl::cord
        pthread
        dl
    )
    catch_discover_tests(${test})
endforeach()

# Utility tests
set(UTILS_TESTS
    kv_options_test
    utils_test
    error_test
)

foreach(test ${UTILS_TESTS})
    add_executable(${test} tests/utils/${test}.cpp)
    target_link_libraries(${test} PRIVATE
        Catch2::Catch2WithMain
        test_fixtures
        test_mocks
        ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
        glog::glog
        Boost::context
        Boost::filesystem
        uring
        absl::flat_hash_map
        absl::raw_hash_set
        absl::hash
        absl::strings
        absl::time
        absl::synchronization
        absl::cord
        pthread
        dl
    )
    catch_discover_tests(${test})
endforeach()

# Integration tests
add_executable(integration_test
    integration/basic_operations_test.cpp
    integration/workflow_test.cpp
)
target_link_libraries(integration_test PRIVATE
    Catch2::Catch2WithMain
    test_fixtures
    ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
    glog::glog
    Boost::context
    Boost::filesystem
    uring
    absl::flat_hash_map
    absl::raw_hash_set
    absl::hash
    absl::strings
    absl::time
    absl::synchronization
    absl::cord
    pthread
    dl
)
catch_discover_tests(integration_test)

# Async tests
add_executable(async_ops_test
    tests/integration/async_ops_test.cpp
)
add_executable(async_pattern_test
    tests/integration/async_pattern_test.cpp
)
target_link_libraries(async_ops_test PRIVATE
    Catch2::Catch2WithMain
    test_fixtures
    ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
    glog::glog
    Boost::context
    Boost::filesystem
    uring
    absl::flat_hash_map
    absl::raw_hash_set
    absl::hash
    absl::strings
    absl::time
    absl::synchronization
    absl::cord
    pthread
    dl
)
catch_discover_tests(async_ops_test)

target_link_libraries(async_pattern_test PRIVATE
    Catch2::Catch2WithMain
    test_fixtures
    -Wl,--whole-archive
    ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
    -Wl,--no-whole-archive
    Boost::context
    Boost::filesystem
    uring
    glog::glog
    absl::flat_hash_map
    absl::raw_hash_set
    absl::hash
    absl::strings
    absl::time
    absl::synchronization
    absl::cord
    pthread
    dl
)
catch_discover_tests(async_pattern_test)

# Stress tests
add_executable(stress_test
    stress/concurrent_operations_test.cpp
)
target_link_libraries(stress_test PRIVATE
    Catch2::Catch2WithMain
    test_fixtures
    ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
    glog::glog
    Boost::context
    Boost::filesystem
    uring
    absl::flat_hash_map
    absl::raw_hash_set
    absl::hash
    absl::strings
    absl::time
    absl::synchronization
    absl::cord
    pthread
    dl
)
catch_discover_tests(stress_test)

# Edge case tests
add_executable(edge_case_test
    edge_cases/boundary_values_test.cpp
)
target_link_libraries(edge_case_test PRIVATE
    Catch2::Catch2WithMain
    test_fixtures
    test_mocks
    ${CMAKE_SOURCE_DIR}/../build/libeloqstore.a
    glog::glog
    Boost::context
    Boost::filesystem
    uring
    absl::flat_hash_map
    absl::raw_hash_set
    absl::hash
    absl::strings
    absl::time
    absl::synchronization
    absl::cord
    pthread
    dl
)
catch_discover_tests(edge_case_test)

# Custom targets
add_custom_target(run-all-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS ${CORE_TESTS} ${IO_TESTS} ${STORAGE_TESTS} ${TASK_TESTS}
            ${SHARDING_TESTS} ${UTILS_TESTS} integration_test stress_test edge_case_test
)

add_custom_target(coverage-report
    COMMAND lcov --capture --directory . --output-file coverage.info
    COMMAND lcov --remove coverage.info '/usr/*' --output-file coverage.info
    COMMAND lcov --remove coverage.info '*/tests/*' --output-file coverage.info
    COMMAND genhtml coverage.info --output-directory coverage-report
    COMMAND echo "Coverage report generated in coverage-report/index.html"
    DEPENDS run-all-tests
)

# Print configuration
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Coverage enabled: ${WITH_COVERAGE}")
message(STATUS "ASAN enabled: ${WITH_ASAN}")
message(STATUS "TSAN enabled: ${WITH_TSAN}")
message(STATUS "UBSAN enabled: ${WITH_UBSAN}")