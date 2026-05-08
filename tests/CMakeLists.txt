project(CatchUnitTests)

find_package(Catch2 REQUIRED)

set(UTEST_DIR ${CMAKE_SOURCE_DIR}/tests)
set(UTEST_INCLUDE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR})

# Tests that use GlobalRegisteredMemory with io_uring fixed-buffer registration
# need RLIMIT_MEMLOCK bumped to 2GB at process start.  They are built with
# large_value_main.cpp (Catch2::Catch2, no bundled main) instead of
# Catch2::Catch2WithMain.
set(LARGE_VALUE_SOURCES
    large_value_e2e.cpp
    segment_compact.cpp
    large_value_concurrency.cpp
    large_value_fault_injection.cpp
    large_value_benchmark.cpp
)

set(UTEST_SOURCES
    scan.cpp
    batch_write.cpp
    delete.cpp
    persist.cpp
    manifest.cpp
    concurrency.cpp
    cloud.cpp
    cloud_term.cpp
    gc.cpp
    chore.cpp
    eloq_store_test.cpp
    replayer_term.cpp
    store_path_lut.cpp
    write_task.cpp
    standby.cpp
    branch_filename_parsing.cpp
    branch_operations.cpp
    branch_gc.cpp
    manifest_payload.cpp
    global_registered_memory.cpp
    io_string_buffer.cpp
    data_page_large_value.cpp
    segment_allocator.cpp
)

string( REPLACE ".cpp" "" BASENAMES_UTEST "${UTEST_SOURCES}" )
set( STANDARD_TARGETS ${BASENAMES_UTEST} )

string( REPLACE ".cpp" "" BASENAMES_LARGE_VALUE "${LARGE_VALUE_SOURCES}" )
set( LARGE_VALUE_TARGETS ${BASENAMES_LARGE_VALUE} )

set( ALL_TARGETS ${STANDARD_TARGETS} ${LARGE_VALUE_TARGETS} )

enable_testing()
include(CTest)
include(Catch)

foreach( name ${STANDARD_TARGETS} )
   add_executable( ${name} ${UTEST_DIR}/${name}.cpp common.cpp)
   target_include_directories(${name} PUBLIC ${UTEST_INCLUDE} ${INCLUDE_DIR})
   target_link_libraries( ${name} PRIVATE Catch2::Catch2WithMain test_util)
   catch_discover_tests(${name})
endforeach()

foreach( name ${LARGE_VALUE_TARGETS} )
   add_executable( ${name} ${UTEST_DIR}/${name}.cpp
                            ${UTEST_DIR}/large_value_main.cpp
                            common.cpp)
   target_include_directories(${name} PUBLIC ${UTEST_INCLUDE} ${INCLUDE_DIR})
   target_link_libraries( ${name} PRIVATE Catch2::Catch2 test_util)
   catch_discover_tests(${name})
endforeach()
