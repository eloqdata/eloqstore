project(CatchUnitTests)

find_package(Catch2 REQUIRED)

set(UTEST_DIR ${CMAKE_SOURCE_DIR}/tests)
set(UTEST_INCLUDE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR})

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
    filename_parsing.cpp
    fileid_term_mapping.cpp
    manifest_payload.cpp
    replayer_term.cpp
)

string( REPLACE ".cpp" "" BASENAMES_UTEST "${UTEST_SOURCES}" )
set( ALL_TARGETS ${BASENAMES_UTEST} )


#add_executable(insert insert_test.cpp)
#target_link_libraries( insert PRIVATE Catch2WithMain)

enable_testing()
include(CTest)
include(Catch)

foreach( name ${ALL_TARGETS} )
   add_executable( ${name} ${UTEST_DIR}/${name}.cpp common.cpp)
   catch_discover_tests(${name})
endforeach()

foreach( name ${ALL_TARGETS} )
    target_include_directories(${name} PUBLIC ${UTEST_INCLUDE} ${INCLUDE_DIR})
endforeach()

foreach( name ${ALL_TARGETS} )
    target_link_libraries( ${name} PRIVATE Catch2::Catch2WithMain test_util)
endforeach()
