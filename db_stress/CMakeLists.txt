project(DBStress)

find_package(gflags REQUIRED)

add_executable(concurrent_test concurrent_test.cpp)
target_link_libraries(concurrent_test PRIVATE gflags test_util)

add_executable(test_client test_client.cpp)
target_link_libraries(test_client PRIVATE gflags test_util)

add_executable(clean_up clean_up.cpp)
target_link_libraries(clean_up PRIVATE gflags)

add_executable(db_stress
    db_stress_common.cpp
    db_stress_test_base.cpp
    db_stress_gflags.cpp
    db_stress_shared_state.cpp
    batched_ops_stress.cpp
    non_batched_ops_stress.cpp
    expected_value.cpp
    db_stress_driver.cpp)

target_include_directories(db_stress PUBLIC ${PROJECT_SOURCE_DIR}
                                            ${PROJECT_SOURCE_DIR}/..
                                            ${CMAKE_SOURCE_DIR}
                                            ${CMAKE_SOURCE_DIR}/include
                                            ${GFLAGS_INCLUDE_DIRS}
                                            ${GLOG_INCLUDE_DIRS})

target_link_libraries(db_stress PRIVATE gflags glog eloqstore)
