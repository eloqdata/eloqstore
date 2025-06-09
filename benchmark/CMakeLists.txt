cmake_minimum_required(VERSION 3.20)

add_executable(io_uring_test io_uring_test.cpp)
target_link_libraries(io_uring_test ${URING_LIB})

add_executable(uring_register_test uring_register_test.cpp)
target_link_libraries(uring_register_test ${URING_LIB})

add_executable(io_test io_test.cpp)
target_link_libraries(io_test ${URING_LIB})

add_executable(queue_sync_test queue_sync_test.cpp)

add_executable(cache_q_test cache_q_test.cpp)
# target_compile_options(cache_q_test PRIVATE -fsanitize=address)
# target_link_options(cache_q_test PRIVATE -fsanitize=address)
target_link_libraries (cache_q_test glog::glog)

add_executable(proxy proxy.cc helpers.cc)
target_link_libraries(proxy ${URING_LIB})