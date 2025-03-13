cmake_minimum_required(VERSION 3.2)
project(eloqkvstore)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(glog REQUIRED)

find_package(Boost REQUIRED COMPONENTS context)
if(Boost_FOUND)
    message(STATUS "Boost found at: ${Boost_INCLUDE_DIRS}")
else()
    message(FATAL_ERROR "Boost.Context not found!")
endif()

find_path(URING_INCLUDE_PATH NAMES liburing.h)
find_library(URING_LIB NAMES uring)
if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
    message(FATAL_ERROR "Fail to find liburing")
endif()

add_executable(io_uring_test io_uring_test.cpp)
target_link_libraries(io_uring_test ${URING_LIB})

add_executable(uring_register_test uring_register_test.cpp)
target_link_libraries(uring_register_test ${URING_LIB})

option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
endif()

SET(ELOQ_STORE_SOURCE_DIR ${CMAKE_SOURCE_DIR})
SET(ELOQ_STORE_INCLUDE
    ${ELOQ_KV_SOURCE_DIR}
    ${URING_INCLUDE_PATH}
    ${Boost_INCLUDE_DIRS}
    )

set(ELOQ_STORE_SOURCES
    coding.cpp
    data_page_builder.cpp
    comparator.cpp
    index_page_builder.cpp
    mem_index_page.cpp
    index_page_manager.cpp
    task.cpp
    write_task.cpp
    read_task.cpp
    scan_task.cpp
    write_tree_stack.cpp
    async_io_manager.cpp
    data_page.cpp
    page_mapper.cpp
    task_manager.cpp
    eloq_store.cpp
    root_meta.cpp
    replayer.cpp
    crc32.cpp)

add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES})

target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(eloqstore PRIVATE ${URING_LIB} Boost::context glog::glog)

add_executable(io_test io_test.cpp)
target_link_libraries(io_test ${URING_LIB})

add_executable(queue_sync_test queue_sync_test.cpp)

add_executable(cache_q_test cache_q_test.cpp)
# target_compile_options(cache_q_test PRIVATE -fsanitize=address)
# target_link_options(cache_q_test PRIVATE -fsanitize=address)
target_link_libraries (cache_q_test glog::glog)

# add_executable(proxy proxy.cc helpers.cc)
# target_link_libraries(proxy ${URING_LIB})

if (WITH_UNIT_TESTS)
    add_subdirectory(tests)
endif()