# Pinned dependencies, fetched and built in-tree via FetchContent. CI hashes
# this file to key its dependency cache, so every version pin must live here.
#
# Consumers call FetchContent_MakeAvailable(<dep>) (or
# eloqstore_provide_liburing()) for the dependencies they actually use.

# Share one download/build pool across build trees (main build, python wheel,
# rust vendor) via the FETCHCONTENT_BASE_DIR env var. This must run BEFORE
# include(FetchContent), which pins the cache variable to <build>/_deps; FORCE
# so it wins over that default (and over a stale value on reconfigure).
if (DEFINED ENV{FETCHCONTENT_BASE_DIR})
    set(FETCHCONTENT_BASE_DIR "$ENV{FETCHCONTENT_BASE_DIR}" CACHE PATH
        "Shared FetchContent download/build directory" FORCE)
endif ()

include(FetchContent)

# Surface clone/build progress at configure time. FETCHCONTENT_QUIET is ON by
# default and swallows populate output, so GIT_PROGRESS below would be hidden
# without this.
set(FETCHCONTENT_QUIET OFF)

# Dependencies are linked statically into the build products.
# FORCE: aws-sdk-cpp overwrites a plain variable with its own ON cache default.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "build static libs" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "dependency self-tests")

# aws-sdk-cpp: only the core component is used (SigV4 signing, credentials,
# presigned URLs, HTTP utils).
set(BUILD_ONLY "core" CACHE STRING "aws-sdk-cpp components to build")
set(ENABLE_TESTING OFF CACHE BOOL "aws-sdk-cpp tests")
set(FORCE_SHARED_CRT OFF CACHE BOOL "aws-sdk-cpp shared CRT")
# Unused; keeps ZLIB::ZLIB out of the SDK link interface (the rust vendor's
# static-first search would resolve it to the non-PIC system libz.a).
set(ENABLE_ZLIB_REQUEST_COMPRESSION OFF CACHE BOOL "aws-sdk-cpp request compression")
FetchContent_Declare(awssdk
        GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
        GIT_TAG 1.11.446
        GIT_SHALLOW TRUE
        GIT_PROGRESS TRUE)

# glog (eloqdata fork)
set(WITH_GTEST OFF CACHE BOOL "glog tests")
FetchContent_Declare(glog
        GIT_REPOSITORY https://github.com/eloqdata/glog.git
        GIT_TAG 0316d0c5a6e8098dc661c30b6c6795cc538e92de
        GIT_PROGRESS TRUE)

# brpc (eloqdata fork with io_uring support); used by benchmark/ only, which
# seeds its glog/liburing discovery variables before making it available.
FetchContent_Declare(brpc
        GIT_REPOSITORY https://github.com/eloqdata/brpc.git
        GIT_TAG 02c3f2de6a79b3d4828a3f75b04096f06be929c5
        GIT_PROGRESS TRUE)

# Catch2 (tests only)
FetchContent_Declare(Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.3.2
        GIT_SHALLOW TRUE
        GIT_PROGRESS TRUE)

# liburing: newer than the Ubuntu 24.04 package and not a CMake project, so it
# is built once at configure time. Sets URING_INCLUDE_PATH / URING_LIB in the
# caller's scope.
FetchContent_Declare(liburing
        GIT_REPOSITORY https://github.com/axboe/liburing.git
        GIT_TAG liburing-2.6
        GIT_PROGRESS TRUE)
macro (eloqstore_provide_liburing)
    FetchContent_MakeAvailable(liburing)
    if (NOT EXISTS "${liburing_SOURCE_DIR}/src/liburing.a")
        message(STATUS "Building liburing")
        execute_process(
                COMMAND ./configure --cc=${CMAKE_C_COMPILER} --cxx=${CMAKE_CXX_COMPILER}
                WORKING_DIRECTORY ${liburing_SOURCE_DIR}
                RESULT_VARIABLE _liburing_rc)
        if (NOT _liburing_rc EQUAL 0)
            message(FATAL_ERROR "liburing configure failed")
        endif ()
        include(ProcessorCount)
        ProcessorCount(_liburing_nproc)
        execute_process(
                COMMAND make -C src -j${_liburing_nproc}
                WORKING_DIRECTORY ${liburing_SOURCE_DIR}
                RESULT_VARIABLE _liburing_rc)
        if (NOT _liburing_rc EQUAL 0)
            message(FATAL_ERROR "liburing build failed")
        endif ()
    endif ()
    set(URING_INCLUDE_PATH ${liburing_SOURCE_DIR}/src/include)
    set(URING_LIB ${liburing_SOURCE_DIR}/src/liburing.a)
endmacro ()
