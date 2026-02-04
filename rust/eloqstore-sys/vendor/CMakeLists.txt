cmake_minimum_required(VERSION 3.20)
project(eloqkvstore)

include(CheckCXXSourceCompiles)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Required for Rust FFI - position independent code
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_FOR_RUST "Whether to build for Rust FFI" OFF)
if (BUILD_FOR_RUST)
    set(WITH_UNIT_TESTS OFF)
    set(WITH_EXAMPLE OFF)
    set(WITH_DB_STRESS OFF)
    set(WITH_BENCHMARK OFF)
    message(STATUS "Building for Rust FFI - disabled unnecessary targets")
    # Default STATIC_ALL_DEPS to ON for Rust FFI if not explicitly set
    if (NOT DEFINED STATIC_ALL_DEPS)
        set(STATIC_ALL_DEPS ON)
    endif()
endif()
option(STATIC_ALL_DEPS "Static link all dependencies into combined .so" OFF)

if (STATIC_ALL_DEPS)
    message(STATUS "Static linking all dependencies into combined library")
    # Prefer static libraries (.a) but allow shared (.so) as fallback
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ".so")
    set(BUILD_SHARED_LIBS OFF)
    # Set linker flags to prefer static linking
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

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

# Handle dependencies based on static linking option
if (STATIC_ALL_DEPS)
    # Try to find static libraries first
    find_library(GLOG_LIBRARY 
        NAMES libglog.a glog
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    if (GLOG_LIBRARY)
        set(glog_FOUND TRUE)
        get_filename_component(GLOG_LIB_DIR ${GLOG_LIBRARY} DIRECTORY)
        find_path(GLOG_INCLUDE_DIR 
            NAMES glog/logging.h
            PATHS /usr/include /usr/local/include
            NO_DEFAULT_PATH
        )
        if (NOT GLOG_INCLUDE_DIR)
            find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h)
        endif()
        # Create imported target if not found via find_package
        if (NOT TARGET glog::glog)
            add_library(glog::glog STATIC IMPORTED)
            set_target_properties(glog::glog PROPERTIES
                IMPORTED_LOCATION ${GLOG_LIBRARY}
                INTERFACE_INCLUDE_DIRECTORIES ${GLOG_INCLUDE_DIR}
            )
        endif()
    else()
        find_package(glog REQUIRED)
    endif()
    
    find_library(JSONCPP_LIBRARY 
        NAMES libjsoncpp.a jsoncpp
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    if (JSONCPP_LIBRARY)
        set(jsoncpp_FOUND TRUE)
        get_filename_component(JSONCPP_LIB_DIR ${JSONCPP_LIBRARY} DIRECTORY)
        find_path(JSONCPP_INCLUDE_DIR 
            NAMES json/json.h
            PATHS /usr/include /usr/local/include
            NO_DEFAULT_PATH
        )
        if (NOT JSONCPP_INCLUDE_DIR)
            find_path(JSONCPP_INCLUDE_DIR NAMES json/json.h)
        endif()
        # Create imported target if not found via find_package
        if (NOT TARGET jsoncpp_lib)
            add_library(jsoncpp_lib STATIC IMPORTED)
            set_target_properties(jsoncpp_lib PROPERTIES
                IMPORTED_LOCATION ${JSONCPP_LIBRARY}
                INTERFACE_INCLUDE_DIRECTORIES ${JSONCPP_INCLUDE_DIR}
            )
        endif()
    else()
        find_package(jsoncpp REQUIRED)
    endif()
    
    find_library(CURL_LIBRARY 
        NAMES libcurl.a curl
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    if (CURL_LIBRARY)
        set(CURL_FOUND TRUE)
        get_filename_component(CURL_LIB_DIR ${CURL_LIBRARY} DIRECTORY)
        find_path(CURL_INCLUDE_DIR 
            NAMES curl/curl.h
            PATHS /usr/include /usr/local/include
            NO_DEFAULT_PATH
        )
    else()
        find_package(CURL REQUIRED)
    endif()
    
    # Find gflags library (required by shard.cpp for DEFINE_uint64)
    # For shared library builds, prefer dynamic library (static may lack PIC)
    if (BUILD_FOR_RUST AND STATIC_ALL_DEPS)
        # Try dynamic library first for shared library builds
        set(CMAKE_FIND_LIBRARY_SUFFIXES_BACKUP ${CMAKE_FIND_LIBRARY_SUFFIXES})
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".so" ".so.2" ".so.2.2" ".so.2.2.2")
        find_library(GFLAGS_DYNAMIC_LIB 
            NAMES gflags libgflags
            PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib
            NO_DEFAULT_PATH
        )
        if (NOT GFLAGS_DYNAMIC_LIB)
            find_library(GFLAGS_DYNAMIC_LIB gflags)
        endif()
        set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_BACKUP})
        
        if (GFLAGS_DYNAMIC_LIB AND EXISTS ${GFLAGS_DYNAMIC_LIB})
            get_filename_component(GFLAGS_EXT ${GFLAGS_DYNAMIC_LIB} EXT)
            if (GFLAGS_EXT MATCHES "\\.so")
                set(GFLAGS_LIBRARY ${GFLAGS_DYNAMIC_LIB})
                set(GFLAGS_USE_DYNAMIC TRUE)
                message(STATUS "Using dynamic gflags library for shared library build: ${GFLAGS_LIBRARY}")
            endif()
        endif()
    endif()
    
    # Fallback to static library if dynamic not found or not building shared library
    if (NOT GFLAGS_USE_DYNAMIC)
        find_library(GFLAGS_LIBRARY 
            NAMES libgflags.a gflags
            PATHS /usr/lib /usr/local/lib
            NO_DEFAULT_PATH
        )
        if (NOT GFLAGS_LIBRARY)
            find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
        endif()
    endif()
    
    if (GFLAGS_LIBRARY)
        set(gflags_FOUND TRUE)
        get_filename_component(GFLAGS_LIB_DIR ${GFLAGS_LIBRARY} DIRECTORY)
        find_path(GFLAGS_INCLUDE_DIR 
            NAMES gflags/gflags.h
            PATHS /usr/include /usr/local/include
            NO_DEFAULT_PATH
        )
        if (NOT GFLAGS_INCLUDE_DIR)
            find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h)
        endif()
    else()
        message(WARNING "gflags not found - shard.cpp uses DEFINE_uint64 which requires gflags")
    endif()
    
    # For shared library, static libs must be compiled with -fPIC
    # Some system static libraries (like zstd) may not have PIC, so we use dynamic libs for those
    if (BUILD_FOR_RUST AND STATIC_ALL_DEPS)
        # When building shared library, prefer dynamic libraries for libraries that may not have PIC
        # Try dynamic library for zstd (common issue - system zstd static lib often lacks PIC)
        # Force find .so files only by temporarily changing suffix
        set(CMAKE_FIND_LIBRARY_SUFFIXES_BACKUP ${CMAKE_FIND_LIBRARY_SUFFIXES})
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".so" ".so.1" ".so.1.5" ".so.1.5.5")
        
        # Try to find dynamic library explicitly
        find_library(ZSTD_DYNAMIC_LIB 
            NAMES zstd libzstd
            PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib
            NO_DEFAULT_PATH
        )
        if (NOT ZSTD_DYNAMIC_LIB)
            find_library(ZSTD_DYNAMIC_LIB zstd)
        endif()
        
        # Restore original suffixes
        set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_BACKUP})
        
        if (ZSTD_DYNAMIC_LIB AND EXISTS ${ZSTD_DYNAMIC_LIB})
            # Verify it's actually a .so file, not .a
            get_filename_component(ZSTD_EXT ${ZSTD_DYNAMIC_LIB} EXT)
            if (ZSTD_EXT MATCHES "\\.so")
                set(ZSTD_LIBRARY ${ZSTD_DYNAMIC_LIB})
                set(ZSTD_USE_DYNAMIC TRUE)
                message(STATUS "Using dynamic zstd library for shared library build: ${ZSTD_LIBRARY}")
            else()
                message(WARNING "Found zstd library but it's not a .so file: ${ZSTD_DYNAMIC_LIB}")
                unset(ZSTD_DYNAMIC_LIB)
            endif()
        endif()
        
        # Fallback to static if dynamic not found
        if (NOT ZSTD_USE_DYNAMIC)
            message(WARNING "Could not find dynamic zstd library, will try static (may fail if no PIC)")
            find_library(ZSTD_LIBRARY 
                NAMES libzstd.a zstd
                PATHS /usr/lib /usr/local/lib
                NO_DEFAULT_PATH
            )
            if (NOT ZSTD_LIBRARY)
                find_library(ZSTD_LIBRARY zstd)
            endif()
        endif()
    else()
        find_library(ZSTD_LIBRARY 
            NAMES libzstd.a zstd
            PATHS /usr/lib /usr/local/lib
            NO_DEFAULT_PATH
        )
        if (NOT ZSTD_LIBRARY)
            find_library(ZSTD_LIBRARY zstd)
        endif()
    endif()
    
    find_path(URING_INCLUDE_PATH NAMES liburing.h
        PATHS /usr/include /usr/local/include
        NO_DEFAULT_PATH
    )
    find_library(URING_LIB 
        NAMES liburing.a uring
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
        find_path(URING_INCLUDE_PATH NAMES liburing.h)
        find_library(URING_LIB NAMES uring)
    endif()
    if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
        message(FATAL_ERROR "Fail to find liburing")
    endif()
    
    # For AWS SDK, we'll need to handle it specially
    # Try to find static libraries
    find_library(AWSSDK_S3_LIB 
        NAMES libaws-cpp-sdk-s3.a aws-cpp-sdk-s3
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    find_library(AWSSDK_CORE_LIB 
        NAMES libaws-cpp-sdk-core.a aws-cpp-sdk-core
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    if (AWSSDK_S3_LIB AND AWSSDK_CORE_LIB)
        set(AWSSDK_FOUND TRUE)
        set(AWSSDK_LINK_LIBRARIES ${AWSSDK_S3_LIB} ${AWSSDK_CORE_LIB})
        # Add other AWS SDK dependencies
        find_library(AWS_C_EVENT_STREAM_LIB aws-c-event-stream PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        find_library(AWS_CHECKSUMS_LIB aws-checksums PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        find_library(AWS_C_AUTH_LIB aws-c-auth PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        find_library(AWS_C_CAL_LIB aws-c-cal PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        find_library(AWS_C_COMMON_LIB aws-c-common PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        if (AWS_C_EVENT_STREAM_LIB)
            list(APPEND AWSSDK_LINK_LIBRARIES ${AWS_C_EVENT_STREAM_LIB})
        endif()
        if (AWS_CHECKSUMS_LIB)
            list(APPEND AWSSDK_LINK_LIBRARIES ${AWS_CHECKSUMS_LIB})
        endif()
        if (AWS_C_AUTH_LIB)
            list(APPEND AWSSDK_LINK_LIBRARIES ${AWS_C_AUTH_LIB})
        endif()
        if (AWS_C_CAL_LIB)
            list(APPEND AWSSDK_LINK_LIBRARIES ${AWS_C_CAL_LIB})
        endif()
        if (AWS_C_COMMON_LIB)
            list(APPEND AWSSDK_LINK_LIBRARIES ${AWS_C_COMMON_LIB})
        endif()
    else()
        find_package(AWSSDK REQUIRED COMPONENTS s3)
    endif()
    
    # Boost context
    find_library(BOOST_CONTEXT_LIBRARY 
        NAMES libboost_context.a boost_context
        PATHS /usr/lib /usr/local/lib
        NO_DEFAULT_PATH
    )
    if (NOT BOOST_CONTEXT_LIBRARY)
        find_library(BOOST_CONTEXT_LIBRARY NAMES boost_context)
    endif()
    if (BOOST_CONTEXT_LIBRARY)
        set(BOOST_CONTEXT_TARGET ${BOOST_CONTEXT_LIBRARY})
    endif()
else()
    find_package(glog REQUIRED)
    find_package(jsoncpp REQUIRED)
    find_package(CURL REQUIRED)
    find_library(ZSTD_LIBRARY zstd)
    find_package(AWSSDK REQUIRED COMPONENTS s3)
    
    find_path(URING_INCLUDE_PATH NAMES liburing.h)
    find_library(URING_LIB NAMES uring)
    if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
        message(FATAL_ERROR "Fail to find liburing")
    endif()
endif()

# vendor/{src,include,external} are soft-linked to repository root, no additional configuration needed
# Skip git submodule update for Rust FFI build - build.rs runs it from repo root
if (NOT BUILD_FOR_RUST)
    find_package(Git QUIET)
    if (GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if (NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif ()
    endif ()
endif ()

if (NOT EXISTS "${PROJECT_SOURCE_DIR}/external/concurrentqueue/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! Run: git submodule update --init --recursive")
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
        ${PROJECT_SOURCE_DIR}/ffi/include
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
        ffi/src/eloqstore_capi.cpp
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

file(GLOB ABSL_LIBS "${PROJECT_SOURCE_DIR}/lib/abseil/*.a")
foreach(lib ${ABSL_LIBS})
    get_filename_component(lib_name ${lib} NAME_WE)
    string(REPLACE "lib" "" lib_name ${lib_name})
    list(APPEND ABSL_LIB_NAMES ${lib_name})
endforeach()

# Build as SHARED library for Rust FFI when static linking is enabled
if (BUILD_FOR_RUST AND STATIC_ALL_DEPS)
    add_library(eloqstore_combine SHARED ${ELOQ_STORE_SOURCES} ${INI_SOURCES})
    set_target_properties(eloqstore_combine PROPERTIES
        OUTPUT_NAME "eloqstore_combine"
        POSITION_INDEPENDENT_CODE ON
    )
    
    # Set linker flags to statically link dependencies into the shared library
    # Use -Wl,-Bstatic to link static libraries, then -Wl,-Bdynamic for system libs
    target_link_options(eloqstore_combine PRIVATE
        -Wl,-Bstatic
    )
    
    target_include_directories(eloqstore_combine PUBLIC ${ELOQ_STORE_INCLUDE})
    
    # Link all dependencies statically into the shared library
    # Use imported targets if available, otherwise use library paths
    if (TARGET glog::glog)
        target_link_libraries(eloqstore_combine PRIVATE glog::glog)
    elseif (GLOG_LIBRARY)
        target_link_libraries(eloqstore_combine PRIVATE ${GLOG_LIBRARY})
        if (GLOG_INCLUDE_DIR)
            target_include_directories(eloqstore_combine PRIVATE ${GLOG_INCLUDE_DIR})
        endif()
    else()
        target_link_libraries(eloqstore_combine PRIVATE glog::glog)
    endif()
    
    if (TARGET jsoncpp_lib)
        target_link_libraries(eloqstore_combine PRIVATE jsoncpp_lib)
    elseif (JSONCPP_LIBRARY)
        target_link_libraries(eloqstore_combine PRIVATE ${JSONCPP_LIBRARY})
        if (JSONCPP_INCLUDE_DIR)
            target_include_directories(eloqstore_combine PRIVATE ${JSONCPP_INCLUDE_DIR})
        endif()
    else()
        target_link_libraries(eloqstore_combine PRIVATE jsoncpp_lib)
    endif()
    
    if (CURL_LIBRARY)
        target_link_libraries(eloqstore_combine PRIVATE ${CURL_LIBRARY})
        if (CURL_INCLUDE_DIR)
            target_include_directories(eloqstore_combine PRIVATE ${CURL_INCLUDE_DIR})
        endif()
        # Link curl dependencies
        find_library(SSL_LIB ssl PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        find_library(CRYPTO_LIB crypto PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH)
        if (SSL_LIB)
            target_link_libraries(eloqstore_combine PRIVATE ${SSL_LIB})
        endif()
        if (CRYPTO_LIB)
            target_link_libraries(eloqstore_combine PRIVATE ${CRYPTO_LIB})
        endif()
    else()
        target_link_libraries(eloqstore_combine PRIVATE ${CURL_LIBRARIES})
    endif()
    
    # Link zstd and gflags with dynamic linking if needed (if static libs lack PIC)
    # This must be done before other static libraries to avoid PIC issues
    if (ZSTD_USE_DYNAMIC OR GFLAGS_USE_DYNAMIC)
        message(STATUS "Linking some libraries dynamically due to PIC requirements")
        # Ensure we're in dynamic linking mode
        target_link_options(eloqstore_combine PRIVATE
            -Wl,-Bdynamic
        )
        if (ZSTD_USE_DYNAMIC)
            target_link_libraries(eloqstore_combine PRIVATE ${ZSTD_LIBRARY})
        endif()
        if (GFLAGS_USE_DYNAMIC)
            target_link_libraries(eloqstore_combine PRIVATE ${GFLAGS_LIBRARY})
            if (GFLAGS_INCLUDE_DIR)
                target_include_directories(eloqstore_combine PRIVATE ${GFLAGS_INCLUDE_DIR})
            endif()
        endif()
        # Switch back to static linking for other libraries
        target_link_options(eloqstore_combine PRIVATE
            -Wl,-Bstatic
        )
    endif()
    
    # Link all abseil libraries that eloqstore depends on
    # Link main abseil library - CMake will handle transitive dependencies
    target_link_libraries(eloqstore_combine PRIVATE
        ${URING_LIB}
        absl::flat_hash_map
        ${BOOST_CONTEXT_TARGET}
        ${AWSSDK_LINK_LIBRARIES}
        Threads::Threads
    )
    
    # Link zstd statically if not using dynamic
    if (NOT ZSTD_USE_DYNAMIC)
        target_link_libraries(eloqstore_combine PRIVATE ${ZSTD_LIBRARY})
    endif()
    
    # Link gflags statically if not using dynamic (required by shard.cpp for DEFINE_uint64)
    if (GFLAGS_LIBRARY AND NOT GFLAGS_USE_DYNAMIC)
        target_link_libraries(eloqstore_combine PRIVATE ${GFLAGS_LIBRARY})
        if (GFLAGS_INCLUDE_DIR)
            target_include_directories(eloqstore_combine PRIVATE ${GFLAGS_INCLUDE_DIR})
        endif()
    endif()
    
    # Switch back to dynamic linking for system libraries (pthread, dl, etc.)
    target_link_options(eloqstore_combine PRIVATE
        -Wl,-Bdynamic
    )
    
    # Also create the static library for compatibility
    add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES} ${INI_SOURCES})
    target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
    # Use imported targets if available
    if (TARGET glog::glog)
        set(GLOG_TARGET glog::glog)
    elseif (GLOG_LIBRARY)
        set(GLOG_TARGET ${GLOG_LIBRARY})
    else()
        set(GLOG_TARGET glog::glog)
    endif()
    if (TARGET jsoncpp_lib)
        set(JSONCPP_TARGET jsoncpp_lib)
    elseif (JSONCPP_LIBRARY)
        set(JSONCPP_TARGET ${JSONCPP_LIBRARY})
    else()
        set(JSONCPP_TARGET jsoncpp_lib)
    endif()
    target_link_libraries(eloqstore PRIVATE
        ${URING_LIB}
        ${GLOG_TARGET}
        absl::flat_hash_map
        ${BOOST_CONTEXT_TARGET}
        ${CURL_LIBRARIES}
        ${JSONCPP_TARGET}
        ${ZSTD_LIBRARY}
        ${AWSSDK_LINK_LIBRARIES}
    )
else()
    add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES} ${INI_SOURCES})
    target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
    target_link_libraries(eloqstore PRIVATE
        ${URING_LIB}
        glog::glog
        absl::flat_hash_map
        ${BOOST_CONTEXT_TARGET}
        ${CURL_LIBRARIES}
        jsoncpp_lib
        ${ZSTD_LIBRARY}
        ${AWSSDK_LINK_LIBRARIES}
    )
endif()

# For Rust FFI: link all abseil libraries that eloqstore depends on
# This is necessary because Rust's static linker cannot resolve transitive dependencies
set(ABSL_LIBS_FOR_RUST
    absl::base
    absl::int128
    absl::city
    absl::hash
    absl::raw_hash_set
    absl::container_internal
    absl::strings
    absl::cord
    absl::cord_internal
    absl::cordz_handle
    absl::cordz_info
    absl::cordz_functions
    absl::cordz_sample_token
    absl::str_format_internal
    absl::time
    absl::civil_time
    absl::time_zone
    absl::status
    absl::statusor
    absl::synchronization
    absl::graphcycles_internal
    absl::kernel_timeout_internal
    absl::random_random
    absl::random_internal_randen
    absl::random_internal_randen_slow
    absl::random_internal_randen_hwaes
    absl::random_internal_randen_hwaes_impl
    absl::random_internal_seed_material
    absl::random_internal_entropy_pool
    absl::random_seed_sequences
    absl::random_distributions
    absl::log_internal
    absl::log_severity
    absl::log_flags
    absl::log_globals
    absl::log_internal_message
    absl::log_internal_check_op
    absl::log_internal_format
    absl::log_internal_proto
    absl::log_internal_structured_proto
    absl::log_internal_fnmatch
    absl::log_internal_log_sink_set
    absl::log_internal_conditions
    absl::log_internal_nullguard
    absl::log_internal_globals
    absl::log_entry
    absl::log_initialize
    absl::log_sink
    absl::flags_internal
    absl::flags_parse
    absl::flags_program_name
    absl::flags_usage
    absl::flags_usage_internal
    absl::flags_commandlineflag
    absl::flags_config
    absl::flags_marshalling
    absl::flags_reflection
    absl::crc32c
    absl::crc_cpu_detect
    absl::crc_internal
    absl::crc_cord_state
    absl::stacktrace
    absl::symbolize
    absl::debugging_internal
    absl::demangle_internal
    absl::examine_stack
    absl::failure_signal_handler
    absl::die_if_null
    absl::throw_delegate
    absl::raw_logging_internal
    absl::malloc_internal
    absl::spinlock_wait
    absl::strerror
    absl::span
    absl::config
    absl::core_headers
    absl::utility
    absl::type_traits
    absl::hash_internal
    absl::hash_function_defaults
    absl::hashtablez_sampler
    absl::hashtable_debug
    absl::scoped_set_env
    absl::leak_check
    absl::dynamic_annotations
    absl::pretty_function
    absl::optional
    absl::variant
    absl::inlined_vector
    absl::fixed_array
    absl::cleanup
    absl::btree_container
    absl::memory
    absl::meta
    absl::algorithm
    absl::numeric
    absl::functional
    absl::any_invocable
    absl::function_ref
    absl::bind_front
    absl::overload
    absl::low_level_alloc
    absl::exponential_biased
    absl::periodic_sampler
    absl::profile_builder
    absl::hashtable_profiler
    absl::tracing_internal
    absl::vlog_config_internal
    absl::utf8_for_code_point
    absl::borrowed_fixup_buffer
    absl::demangle_rust
    absl::decode_rust_punycode
    absl::generic_printer_internal
    absl::flags_commandlineflag_internal
    absl::flags_private_handle_accessor
    absl::strings_internal
    absl::cord_data_edge
    absl::cord_rep_btree
    absl::cord_rep_flat
    absl::cord_rep_consume
    absl::low_level_scheduling
    absl::thread_identity
    absl::thread_annotations
)

# Create a list of library names for Rust to link (strip absl:: prefix)
foreach(lib ${ABSL_LIBS_FOR_RUST})
    string(REPLACE "absl::" "" lib_name ${lib})
    list(APPEND ABSL_LIB_NAMES ${lib_name})
endforeach()
