# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.16)
project(kvrocks
        DESCRIPTION "NoSQL which is based on RocksDB and compatible with the Redis protocol"
        LANGUAGES C CXX)

option(DISABLE_JEMALLOC "disable use of the jemalloc library" OFF)
option(ENABLE_ASAN "enable address sanitizer" OFF)
option(ENABLE_TSAN "enable thread sanitizer" OFF)
option(ENABLE_UBSAN "enable undefined behavior sanitizer" OFF)
option(ASAN_WITH_LSAN "enable leak sanitizer while address sanitizer is enabled" ON)
option(ENABLE_STATIC_LIBSTDCXX "link kvrocks with static library of libstd++ instead of shared library" ON)
option(ENABLE_LUAJIT "enable use of luaJIT instead of lua" ON)
option(ENABLE_OPENSSL "enable openssl to support tls connection" OFF)
option(ENABLE_IPO "enable interprocedural optimization" ON)
set(SYMBOLIZE_BACKEND "" CACHE STRING "symbolization backend library for cpptrace (libbacktrace, libdwarf, or empty)")
set(PORTABLE 0 CACHE STRING "build a portable binary (disable arch-specific optimizations)")
# TODO: set ENABLE_NEW_ENCODING to ON when we are ready
option(ENABLE_NEW_ENCODING "enable new encoding (#1033) for storing 64bit size and expire time in milliseconds" ON)

if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
        message(FATAL_ERROR "It is expected to build kvrocks with GCC 8 or above")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
        message(FATAL_ERROR "It is expected to build kvrocks with Clang 8 or above")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
        message(FATAL_ERROR "It is expected to build kvrocks with Xcode toolchains 11 or above")
    endif()
else()
    message(WARNING "The compiler you are currently using is not officially supported,
        so you can try switching to GCC>=8 or Clang>=8 if you encounter problems")
endif()

if(CMAKE_GENERATOR STREQUAL "Ninja")
    set(MAKE_COMMAND make)
    set(NINJA_MAKE_JOBS 4 CACHE STRING "specify concurrent level while ninja calling make")
    set(NINJA_MAKE_JOBS_FLAG -j${NINJA_MAKE_JOBS})
else()
    set(MAKE_COMMAND $(MAKE))
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(DEPS_FETCH_PROXY "" CACHE STRING
    "a template URL to proxy the traffic for fetching dependencies, e.g. with DEPS_FETCH_PROXY = https://some-proxy/,
     https://example/some-dep.zip -> https://some-proxy/https://example/some-dep.zip")

if(ENABLE_ASAN AND ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSan cannot be used at the same time")
endif()

if((ENABLE_ASAN OR ENABLE_TSAN) AND (NOT DISABLE_JEMALLOC))
    message(FATAL_ERROR "ASan/TSan does not work well with JeMalloc")
endif()

if(ENABLE_ASAN)
    if(ASAN_WITH_LSAN)
        if((CMAKE_CXX_COMPILER_ID STREQUAL "GNU") AND (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5"))
            message(FATAL_ERROR "leak sanitizer is not supported until gcc 5")
        endif()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=leak")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=leak")
    endif()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

# Copied from https://github.com/apache/arrow/blob/main/cpp/cmake_modules/san-config.cmake
#
# Flag to enable clang undefined behavior sanitizer
# We explicitly don't enable all of the sanitizer flags:
# - disable 'vptr' because of RTTI issues across shared libraries (?)
# - disable 'alignment' because unaligned access is really OK on Nehalem and we do it
#   all over the place.
# - disable 'function' because it appears to give a false positive
#   (https://github.com/google/sanitizers/issues/911)
# - disable 'float-divide-by-zero' on clang, which considers it UB
#   (https://bugs.llvm.org/show_bug.cgi?id=17000#c1)
#   Note: GCC does not support the 'function' flag.
if(ENABLE_UBSAN)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr,function,float-divide-by-zero")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr,function,float-divide-by-zero")
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "5.1")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr")
        set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr")
    else()
        message(FATAL_ERROR "Cannot use UBSAN without clang or gcc >= 5.1")
    endif()
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-sanitize-recover=all")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-sanitize-recover=all")
endif()
if(ENABLE_TSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# GLIBC < 2.17 should explicitly specify the real-time library when using clock_*
find_library(REALTIME_LIB rt)
if (REALTIME_LIB)
    list(APPEND EXTERNAL_LIBS PRIVATE rt)
endif()

if (CMAKE_HOST_APPLE)
    set(DISABLE_JEMALLOC ON)
    set(ENABLE_IPO OFF)
endif ()

if(NOT DISABLE_JEMALLOC)
    include(cmake/jemalloc.cmake)
    list(APPEND EXTERNAL_LIBS PRIVATE jemalloc)
endif()

set(BUILD_SHARED_LIBS OFF CACHE BOOL "do not build shared libs by default")

if(ENABLE_OPENSSL)
    find_package(OpenSSL REQUIRED)
endif()

include(cmake/gtest.cmake)
include(cmake/glog.cmake)
include(cmake/snappy.cmake)
include(cmake/lz4.cmake)
include(cmake/zlib.cmake)
include(cmake/zstd.cmake)
include(cmake/tbb.cmake)
include(cmake/rocksdb.cmake)
include(cmake/libevent.cmake)
include(cmake/fmt.cmake)
include(cmake/jsoncons.cmake)
include(cmake/xxhash.cmake)
include(cmake/span.cmake)
include(cmake/trie.cmake)
include(cmake/pegtl.cmake)
include(cmake/rangev3.cmake)
include(cmake/cpptrace.cmake)

if (ENABLE_LUAJIT)
    include(cmake/luajit.cmake)
else()
    include(cmake/lua.cmake)
endif()

find_package(Threads REQUIRED)

list(APPEND EXTERNAL_LIBS glog)
list(APPEND EXTERNAL_LIBS snappy)
list(APPEND EXTERNAL_LIBS rocksdb_with_headers)
list(APPEND EXTERNAL_LIBS event_with_headers)
list(APPEND EXTERNAL_LIBS lz4)
list(APPEND EXTERNAL_LIBS zstd)
list(APPEND EXTERNAL_LIBS zlib_with_headers)
list(APPEND EXTERNAL_LIBS fmt)
if (ENABLE_LUAJIT)
    list(APPEND EXTERNAL_LIBS luajit)
else()
    list(APPEND EXTERNAL_LIBS lua)
endif()
if (ENABLE_OPENSSL)
    list(APPEND EXTERNAL_LIBS OpenSSL::SSL)
endif()
list(APPEND EXTERNAL_LIBS tbb)
list(APPEND EXTERNAL_LIBS jsoncons)
list(APPEND EXTERNAL_LIBS Threads::Threads)
list(APPEND EXTERNAL_LIBS ${Backtrace_LIBRARY})
list(APPEND EXTERNAL_LIBS xxhash)
list(APPEND EXTERNAL_LIBS span-lite)
list(APPEND EXTERNAL_LIBS tsl_hat_trie)
list(APPEND EXTERNAL_LIBS pegtl)
list(APPEND EXTERNAL_LIBS range-v3)
list(APPEND EXTERNAL_LIBS cpptrace::cpptrace)

# Add git sha to version.h
find_package(Git REQUIRED)
execute_process(COMMAND sh -c "cat src/VERSION.txt"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE PROJECT_VERSION)
execute_process(COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE GIT_SHA)
string(STRIP "${GIT_SHA}" GIT_SHA)
if ((PROJECT_VERSION STREQUAL "unstable") AND (GIT_SHA STREQUAL ""))
	message(WARNING "It is highly recommended to build the unstable branch in a Git repo")
endif ()
configure_file(src/version.h.in ${PROJECT_BINARY_DIR}/version.h)

if ((CMAKE_CXX_COMPILER_ID STREQUAL "GNU") OR (CMAKE_CXX_COMPILER_ID STREQUAL "Clang"))
    if (NOT APPLE)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc")
    endif()

    if(ENABLE_STATIC_LIBSTDCXX)
        try_compile(FOUND_STATIC_LIBSTDCXX ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/cmake/checks/static_libstdcxx.cc
            LINK_OPTIONS -static-libstdc++ CXX_STANDARD 11)

        if(NOT FOUND_STATIC_LIBSTDCXX)
            message(FATAL_ERROR "cannot find static library of libstdc++, please add ENABLE_STATIC_LIBSTDCXX=OFF to disable")
        endif()

        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
    endif()
endif()

# kvrocks objects target
file(GLOB_RECURSE KVROCKS_SRCS src/*.cc)
list(FILTER KVROCKS_SRCS EXCLUDE REGEX src/cli/main.cc)

add_library(kvrocks_objs OBJECT ${KVROCKS_SRCS})

target_include_directories(kvrocks_objs PUBLIC src src/common src/vendor ${PROJECT_BINARY_DIR} ${Backtrace_INCLUDE_DIR})
target_compile_features(kvrocks_objs PUBLIC cxx_std_17)
target_compile_options(kvrocks_objs PUBLIC -Wall -Wpedantic -Wsign-compare -Wreturn-type -fno-omit-frame-pointer)
target_compile_options(kvrocks_objs PUBLIC -Werror=unused-parameter)
target_compile_options(kvrocks_objs PUBLIC -Werror=unused-result)

# disable unused-variable check on GCC < 8 due to the structure bindings
# https://gcc.gnu.org/bugzilla/show_bug.cgi?format=multiple&id=81767
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
    target_compile_options(kvrocks_objs PUBLIC -Wno-error=unused-variable)
else()
    target_compile_options(kvrocks_objs PUBLIC -Werror=unused-variable)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(kvrocks_objs PUBLIC -Wno-pedantic)
elseif((CMAKE_CXX_COMPILER_ID STREQUAL "Clang") OR (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"))
    target_compile_options(kvrocks_objs PUBLIC -Wno-gnu-statement-expression)
endif()
target_link_libraries(kvrocks_objs PUBLIC -fno-omit-frame-pointer)
target_link_libraries(kvrocks_objs PUBLIC ${EXTERNAL_LIBS})
target_compile_definitions(kvrocks_objs PUBLIC KVROCKS_STORAGE_ENGINE=RocksDB)
if(ENABLE_OPENSSL)
    target_compile_definitions(kvrocks_objs PUBLIC ENABLE_OPENSSL)
endif()
if(ENABLE_NEW_ENCODING)
    target_compile_definitions(kvrocks_objs PUBLIC METADATA_ENCODING_VERSION=1)
else()
    target_compile_definitions(kvrocks_objs PUBLIC METADATA_ENCODING_VERSION=0)
endif()

# disable LTO on GCC <= 9 due to an ICE
if((CMAKE_CXX_COMPILER_ID STREQUAL "GNU") AND (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 10))
    set(ENABLE_IPO OFF)
endif()

if(ENABLE_IPO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT ipo_result OUTPUT ipo_output LANGUAGES CXX)

    if(ipo_result)
        set_property(TARGET kvrocks_objs PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
        if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            target_link_libraries(kvrocks_objs PUBLIC "-fuse-ld=lld")
        endif()
    else()
        message(WARNING "IPO is not supported: ${ipo_output}")
    endif()
endif()

# kvrocks main target
add_executable(kvrocks src/cli/main.cc)
target_link_libraries(kvrocks PRIVATE kvrocks_objs ${EXTERNAL_LIBS})

# kvrocks2redis sync tool
file(GLOB KVROCKS2REDIS_SRCS utils/kvrocks2redis/*.cc)
add_executable(kvrocks2redis ${KVROCKS2REDIS_SRCS})

target_link_libraries(kvrocks2redis PRIVATE kvrocks_objs ${EXTERNAL_LIBS})

# kvrocks unit tests
file(GLOB_RECURSE TESTS_SRCS tests/cppunit/*.cc)
add_executable(unittest ${TESTS_SRCS})
target_include_directories(unittest PRIVATE tests/cppunit)

target_link_libraries(unittest PRIVATE kvrocks_objs gtest_main gmock ${EXTERNAL_LIBS})
