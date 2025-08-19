cmake_minimum_required(VERSION 3.21)

project(mdbx_demo LANGUAGES CXX)

# --- C++ Standard ---
# Set the C++ standard to C++23 as required by the design document.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- Vcpkg Integration ---
# This assumes you are configuring the project with the vcpkg toolchain file.
# Example: cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake
message(STATUS "Looking for dependencies...")

# Try to find MDBX - could be system installed or vcpkg
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(MDBX libmdbx)
endif()

# 引入 CPM.cmake
include(cmake/CPM.cmake)

# RocksDB / Boost / OpenSSL 通过 vcpkg.json 管理
# 这里不用手写 find_package，vcpkg 会自动集成

# MDBX 使用 CPM 拉源码编译
CPMAddPackage(
  NAME libmdbx
  GITHUB_REPOSITORY erthink/libmdbx
  VERSION 0.12.8
  OPTIONS 
    "MDBX_BUILD_SHARED_LIBS OFF" 
    "MDBX_BUILD_TOOLS OFF"
    "MDBX_ENABLE_TESTING OFF"
    "MDBX_ENABLE_EXAMPLES OFF"
    "MDBX_ENABLE_PROGRAMS OFF"
)

# 设置 MDBX 为已找到
if(libmdbx_ADDED)
    set(MDBX_FOUND TRUE)
    set(MDBX_LIBRARIES libmdbx)
    set(MDBX_INCLUDE_DIRS ${libmdbx_SOURCE_DIR})
    message(STATUS "MDBX found via CPM: ${libmdbx_SOURCE_DIR}")
    message(STATUS "MDBX build dir: ${libmdbx_BINARY_DIR}")
else()
    message(WARNING "Failed to add MDBX via CPM")
endif()

if(NOT MDBX_FOUND)
    # Try system installation
    find_path(MDBX_INCLUDE_DIR mdbx.h++)
    find_library(MDBX_LIBRARY mdbx-static)
    if(NOT MDBX_LIBRARY)
        find_library(MDBX_LIBRARY mdbx)
    endif()
    if(MDBX_INCLUDE_DIR AND MDBX_LIBRARY)
        set(MDBX_FOUND TRUE)
        set(MDBX_LIBRARIES ${MDBX_LIBRARY})
        set(MDBX_INCLUDE_DIRS ${MDBX_INCLUDE_DIR})
    endif()
endif()

find_package(RocksDB CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
message(STATUS "All dependencies found.")

# --- Core Logic Library ---
# This library contains the database abstractions and query engine,
# shared between the main demo and the benchmark runner.
set(CORE_LOGIC_SOURCES
    src/core/query_engine.cpp
    src/db/rocksdb_impl.cpp
)

if(MDBX_FOUND)
    list(APPEND CORE_LOGIC_SOURCES src/db/mdbx_impl.cpp)
else()
    list(APPEND CORE_LOGIC_SOURCES src/db/mdbx_stub.cpp)
endif()

add_library(core_logic ${CORE_LOGIC_SOURCES})

# Add the 'src' directory to the include path for all targets that link against core_logic.
target_include_directories(core_logic PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

# Link the required libraries to our core_logic library.
target_link_libraries(core_logic PUBLIC
    RocksDB::rocksdb
    fmt::fmt
)

# Add MDBX if found
if(MDBX_FOUND)
    target_include_directories(core_logic PUBLIC ${MDBX_INCLUDE_DIRS})
    target_link_libraries(core_logic PUBLIC mdbx-static)
    target_compile_definitions(core_logic PUBLIC HAVE_MDBX=1)
    message(STATUS "MDBX linked to core_logic")
else()
    message(WARNING "MDBX not found, MDBX functionality will be disabled")
    target_compile_definitions(core_logic PUBLIC HAVE_MDBX=0)
endif()

# --- Main Demo Executable ---
# This is the entry point for the functional PoC.
add_executable(mdbx_demo
    src/main.cpp
)
target_link_libraries(mdbx_demo PRIVATE core_logic)


# --- Benchmark Executable ---
# This is the entry point for running the performance benchmarks.
add_executable(benchmark_runner
    src/benchmark.cpp
)
target_link_libraries(benchmark_runner PRIVATE
    core_logic
    benchmark::benchmark_main # google benchmark provides a main function
)

# --- Test Executables ---
# Endian test
add_executable(test_endian test_endian.cpp)
target_include_directories(test_endian PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(test_endian PRIVATE fmt::fmt)

# RocksDB test
add_executable(test_rocksdb test_rocksdb.cpp)
target_include_directories(test_rocksdb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(test_rocksdb PRIVATE core_logic)

# Simple MDBX test
if(MDBX_FOUND)
    add_executable(test_mdbx_simple test_mdbx_simple.cpp)
    target_include_directories(test_mdbx_simple PRIVATE ${MDBX_INCLUDE_DIRS})
    target_link_libraries(test_mdbx_simple PRIVATE mdbx-static)
    target_compile_definitions(test_mdbx_simple PRIVATE HAVE_MDBX=1)
endif()

# --- Installation Rules (Optional) ---
install(TARGETS mdbx_demo benchmark_runner
    RUNTIME DESTINATION bin
)
install(TARGETS core_logic
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
