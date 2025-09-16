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

# 引入 CPM.cmake
include(cmake/CPM.cmake)

# 配置CPM缓存以避免重复下载和编译
set(CPM_SOURCE_CACHE ${CMAKE_CURRENT_SOURCE_DIR}/.cpm_cache)

# MDBX 是默认必需的依赖，使用 CPM 拉源码编译
message(STATUS "Setting up MDBX (required dependency)...")
CPMAddPackage(
  NAME libmdbx
  GITHUB_REPOSITORY erthink/libmdbx
  VERSION 0.14.1
  OPTIONS 
    "MDBX_BUILD_SHARED_LIBS OFF" 
    "MDBX_BUILD_TOOLS OFF"
    "MDBX_ENABLE_TESTING OFF"
    "MDBX_ENABLE_EXAMPLES OFF"
    "MDBX_ENABLE_PROGRAMS OFF"
    "MDBX_ALLOY_BUILD ON"
    "MDBX_INSTALL_STATIC OFF"
    "MDBX_TXN_CHECKOWNER ON"
    "MDBX_ENV_CHECKPID ON"
    "MDBX_DISABLE_GNU_SOURCE OFF"
)

# 确保 MDBX 添加成功
if(libmdbx_ADDED)
    set(MDBX_FOUND TRUE)
    set(MDBX_LIBRARIES libmdbx)
    set(MDBX_INCLUDE_DIRS ${libmdbx_SOURCE_DIR})
    message(STATUS "MDBX found via CPM: ${libmdbx_SOURCE_DIR}")
    message(STATUS "MDBX build dir: ${libmdbx_BINARY_DIR}")
else()
    message(FATAL_ERROR "Failed to add MDBX via CPM")
endif()

# RocksDB 是可选依赖
option(ENABLE_ROCKSDB "Enable RocksDB support" OFF)
if(ENABLE_ROCKSDB)
    message(STATUS "RocksDB support requested - checking availability...")
    
    # 首先尝试通过 vcpkg 查找 RocksDB
    find_package(RocksDB CONFIG QUIET)
    
    if(RocksDB_FOUND)
        message(STATUS "RocksDB found via vcpkg: ${RocksDB_DIR}")
        message(STATUS "RocksDB support enabled")
    else()
        message(STATUS "RocksDB not found via vcpkg. Installing via vcpkg...")
        
        # 使用 vcpkg 安装 RocksDB
        execute_process(
            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/third_party/vcpkg/vcpkg install rocksdb
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE VCPKG_INSTALL_RESULT
        )
        
        if(VCPKG_INSTALL_RESULT EQUAL 0)
            message(STATUS "RocksDB installed successfully via vcpkg")
            # 再次查找
            find_package(RocksDB CONFIG REQUIRED)
            message(STATUS "RocksDB found after installation: ${RocksDB_DIR}")
        else()
            message(FATAL_ERROR "Failed to install RocksDB via vcpkg. Please install it manually: ./third_party/vcpkg/vcpkg install rocksdb")
        endif()
    endif()
else()
    message(STATUS "RocksDB support disabled")
endif()

# 其他必需依赖
find_package(benchmark CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(jsoncpp CONFIG REQUIRED)
message(STATUS "All dependencies found.")

# --- Core Logic Library ---
# This library contains the database abstractions and query engine,
# shared between the main demo and the benchmark runner.
set(CORE_LOGIC_SOURCES
    src/core/query_engine.cpp
    src/db/mdbx_impl.cpp
    src/db/mdbx.cpp
)

if(ENABLE_ROCKSDB)
    list(APPEND CORE_LOGIC_SOURCES src/db/rocksdb_impl.cpp)
else()
    list(APPEND CORE_LOGIC_SOURCES src/db/rocksdb_stub.cpp)
endif()

add_library(core_logic ${CORE_LOGIC_SOURCES})

# Add the 'src' directory to the include path for all targets that link against core_logic.
target_include_directories(core_logic PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

# Link the required libraries to our core_logic library.
target_link_libraries(core_logic PUBLIC
    fmt::fmt
)

# MDBX 是必需的，总是链接
target_include_directories(core_logic PUBLIC ${MDBX_INCLUDE_DIRS})
target_link_libraries(core_logic PUBLIC mdbx-static)
message(STATUS "MDBX linked to core_logic")

# 条件链接 RocksDB
if(ENABLE_ROCKSDB)
    target_link_libraries(core_logic PUBLIC RocksDB::rocksdb)
    target_compile_definitions(core_logic PUBLIC HAVE_ROCKSDB=1)
    message(STATUS "RocksDB linked to core_logic")
else()
    target_compile_definitions(core_logic PUBLIC HAVE_ROCKSDB=0)
    message(STATUS "RocksDB support disabled in core_logic")
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

# --- MDBX Bench Executable ---
# This is the MDBX performance benchmark tool
add_executable(mdbx_bench
    src/mdbx_bench.cpp
    src/utils/string_utils.cpp
)
target_link_libraries(mdbx_bench PRIVATE
    core_logic
    JsonCpp::JsonCpp
)

# --- RocksDB Bench Executable ---
# This is the RocksDB performance benchmark tool (only build if RocksDB is enabled)
if(ENABLE_ROCKSDB)
    add_executable(rocksdb_bench
        src/rocksdb_bench.cpp
        src/utils/string_utils.cpp
    )
    target_link_libraries(rocksdb_bench PRIVATE
        core_logic
        JsonCpp::JsonCpp
        RocksDB::rocksdb
    )
endif()

# --- Test Suite ---
# All test configurations are now managed in tests/CMakeLists.txt
# This provides better organization and separation of concerns
add_subdirectory(tests)

# --- Installation Rules (Optional) ---
set(INSTALL_TARGETS mdbx_demo benchmark_runner mdbx_bench)
if(ENABLE_ROCKSDB)
    list(APPEND INSTALL_TARGETS rocksdb_bench)
endif()

install(TARGETS ${INSTALL_TARGETS}
    RUNTIME DESTINATION bin
)
install(TARGETS core_logic
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
