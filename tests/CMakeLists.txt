# =============================================================================
# MDBX Demo - Test Suite Configuration
# =============================================================================
# This CMakeLists.txt manages all test executables for the project
# =============================================================================

# --- Unit Tests ---
# These tests focus on individual components/modules

# Endian utility test
add_executable(test_endian unit/test_endian.cpp)
target_include_directories(test_endian PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(test_endian PRIVATE fmt::fmt)

# MDBX simple functionality test
add_executable(test_mdbx_simple unit/test_mdbx_simple.cpp ${CMAKE_SOURCE_DIR}/src/db/mdbx.cpp)
target_include_directories(test_mdbx_simple PRIVATE 
    ${CMAKE_SOURCE_DIR}/src
    ${MDBX_INCLUDE_DIRS}
)
target_link_libraries(test_mdbx_simple PRIVATE mdbx-static)

# RocksDB test (conditional on ENABLE_ROCKSDB)
if(ENABLE_ROCKSDB)
    add_executable(test_rocksdb unit/test_rocksdb.cpp)
    target_include_directories(test_rocksdb PRIVATE ${CMAKE_SOURCE_DIR}/src)
    target_link_libraries(test_rocksdb PRIVATE core_logic)
endif()

# --- Integration Tests ---
# These tests verify end-to-end functionality and system integration

# MDBX comprehensive demand test
add_executable(test_mdbx_demand integration/test_mdbx_demand.cpp ${CMAKE_SOURCE_DIR}/src/db/mdbx.cpp)
target_include_directories(test_mdbx_demand PRIVATE 
    ${CMAKE_SOURCE_DIR}/src
    ${MDBX_INCLUDE_DIRS}
)
target_link_libraries(test_mdbx_demand PRIVATE mdbx-static)

# --- Test Discovery and Organization ---
# Set up test properties for better organization

# Unit tests
set_target_properties(test_endian PROPERTIES FOLDER "Tests/Unit")
set_target_properties(test_mdbx_simple PROPERTIES FOLDER "Tests/Unit")
if(TARGET test_rocksdb)
    set_target_properties(test_rocksdb PROPERTIES FOLDER "Tests/Unit")
endif()

# Integration tests  
set_target_properties(test_mdbx_demand PROPERTIES FOLDER "Tests/Integration")

# --- Test Installation (Optional) ---
# Uncomment if you want test executables installed
# install(TARGETS test_endian test_mdbx_simple test_mdbx_demand
#     RUNTIME DESTINATION bin/tests
# )
# if(TARGET test_rocksdb)
#     install(TARGETS test_rocksdb RUNTIME DESTINATION bin/tests)
# endif()