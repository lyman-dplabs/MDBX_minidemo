#!/bin/bash

# =============================================================================
# MDBX Blockchain State Read Accelerator - Build & Run Script
# =============================================================================
# This script automates the build process and execution of the demo project.
# It supports conditional compilation with/without MDBX and provides options
# to run different components (demo, benchmark, tests).
# =============================================================================

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# =============================================================================
# Color and Logging Configuration
# =============================================================================

# ANSI color codes
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly PURPLE='\033[0;35m'
readonly CYAN='\033[0;36m'
readonly WHITE='\033[1;37m'
readonly BOLD='\033[1m'
readonly NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${CYAN}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_step() {
    echo -e "\n${PURPLE}▶${NC} ${BOLD}$*${NC}"
}

log_substep() {
    echo -e "  ${BLUE}→${NC} $*"
}

# =============================================================================
# Configuration Variables
# =============================================================================

# Project paths
readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly BUILD_DIR="${PROJECT_ROOT}/build"
readonly VCPKG_DIR="${PROJECT_ROOT}/third_party/vcpkg"

# Build configuration
ENABLE_ROCKSDB=false
BUILD_TYPE="Debug"
CLEAN_BUILD=false
RUN_DEMO=false
RUN_BENCHMARK=false
RUN_TESTS=false
RUN_MDBX_DEMAND_TEST=false
VERBOSE=false
PARALLEL_JOBS=$(nproc)

# Benchmark configuration
BENCHMARK_FILTER="*"
BENCHMARK_TIME_UNIT="ms"
BENCHMARK_MIN_TIME="0.1s"
BENCHMARK_FORMAT="console"

# Benchmark data configuration (can be overridden by environment variables or command line)
BENCH_NUM_ACCOUNTS="10"
BENCH_NUM_BLOCKS_PER_ACCOUNT="100"
BENCH_MAX_BLOCK_NUMBER="10000"

# =============================================================================
# Help Function
# =============================================================================

show_help() {
    echo -e "${BOLD}MDBX Blockchain State Read Accelerator - Build & Run Script${NC}"
    echo
    echo -e "${BOLD}USAGE:${NC}"
    echo -e "    $0 [OPTIONS]"
    echo
    echo -e "${BOLD}BUILD OPTIONS:${NC}"
    echo -e "    --rocksdb           Enable RocksDB support (optional)"
    echo -e "    --release           Build in Release mode (default: Debug)"
    echo -e "    --clean             Clean build directory before building"
    echo -e "    --jobs N            Number of parallel build jobs (default: $(nproc))"
    echo -e "    --verbose           Enable verbose build output"
    echo
    echo -e "${BOLD}RUN OPTIONS:${NC}"
    echo -e "    --demo              Run the main demo application"
    echo -e "    --benchmark         Run performance benchmarks"
    echo -e "    --tests             Run unit tests (if available)"
    echo -e "    --test-demand       Run MDBX demand requirements test"
    echo
    echo -e "${BOLD}BENCHMARK OPTIONS:${NC}"
    echo -e "    --filter PATTERN    Benchmark filter pattern (default: \"*\")"
    echo -e "    --time-unit UNIT    Time unit: ns, us, ms, s (default: ms)"
    echo -e "    --min-time TIME     Minimum time per benchmark (default: 0.1s)"
    echo -e "    --format FORMAT     Output format: console, json, csv (default: console)"
    echo
    echo -e "${BOLD}BENCHMARK DATA OPTIONS:${NC}"
    echo -e "    --accounts NUM      Number of accounts for benchmark (default: 10)"
    echo -e "    --blocks NUM        Number of blocks per account (default: 100)"
    echo -e "    --max-block NUM     Maximum block number (default: 10000)"
    echo
    echo -e "${BOLD}EXAMPLES:${NC}"
    echo -e "    # Basic build and run demo with MDBX only (default)"
    echo -e "    $0 --demo"
    echo
    echo -e "    # Build with RocksDB support and run benchmarks"
    echo -e "    $0 --rocksdb --benchmark"
    echo
    echo -e "    # Clean release build and run specific benchmarks"
    echo -e "    $0 --clean --release --benchmark --filter \"MDBX*\""
    echo
    echo -e "    # Run benchmark with custom data size"
    echo -e "    $0 --benchmark --accounts 50 --blocks 200 --max-block 50000"
    echo
    echo -e "    # Build with RocksDB and run everything"
    echo -e "    $0 --rocksdb --demo --benchmark --tests"
    echo
    echo -e "${BOLD}REQUIREMENTS:${NC}"
    echo -e "    - CMake 3.21+"
    echo -e "    - C++23 compatible compiler (GCC 12+, Clang 14+)"
    echo -e "    - vcpkg (automatically set up as submodule)"
    echo -e "    - MDBX (automatically downloaded and built via CPM)"
    echo
}

# =============================================================================
# Command Line Argument Parsing
# =============================================================================

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --rocksdb)
                ENABLE_ROCKSDB=true
                shift
                ;;
            --release)
                BUILD_TYPE="Release"
                shift
                ;;
            --clean)
                CLEAN_BUILD=true
                shift
                ;;
            --demo)
                RUN_DEMO=true
                shift
                ;;
            --benchmark)
                RUN_BENCHMARK=true
                shift
                ;;
            --tests)
                RUN_TESTS=true
                shift
                ;;
            --test-demand)
                RUN_MDBX_DEMAND_TEST=true
                shift
                ;;
            --jobs)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --filter)
                BENCHMARK_FILTER="$2"
                shift 2
                ;;
            --time-unit)
                BENCHMARK_TIME_UNIT="$2"
                shift 2
                ;;
            --min-time)
                BENCHMARK_MIN_TIME="$2"
                shift 2
                ;;
            --format)
                BENCHMARK_FORMAT="$2"
                shift 2
                ;;
            --accounts)
                BENCH_NUM_ACCOUNTS="$2"
                shift 2
                ;;
            --blocks)
                BENCH_NUM_BLOCKS_PER_ACCOUNT="$2"
                shift 2
                ;;
            --max-block)
                BENCH_MAX_BLOCK_NUMBER="$2"
                shift 2
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                echo "Use --help for usage information."
                exit 1
                ;;
        esac
    done
}

# =============================================================================
# Utility Functions
# =============================================================================

check_dependencies() {
    log_step "Checking dependencies"
    
    # Check CMake
    if ! command -v cmake &> /dev/null; then
        log_error "CMake not found. Please install CMake 3.21 or later."
        exit 1
    fi
    
    local cmake_version
    cmake_version=$(cmake --version | head -n1 | cut -d' ' -f3)
    log_substep "CMake version: ${cmake_version}"
    
    # Check compiler
    if command -v clang++ &> /dev/null; then
        local clang_version
        clang_version=$(clang++ --version | head -n1)
        log_substep "Compiler: ${clang_version}"
    elif command -v g++ &> /dev/null; then
        local gcc_version
        gcc_version=$(g++ --version | head -n1)
        log_substep "Compiler: ${gcc_version}"
    else
        log_error "No suitable C++ compiler found (clang++ or g++)"
        exit 1
    fi
    
    # MDBX 是默认启用的，会通过CPM自动下载构建
    log_substep "MDBX support: ${GREEN}ENABLED${NC} (via CPM)"
    
    # Check RocksDB if requested
    if [[ "${ENABLE_ROCKSDB}" == "true" ]]; then
        log_substep "RocksDB support: ${GREEN}ENABLED${NC}"
    else
        log_substep "RocksDB support: ${YELLOW}DISABLED${NC}"
    fi
    
    log_success "Dependency check completed"
}

setup_vcpkg() {
    log_step "Setting up vcpkg"
    
    if [[ ! -d "${VCPKG_DIR}" ]]; then
        log_substep "Initializing vcpkg submodule..."
        git submodule update --init --recursive "${VCPKG_DIR}"
    fi
    
    if [[ ! -x "${VCPKG_DIR}/vcpkg" ]]; then
        log_substep "Bootstrapping vcpkg..."
        cd "${VCPKG_DIR}"
        ./bootstrap-vcpkg.sh
        cd "${PROJECT_ROOT}"
    fi
    
    log_substep "Installing dependencies..."
    "${VCPKG_DIR}/vcpkg" install
    
    log_success "vcpkg setup completed"
}

clean_build_directory() {
    if [[ "${CLEAN_BUILD}" == "true" ]]; then
        log_step "Cleaning build directory"
        rm -rf "${BUILD_DIR}"
        log_success "Build directory cleaned"
    fi
}

configure_build() {
    log_step "Configuring CMake build"
    
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    
    local cmake_args=(
        "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
        "-DCMAKE_TOOLCHAIN_FILE=${VCPKG_DIR}/scripts/buildsystems/vcpkg.cmake"
    )
    
    if [[ "${ENABLE_ROCKSDB}" == "true" ]]; then
        cmake_args+=("-DENABLE_ROCKSDB=ON")
        log_substep "RocksDB support: ${GREEN}ENABLED${NC}"
    else
        cmake_args+=("-DENABLE_ROCKSDB=OFF")
        log_substep "RocksDB support: ${YELLOW}DISABLED${NC}"
    fi
    
    log_substep "MDBX support: ${GREEN}ENABLED${NC} (default)"
    
    log_substep "Build type: ${BUILD_TYPE}"
    log_substep "Toolchain: vcpkg"
    
    if [[ "${VERBOSE}" == "true" ]]; then
        cmake "${cmake_args[@]}" "${PROJECT_ROOT}"
    else
        cmake "${cmake_args[@]}" "${PROJECT_ROOT}"
    fi
    
    cd "${PROJECT_ROOT}"
    log_success "CMake configuration completed"
}

build_project() {
    log_step "Building project"
    
    cd "${BUILD_DIR}"
    
    local build_args=("--build" "." "--parallel" "${PARALLEL_JOBS}")
    
    if [[ "${VERBOSE}" == "true" ]]; then
        build_args+=("--verbose")
    fi
    
    log_substep "Using ${PARALLEL_JOBS} parallel jobs"
    log_substep "Build type: ${BUILD_TYPE}"
    
    if cmake "${build_args[@]}"; then
        cd "${PROJECT_ROOT}"
        log_success "Build completed successfully"
    else
        cd "${PROJECT_ROOT}"
        log_error "Build failed"
        exit 1
    fi
}

# =============================================================================
# Execution Functions
# =============================================================================

run_demo() {
    log_step "Running demo application"
    
    if [[ ! -x "${BUILD_DIR}/mdbx_demo" ]]; then
        log_error "Demo executable not found. Build the project first."
        return 1
    fi
    
    log_substep "Starting blockchain state query demo..."
    echo
    
    cd "${BUILD_DIR}"
    ./mdbx_demo
    cd "${PROJECT_ROOT}"
    
    echo
    log_success "Demo completed"
}

run_benchmarks() {
    log_step "Running performance benchmarks"
    
    if [[ ! -x "${BUILD_DIR}/benchmark_runner" ]]; then
        log_error "Benchmark executable not found. Build the project first."
        return 1
    fi
    
    local benchmark_args=()
    if [[ "${BENCHMARK_FILTER}" != "*" ]]; then
        benchmark_args+=("--benchmark_filter=${BENCHMARK_FILTER}")
    fi
    benchmark_args+=(
        "--benchmark_time_unit=${BENCHMARK_TIME_UNIT}"
        "--benchmark_min_time=${BENCHMARK_MIN_TIME}"
        "--benchmark_format=${BENCHMARK_FORMAT}"
    )
    
    log_substep "Filter: ${BENCHMARK_FILTER}"
    log_substep "Time unit: ${BENCHMARK_TIME_UNIT}"
    log_substep "Min time: ${BENCHMARK_MIN_TIME}"
    log_substep "Format: ${BENCHMARK_FORMAT}"
    echo
    
    cd "${BUILD_DIR}"
    # Set environment variables for benchmark data configuration
    export BENCH_NUM_ACCOUNTS="${BENCH_NUM_ACCOUNTS}"
    export BENCH_NUM_BLOCKS_PER_ACCOUNT="${BENCH_NUM_BLOCKS_PER_ACCOUNT}"
    export BENCH_MAX_BLOCK_NUMBER="${BENCH_MAX_BLOCK_NUMBER}"
    
    log_substep "Data config: ${BENCH_NUM_ACCOUNTS} accounts, ${BENCH_NUM_BLOCKS_PER_ACCOUNT} blocks/account, max block ${BENCH_MAX_BLOCK_NUMBER}"
    echo
    
    ./benchmark_runner "${benchmark_args[@]}"
    cd "${PROJECT_ROOT}"
    
    echo
    log_success "Benchmarks completed"
}

run_tests() {
    log_step "Running tests"
    
    # Check for test executables (now located in tests subdirectory)
    local test_files=()
    if [[ -x "${BUILD_DIR}/tests/test_endian" ]]; then
        test_files+=("${BUILD_DIR}/tests/test_endian")
    fi
    if [[ -x "${BUILD_DIR}/tests/test_rocksdb" && "${ENABLE_ROCKSDB}" == "true" ]]; then
        test_files+=("${BUILD_DIR}/tests/test_rocksdb")
    fi
    if [[ -x "${BUILD_DIR}/tests/test_mdbx_simple" ]]; then
        test_files+=("${BUILD_DIR}/tests/test_mdbx_simple")
    fi
    if [[ -x "${BUILD_DIR}/tests/test_mdbx_demand" ]]; then
        test_files+=("${BUILD_DIR}/tests/test_mdbx_demand")
    fi
    
    if [[ ${#test_files[@]} -eq 0 ]]; then
        log_warning "No test executables found"
        return 0
    fi
    
    for test_file in "${test_files[@]}"; do
        local test_name
        test_name=$(basename "${test_file}")
        log_substep "Running ${test_name}..."
        
        if "${test_file}"; then
            log_success "${test_name} passed"
        else
            log_error "${test_name} failed"
        fi
        echo
    done
    
    log_success "All tests completed"
}

run_mdbx_demand_test() {
    log_step "Running MDBX demand requirements test"
    
    if [[ ! -x "${BUILD_DIR}/tests/test_mdbx_demand" ]]; then
        log_error "MDBX demand test executable not found. Build the project first."
        return 1
    fi
    
    log_substep "Running comprehensive MDBX functionality tests..."
    echo
    
    cd "${BUILD_DIR}"
    if ./tests/test_mdbx_demand; then
        cd "${PROJECT_ROOT}"
        echo
        log_success "MDBX demand test passed"
    else
        cd "${PROJECT_ROOT}"
        echo
        log_error "MDBX demand test failed"
        return 1
    fi
}

# =============================================================================
# Main Execution Flow
# =============================================================================

print_banner() {
    echo
    echo -e "${PURPLE}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${PURPLE}║${NC}          ${BOLD}MDBX Blockchain State Read Accelerator${NC}             ${PURPLE}║${NC}"
    echo -e "${PURPLE}║${NC}                     ${CYAN}Build & Run Script${NC}                      ${PURPLE}║${NC}"
    echo -e "${PURPLE}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo
}

print_summary() {
    echo
    log_step "Build Summary"
    log_substep "MDBX Support: ${GREEN}Enabled${NC} (default)"
    log_substep "RocksDB Support: $([ "${ENABLE_ROCKSDB}" == "true" ] && echo "${GREEN}Enabled${NC}" || echo "${YELLOW}Disabled${NC}")"
    log_substep "Build Type: ${BUILD_TYPE}"
    log_substep "Parallel Jobs: ${PARALLEL_JOBS}"
    
    if [[ "${RUN_DEMO}" == "true" ]] || [[ "${RUN_BENCHMARK}" == "true" ]] || [[ "${RUN_TESTS}" == "true" ]] || [[ "${RUN_MDBX_DEMAND_TEST}" == "true" ]]; then
        echo
        log_step "Execution Plan"
        [[ "${RUN_DEMO}" == "true" ]] && log_substep "${GREEN}✓${NC} Run demo application"
        [[ "${RUN_BENCHMARK}" == "true" ]] && log_substep "${GREEN}✓${NC} Run benchmarks (filter: ${BENCHMARK_FILTER})"
        [[ "${RUN_TESTS}" == "true" ]] && log_substep "${GREEN}✓${NC} Run tests"
        [[ "${RUN_MDBX_DEMAND_TEST}" == "true" ]] && log_substep "${GREEN}✓${NC} Run MDBX demand requirements test"
    fi
    echo
}

main() {
    print_banner
    
    # Parse command line arguments
    parse_arguments "$@"
    
    # Show summary
    print_summary
    
    # Execute build pipeline
    check_dependencies
    setup_vcpkg
    clean_build_directory
    configure_build
    build_project
    
    # Execute requested operations
    local executed_something=false
    
    if [[ "${RUN_DEMO}" == "true" ]]; then
        run_demo
        executed_something=true
    fi
    
    if [[ "${RUN_BENCHMARK}" == "true" ]]; then
        run_benchmarks
        executed_something=true
    fi
    
    if [[ "${RUN_TESTS}" == "true" ]]; then
        run_tests
        executed_something=true
    fi
    
    if [[ "${RUN_MDBX_DEMAND_TEST}" == "true" ]]; then
        run_mdbx_demand_test
        executed_something=true
    fi
    
    # Final message
    echo
    if [[ "${executed_something}" == "true" ]]; then
        log_success "All operations completed successfully! 🎉"
    else
        log_success "Build completed successfully! 🎉"
        echo
        log_info "To run the applications:"
        log_info "  Demo:         $0 --demo"
        log_info "  Benchmark:    $0 --benchmark"
        log_info "  Tests:        $0 --tests"
        log_info "  MDBX Demand:  $0 --test-demand"
        log_info ""
        log_info "Use --help for more options."
    fi
}

# Execute main function with all arguments
main "$@"