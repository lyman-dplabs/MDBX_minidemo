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
ENABLE_MDBX=false
BUILD_TYPE="Debug"
CLEAN_BUILD=false
RUN_DEMO=false
RUN_BENCHMARK=false
RUN_TESTS=false
VERBOSE=false
PARALLEL_JOBS=$(nproc)

# Benchmark configuration
BENCHMARK_FILTER="*"
BENCHMARK_TIME_UNIT="ms"
BENCHMARK_MIN_TIME="0.1s"
BENCHMARK_FORMAT="console"

# =============================================================================
# Help Function
# =============================================================================

show_help() {
    cat << EOF
${BOLD}MDBX Blockchain State Read Accelerator - Build & Run Script${NC}

${BOLD}USAGE:${NC}
    $0 [OPTIONS]

${BOLD}BUILD OPTIONS:${NC}
    --mdbx              Enable MDBX support (requires libmdbx installation)
    --release           Build in Release mode (default: Debug)
    --clean             Clean build directory before building
    --jobs N            Number of parallel build jobs (default: $(nproc))
    --verbose           Enable verbose build output

${BOLD}RUN OPTIONS:${NC}
    --demo              Run the main demo application
    --benchmark         Run performance benchmarks
    --tests             Run unit tests (if available)

${BOLD}BENCHMARK OPTIONS:${NC}
    --filter PATTERN    Benchmark filter pattern (default: "*")
    --time-unit UNIT    Time unit: ns, us, ms, s (default: ms)
    --min-time TIME     Minimum time per benchmark (default: 0.1s)
    --format FORMAT     Output format: console, json, csv (default: console)

${BOLD}EXAMPLES:${NC}
    # Basic build and run demo with RocksDB only
    $0 --demo

    # Build with MDBX support and run benchmarks
    $0 --mdbx --benchmark

    # Clean release build and run specific benchmarks
    $0 --clean --release --benchmark --filter "RocksDB*"

    # Build with MDBX and run everything
    $0 --mdbx --demo --benchmark --tests

${BOLD}REQUIREMENTS:${NC}
    - CMake 3.21+
    - C++23 compatible compiler (GCC 12+, Clang 14+)
    - vcpkg (automatically set up as submodule)
    - libmdbx (optional, for --mdbx flag)

EOF
}

# =============================================================================
# Command Line Argument Parsing
# =============================================================================

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --mdbx)
                ENABLE_MDBX=true
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
    
    # Check MDBX if requested
    if [[ "${ENABLE_MDBX}" == "true" ]]; then
        log_substep "Checking for MDBX..."
        if pkg-config --exists libmdbx; then
            local mdbx_version
            mdbx_version=$(pkg-config --modversion libmdbx 2>/dev/null || echo "unknown")
            log_substep "Found libmdbx version: ${mdbx_version}"
        else
            log_warning "libmdbx not found via pkg-config"
            log_warning "MDBX functionality may be disabled during build"
        fi
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
    
    if [[ "${ENABLE_MDBX}" == "true" ]]; then
        log_substep "MDBX support: ${GREEN}ENABLED${NC}"
    else
        log_substep "MDBX support: ${YELLOW}DISABLED${NC}"
    fi
    
    log_substep "Build type: ${BUILD_TYPE}"
    log_substep "Toolchain: vcpkg"
    
    if [[ "${VERBOSE}" == "true" ]]; then
        cmake "${cmake_args[@]}" "${PROJECT_ROOT}"
    else
        cmake "${cmake_args[@]}" "${PROJECT_ROOT}" > /dev/null
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
    
    local benchmark_args=(
        "--benchmark_filter=${BENCHMARK_FILTER}"
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
    ./benchmark_runner "${benchmark_args[@]}"
    cd "${PROJECT_ROOT}"
    
    echo
    log_success "Benchmarks completed"
}

run_tests() {
    log_step "Running tests"
    
    # Check for test executables
    local test_files=()
    if [[ -x "${BUILD_DIR}/test_endian" ]]; then
        test_files+=("${BUILD_DIR}/test_endian")
    fi
    if [[ -x "${BUILD_DIR}/test_rocksdb" ]]; then
        test_files+=("${BUILD_DIR}/test_rocksdb")
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

# =============================================================================
# Main Execution Flow
# =============================================================================

print_banner() {
    cat << EOF

${PURPLE}╔═══════════════════════════════════════════════════════════════╗${NC}
${PURPLE}║${NC}          ${BOLD}MDBX Blockchain State Read Accelerator${NC}             ${PURPLE}║${NC}
${PURPLE}║${NC}                     ${CYAN}Build & Run Script${NC}                      ${PURPLE}║${NC}
${PURPLE}╚═══════════════════════════════════════════════════════════════╝${NC}

EOF
}

print_summary() {
    echo
    log_step "Build Summary"
    log_substep "MDBX Support: $([ "${ENABLE_MDBX}" == "true" ] && echo "${GREEN}Enabled${NC}" || echo "${YELLOW}Disabled${NC}")"
    log_substep "Build Type: ${BUILD_TYPE}"
    log_substep "Parallel Jobs: ${PARALLEL_JOBS}"
    
    if [[ "${RUN_DEMO}" == "true" ]] || [[ "${RUN_BENCHMARK}" == "true" ]] || [[ "${RUN_TESTS}" == "true" ]]; then
        echo
        log_step "Execution Plan"
        [[ "${RUN_DEMO}" == "true" ]] && log_substep "${GREEN}✓${NC} Run demo application"
        [[ "${RUN_BENCHMARK}" == "true" ]] && log_substep "${GREEN}✓${NC} Run benchmarks (filter: ${BENCHMARK_FILTER})"
        [[ "${RUN_TESTS}" == "true" ]] && log_substep "${GREEN}✓${NC} Run tests"
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
    
    # Final message
    echo
    if [[ "${executed_something}" == "true" ]]; then
        log_success "All operations completed successfully! 🎉"
    else
        log_success "Build completed successfully! 🎉"
        echo
        log_info "To run the applications:"
        log_info "  Demo:      $0 --demo"
        log_info "  Benchmark: $0 --benchmark"
        log_info "  Tests:     $0 --tests"
        log_info ""
        log_info "Use --help for more options."
    fi
}

# Execute main function with all arguments
main "$@"