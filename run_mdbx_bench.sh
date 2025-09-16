#!/bin/bash

# =============================================================================
# MDBX Performance Benchmark Runner Script
# =============================================================================
# This script builds and runs the MDBX performance benchmark tool.
# =============================================================================

set -euo pipefail

# ANSI color codes
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly PURPLE='\033[0;35m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly NC='\033[0m'

# Logging functions
log_info() {
    echo -e "${CYAN}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_step() {
    echo -e "\n${PURPLE}▶${NC} ${BOLD}$*${NC}"
}

# Project configuration
readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly BUILD_DIR="${PROJECT_ROOT}/build"

# Default values
BUILD_TYPE="Release"
CLEAN_BUILD=false
RUN_ONLY=false
CONFIG_FILE=""
BENCH_CONFIG_FILE=""
CREATE_SAMPLE_CONFIG=false

show_help() {
    echo -e "${BOLD}MDBX Performance Benchmark Runner${NC}"
    echo
    echo -e "${BOLD}USAGE:${NC}"
    echo -e "    $0 [OPTIONS]"
    echo
    echo -e "${BOLD}BUILD OPTIONS:${NC}"
    echo -e "    --debug             Build in Debug mode (default: Release)"
    echo -e "    --clean             Clean build directory before building"
    echo -e "    --run-only          Skip build, only run existing benchmark"
    echo
    echo -e "${BOLD}BENCHMARK OPTIONS:${NC}"
    echo -e "    -c, --config FILE      Path to EnvConfig JSON file"
    echo -e "    -b, --bench-config FILE Path to BenchConfig JSON file"
    echo -e "    --sample-config        Create a sample config file and exit"
    echo
    echo -e "${BOLD}EXAMPLES:${NC}"
    echo -e "    # Basic build and run with default config"
    echo -e "    $0"
    echo
    echo -e "    # Run with custom config"
    echo -e "    $0 --config my_config.json"
    echo
    echo -e "    # Run with both env and bench configs"
    echo -e "    $0 --config env_config.json --bench-config bench_config.json"
    echo
    echo -e "    # Clean release build"
    echo -e "    $0 --clean"
    echo
    echo -e "    # Create sample config file"
    echo -e "    $0 --sample-config"
    echo
}

create_sample_config() {
    local config_file="mdbx_bench_config.json"
    
    log_step "Creating sample configuration file"
    
    cat > "${config_file}" << 'EOF'
{
  "path": "/tmp/mdbx_bench",
  "create": true,
  "readonly": false,
  "exclusive": false,
  "in_memory": false,
  "shared": false,
  "read_ahead": false,
  "write_map": false,
  "page_size": 4096,
  "max_size": 8589934592,
  "growth_size": 1073741824,
  "max_tables": 64,
  "max_readers": 100
}
EOF
    
    log_success "Sample configuration created: ${config_file}"
    echo
    log_info "Configuration options:"
    log_info "  path:        Database file path"
    log_info "  max_size:    Maximum database size in bytes (8GB default)"
    log_info "  page_size:   MDBX page size in bytes (4KB default)"
    log_info "  max_tables:  Maximum number of tables"
    log_info "  max_readers: Maximum number of concurrent readers"
    echo
    log_info "Use this file with: $0 --config ${config_file}"
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --debug)
                BUILD_TYPE="Debug"
                shift
                ;;
            --clean)
                CLEAN_BUILD=true
                shift
                ;;
            --run-only)
                RUN_ONLY=true
                shift
                ;;
            -c|--config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            -b|--bench-config)
                BENCH_CONFIG_FILE="$2"
                shift 2
                ;;
            --sample-config)
                CREATE_SAMPLE_CONFIG=true
                shift
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

build_benchmark() {
    if [[ "${RUN_ONLY}" == "true" ]]; then
        log_step "Skipping build (--run-only specified)"
        return
    fi
    
    log_step "Building MDBX benchmark"
    
    if [[ "${CLEAN_BUILD}" == "true" ]]; then
        log_info "Cleaning build directory"
        rm -rf "${BUILD_DIR}"
    fi
    
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    
    log_info "Configuring CMake (${BUILD_TYPE} mode)"
    cmake -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -DCMAKE_TOOLCHAIN_FILE="${PROJECT_ROOT}/third_party/vcpkg/scripts/buildsystems/vcpkg.cmake" \
          "${PROJECT_ROOT}"
    
    log_info "Building mdbx_bench target"
    cmake --build . --target mdbx_bench --parallel $(nproc)
    
    cd "${PROJECT_ROOT}"
    log_success "Build completed"
}

run_benchmark() {
    log_step "Running MDBX performance benchmark"
    
    if [[ ! -x "${BUILD_DIR}/mdbx_bench" ]]; then
        log_error "mdbx_bench executable not found. Build the project first."
        exit 1
    fi
    
    local args=()
    if [[ -n "${CONFIG_FILE}" ]]; then
        # Convert relative path to absolute path if needed
        if [[ "${CONFIG_FILE}" != /* ]]; then
            CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_FILE}"
        fi
        if [[ ! -f "${CONFIG_FILE}" ]]; then
            log_error "Config file not found: ${CONFIG_FILE}"
            exit 1
        fi
        args+=("--config" "${CONFIG_FILE}")
        log_info "Using config file: ${CONFIG_FILE}"
    else
        log_info "Using default configuration"
    fi
    
    if [[ -n "${BENCH_CONFIG_FILE}" ]]; then
        # Convert relative path to absolute path if needed
        if [[ "${BENCH_CONFIG_FILE}" != /* ]]; then
            BENCH_CONFIG_FILE="${PROJECT_ROOT}/${BENCH_CONFIG_FILE}"
        fi
        if [[ ! -f "${BENCH_CONFIG_FILE}" ]]; then
            log_error "Bench config file not found: ${BENCH_CONFIG_FILE}"
            exit 1
        fi
        args+=("--bench-config" "${BENCH_CONFIG_FILE}")
        log_info "Using bench config file: ${BENCH_CONFIG_FILE}"
    fi
    
    echo
    cd "${BUILD_DIR}"
    ./mdbx_bench "${args[@]}"
    cd "${PROJECT_ROOT}"
    
    echo
    log_success "Benchmark completed successfully!"
}

print_banner() {
    echo
    echo -e "${PURPLE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${PURPLE}║${NC}              ${BOLD}MDBX Performance Benchmark${NC}                  ${PURPLE}║${NC}"
    echo -e "${PURPLE}║${NC}                  ${CYAN}Benchmark Runner${NC}                       ${PURPLE}║${NC}"
    echo -e "${PURPLE}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo
}

main() {
    print_banner
    
    # Parse command line arguments
    parse_arguments "$@"
    
    # Handle special operations
    if [[ "${CREATE_SAMPLE_CONFIG}" == "true" ]]; then
        create_sample_config
        exit 0
    fi
    
    # Main execution flow
    build_benchmark
    run_benchmark
    
    echo
    log_success "All operations completed! 🎉"
}

# Execute main function with all arguments
main "$@"