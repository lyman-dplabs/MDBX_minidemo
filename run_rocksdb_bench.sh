#!/bin/bash

# =============================================================================
# RocksDB Performance Benchmark Runner Script
# =============================================================================
# This script builds and runs the RocksDB performance benchmark tool.
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
    echo -e "${BOLD}RocksDB Performance Benchmark Runner${NC}"
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
    echo -e "    -c, --config FILE   Path to RocksDBConfig JSON file"
    echo -e "    -b, --bench-config FILE  Path to BenchConfig JSON file"
    echo -e "    --sample-config     Create sample config files and exit"
    echo
    echo -e "${BOLD}EXAMPLES:${NC}"
    echo -e "    # Basic build and run with default config"
    echo -e "    $0"
    echo
    echo -e "    # Run with custom configs"
    echo -e "    $0 --config rocksdb_config.json --bench-config bench_config.json"
    echo
    echo -e "    # Clean release build"
    echo -e "    $0 --clean"
    echo
    echo -e "    # Create sample config files"
    echo -e "    $0 --sample-config"
    echo
}

create_sample_config() {
    local rocksdb_config_file="rocksdb_bench_config.json"
    local bench_config_file="bench_config.json"
    
    log_step "Creating sample configuration files"
    
    # Create RocksDBConfig file
    cat > "${rocksdb_config_file}" << 'EOF'
{
  "path": "/tmp/rocksdb_bench",
  "create_if_missing": true,
  "max_open_files": 300,
  "write_buffer_size": 67108864,
  "max_write_buffer_number": 3,
  "target_file_size_base": 67108864,
  "max_bytes_for_level_base": 268435456,
  "level0_file_num_compaction_trigger": 4,
  "level0_slowdown_writes_trigger": 20,
  "level0_stop_writes_trigger": 36
}
EOF
    
    # Create BenchConfig file
    cat > "${bench_config_file}" << 'EOF'
{
  "total_kv_pairs": 1000000,
  "test_kv_pairs": 100000,
  "test_rounds": 2,
  "db_path": "/tmp/rocksdb_bench"
}
EOF
    
    log_success "Sample configurations created:"
    log_info "  RocksDB config: ${rocksdb_config_file}"
    log_info "  Bench config:   ${bench_config_file}"
    echo
    log_info "RocksDB Configuration options:"
    log_info "  path:                   Database directory path"
    log_info "  write_buffer_size:      Write buffer size in bytes (64MB default)"
    log_info "  max_write_buffer_number: Maximum write buffers"
    log_info "  target_file_size_base:  Target SST file size (64MB default)"
    log_info "  max_bytes_for_level_base: Level 0 max bytes (256MB default)"
    echo
    log_info "Benchmark Configuration options:"
    log_info "  total_kv_pairs: Total KV pairs in database (1M default)"
    log_info "  test_kv_pairs:  KV pairs to test per round (100K default)"
    log_info "  test_rounds:    Number of test rounds (2 default)"
    log_info "  db_path:        Database path override"
    echo
    log_info "Use these files with:"
    log_info "  $0 --config ${rocksdb_config_file} --bench-config ${bench_config_file}"
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
    
    log_step "Building RocksDB benchmark"
    
    if [[ "${CLEAN_BUILD}" == "true" ]]; then
        log_info "Cleaning build directory"
        rm -rf "${BUILD_DIR}"
    fi
    
    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"
    
    log_info "Configuring CMake (${BUILD_TYPE} mode) with RocksDB support"
    cmake -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
          -DENABLE_ROCKSDB=ON \
          -DCMAKE_TOOLCHAIN_FILE="${PROJECT_ROOT}/third_party/vcpkg/scripts/buildsystems/vcpkg.cmake" \
          "${PROJECT_ROOT}"
    
    log_info "Building rocksdb_bench target"
    cmake --build . --target rocksdb_bench --parallel $(nproc)
    
    cd "${PROJECT_ROOT}"
    log_success "Build completed"
}

run_benchmark() {
    log_step "Running RocksDB performance benchmark"
    
    if [[ ! -x "${BUILD_DIR}/rocksdb_bench" ]]; then
        log_error "rocksdb_bench executable not found. Build the project first."
        log_error "Make sure to enable RocksDB support with --rocksdb flag during build."
        exit 1
    fi
    
    local args=()
    
    if [[ -n "${CONFIG_FILE}" ]]; then
        # Convert relative path to absolute path if needed
        if [[ "${CONFIG_FILE}" != /* ]]; then
            CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_FILE}"
        fi
        if [[ ! -f "${CONFIG_FILE}" ]]; then
            log_error "RocksDB config file not found: ${CONFIG_FILE}"
            exit 1
        fi
        args+=("--config" "${CONFIG_FILE}")
        log_info "Using RocksDB config file: ${CONFIG_FILE}"
    else
        log_info "Using default RocksDB configuration"
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
    else
        log_info "Using default benchmark configuration"
    fi
    
    # Display environment variables if set
    if [[ -n "${ROCKSDB_BENCH_TOTAL_KV_PAIRS:-}" ]] || \
       [[ -n "${ROCKSDB_BENCH_TEST_KV_PAIRS:-}" ]] || \
       [[ -n "${ROCKSDB_BENCH_TEST_ROUNDS:-}" ]] || \
       [[ -n "${ROCKSDB_BENCH_DB_PATH:-}" ]]; then
        log_info "Using environment variable overrides:"
        [[ -n "${ROCKSDB_BENCH_TOTAL_KV_PAIRS:-}" ]] && log_info "  ROCKSDB_BENCH_TOTAL_KV_PAIRS=${ROCKSDB_BENCH_TOTAL_KV_PAIRS}"
        [[ -n "${ROCKSDB_BENCH_TEST_KV_PAIRS:-}" ]] && log_info "  ROCKSDB_BENCH_TEST_KV_PAIRS=${ROCKSDB_BENCH_TEST_KV_PAIRS}"
        [[ -n "${ROCKSDB_BENCH_TEST_ROUNDS:-}" ]] && log_info "  ROCKSDB_BENCH_TEST_ROUNDS=${ROCKSDB_BENCH_TEST_ROUNDS}"
        [[ -n "${ROCKSDB_BENCH_DB_PATH:-}" ]] && log_info "  ROCKSDB_BENCH_DB_PATH=${ROCKSDB_BENCH_DB_PATH}"
    fi
    
    echo
    cd "${BUILD_DIR}"
    ./rocksdb_bench "${args[@]}"
    cd "${PROJECT_ROOT}"
    
    echo
    log_success "Benchmark completed successfully!"
}

print_banner() {
    echo
    echo -e "${PURPLE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${PURPLE}║${NC}              ${BOLD}RocksDB Performance Benchmark${NC}                ${PURPLE}║${NC}"
    echo -e "${PURPLE}║${NC}                   ${CYAN}Benchmark Runner${NC}                      ${PURPLE}║${NC}"
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