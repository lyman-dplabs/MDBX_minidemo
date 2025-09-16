#!/bin/bash

# =============================================================================
# MDBX 20亿KV基准测试脚本
# =============================================================================
# 在20亿条KV数据中选择10万条进行100次测试
# 自动保存日志文件，包含时间戳和测试参数
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

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_critical() {
    echo -e "${RED}[CRITICAL]${NC} $*"
}

# Project configuration
readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly BUILD_DIR="${PROJECT_ROOT}/build"
readonly LOGS_DIR="${PROJECT_ROOT}/logs"

# Test configuration
readonly TOTAL_KV_PAIRS=2000000000  # 20亿
readonly TEST_KV_PAIRS=100000       # 10万
readonly TEST_ROUNDS=100            # 100次
readonly DB_PATH="/tmp/mdbx_bench_2billion"

# Generate timestamp for log files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOGS_DIR}/mdbx_bench_2billion_${TIMESTAMP}.log"
SUMMARY_FILE="${LOGS_DIR}/mdbx_bench_2billion_${TIMESTAMP}_summary.txt"

# Default values
BUILD_TYPE="Release"
CLEAN_BUILD=false
RUN_ONLY=false
CONFIG_FILE=""
BENCH_CONFIG_FILE=""

# MDBX mapsize requirements for 2 billion KV pairs
readonly REQUIRED_MAPSIZE_2B=214748364800  # 200GB in bytes
readonly RECOMMENDED_CONFIG="configs/mdbx_env_2billion.json"

show_help() {
    echo -e "${BOLD}MDBX 20亿KV基准测试脚本${NC}"
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
    echo
    echo -e "${BOLD}TEST PARAMETERS:${NC}"
    echo -e "    Total KV pairs: ${TOTAL_KV_PAIRS} (20亿)"
    echo -e "    Test KV pairs:  ${TEST_KV_PAIRS} (10万)"
    echo -e "    Test rounds:    ${TEST_ROUNDS} (100次)"
    echo -e "    Database path:  ${DB_PATH}"
    echo
    echo -e "${BOLD}CRITICAL REQUIREMENTS:${NC}"
    echo -e "    ${RED}MDBX mapsize: 200GB minimum (214,748,364,800 bytes)${NC}"
    echo -e "    ${RED}System RAM:   200GB+ recommended${NC}"
    echo -e "    ${RED}Disk space:   200GB+ required${NC}"
    echo -e "    ${RED}Architecture: 64-bit system required${NC}"
    echo
    echo -e "${BOLD}OUTPUT:${NC}"
    echo -e "    Log file:       ${LOG_FILE}"
    echo -e "    Summary file:   ${SUMMARY_FILE}"
    echo
    echo -e "${BOLD}EXAMPLES:${NC}"
    echo -e "    # Run with recommended config (200GB mapsize)"
    echo -e "    $0"
    echo
    echo -e "    # Run with custom config (ensure mapsize >= 200GB)"
    echo -e "    $0 --config configs/mdbx_env_2billion.json"
    echo
    echo -e "    # Run with both configs"
    echo -e "    $0 --config configs/mdbx_env_2billion.json --bench-config configs/bench_2billion.json"
    echo
    echo -e "${BOLD}WARNING:${NC}"
    echo -e "    ${YELLOW}Using configs with mapsize < 200GB will cause MDBX_MAP_FULL error!${NC}"
    echo
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

# Validate MDBX mapsize configuration
validate_mapsize_config() {
    local config_file="$1"
    
    if [[ ! -f "${config_file}" ]]; then
        log_error "Config file not found: ${config_file}"
        return 1
    fi
    
    # Extract max_size from JSON config
    local max_size
    max_size=$(grep -o '"max_size"[[:space:]]*:[[:space:]]*[0-9]*' "${config_file}" | grep -o '[0-9]*$')
    
    if [[ -z "${max_size}" ]]; then
        log_error "Could not find max_size in config file: ${config_file}"
        return 1
    fi
    
    log_info "Config file max_size: ${max_size} bytes ($((max_size / 1024 / 1024 / 1024))GB)"
    log_info "Required max_size for 2B KV pairs: ${REQUIRED_MAPSIZE_2B} bytes ($((REQUIRED_MAPSIZE_2B / 1024 / 1024 / 1024))GB)"
    
    if [[ "${max_size}" -lt "${REQUIRED_MAPSIZE_2B}" ]]; then
        log_critical "MDBX mapsize is too small for 2 billion KV pairs!"
        log_critical "Current max_size: ${max_size} bytes ($((max_size / 1024 / 1024 / 1024))GB)"
        log_critical "Required max_size: ${REQUIRED_MAPSIZE_2B} bytes ($((REQUIRED_MAPSIZE_2B / 1024 / 1024 / 1024))GB)"
        log_critical "This will cause MDBX_MAP_FULL error during testing!"
        echo
        log_warning "Recommendation: Use the provided config file: ${RECOMMENDED_CONFIG}"
        log_warning "Or manually set max_size to at least ${REQUIRED_MAPSIZE_2B} bytes in your config"
        echo
        return 1
    fi
    
    log_success "MDBX mapsize configuration is adequate for 2 billion KV pairs"
    return 0
}

# Display mapsize requirements warning
show_mapsize_warning() {
    echo
    log_critical "⚠️  CRITICAL: MDBX Mapsize Requirements for 2 Billion KV Pairs ⚠️"
    echo
    echo -e "${BOLD}Memory Requirements:${NC}"
    echo -e "  • Each KV pair: 64 bytes (32-byte key + 32-byte value)"
    echo -e "  • 2 billion KV pairs: ~128GB raw data"
    echo -e "  • MDBX overhead: ~30% additional space"
    echo -e "  • ${BOLD}Required mapsize: 200GB (214,748,364,800 bytes)${NC}"
    echo
    echo -e "${BOLD}Configuration:${NC}"
    echo -e "  • Recommended config: ${RECOMMENDED_CONFIG}"
    echo -e "  • This config has max_size set to 200GB"
    echo -e "  • Using smaller mapsize will cause MDBX_MAP_FULL error"
    echo
    echo -e "${BOLD}System Requirements:${NC}"
    echo -e "  • Available RAM: At least 200GB recommended"
    echo -e "  • Available disk space: At least 200GB"
    echo -e "  • 64-bit system required"
    echo
    log_warning "If you use a custom config file, ensure max_size >= 200GB!"
    echo
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

setup_logging() {
    log_step "Setting up logging"
    
    # Create logs directory
    mkdir -p "${LOGS_DIR}"
    
    # Create log file with header
    {
        echo "============================================================================="
        echo "MDBX 20亿KV基准测试日志"
        echo "============================================================================="
        echo "开始时间: $(date)"
        echo "测试参数:"
        echo "  - 总KV对数: ${TOTAL_KV_PAIRS} (20亿)"
        echo "  - 测试KV对数: ${TEST_KV_PAIRS} (10万)"
        echo "  - 测试轮次: ${TEST_ROUNDS} (100次)"
        echo "  - 数据库路径: ${DB_PATH}"
        echo "  - 构建类型: ${BUILD_TYPE}"
        if [[ -n "${CONFIG_FILE}" ]]; then
            echo "  - 环境配置文件: ${CONFIG_FILE}"
        fi
        if [[ -n "${BENCH_CONFIG_FILE}" ]]; then
            echo "  - 基准测试配置文件: ${BENCH_CONFIG_FILE}"
        fi
        echo "============================================================================="
        echo
    } > "${LOG_FILE}"
    
    log_success "日志文件已创建: ${LOG_FILE}"
}

run_benchmark() {
    log_step "Running MDBX 20亿KV基准测试"
    
    if [[ ! -x "${BUILD_DIR}/mdbx_bench" ]]; then
        log_error "mdbx_bench executable not found. Build the project first."
        exit 1
    fi
    
    local args=()
    
    # Set default config files if not specified
    if [[ -z "${CONFIG_FILE}" ]]; then
        CONFIG_FILE="${RECOMMENDED_CONFIG}"
        log_info "Using recommended config for 2B KV pairs: ${CONFIG_FILE}"
    fi
    if [[ -z "${BENCH_CONFIG_FILE}" ]]; then
        BENCH_CONFIG_FILE="configs/bench_2billion.json"
    fi
    
    # Convert relative paths to absolute paths if needed
    if [[ "${CONFIG_FILE}" != /* ]]; then
        CONFIG_FILE="${PROJECT_ROOT}/${CONFIG_FILE}"
    fi
    if [[ "${BENCH_CONFIG_FILE}" != /* ]]; then
        BENCH_CONFIG_FILE="${PROJECT_ROOT}/${BENCH_CONFIG_FILE}"
    fi
    
    # Check if config files exist
    if [[ ! -f "${CONFIG_FILE}" ]]; then
        log_error "Config file not found: ${CONFIG_FILE}"
        exit 1
    fi
    if [[ ! -f "${BENCH_CONFIG_FILE}" ]]; then
        log_error "Bench config file not found: ${BENCH_CONFIG_FILE}"
        exit 1
    fi
    
    # Validate MDBX mapsize configuration
    log_step "Validating MDBX mapsize configuration"
    if ! validate_mapsize_config "${CONFIG_FILE}"; then
        show_mapsize_warning
        log_error "MDBX mapsize validation failed. Please fix the configuration."
        exit 1
    fi
    
    args+=("--config" "${CONFIG_FILE}")
    args+=("--bench-config" "${BENCH_CONFIG_FILE}")
    
    log_info "Using config file: ${CONFIG_FILE}"
    log_info "Using bench config file: ${BENCH_CONFIG_FILE}"
    log_info "Logging to: ${LOG_FILE}"
    
    # Run benchmark and capture output
    echo "开始基准测试..." | tee -a "${LOG_FILE}"
    echo "开始时间: $(date)" | tee -a "${LOG_FILE}"
    echo | tee -a "${LOG_FILE}"
    
    cd "${BUILD_DIR}"
    if ./mdbx_bench "${args[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
        echo | tee -a "${LOG_FILE}"
        echo "基准测试完成时间: $(date)" | tee -a "${LOG_FILE}"
        echo "=============================================================================" | tee -a "${LOG_FILE}"
        log_success "基准测试完成!"
    else
        echo | tee -a "${LOG_FILE}"
        echo "基准测试失败时间: $(date)" | tee -a "${LOG_FILE}"
        echo "=============================================================================" | tee -a "${LOG_FILE}"
        log_error "基准测试失败!"
        exit 1
    fi
    
    cd "${PROJECT_ROOT}"
}

generate_summary() {
    log_step "生成测试摘要"
    
    # Extract key metrics from log file
    {
        echo "============================================================================="
        echo "MDBX 20亿KV基准测试摘要"
        echo "============================================================================="
        echo "测试时间: $(date)"
        echo "日志文件: ${LOG_FILE}"
        echo
        echo "测试参数:"
        echo "  - 总KV对数: ${TOTAL_KV_PAIRS} (20亿)"
        echo "  - 测试KV对数: ${TEST_KV_PAIRS} (10万)"
        echo "  - 测试轮次: ${TEST_ROUNDS} (100次)"
        echo "  - 数据库路径: ${DB_PATH}"
        echo
        
        # Extract performance metrics from log
        echo "性能指标摘要:"
        echo "  - 读取测试:"
        grep -E "✓ Read.*KV pairs in.*ms" "${LOG_FILE}" | tail -5 | sed 's/^/    /'
        echo "  - 写入测试:"
        grep -E "✓ Wrote.*KV pairs in.*ms" "${LOG_FILE}" | tail -5 | sed 's/^/    /'
        echo "  - 更新测试:"
        grep -E "✓ Updated and committed.*KV pairs" "${LOG_FILE}" | tail -5 | sed 's/^/    /'
        echo "  - 混合测试:"
        grep -E "✓ Completed.*mixed operations" "${LOG_FILE}" | tail -5 | sed 's/^/    /'
        echo
        
        echo "平均延迟摘要:"
        grep -E "Average.*latency:" "${LOG_FILE}" | tail -10 | sed 's/^/    /'
        echo
        
        echo "吞吐量摘要:"
        grep -E "throughput:" "${LOG_FILE}" | tail -10 | sed 's/^/    /'
        echo
        
        echo "============================================================================="
    } > "${SUMMARY_FILE}"
    
    log_success "测试摘要已生成: ${SUMMARY_FILE}"
}

print_banner() {
    echo
    echo -e "${PURPLE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${PURPLE}║${NC}              ${BOLD}MDBX 20亿KV基准测试${NC}                    ${PURPLE}║${NC}"
    echo -e "${PURPLE}║${NC}                  ${CYAN}专用测试脚本${NC}                       ${PURPLE}║${NC}"
    echo -e "${PURPLE}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo
}

main() {
    print_banner
    
    # Show critical mapsize requirements warning
    show_mapsize_warning
    
    # Parse command line arguments
    parse_arguments "$@"
    
    # Setup logging
    setup_logging
    
    # Main execution flow
    build_benchmark
    run_benchmark
    generate_summary
    
    echo
    log_success "所有操作完成! 🎉"
    log_info "日志文件: ${LOG_FILE}"
    log_info "摘要文件: ${SUMMARY_FILE}"
}

# Execute main function with all arguments
main "$@"
