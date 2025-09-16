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
    echo -e "${BOLD}OUTPUT:${NC}"
    echo -e "    Log file:       ${LOG_FILE}"
    echo -e "    Summary file:   ${SUMMARY_FILE}"
    echo
    echo -e "${BOLD}EXAMPLES:${NC}"
    echo -e "    # Run with default config"
    echo -e "    $0"
    echo
    echo -e "    # Run with custom config"
    echo -e "    $0 --config configs/mdbx_env_performance.json"
    echo
    echo -e "    # Run with both configs"
    echo -e "    $0 --config configs/mdbx_env_performance.json --bench-config configs/bench_2billion.json"
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
        CONFIG_FILE="configs/mdbx_env_performance.json"
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
