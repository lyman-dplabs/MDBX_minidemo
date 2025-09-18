#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <functional>
#include <json/json.h>
#include "db/mdbx.hpp"

// Configuration structures for benchmark parameters
struct BenchConfig {
    // Data parameters
    size_t total_kv_pairs = 1000000;    // 1M total KV pairs in database
    size_t test_kv_pairs = 100000;      // 100K KV pairs to test per round
    static constexpr size_t key_size = 32;    // Fixed 32-byte keys
    static constexpr size_t value_size = 32;  // Fixed 32-byte values
    
    // Test parameters
    size_t test_rounds = 2;             // Number of test rounds to run
    
    // Batch processing parameters
    size_t batch_size = 5000000;        // Batch size for database population (5M default)
    
    // Database path
    std::string db_path = "/data/mdbx_bench";
};

// Structure to hold timing results for each round
struct RoundResult {
    size_t round_number;
    double read_time_ms;
    double write_time_ms;
    double mixed_time_ms;
    double commit_time_ms;
    size_t successful_reads;
    size_t successful_writes;
    size_t successful_mixed;
    size_t test_kv_count;
    
    // Latency statistics
    std::vector<double> read_latencies_us;   // Individual read latencies in microseconds
    std::vector<double> write_latencies_us;  // Individual write latencies in microseconds
    std::vector<double> mixed_latencies_us;  // Individual mixed operation latencies
    
    double avg_read_latency_us = 0.0;
    double tp99_read_latency_us = 0.0;
    double avg_write_latency_us = 0.0;
    double tp99_write_latency_us = 0.0;
    double avg_mixed_latency_us = 0.0;
    double tp99_mixed_latency_us = 0.0;
};

// Forward declaration to avoid circular dependency
namespace mdbx {
    enum class key_mode : int;
    enum class value_mode : int;
}


// Test context for common test initialization
struct TestContext {
    RoundResult result;
    std::vector<size_t> test_indices;
    
    TestContext() = default;
};


// Configuration loading functions
BenchConfig create_default_bench_config();
void load_bench_config_from_env(BenchConfig& config);
void load_bench_config_from_json(const Json::Value& root, BenchConfig& config);
BenchConfig load_bench_config(const std::string& config_file);

// EnvConfig loading functions
datastore::kvdb::EnvConfig create_default_env_config();
void load_env_config_from_json(const Json::Value& root, datastore::kvdb::EnvConfig& config);
datastore::kvdb::EnvConfig load_env_config(const std::string& config_file);

// Data generation functions
std::string generate_key(size_t index);
std::string generate_value(size_t index);
std::vector<size_t> generate_random_indices(size_t count, size_t max_index);

// Latency and statistics functions
void calc_latency_stats(const std::vector<double>& latencies, double& avg, double& tp99);
void calculate_latency_stats(RoundResult& result);

// Test utility functions
TestContext init_test_context(size_t round_number, const BenchConfig& config, const std::string& test_name);

template<typename Func>
double measure_operation_us(Func&& operation) {
    auto start = std::chrono::high_resolution_clock::now();
    operation();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

// Summary and output functions
void print_comprehensive_summary(const std::vector<RoundResult>& results, const BenchConfig& config);
void print_usage(const char* program_name);

// Generic utility functions
bool load_json_config_generic(const std::string& config_file, const std::function<void(const Json::Value&)>& loader);
void load_env_var_size_t(const char* env_name, size_t& value);
void load_env_var_string(const char* env_name, std::string& value);