#include "db/mdbx.hpp"
#include "utils/string_utils.hpp"
#include <fmt/format.h>
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <unordered_set>
#include <json/json.h>
#include <cstdlib>

using namespace datastore::kvdb;
using namespace utils;

// Configuration structure for benchmark parameters
struct BenchConfig {
    // Data parameters
    size_t total_kv_pairs = 1000000;    // 1M total KV pairs in database
    size_t test_kv_pairs = 100000;      // 100K KV pairs to test per round
    static constexpr size_t key_size = 32;    // Fixed 32-byte keys
    static constexpr size_t value_size = 32;  // Fixed 32-byte values
    
    // Test parameters
    size_t test_rounds = 2;             // Number of test rounds to run
    
    // Database path
    std::string db_path = "/tmp/mdbx_bench";
};

// EnvConfig loader with JSON support
EnvConfig load_env_config(const std::string& config_file) {
    EnvConfig config;
    
    // Set defaults
    config.path = "/tmp/mdbx_bench";
    config.create = true;
    config.readonly = false;
    config.exclusive = false;
    config.in_memory = false;
    config.shared = false;
    config.read_ahead = false;
    config.write_map = false;
    config.page_size = 4_Kibi;
    config.max_size = 8_Gibi;  // Larger for bench data
    config.growth_size = 1_Gibi;
    config.max_tables = 64;
    config.max_readers = 50;
    
    // Try to load from file if it exists
    if (!config_file.empty() && std::filesystem::exists(config_file)) {
        try {
            std::ifstream file(config_file);
            Json::Value root;
            file >> root;
            
            if (root.isMember("path")) config.path = root["path"].asString();
            if (root.isMember("create")) config.create = root["create"].asBool();
            if (root.isMember("readonly")) config.readonly = root["readonly"].asBool();
            if (root.isMember("exclusive")) config.exclusive = root["exclusive"].asBool();
            if (root.isMember("in_memory")) config.in_memory = root["in_memory"].asBool();
            if (root.isMember("shared")) config.shared = root["shared"].asBool();
            if (root.isMember("read_ahead")) config.read_ahead = root["read_ahead"].asBool();
            if (root.isMember("write_map")) config.write_map = root["write_map"].asBool();
            if (root.isMember("page_size")) config.page_size = root["page_size"].asUInt64();
            if (root.isMember("max_size")) config.max_size = root["max_size"].asUInt64();
            if (root.isMember("growth_size")) config.growth_size = root["growth_size"].asUInt64();
            if (root.isMember("max_tables")) config.max_tables = root["max_tables"].asUInt();
            if (root.isMember("max_readers")) config.max_readers = root["max_readers"].asUInt();
            
            fmt::println("✓ Loaded EnvConfig from: {}", config_file);
        } catch (const std::exception& e) {
            fmt::println("⚠ Failed to load config file {}, using defaults: {}", config_file, e.what());
        }
    } else {
        fmt::println("✓ Using default EnvConfig (file not found: {})", config_file);
    }
    
    return config;
}

// BenchConfig loader with JSON and environment variable support
BenchConfig load_bench_config(const std::string& config_file) {
    BenchConfig config;
    
    // Set defaults
    config.total_kv_pairs = 1000000;
    config.test_kv_pairs = 100000;
    config.test_rounds = 2;
    config.db_path = "/tmp/mdbx_bench";
    
    // Load from environment variables first
    if (const char* env_val = std::getenv("MDBX_BENCH_TOTAL_KV_PAIRS")) {
        try {
            config.total_kv_pairs = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid MDBX_BENCH_TOTAL_KV_PAIRS: {}", env_val);
        }
    }
    
    if (const char* env_val = std::getenv("MDBX_BENCH_TEST_KV_PAIRS")) {
        try {
            config.test_kv_pairs = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid MDBX_BENCH_TEST_KV_PAIRS: {}", env_val);
        }
    }
    
    if (const char* env_val = std::getenv("MDBX_BENCH_TEST_ROUNDS")) {
        try {
            config.test_rounds = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid MDBX_BENCH_TEST_ROUNDS: {}", env_val);
        }
    }
    
    if (const char* env_val = std::getenv("MDBX_BENCH_DB_PATH")) {
        config.db_path = env_val;
    }
    
    // Try to load from file if it exists (overrides environment variables)
    if (!config_file.empty() && std::filesystem::exists(config_file)) {
        try {
            std::ifstream file(config_file);
            Json::Value root;
            file >> root;
            
            if (root.isMember("total_kv_pairs")) config.total_kv_pairs = root["total_kv_pairs"].asUInt64();
            if (root.isMember("test_kv_pairs")) config.test_kv_pairs = root["test_kv_pairs"].asUInt64();
            if (root.isMember("test_rounds")) config.test_rounds = root["test_rounds"].asUInt64();
            if (root.isMember("db_path")) config.db_path = root["db_path"].asString();
            
            // Ignore key_size and value_size from config file since they are fixed
            if (root.isMember("key_size") || root.isMember("value_size")) {
                fmt::println("⚠ key_size and value_size are fixed at 32 bytes, ignoring config file values");
            }
            
            fmt::println("✓ Loaded BenchConfig from: {}", config_file);
        } catch (const std::exception& e) {
            fmt::println("⚠ Failed to load BenchConfig file {}, using environment/defaults: {}", config_file, e.what());
        }
    } else if (!config_file.empty()) {
        fmt::println("⚠ BenchConfig file not found: {}, using environment/defaults", config_file);
    } else {
        fmt::println("✓ Using default BenchConfig (no file specified)");
    }
    
    return config;
}

// Generate a fixed 32-byte key from an index
std::string generate_key(size_t index) {
    std::string key = fmt::format("key_{:016x}", index);
    // Pad or truncate to exactly 32 bytes
    key.resize(32, '0');
    return key;
}

// Generate a fixed 32-byte value from an index
std::string generate_value(size_t index) {
    std::string value = fmt::format("value_{:016x}_data", index);
    // Pad or truncate to exactly 32 bytes
    value.resize(32, 'x');
    return value;
}

// Populate MDBX database with initial dataset
void populate_database(::mdbx::env_managed& env, const BenchConfig& config) {
    fmt::println("\n=== Populating Database ===");
    fmt::println("Inserting {} KV pairs into database", config.total_kv_pairs);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        for (size_t i = 0; i < config.total_kv_pairs; ++i) {
            std::string key = generate_key(i);
            std::string value = generate_value(i);
            cursor->insert(str_to_slice(key), str_to_slice(value));
            
            // Progress indicator for large datasets
            if ((i + 1) % 100000 == 0) {
                fmt::println("  Inserted {}/{} KV pairs", i + 1, config.total_kv_pairs);
            }
        }
        
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        auto commit_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
        fmt::println("✓ Initial population commit time: {} ms", commit_duration);
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
        end_time - start_time).count();
    
    fmt::println("✓ Database populated with {} KV pairs in {} seconds", 
                 config.total_kv_pairs, total_duration);
    fmt::println("  Key size: {} bytes", BenchConfig::key_size);
    fmt::println("  Value size: {} bytes", BenchConfig::value_size);
}

// Generate random indices for testing
std::vector<size_t> generate_random_indices(size_t count, size_t max_index) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(0, max_index - 1);
    std::unordered_set<size_t> used_indices;
    std::vector<size_t> indices;
    indices.reserve(count);
    
    while (indices.size() < count) {
        size_t index = dis(gen);
        if (used_indices.insert(index).second) {  // Only add if not already used
            indices.push_back(index);
        }
    }
    
    return indices;
}

// Structure to hold timing results for each round
struct RoundResult {
    size_t round_number;
    double read_time_ms;
    double commit_time_ms;
    size_t successful_reads;
    size_t test_kv_count;
};

// Perform one round of read-update-commit test
RoundResult perform_test_round(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Test Round {} ===", round_number);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    RoundResult result;
    result.round_number = round_number;
    result.test_kv_count = config.test_kv_pairs;
    
    // Step 1: Generate random indices for this round
    fmt::println("Generating {} random indices from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    auto test_indices = generate_random_indices(config.test_kv_pairs, config.total_kv_pairs);
    
    // Step 2: Read the selected KV pairs
    fmt::println("Reading {} randomly selected KV pairs", config.test_kv_pairs);
    auto read_start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::pair<std::string, std::string>> read_data;
    read_data.reserve(config.test_kv_pairs);
    
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(table_config);
        
        result.successful_reads = 0;
        for (size_t index : test_indices) {
            std::string key = generate_key(index);
            auto find_result = cursor->find(str_to_slice(key), false);
            if (find_result.done) {
                std::string value = std::string(find_result.value.as_string());
                read_data.emplace_back(std::move(key), std::move(value));
                result.successful_reads++;
            }
        }
        
        ro_txn.abort();
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", result.successful_reads, result.read_time_ms);
    
    // Step 3: Update the read data and commit
    fmt::println("Updating and committing {} KV pairs", result.successful_reads);
    
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        // Update all read data with new values
        for (size_t i = 0; i < read_data.size(); ++i) {
            const auto& [key, old_value] = read_data[i];
            // Generate a new value for update (add round number to make it unique)
            std::string new_value = generate_value(i + round_number * 1000000);
            cursor->upsert(str_to_slice(key), str_to_slice(new_value));
        }
        
        // Measure commit time specifically
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
    }
    
    fmt::println("✓ Updated and committed {} KV pairs", result.successful_reads);
    fmt::println("✓ Read time: {:.2f} ms", result.read_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", result.commit_time_ms);
    fmt::println("✓ Read throughput: {:.2f} ops/sec", 
                 static_cast<double>(result.successful_reads) / (result.read_time_ms / 1000.0));
    
    return result;
}

// Run all test rounds and collect results
std::vector<RoundResult> run_benchmark_rounds(::mdbx::env_managed& env, const BenchConfig& config) {
    fmt::println("\n=== Running {} Test Rounds ===", config.test_rounds);
    
    std::vector<RoundResult> results;
    results.reserve(config.test_rounds);
    
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_test_round(env, round, config);
        results.push_back(result);
    }
    
    return results;
}

// Print summary statistics
void print_benchmark_summary(const std::vector<RoundResult>& results, const BenchConfig& config) {
    fmt::println("\n=== Benchmark Summary ===");
    fmt::println("Completed {} test rounds", results.size());
    fmt::println("Database contains {} total KV pairs", config.total_kv_pairs);
    fmt::println("Each round tested {} KV pairs", config.test_kv_pairs);
    
    if (results.empty()) {
        fmt::println("No results to summarize");
        return;
    }
    
    // Calculate statistics
    double total_read_time = 0.0;
    double total_commit_time = 0.0;
    double min_read_time = results[0].read_time_ms;
    double max_read_time = results[0].read_time_ms;
    double min_commit_time = results[0].commit_time_ms;
    double max_commit_time = results[0].commit_time_ms;
    
    fmt::println("\nPer-Round Results:");
    for (const auto& result : results) {
        fmt::println("  Round {}: Read={:.2f}ms, Commit={:.2f}ms, Success={}/{}",
                     result.round_number, result.read_time_ms, result.commit_time_ms,
                     result.successful_reads, result.test_kv_count);
        
        total_read_time += result.read_time_ms;
        total_commit_time += result.commit_time_ms;
        min_read_time = std::min(min_read_time, result.read_time_ms);
        max_read_time = std::max(max_read_time, result.read_time_ms);
        min_commit_time = std::min(min_commit_time, result.commit_time_ms);
        max_commit_time = std::max(max_commit_time, result.commit_time_ms);
    }
    
    double avg_read_time = total_read_time / results.size();
    double avg_commit_time = total_commit_time / results.size();
    
    fmt::println("\nStatistics:");
    fmt::println("  Read Time  - Avg: {:.2f}ms, Min: {:.2f}ms, Max: {:.2f}ms", 
                 avg_read_time, min_read_time, max_read_time);
    fmt::println("  Commit Time - Avg: {:.2f}ms, Min: {:.2f}ms, Max: {:.2f}ms", 
                 avg_commit_time, min_commit_time, max_commit_time);
    fmt::println("  Average Read Throughput: {:.2f} ops/sec", 
                 static_cast<double>(config.test_kv_pairs) / (avg_read_time / 1000.0));
}

void setup_environment(const std::string& db_path) {
    fmt::println("\n=== Setting up Test Environment ===");
    
    // Clean up any existing database
    if (std::filesystem::exists(db_path)) {
        std::filesystem::remove_all(db_path);
        fmt::println("✓ Cleaned existing database at: {}", db_path);
    }
}

void print_usage(const char* program_name) {
    fmt::println("Usage: {} [options]", program_name);
    fmt::println("Options:");
    fmt::println("  -c, --config FILE    Path to EnvConfig JSON file");
    fmt::println("  -b, --bench-config FILE  Path to BenchConfig JSON file");
    fmt::println("  -h, --help          Show this help message");
    fmt::println("");
    fmt::println("Environment Variables:");
    fmt::println("  MDBX_BENCH_TOTAL_KV_PAIRS  Total KV pairs in database");
    fmt::println("  MDBX_BENCH_TEST_KV_PAIRS   KV pairs to test per round");
    fmt::println("  MDBX_BENCH_TEST_ROUNDS     Number of test rounds");
    fmt::println("  MDBX_BENCH_DB_PATH         Database path");
    fmt::println("  Note: Key and value sizes are fixed at 32 bytes");
    fmt::println("");
    fmt::println("Example EnvConfig JSON file:");
    fmt::println("{{");
    fmt::println("  \"path\": \"/tmp/mdbx_bench\",");
    fmt::println("  \"max_size\": 8589934592,");
    fmt::println("  \"page_size\": 4096,");
    fmt::println("  \"max_tables\": 64,");
    fmt::println("  \"max_readers\": 100");
    fmt::println("}}");
    fmt::println("");
    fmt::println("Example BenchConfig JSON file:");
    fmt::println("{{");
    fmt::println("  \"total_kv_pairs\": 2000000,");
    fmt::println("  \"test_kv_pairs\": 200000,");
    fmt::println("  \"test_rounds\": 5,");
    fmt::println("  \"db_path\": \"/tmp/mdbx_bench_custom\"");
    fmt::println("  \"Note\": \"key_size and value_size are fixed at 32 bytes\"");
    fmt::println("}}");
}

int main(int argc, char* argv[]) {
    std::string config_file;
    std::string bench_config_file;
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" || arg == "--config") {
            if (i + 1 < argc) {
                config_file = argv[++i];
            } else {
                fmt::println(stderr, "Error: --config requires a file path");
                return 1;
            }
        } else if (arg == "-b" || arg == "--bench-config") {
            if (i + 1 < argc) {
                bench_config_file = argv[++i];
            } else {
                fmt::println(stderr, "Error: --bench-config requires a file path");
                return 1;
            }
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else {
            fmt::println(stderr, "Error: Unknown option: {}", arg);
            print_usage(argv[0]);
            return 1;
        }
    }
    
    // Load configurations
    EnvConfig env_config = load_env_config(config_file);
    BenchConfig bench_config = load_bench_config(bench_config_file);
    
    // Override db_path from env_config if not set in bench_config
    if (bench_config.db_path == "/tmp/mdbx_bench" && !env_config.path.empty()) {
        bench_config.db_path = env_config.path;
    }
    
    fmt::println("=== MDBX Performance Benchmark ===");
    fmt::println("Testing MDBX performance with {}-byte keys and {}-byte values", 
                 BenchConfig::key_size, BenchConfig::value_size);
    fmt::println("Total KV pairs in DB: {}", bench_config.total_kv_pairs);
    fmt::println("KV pairs per test round: {}", bench_config.test_kv_pairs);
    fmt::println("Number of test rounds: {}", bench_config.test_rounds);
    fmt::println("Database path: {}", bench_config.db_path);
    
    try {
        // Setup environment
        setup_environment(bench_config.db_path);
        
        // Open MDBX environment with Durable persistence
        auto env = open_env(env_config);
        fmt::println("✓ Opened MDBX environment at: {}", env_config.path);
        
        // Populate database with initial data
        populate_database(env, bench_config);
        
        // Run benchmark rounds
        auto results = run_benchmark_rounds(env, bench_config);
        
        // Print final summary
        print_benchmark_summary(results, bench_config);
        
        fmt::println("\n✓ All benchmarks completed successfully! 🎉");
        
    } catch (const std::exception& e) {
        fmt::println(stderr, "\n❌ Benchmark failed: {}", e.what());
        return 1;
    }
    
    return 0;
}