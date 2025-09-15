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

using namespace datastore::kvdb;
using namespace utils;

// Configuration structure for benchmark parameters
struct BenchConfig {
    // Data parameters
    size_t total_kv_pairs = 100000000;  // 100M total KV pairs 
    size_t test_kv_pairs = 100000;      // 100K KV pairs to test
    size_t key_size = 32;               // 32-byte keys
    size_t value_size = 32;             // 32-byte values
    
    // Test parameters
    size_t random_read_count = 10000;   // Number of random reads to perform
    size_t update_count = 10000;        // Number of updates to perform
    
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

// Generate a fixed 32-byte key from an index
std::string generate_key(size_t index, size_t key_size = 32) {
    std::string key = fmt::format("key_{:016x}", index);
    // Pad or truncate to exactly key_size bytes
    key.resize(key_size, '0');
    return key;
}

// Generate a fixed 32-byte value from an index
std::string generate_value(size_t index, size_t value_size = 32) {
    std::string value = fmt::format("value_{:016x}_data", index);
    // Pad or truncate to exactly value_size bytes
    value.resize(value_size, 'x');
    return value;
}

// Generate a dataset of key-value pairs for testing
std::vector<std::pair<std::string, std::string>> generate_test_dataset(const BenchConfig& config) {
    fmt::println("\n=== Generating Test Dataset ===");
    fmt::println("Generating {} KV pairs from a simulated dataset of {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    
    std::vector<std::pair<std::string, std::string>> dataset;
    dataset.reserve(config.test_kv_pairs);
    
    // Use random selection to simulate picking from 100M dataset, but ensure uniqueness
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(0, config.total_kv_pairs - 1);
    std::unordered_set<size_t> used_indices;
    
    while (dataset.size() < config.test_kv_pairs) {
        size_t index = dis(gen);
        if (used_indices.insert(index).second) {  // Only add if not already used
            std::string key = generate_key(index, config.key_size);
            std::string value = generate_value(index, config.value_size);
            dataset.emplace_back(std::move(key), std::move(value));
        }
    }
    
    fmt::println("✓ Generated {} test KV pairs", dataset.size());
    fmt::println("  Key size: {} bytes", config.key_size);
    fmt::println("  Value size: {} bytes", config.value_size);
    
    return dataset;
}

// Benchmark write operations with commit time measurement
void benchmark_write_performance(::mdbx::env_managed& env, 
                                const std::vector<std::pair<std::string, std::string>>& dataset,
                                const BenchConfig& config) {
    fmt::println("\n=== Write Performance Benchmark ===");
    fmt::println("Testing {} KV pairs with Durable persistence", dataset.size());
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    // Measure write time
    auto write_start = std::chrono::high_resolution_clock::now();
    
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        for (const auto& [key, value] : dataset) {
            cursor->insert(str_to_slice(key), str_to_slice(value));
        }
        
        // Measure commit time specifically
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        auto commit_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
        
        fmt::println("✓ Commit time: {} ms", commit_duration);
    }
    
    auto write_end = std::chrono::high_resolution_clock::now();
    auto total_write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        write_end - write_start).count();
    
    fmt::println("✓ Total write time: {} ms", total_write_duration);
    fmt::println("✓ Write throughput: {:.2f} ops/sec", 
                 static_cast<double>(dataset.size()) / (total_write_duration / 1000.0));
}

// Benchmark random read operations
void benchmark_read_performance(::mdbx::env_managed& env,
                               const std::vector<std::pair<std::string, std::string>>& dataset,
                               const BenchConfig& config) {
    fmt::println("\n=== Random Read Performance Benchmark ===");
    fmt::println("Testing {} random reads", config.random_read_count);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    // Prepare random indices for reading
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(0, dataset.size() - 1);
    
    std::vector<size_t> read_indices;
    read_indices.reserve(config.random_read_count);
    for (size_t i = 0; i < config.random_read_count; ++i) {
        read_indices.push_back(dis(gen));
    }
    
    // Measure read time
    auto read_start = std::chrono::high_resolution_clock::now();
    
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(table_config);
        
        size_t successful_reads = 0;
        for (size_t idx : read_indices) {
            const auto& key = dataset[idx].first;
            auto result = cursor->find(str_to_slice(key), false);
            if (result.done) {
                successful_reads++;
            }
        }
        
        ro_txn.abort();
        fmt::println("✓ Successful reads: {}/{}", successful_reads, config.random_read_count);
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    fmt::println("✓ Total read time: {} ms", read_duration);
    fmt::println("✓ Read throughput: {:.2f} ops/sec", 
                 static_cast<double>(config.random_read_count) / (read_duration / 1000.0));
    fmt::println("✓ Average read latency: {:.3f} ms", 
                 static_cast<double>(read_duration) / config.random_read_count);
}

// Benchmark update operations
void benchmark_update_performance(::mdbx::env_managed& env,
                                 const std::vector<std::pair<std::string, std::string>>& dataset,
                                 const BenchConfig& config) {
    fmt::println("\n=== Update Performance Benchmark ===");
    fmt::println("Testing {} update operations", config.update_count);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    // Prepare random indices for updating
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(0, dataset.size() - 1);
    
    std::vector<size_t> update_indices;
    update_indices.reserve(config.update_count);
    for (size_t i = 0; i < config.update_count; ++i) {
        update_indices.push_back(dis(gen));
    }
    
    // Measure update time
    auto update_start = std::chrono::high_resolution_clock::now();
    
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        for (size_t idx : update_indices) {
            const auto& key = dataset[idx].first;
            std::string new_value = generate_value(idx + 999999, config.value_size); // Different value
            cursor->upsert(str_to_slice(key), str_to_slice(new_value));
        }
        
        // Measure commit time for updates
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        auto commit_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
        
        fmt::println("✓ Update commit time: {} ms", commit_duration);
    }
    
    auto update_end = std::chrono::high_resolution_clock::now();
    auto update_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        update_end - update_start).count();
    
    fmt::println("✓ Total update time: {} ms", update_duration);
    fmt::println("✓ Update throughput: {:.2f} ops/sec", 
                 static_cast<double>(config.update_count) / (update_duration / 1000.0));
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
    fmt::println("  -h, --help          Show this help message");
    fmt::println("");
    fmt::println("Example EnvConfig JSON file:");
    fmt::println("{{");
    fmt::println("  \"path\": \"/tmp/mdbx_bench\",");
    fmt::println("  \"max_size\": 8589934592,");
    fmt::println("  \"page_size\": 4096,");
    fmt::println("  \"max_tables\": 64,");
    fmt::println("  \"max_readers\": 100");
    fmt::println("}}");
}

int main(int argc, char* argv[]) {
    std::string config_file;
    
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
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else {
            fmt::println(stderr, "Error: Unknown option: {}", arg);
            print_usage(argv[0]);
            return 1;
        }
    }
    
    fmt::println("=== MDBX Performance Benchmark ===");
    fmt::println("Testing MDBX performance with 32-byte KV pairs");
    
    try {
        // Load configuration
        EnvConfig env_config = load_env_config(config_file);
        BenchConfig bench_config;
        bench_config.db_path = env_config.path;
        
        // Setup environment
        setup_environment(bench_config.db_path);
        
        // Open MDBX environment with Durable persistence
        auto env = open_env(env_config);
        fmt::println("✓ Opened MDBX environment at: {}", env_config.path);
        
        // Generate test dataset
        auto dataset = generate_test_dataset(bench_config);
        
        // Run benchmarks
        benchmark_write_performance(env, dataset, bench_config);
        benchmark_read_performance(env, dataset, bench_config);
        benchmark_update_performance(env, dataset, bench_config);
        
        fmt::println("\n=== Benchmark Summary ===");
        fmt::println("✓ Tested {} KV pairs ({}B keys, {}B values)", 
                     dataset.size(), bench_config.key_size, bench_config.value_size);
        fmt::println("✓ Performed {} random reads", bench_config.random_read_count);
        fmt::println("✓ Performed {} updates", bench_config.update_count);
        fmt::println("✓ Database path: {}", bench_config.db_path);
        fmt::println("✓ All benchmarks completed successfully! 🎉");
        
    } catch (const std::exception& e) {
        fmt::println(stderr, "\n❌ Benchmark failed: {}", e.what());
        return 1;
    }
    
    return 0;
}