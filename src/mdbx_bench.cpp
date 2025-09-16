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
#include <algorithm>

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
    
    // Batch processing parameters
    size_t batch_size = 5000000;        // Batch size for database population (5M default)
    
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
    config.batch_size = 5000000;  // 5M default batch size
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
    
    if (const char* env_val = std::getenv("MDBX_BENCH_BATCH_SIZE")) {
        try {
            config.batch_size = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid MDBX_BENCH_BATCH_SIZE: {}", env_val);
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
            if (root.isMember("batch_size")) config.batch_size = root["batch_size"].asUInt64();
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

// Populate MDBX database with initial dataset using batch commits
void populate_database(::mdbx::env_managed& env, const BenchConfig& config) {
    fmt::println("\n=== Populating Database ===");
    fmt::println("Inserting {} KV pairs into database", config.total_kv_pairs);
    fmt::println("Using batch size: {} KV pairs per transaction", config.batch_size);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    size_t total_committed = 0;
    size_t batch_count = 0;
    auto total_commit_time = std::chrono::milliseconds(0);
    
    // Process data in batches to avoid memory explosion
    for (size_t batch_start = 0; batch_start < config.total_kv_pairs; batch_start += config.batch_size) {
        size_t batch_end = std::min(batch_start + config.batch_size, config.total_kv_pairs);
        size_t batch_size_actual = batch_end - batch_start;
        batch_count++;
        
        fmt::println("  Processing batch {}/{}: KV pairs {} to {} ({} pairs)", 
                     batch_count, 
                     (config.total_kv_pairs + config.batch_size - 1) / config.batch_size,
                     batch_start, batch_end - 1, batch_size_actual);
        
        auto batch_start_time = std::chrono::high_resolution_clock::now();
        
        {
            RWTxnManaged rw_txn(env);
            auto cursor = rw_txn.rw_cursor(table_config);
            
            for (size_t i = batch_start; i < batch_end; ++i) {
                std::string key = generate_key(i);
                std::string value = generate_value(i);
                cursor->insert(str_to_slice(key), str_to_slice(value));
            }
            
            auto commit_start = std::chrono::high_resolution_clock::now();
            rw_txn.commit_and_stop();
            auto commit_end = std::chrono::high_resolution_clock::now();
            
            auto commit_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                commit_end - commit_start);
            total_commit_time += commit_duration;
            
            total_committed += batch_size_actual;
            
            auto batch_end_time = std::chrono::high_resolution_clock::now();
            auto batch_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                batch_end_time - batch_start_time).count();
            
            fmt::println("    ✓ Batch {} completed: {} pairs in {} ms (commit: {} ms)", 
                         batch_count, batch_size_actual, batch_duration, commit_duration.count());
        }
        
        // Progress indicator for large datasets
        if (total_committed % (config.batch_size * 10) == 0 || total_committed == config.total_kv_pairs) {
            auto elapsed = std::chrono::high_resolution_clock::now() - start_time;
            auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
            double rate = total_committed / static_cast<double>(elapsed_seconds);
            fmt::println("  Progress: {}/{} KV pairs ({:.1f}%) - Rate: {:.0f} pairs/sec", 
                         total_committed, config.total_kv_pairs,
                         100.0 * total_committed / config.total_kv_pairs, rate);
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
        end_time - start_time).count();
    
    fmt::println("✓ Database populated with {} KV pairs in {} seconds", 
                 config.total_kv_pairs, total_duration);
    fmt::println("  Key size: {} bytes", BenchConfig::key_size);
    fmt::println("  Value size: {} bytes", BenchConfig::value_size);
    fmt::println("  Batch size: {} KV pairs", config.batch_size);
    fmt::println("  Total batches: {}", batch_count);
    fmt::println("  Total commit time: {} ms", total_commit_time.count());
    fmt::println("  Average commit time per batch: {:.1f} ms", 
                 total_commit_time.count() / static_cast<double>(batch_count));
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

// Calculate statistics from latency vectors
void calculate_latency_stats(RoundResult& result) {
    auto calc_stats = [](const std::vector<double>& latencies, double& avg, double& tp99) {
        if (latencies.empty()) {
            avg = tp99 = 0.0;
            return;
        }
        
        // Calculate average
        double sum = 0.0;
        for (double lat : latencies) {
            sum += lat;
        }
        avg = sum / latencies.size();
        
        // Calculate Tp99
        std::vector<double> sorted_latencies = latencies;
        std::sort(sorted_latencies.begin(), sorted_latencies.end());
        size_t tp99_index = static_cast<size_t>(sorted_latencies.size() * 0.99);
        if (tp99_index >= sorted_latencies.size()) {
            tp99_index = sorted_latencies.size() - 1;
        }
        tp99 = sorted_latencies[tp99_index];
    };
    
    calc_stats(result.read_latencies_us, result.avg_read_latency_us, result.tp99_read_latency_us);
    calc_stats(result.write_latencies_us, result.avg_write_latency_us, result.tp99_write_latency_us);
    calc_stats(result.mixed_latencies_us, result.avg_mixed_latency_us, result.tp99_mixed_latency_us);
}

// Test modes
enum class TestMode {
    READ_ONLY,
    WRITE_ONLY,  
    MIXED_READ_WRITE
};

// Perform read-only test
RoundResult perform_read_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Read Test Round {} ===", round_number);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    RoundResult result;
    result.round_number = round_number;
    result.test_kv_count = config.test_kv_pairs;
    result.successful_reads = 0;
    
    // Generate random indices for this round
    fmt::println("Generating {} random indices from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    auto test_indices = generate_random_indices(config.test_kv_pairs, config.total_kv_pairs);
    
    // Read the selected KV pairs with individual latency tracking
    fmt::println("Reading {} randomly selected KV pairs", config.test_kv_pairs);
    auto read_start = std::chrono::high_resolution_clock::now();
    
    result.read_latencies_us.reserve(config.test_kv_pairs);
    
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(table_config);
        
        for (size_t index : test_indices) {
            std::string key = generate_key(index);
            
            auto op_start = std::chrono::high_resolution_clock::now();
            auto find_result = cursor->find(str_to_slice(key), false);
            auto op_end = std::chrono::high_resolution_clock::now();
            
            if (find_result.done) {
                result.successful_reads++;
            }
            
            double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.read_latencies_us.push_back(latency_us);
        }
        
        ro_txn.abort();
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    calculate_latency_stats(result);
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", result.successful_reads, result.read_time_ms);
    fmt::println("✓ Average read latency: {:.2f} μs", result.avg_read_latency_us);
    fmt::println("✓ Tp99 read latency: {:.2f} μs", result.tp99_read_latency_us);
    fmt::println("✓ Read throughput: {:.2f} ops/sec", 
                 static_cast<double>(result.successful_reads) / (result.read_time_ms / 1000.0));
    
    return result;
}

// Perform write-only test
RoundResult perform_write_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Write Test Round {} ===", round_number);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    RoundResult result;
    result.round_number = round_number;
    result.test_kv_count = config.test_kv_pairs;
    result.successful_writes = 0;
    
    // Generate random indices for this round
    fmt::println("Generating {} random indices from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    auto test_indices = generate_random_indices(config.test_kv_pairs, config.total_kv_pairs);
    
    fmt::println("Writing {} randomly selected KV pairs", config.test_kv_pairs);
    
    result.write_latencies_us.reserve(config.test_kv_pairs);
    
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        auto write_start = std::chrono::high_resolution_clock::now();
        
        for (size_t i = 0; i < test_indices.size(); ++i) {
            size_t index = test_indices[i];
            std::string key = generate_key(index);
            std::string new_value = generate_value(index + round_number * 1000000);
            
            auto op_start = std::chrono::high_resolution_clock::now();
            cursor->upsert(str_to_slice(key), str_to_slice(new_value));
            auto op_end = std::chrono::high_resolution_clock::now();
            
            result.successful_writes++;
            
            double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.write_latencies_us.push_back(latency_us);
        }
        
        auto write_end = std::chrono::high_resolution_clock::now();
        result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start).count();
        
        // Measure commit time specifically
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
    }
    
    calculate_latency_stats(result);
    
    fmt::println("✓ Wrote {} KV pairs in {:.2f} ms", result.successful_writes, result.write_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", result.commit_time_ms);
    fmt::println("✓ Average write latency: {:.2f} μs", result.avg_write_latency_us);
    fmt::println("✓ Tp99 write latency: {:.2f} μs", result.tp99_write_latency_us);
    fmt::println("✓ Write throughput: {:.2f} ops/sec", 
                 static_cast<double>(result.successful_writes) / (result.write_time_ms / 1000.0));
    
    return result;
}

// Perform update test (read-then-update pattern like legacy mode)
RoundResult perform_update_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Update Test Round {} ===", round_number);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    RoundResult result;
    result.round_number = round_number;
    result.test_kv_count = config.test_kv_pairs;
    
    // Step 1: Generate random indices for this round
    fmt::println("Generating {} random indices from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    auto test_indices = generate_random_indices(config.test_kv_pairs, config.total_kv_pairs);
    
    // Step 2: Read the selected KV pairs with individual latency tracking
    fmt::println("Reading {} randomly selected KV pairs", config.test_kv_pairs);
    auto read_start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::pair<std::string, std::string>> read_data;
    read_data.reserve(config.test_kv_pairs);
    result.read_latencies_us.reserve(config.test_kv_pairs);
    
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(table_config);
        
        result.successful_reads = 0;
        for (size_t index : test_indices) {
            std::string key = generate_key(index);
            
            auto op_start = std::chrono::high_resolution_clock::now();
            auto find_result = cursor->find(str_to_slice(key), false);
            auto op_end = std::chrono::high_resolution_clock::now();
            
            if (find_result.done) {
                std::string value = std::string(find_result.value.as_string());
                read_data.emplace_back(std::move(key), std::move(value));
                result.successful_reads++;
            }
            
            double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.read_latencies_us.push_back(latency_us);
        }
        
        ro_txn.abort();
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", result.successful_reads, result.read_time_ms);
    
    // Step 3: Update the read data and commit with individual latency tracking
    fmt::println("Updating and committing {} KV pairs", result.successful_reads);
    
    result.write_latencies_us.reserve(result.successful_reads);
    
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        auto write_start = std::chrono::high_resolution_clock::now();
        
        // Update all read data with new values
        result.successful_writes = 0;
        for (size_t i = 0; i < read_data.size(); ++i) {
            const auto& [key, old_value] = read_data[i];
            // Generate a new value for update (add round number to make it unique)
            std::string new_value = generate_value(i + round_number * 1000000);
            
            auto op_start = std::chrono::high_resolution_clock::now();
            cursor->upsert(str_to_slice(key), str_to_slice(new_value));
            auto op_end = std::chrono::high_resolution_clock::now();
            
            result.successful_writes++;
            
            double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.write_latencies_us.push_back(latency_us);
        }
        
        auto write_end = std::chrono::high_resolution_clock::now();
        result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start).count();
        
        // Measure commit time specifically
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
    }
    
    // Calculate mixed metrics as combination of read and write
    result.successful_mixed = result.successful_reads + result.successful_writes;
    result.mixed_time_ms = result.read_time_ms + result.write_time_ms;
    
    // Combine read and write latencies for mixed latency stats
    result.mixed_latencies_us.reserve(result.read_latencies_us.size() + result.write_latencies_us.size());
    result.mixed_latencies_us.insert(result.mixed_latencies_us.end(), 
                                   result.read_latencies_us.begin(), result.read_latencies_us.end());
    result.mixed_latencies_us.insert(result.mixed_latencies_us.end(), 
                                   result.write_latencies_us.begin(), result.write_latencies_us.end());
    
    calculate_latency_stats(result);
    
    fmt::println("✓ Updated and committed {} KV pairs", result.successful_writes);
    fmt::println("✓ Total mixed operations: {} (read: {}, write: {})", 
                 result.successful_mixed, result.successful_reads, result.successful_writes);
    fmt::println("✓ Read time: {:.2f} ms, Write time: {:.2f} ms", result.read_time_ms, result.write_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", result.commit_time_ms);
    fmt::println("✓ Average read latency: {:.2f} μs", result.avg_read_latency_us);
    fmt::println("✓ Average write latency: {:.2f} μs", result.avg_write_latency_us);
    fmt::println("✓ Average mixed latency: {:.2f} μs", result.avg_mixed_latency_us);
    fmt::println("✓ Tp99 mixed latency: {:.2f} μs", result.tp99_mixed_latency_us);
    fmt::println("✓ Mixed throughput: {:.2f} ops/sec", 
                 static_cast<double>(result.successful_mixed) / (result.mixed_time_ms / 1000.0));
    
    return result;
}

// Perform mixed read-write test with 80:20 ratio
RoundResult perform_mixed_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Mixed Read-Write Test Round {} ===", round_number);
    
    MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    RoundResult result;
    result.round_number = round_number;
    result.test_kv_count = config.test_kv_pairs;
    result.successful_reads = 0;
    result.successful_writes = 0;
    result.successful_mixed = 0;
    
    // Generate random indices for this round
    fmt::println("Generating {} mixed operations from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    auto test_indices = generate_random_indices(config.test_kv_pairs, config.total_kv_pairs);
    
    // Calculate 8:2 ratio (every 8 reads followed by 2 writes)
    size_t batch_size = 10; // 8 reads + 2 writes = 10 operations per batch
    size_t num_batches = config.test_kv_pairs / batch_size;
    size_t remaining_ops = config.test_kv_pairs % batch_size;
    
    size_t read_count = num_batches * 8 + std::min(remaining_ops, static_cast<size_t>(8));
    size_t write_count = num_batches * 2 + std::max(static_cast<int>(remaining_ops) - 8, 0);
    
    fmt::println("Mixed operations: {} reads, {} writes (8:2 pattern)", read_count, write_count);
    
    result.read_latencies_us.reserve(read_count);
    result.write_latencies_us.reserve(write_count);
    
    auto test_start = std::chrono::high_resolution_clock::now();
    
    {
        // Use read-write transaction for mixed operations
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(table_config);
        
        // Perform mixed operations with 8:2 pattern
        size_t op_index = 0;
        for (size_t batch = 0; batch < num_batches; ++batch) {
            // 8 reads in this batch
            for (int read_in_batch = 0; read_in_batch < 8 && op_index < config.test_kv_pairs; ++read_in_batch) {
                size_t index = test_indices[op_index];
                std::string key = generate_key(index);
                
                auto op_start = std::chrono::high_resolution_clock::now();
                auto find_result = cursor->find(str_to_slice(key), false);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                if (find_result.done) {
                    result.successful_reads++;
                }
                
                double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start).count();
                result.read_latencies_us.push_back(latency_us);
                
                op_index++;
            }
            
            // 2 writes in this batch
            for (int write_in_batch = 0; write_in_batch < 2 && op_index < config.test_kv_pairs; ++write_in_batch) {
                size_t index = test_indices[op_index];
                std::string key = generate_key(index);
                std::string new_value = generate_value(index + round_number * 1000000);
                
                auto op_start = std::chrono::high_resolution_clock::now();
                cursor->upsert(str_to_slice(key), str_to_slice(new_value));
                auto op_end = std::chrono::high_resolution_clock::now();
                
                result.successful_writes++;
                
                double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start).count();
                result.write_latencies_us.push_back(latency_us);
                
                op_index++;
            }
        }
        
        // Handle remaining operations (if any)
        while (op_index < config.test_kv_pairs) {
            size_t index = test_indices[op_index];
            std::string key = generate_key(index);
            
            if (op_index % batch_size < 8) {
                // Read operation
                auto op_start = std::chrono::high_resolution_clock::now();
                auto find_result = cursor->find(str_to_slice(key), false);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                if (find_result.done) {
                    result.successful_reads++;
                }
                
                double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start).count();
                result.read_latencies_us.push_back(latency_us);
            } else {
                // Write operation
                std::string new_value = generate_value(index + round_number * 1000000);
                
                auto op_start = std::chrono::high_resolution_clock::now();
                cursor->upsert(str_to_slice(key), str_to_slice(new_value));
                auto op_end = std::chrono::high_resolution_clock::now();
                
                result.successful_writes++;
                
                double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start).count();
                result.write_latencies_us.push_back(latency_us);
            }
            
            op_index++;
        }
        
        // Measure commit time
        auto commit_start = std::chrono::high_resolution_clock::now();
        rw_txn.commit_and_stop();
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
    }
    
    auto test_end = std::chrono::high_resolution_clock::now();
    result.mixed_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        test_end - test_start).count();
    
    // Calculate separate read and write times
    result.read_time_ms = 0; // Individual read timing not tracked in mixed mode
    result.write_time_ms = 0; // Individual write timing not tracked in mixed mode
    
    result.successful_mixed = result.successful_reads + result.successful_writes;
    
    // Combine read and write latencies for mixed latency stats
    result.mixed_latencies_us.reserve(result.read_latencies_us.size() + result.write_latencies_us.size());
    result.mixed_latencies_us.insert(result.mixed_latencies_us.end(), 
                                   result.read_latencies_us.begin(), result.read_latencies_us.end());
    result.mixed_latencies_us.insert(result.mixed_latencies_us.end(), 
                                   result.write_latencies_us.begin(), result.write_latencies_us.end());
    
    calculate_latency_stats(result);
    
    fmt::println("✓ Completed {} mixed operations (reads: {}, writes: {})", 
                 result.successful_mixed, result.successful_reads, result.successful_writes);
    fmt::println("✓ Total mixed time: {:.2f} ms", result.mixed_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", result.commit_time_ms);
    fmt::println("✓ Average read latency: {:.2f} μs", result.avg_read_latency_us);
    fmt::println("✓ Average write latency: {:.2f} μs", result.avg_write_latency_us);
    fmt::println("✓ Average mixed latency: {:.2f} μs", result.avg_mixed_latency_us);
    fmt::println("✓ Tp99 mixed latency: {:.2f} μs", result.tp99_mixed_latency_us);
    fmt::println("✓ Mixed throughput: {:.2f} ops/sec", 
                 static_cast<double>(result.successful_mixed) / (result.mixed_time_ms / 1000.0));
    
    return result;
}

// Run comprehensive benchmark with all test modes
std::vector<RoundResult> run_comprehensive_benchmark(::mdbx::env_managed& env, const BenchConfig& config) {
    fmt::println("\n=== Running Comprehensive Benchmark Suite ===");
    fmt::println("Test rounds per mode: {}", config.test_rounds);
    
    std::vector<RoundResult> results;
    results.reserve(config.test_rounds * 4); // 4 test modes
    
    // Test Mode 1: Read-only tests
    fmt::println("\n--- READ-ONLY TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_read_test(env, round, config);
        results.push_back(result);
    }
    
    // Test Mode 2: Write-only tests
    fmt::println("\n--- WRITE-ONLY TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_write_test(env, round, config);
        results.push_back(result);
    }
    
    // Test Mode 3: Update tests
    fmt::println("\n--- UPDATE TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_update_test(env, round, config);
        results.push_back(result);
    }
    
    // Test Mode 4: Mixed read-write tests
    fmt::println("\n--- MIXED READ-WRITE TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_mixed_test(env, round, config);
        results.push_back(result);
    }
    
    return results;
}

// Print comprehensive summary statistics
void print_comprehensive_summary(const std::vector<RoundResult>& results, const BenchConfig& config) {
    fmt::println("\n=== Comprehensive Benchmark Summary ===");
    fmt::println("Total test results: {}", results.size());
    fmt::println("Database contains {} total KV pairs", config.total_kv_pairs);
    fmt::println("Each round tested {} KV pairs", config.test_kv_pairs);
    
    if (results.empty()) {
        fmt::println("No results to summarize");
        return;
    }
    
    // Separate results by test type
    std::vector<RoundResult> read_results, write_results, update_results, mixed_results;
    for (const auto& result : results) {
        if (result.successful_reads > 0 && result.successful_writes == 0 && result.successful_mixed == 0) {
            read_results.push_back(result);
        } else if (result.successful_writes > 0 && result.successful_reads == 0 && result.successful_mixed == 0) {
            write_results.push_back(result);
        } else if (result.successful_mixed > 0 && result.read_time_ms > 0 && result.write_time_ms > 0) {
            // UPDATE mode: has separate read and write times
            update_results.push_back(result);
        } else if (result.successful_mixed > 0 && result.read_time_ms == 0 && result.write_time_ms == 0 && result.mixed_time_ms > 0) {
            // MIXED mode: has only mixed time
            mixed_results.push_back(result);
        }
    }
    
    auto print_mode_stats = [](const std::vector<RoundResult>& mode_results, const std::string& mode_name) {
        if (mode_results.empty()) return;
        
        fmt::println("\n--- {} TEST RESULTS ---", mode_name);
        
        double total_avg_latency = 0.0, total_tp99_latency = 0.0;
        double total_time = 0.0, total_commit_time = 0.0;
        size_t total_operations = 0;
        
        fmt::println("Per-Round Results:");
        for (const auto& result : mode_results) {
            if (mode_name == "READ-ONLY") {
                fmt::println("  Round {}: Time={:.2f}ms, Success={}, Avg={:.1f}μs, Tp99={:.1f}μs",
                           result.round_number, result.read_time_ms, result.successful_reads,
                           result.avg_read_latency_us, result.tp99_read_latency_us);
                total_avg_latency += result.avg_read_latency_us;
                total_tp99_latency += result.tp99_read_latency_us;
                total_time += result.read_time_ms;
                total_operations += result.successful_reads;
            } else if (mode_name == "WRITE-ONLY") {
                fmt::println("  Round {}: Time={:.2f}ms, Commit={:.2f}ms, Success={}, Avg={:.1f}μs, Tp99={:.1f}μs",
                           result.round_number, result.write_time_ms, result.commit_time_ms, 
                           result.successful_writes, result.avg_write_latency_us, result.tp99_write_latency_us);
                total_avg_latency += result.avg_write_latency_us;
                total_tp99_latency += result.tp99_write_latency_us;
                total_time += result.write_time_ms;
                total_commit_time += result.commit_time_ms;
                total_operations += result.successful_writes;
            } else if (mode_name == "UPDATE") {
                fmt::println("  Round {}: ReadTime={:.2f}ms, WriteTime={:.2f}ms, Commit={:.2f}ms, Success={} (r:{}, w:{}), Avg={:.1f}μs, Tp99={:.1f}μs",
                           result.round_number, result.read_time_ms, result.write_time_ms, result.commit_time_ms,
                           result.successful_mixed, result.successful_reads, result.successful_writes, 
                           result.avg_mixed_latency_us, result.tp99_mixed_latency_us);
                total_avg_latency += result.avg_mixed_latency_us;
                total_tp99_latency += result.tp99_mixed_latency_us;
                total_time += result.read_time_ms + result.write_time_ms;
                total_commit_time += result.commit_time_ms;
                total_operations += result.successful_mixed;
            } else if (mode_name == "MIXED") {
                fmt::println("  Round {}: Time={:.2f}ms, Commit={:.2f}ms, Success={} (r:{}, w:{}), Avg={:.1f}μs, Tp99={:.1f}μs",
                           result.round_number, result.mixed_time_ms, result.commit_time_ms,
                           result.successful_mixed, result.successful_reads, result.successful_writes,
                           result.avg_mixed_latency_us, result.tp99_mixed_latency_us);
                total_avg_latency += result.avg_mixed_latency_us;
                total_tp99_latency += result.tp99_mixed_latency_us;
                total_time += result.mixed_time_ms;
                total_commit_time += result.commit_time_ms;
                total_operations += result.successful_mixed;
            }
        }
        
        double avg_avg_latency = total_avg_latency / mode_results.size();
        double avg_tp99_latency = total_tp99_latency / mode_results.size();
        double avg_time = total_time / mode_results.size();
        double avg_commit_time = total_commit_time / mode_results.size();
        double avg_throughput = (static_cast<double>(total_operations) / mode_results.size()) / (avg_time / 1000.0);
        
        fmt::println("Summary Statistics:");
        fmt::println("  Average Latency: {:.1f} μs", avg_avg_latency);
        fmt::println("  Tp99 Latency: {:.1f} μs", avg_tp99_latency);
        fmt::println("  Average Time: {:.2f} ms", avg_time);
        if (avg_commit_time > 0) {
            fmt::println("  Average Commit Time: {:.2f} ms", avg_commit_time);
        }
        fmt::println("  Average Throughput: {:.2f} ops/sec", avg_throughput);
    };
    
    print_mode_stats(read_results, "READ-ONLY");
    print_mode_stats(write_results, "WRITE-ONLY");  
    print_mode_stats(update_results, "UPDATE");
    print_mode_stats(mixed_results, "MIXED");
}

void setup_environment(const std::string& db_path) {
    fmt::println("\n=== Setting up Test Environment ===");
    
    // Check if database directory already exists
    if (std::filesystem::exists(db_path)) {
        fmt::println(stderr, "❌ Error: Database directory already exists: {}", db_path);
        fmt::println(stderr, "");
        fmt::println(stderr, "Please manually remove or rename the existing database directory:");
        fmt::println(stderr, "  rm -rf {}", db_path);
        fmt::println(stderr, "  or");
        fmt::println(stderr, "  mv {} {}_backup_$(date +%%Y%%m%%d_%%H%%M%%S)", db_path, db_path);
        fmt::println(stderr, "");
        fmt::println(stderr, "This prevents accidental data loss during benchmark testing.");
        throw std::runtime_error("Database directory already exists");
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
    fmt::println("  MDBX_BENCH_BATCH_SIZE      Batch size for database population");
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
    fmt::println("  \"batch_size\": 1000000,");
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
        
        // Run comprehensive benchmark suite
        auto results = run_comprehensive_benchmark(env, bench_config);
        
        // Print comprehensive summary
        print_comprehensive_summary(results, bench_config);
        
        fmt::println("\n✓ All benchmarks completed successfully! 🎉");
        
    } catch (const std::exception& e) {
        fmt::println(stderr, "\n❌ Benchmark failed: {}", e.what());
        return 1;
    }
    
    return 0;
}