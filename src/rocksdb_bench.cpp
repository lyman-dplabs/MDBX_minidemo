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
#include <memory>
#include <algorithm>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>


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
    std::string db_path = "/tmp/rocksdb_bench";
};

// RocksDBConfig loader with JSON support
struct RocksDBConfig {
    std::string path = "/tmp/rocksdb_bench";
    bool create_if_missing = true;
    int max_open_files = 300;
    size_t write_buffer_size = 64 << 20;    // 64MB
    int max_write_buffer_number = 3;
    size_t target_file_size_base = 64 << 20;  // 64MB
    size_t max_bytes_for_level_base = 256 << 20;  // 256MB
    int level0_file_num_compaction_trigger = 4;
    int level0_slowdown_writes_trigger = 20;
    int level0_stop_writes_trigger = 36;
};

RocksDBConfig load_rocksdb_config(const std::string& config_file) {
    RocksDBConfig config;
    
    // Set defaults
    config.path = "/tmp/rocksdb_bench";
    config.create_if_missing = true;
    config.max_open_files = 300;
    config.write_buffer_size = 64 << 20;
    config.max_write_buffer_number = 3;
    config.target_file_size_base = 64 << 20;
    config.max_bytes_for_level_base = 256 << 20;
    config.level0_file_num_compaction_trigger = 4;
    config.level0_slowdown_writes_trigger = 20;
    config.level0_stop_writes_trigger = 36;
    
    // Try to load from file if it exists
    if (!config_file.empty() && std::filesystem::exists(config_file)) {
        try {
            std::ifstream file(config_file);
            Json::Value root;
            file >> root;
            
            if (root.isMember("path")) config.path = root["path"].asString();
            if (root.isMember("create_if_missing")) config.create_if_missing = root["create_if_missing"].asBool();
            if (root.isMember("max_open_files")) config.max_open_files = root["max_open_files"].asInt();
            if (root.isMember("write_buffer_size")) config.write_buffer_size = root["write_buffer_size"].asUInt64();
            if (root.isMember("max_write_buffer_number")) config.max_write_buffer_number = root["max_write_buffer_number"].asInt();
            if (root.isMember("target_file_size_base")) config.target_file_size_base = root["target_file_size_base"].asUInt64();
            if (root.isMember("max_bytes_for_level_base")) config.max_bytes_for_level_base = root["max_bytes_for_level_base"].asUInt64();
            if (root.isMember("level0_file_num_compaction_trigger")) config.level0_file_num_compaction_trigger = root["level0_file_num_compaction_trigger"].asInt();
            if (root.isMember("level0_slowdown_writes_trigger")) config.level0_slowdown_writes_trigger = root["level0_slowdown_writes_trigger"].asInt();
            if (root.isMember("level0_stop_writes_trigger")) config.level0_stop_writes_trigger = root["level0_stop_writes_trigger"].asInt();
            
            fmt::println("✓ Loaded RocksDBConfig from: {}", config_file);
        } catch (const std::exception& e) {
            fmt::println("⚠ Failed to load config file {}, using defaults: {}", config_file, e.what());
        }
    } else {
        fmt::println("✓ Using default RocksDBConfig (file not found: {})", config_file);
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
    config.db_path = "/tmp/rocksdb_bench";
    
    // Load from environment variables first
    if (const char* env_val = std::getenv("ROCKSDB_BENCH_TOTAL_KV_PAIRS")) {
        try {
            config.total_kv_pairs = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid ROCKSDB_BENCH_TOTAL_KV_PAIRS: {}", env_val);
        }
    }
    
    if (const char* env_val = std::getenv("ROCKSDB_BENCH_TEST_KV_PAIRS")) {
        try {
            config.test_kv_pairs = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid ROCKSDB_BENCH_TEST_KV_PAIRS: {}", env_val);
        }
    }
    
    if (const char* env_val = std::getenv("ROCKSDB_BENCH_TEST_ROUNDS")) {
        try {
            config.test_rounds = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid ROCKSDB_BENCH_TEST_ROUNDS: {}", env_val);
        }
    }
    
    if (const char* env_val = std::getenv("ROCKSDB_BENCH_DB_PATH")) {
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

// RocksDB wrapper class for consistent interface
class RocksDBBench {
private:
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::Options options_;
    
public:
    RocksDBBench(const RocksDBConfig& config) {
        // Configure RocksDB options
        options_.create_if_missing = config.create_if_missing;
        options_.max_open_files = config.max_open_files;
        options_.write_buffer_size = config.write_buffer_size;
        options_.max_write_buffer_number = config.max_write_buffer_number;
        options_.target_file_size_base = config.target_file_size_base;
        options_.max_bytes_for_level_base = config.max_bytes_for_level_base;
        options_.level0_file_num_compaction_trigger = config.level0_file_num_compaction_trigger;
        options_.level0_slowdown_writes_trigger = config.level0_slowdown_writes_trigger;
        options_.level0_stop_writes_trigger = config.level0_stop_writes_trigger;
        
        // Open database
        rocksdb::DB* raw_db = nullptr;
        rocksdb::Status status = rocksdb::DB::Open(options_, config.path, &raw_db);
        
        if (!status.ok()) {
            throw std::runtime_error(fmt::format("RocksDB initialization failed: {}", status.ToString()));
        }
        
        db_.reset(raw_db);
    }
    
    void put(const std::string& key, const std::string& value) {
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
        if (!status.ok()) {
            throw std::runtime_error(fmt::format("RocksDB put operation failed: {}", status.ToString()));
        }
    }
    
    bool get(const std::string& key, std::string& value) {
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
        return status.ok();
    }
    
    rocksdb::DB* get_db() { return db_.get(); }
};

// Populate RocksDB database with initial dataset
void populate_database(RocksDBBench& db, const BenchConfig& config) {
    fmt::println("\n=== Populating Database ===");
    fmt::println("Inserting {} KV pairs into database", config.total_kv_pairs);
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Use write batch for better performance
    rocksdb::WriteBatch batch;
    const size_t batch_size = 10000;
    
    for (size_t i = 0; i < config.total_kv_pairs; ++i) {
        std::string key = generate_key(i);
        std::string value = generate_value(i);
        batch.Put(key, value);
        
        // Commit batch every batch_size operations
        if ((i + 1) % batch_size == 0) {
            auto commit_start = std::chrono::high_resolution_clock::now();
            rocksdb::Status status = db.get_db()->Write(rocksdb::WriteOptions(), &batch);
            auto commit_end = std::chrono::high_resolution_clock::now();
            
            if (!status.ok()) {
                throw std::runtime_error(fmt::format("RocksDB batch write failed: {}", status.ToString()));
            }
            
            batch.Clear();
            
            // Progress indicator for large datasets
            if ((i + 1) % 100000 == 0) {
                auto commit_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    commit_end - commit_start).count();
                fmt::println("  Inserted {}/{} KV pairs, batch commit: {} ms", i + 1, config.total_kv_pairs, commit_duration);
            }
        }
    }
    
    // Commit remaining items
    if (batch.Count() > 0) {
        auto commit_start = std::chrono::high_resolution_clock::now();
        rocksdb::Status status = db.get_db()->Write(rocksdb::WriteOptions(), &batch);
        auto commit_end = std::chrono::high_resolution_clock::now();
        
        if (!status.ok()) {
            throw std::runtime_error(fmt::format("RocksDB final batch write failed: {}", status.ToString()));
        }
        
        auto commit_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            commit_end - commit_start).count();
        fmt::println("✓ Final batch commit time: {} ms", commit_duration);
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

// Perform one round of read-update-commit test
RoundResult perform_test_round(RocksDBBench& db, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Test Round {} ===", round_number);
    
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
    
    result.successful_reads = 0;
    for (size_t index : test_indices) {
        std::string key = generate_key(index);
        std::string value;
        if (db.get(key, value)) {
            read_data.emplace_back(std::move(key), std::move(value));
            result.successful_reads++;
        }
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", result.successful_reads, result.read_time_ms);
    
    // Step 3: Update the read data and commit
    fmt::println("Updating and committing {} KV pairs", result.successful_reads);
    
    auto commit_start = std::chrono::high_resolution_clock::now();
    
    // Use write batch for better performance
    rocksdb::WriteBatch batch;
    
    // Update all read data with new values
    for (size_t i = 0; i < read_data.size(); ++i) {
        const auto& [key, old_value] = read_data[i];
        // Generate a new value for update (add round number to make it unique)
        std::string new_value = generate_value(i + round_number * 1000000);
        batch.Put(key, new_value);
    }
    
    // Commit all updates
    rocksdb::Status status = db.get_db()->Write(rocksdb::WriteOptions(), &batch);
    auto commit_end = std::chrono::high_resolution_clock::now();
    
    if (!status.ok()) {
        throw std::runtime_error(fmt::format("RocksDB batch update failed: {}", status.ToString()));
    }
    
    result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        commit_end - commit_start).count();
    
    fmt::println("✓ Updated and committed {} KV pairs", result.successful_reads);
    fmt::println("✓ Read time: {:.2f} ms", result.read_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", result.commit_time_ms);
    fmt::println("✓ Read throughput: {:.2f} ops/sec", 
                 static_cast<double>(result.successful_reads) / (result.read_time_ms / 1000.0));
    
    return result;
}

// Perform read-only test
RoundResult perform_read_test(RocksDBBench& db, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Read Test Round {} ===", round_number);
    
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
    
    for (size_t index : test_indices) {
        std::string key = generate_key(index);
        std::string value;
        
        auto op_start = std::chrono::high_resolution_clock::now();
        bool found = db.get(key, value);
        auto op_end = std::chrono::high_resolution_clock::now();
        
        if (found) {
            result.successful_reads++;
        }
        
        double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        result.read_latencies_us.push_back(latency_us);
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
RoundResult perform_write_test(RocksDBBench& db, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Write Test Round {} ===", round_number);
    
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
    
    auto write_start = std::chrono::high_resolution_clock::now();
    
    // Use write batch for better performance
    rocksdb::WriteBatch batch;
    
    for (size_t i = 0; i < test_indices.size(); ++i) {
        size_t index = test_indices[i];
        std::string key = generate_key(index);
        std::string new_value = generate_value(index + round_number * 1000000);
        
        auto op_start = std::chrono::high_resolution_clock::now();
        batch.Put(key, new_value);
        auto op_end = std::chrono::high_resolution_clock::now();
        
        result.successful_writes++;
        
        double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        result.write_latencies_us.push_back(latency_us);
    }
    
    auto write_end = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        write_end - write_start).count();
    
    // Commit all writes
    auto commit_start = std::chrono::high_resolution_clock::now();
    rocksdb::Status status = db.get_db()->Write(rocksdb::WriteOptions(), &batch);
    auto commit_end = std::chrono::high_resolution_clock::now();
    
    if (!status.ok()) {
        throw std::runtime_error(fmt::format("RocksDB batch write failed: {}", status.ToString()));
    }
    
    result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        commit_end - commit_start).count();
    
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
RoundResult perform_update_test(RocksDBBench& db, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Update Test Round {} ===", round_number);
    
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
    
    result.successful_reads = 0;
    for (size_t index : test_indices) {
        std::string key = generate_key(index);
        std::string value;
        
        auto op_start = std::chrono::high_resolution_clock::now();
        bool found = db.get(key, value);
        auto op_end = std::chrono::high_resolution_clock::now();
        
        if (found) {
            read_data.emplace_back(std::move(key), std::move(value));
            result.successful_reads++;
        }
        
        double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        result.read_latencies_us.push_back(latency_us);
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", result.successful_reads, result.read_time_ms);
    
    // Step 3: Update the read data and commit with individual latency tracking
    fmt::println("Updating and committing {} KV pairs", result.successful_reads);
    
    result.write_latencies_us.reserve(result.successful_reads);
    
    auto write_start = std::chrono::high_resolution_clock::now();
    
    // Use write batch for better performance
    rocksdb::WriteBatch batch;
    
    // Update all read data with new values
    result.successful_writes = 0;
    for (size_t i = 0; i < read_data.size(); ++i) {
        const auto& [key, old_value] = read_data[i];
        // Generate a new value for update (add round number to make it unique)
        std::string new_value = generate_value(i + round_number * 1000000);
        
        auto op_start = std::chrono::high_resolution_clock::now();
        batch.Put(key, new_value);
        auto op_end = std::chrono::high_resolution_clock::now();
        
        result.successful_writes++;
        
        double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        result.write_latencies_us.push_back(latency_us);
    }
    
    auto write_end = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        write_end - write_start).count();
    
    // Commit all updates
    auto commit_start = std::chrono::high_resolution_clock::now();
    rocksdb::Status status = db.get_db()->Write(rocksdb::WriteOptions(), &batch);
    auto commit_end = std::chrono::high_resolution_clock::now();
    
    if (!status.ok()) {
        throw std::runtime_error(fmt::format("RocksDB mixed batch update failed: {}", status.ToString()));
    }
    
    result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        commit_end - commit_start).count();
    
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
RoundResult perform_mixed_test(RocksDBBench& db, size_t round_number, const BenchConfig& config) {
    fmt::println("\n=== Mixed Read-Write Test Round {} ===", round_number);
    
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
    
    // Use WriteBatch for efficient writing
    rocksdb::WriteBatch batch;
    
    // Perform mixed operations with 8:2 pattern
    size_t op_index = 0;
    for (size_t batch_num = 0; batch_num < num_batches; ++batch_num) {
        // 8 reads in this batch
        for (int read_in_batch = 0; read_in_batch < 8 && op_index < config.test_kv_pairs; ++read_in_batch) {
            size_t index = test_indices[op_index];
            std::string key = generate_key(index);
            
            auto op_start = std::chrono::high_resolution_clock::now();
            std::string value;
            rocksdb::Status status = db.get_db()->Get(rocksdb::ReadOptions(), key, &value);
            auto op_end = std::chrono::high_resolution_clock::now();
            
            if (status.ok()) {
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
            batch.Put(key, new_value);
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
            std::string value;
            rocksdb::Status status = db.get_db()->Get(rocksdb::ReadOptions(), key, &value);
            auto op_end = std::chrono::high_resolution_clock::now();
            
            if (status.ok()) {
                result.successful_reads++;
            }
            
            double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.read_latencies_us.push_back(latency_us);
        } else {
            // Write operation (add to batch)
            std::string new_value = generate_value(index + round_number * 1000000);
            
            auto op_start = std::chrono::high_resolution_clock::now();
            batch.Put(key, new_value);
            auto op_end = std::chrono::high_resolution_clock::now();
            
            result.successful_writes++;
            
            double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.write_latencies_us.push_back(latency_us);
        }
        
        op_index++;
    }
    
    // Measure commit time (batch write)
    auto commit_start = std::chrono::high_resolution_clock::now();
    rocksdb::Status commit_status = db.get_db()->Write(rocksdb::WriteOptions(), &batch);
    auto commit_end = std::chrono::high_resolution_clock::now();
    
    result.commit_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        commit_end - commit_start).count();
    
    if (!commit_status.ok()) {
        fmt::println("Warning: Batch write failed: {}", commit_status.ToString());
        result.successful_writes = 0; // Reset write count on failure
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
std::vector<RoundResult> run_comprehensive_benchmark(RocksDBBench& db, const BenchConfig& config) {
    fmt::println("\n=== Running Comprehensive Benchmark Suite ===");
    fmt::println("Test rounds per mode: {}", config.test_rounds);
    
    std::vector<RoundResult> results;
    results.reserve(config.test_rounds * 4); // 4 test modes
    
    // Test Mode 1: Read-only tests
    fmt::println("\n--- READ-ONLY TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_read_test(db, round, config);
        results.push_back(result);
    }
    
    // Test Mode 2: Write-only tests
    fmt::println("\n--- WRITE-ONLY TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_write_test(db, round, config);
        results.push_back(result);
    }
    
    // Test Mode 3: Update tests
    fmt::println("\n--- UPDATE TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_update_test(db, round, config);
        results.push_back(result);
    }
    
    // Test Mode 4: Mixed read-write tests
    fmt::println("\n--- MIXED READ-WRITE TESTS ---");
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_mixed_test(db, round, config);
        results.push_back(result);
    }
    
    return results;
}

// Run all test rounds and collect results (legacy compatibility)
std::vector<RoundResult> run_benchmark_rounds(RocksDBBench& db, const BenchConfig& config) {
    fmt::println("\n=== Running {} Test Rounds ===", config.test_rounds);
    
    std::vector<RoundResult> results;
    results.reserve(config.test_rounds);
    
    for (size_t round = 1; round <= config.test_rounds; ++round) {
        auto result = perform_test_round(db, round, config);
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
    
    // Clean up any existing database
    if (std::filesystem::exists(db_path)) {
        std::filesystem::remove_all(db_path);
        fmt::println("✓ Cleaned existing database at: {}", db_path);
    }
}

void print_usage(const char* program_name) {
    fmt::println("Usage: {} [options]", program_name);
    fmt::println("Options:");
    fmt::println("  -c, --config FILE    Path to RocksDBConfig JSON file");
    fmt::println("  -b, --bench-config FILE  Path to BenchConfig JSON file");
    fmt::println("  -h, --help          Show this help message");
    fmt::println("");
    fmt::println("Environment Variables:");
    fmt::println("  ROCKSDB_BENCH_TOTAL_KV_PAIRS  Total KV pairs in database");
    fmt::println("  ROCKSDB_BENCH_TEST_KV_PAIRS   KV pairs to test per round");
    fmt::println("  ROCKSDB_BENCH_TEST_ROUNDS     Number of test rounds");
    fmt::println("  ROCKSDB_BENCH_DB_PATH         Database path");
    fmt::println("  Note: Key and value sizes are fixed at 32 bytes");
    fmt::println("");
    fmt::println("Example RocksDBConfig JSON file:");
    fmt::println("{{");
    fmt::println("  \"path\": \"/tmp/rocksdb_bench\",");
    fmt::println("  \"create_if_missing\": true,");
    fmt::println("  \"max_open_files\": 300,");
    fmt::println("  \"write_buffer_size\": 67108864,");
    fmt::println("  \"max_write_buffer_number\": 3");
    fmt::println("}}");
    fmt::println("");
    fmt::println("Example BenchConfig JSON file:");
    fmt::println("{{");
    fmt::println("  \"total_kv_pairs\": 2000000,");
    fmt::println("  \"test_kv_pairs\": 200000,");
    fmt::println("  \"test_rounds\": 5,");
    fmt::println("  \"db_path\": \"/tmp/rocksdb_bench_custom\"");
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
    RocksDBConfig rocksdb_config = load_rocksdb_config(config_file);
    BenchConfig bench_config = load_bench_config(bench_config_file);
    
    // Override db_path from rocksdb_config if not set in bench_config
    if (bench_config.db_path == "/tmp/rocksdb_bench" && !rocksdb_config.path.empty()) {
        bench_config.db_path = rocksdb_config.path;
    }
    
    fmt::println("=== RocksDB Performance Benchmark ===");
    fmt::println("Testing RocksDB performance with {}-byte keys and {}-byte values", 
                 BenchConfig::key_size, BenchConfig::value_size);
    fmt::println("Total KV pairs in DB: {}", bench_config.total_kv_pairs);
    fmt::println("KV pairs per test round: {}", bench_config.test_kv_pairs);
    fmt::println("Number of test rounds: {}", bench_config.test_rounds);
    fmt::println("Database path: {}", bench_config.db_path);
    
    try {
        // Setup environment
        setup_environment(bench_config.db_path);
        
        // Update rocksdb_config path to match bench_config
        rocksdb_config.path = bench_config.db_path;
        
        // Open RocksDB database
        RocksDBBench db(rocksdb_config);
        fmt::println("✓ Opened RocksDB database at: {}", rocksdb_config.path);
        
        // Populate database with initial data
        populate_database(db, bench_config);
        
        // Run comprehensive benchmark suite
        auto results = run_comprehensive_benchmark(db, bench_config);
        
        // Print comprehensive summary
        print_comprehensive_summary(results, bench_config);
        
        fmt::println("\n✓ All benchmarks completed successfully! 🎉");
        
    } catch (const std::exception& e) {
        fmt::println(stderr, "\n❌ Benchmark failed: {}", e.what());
        return 1;
    }
    
    return 0;
}