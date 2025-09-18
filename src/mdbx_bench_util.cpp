#include "mdbx_bench_util.hpp"
#include "db/mdbx.hpp"
#include "utils/string_utils.hpp"
#include <fmt/format.h>
#include <random>
#include <unordered_set>
#include <filesystem>
#include <fstream>
#include <cstdlib>
#include <algorithm>
#include <functional>

using namespace datastore::kvdb;
using namespace utils;

// EnvConfig defaults initializer
EnvConfig create_default_env_config() {
    EnvConfig config;
    config.path = "/data/mdbx_bench";
    config.create = true;
    config.readonly = false;
    config.exclusive = false;
    config.in_memory = false;
    config.no_meta_sync = false;
    config.rp_augment_limit = 32_Mebi;
    config.txn_dp_initial = 16_Kibi;
    config.dp_reserve_limit = 16_Kibi;
    config.txn_dp_limit_multiplier = 2;
    config.merge_threshold = 32_Kibi;
    config.enable_coalesce = true;
    config.enable_sync_durable = true;
    config.enable_notls = true;
    config.shared = false;
    config.read_ahead = false;
    config.write_map = false;
    config.page_size = 4_Kibi;
    config.max_size = 8_Gibi;
    config.growth_size = 1_Gibi;
    config.max_tables = 64;
    config.max_readers = 50;
    return config;
}

// EnvConfig JSON loader
void load_env_config_from_json(const Json::Value& root, EnvConfig& config) {
    if (root.isMember("path")) config.path = root["path"].asString();
    if (root.isMember("create")) config.create = root["create"].asBool();
    if (root.isMember("readonly")) config.readonly = root["readonly"].asBool();
    if (root.isMember("exclusive")) config.exclusive = root["exclusive"].asBool();
    if (root.isMember("in_memory")) config.in_memory = root["in_memory"].asBool();
    if (root.isMember("no_meta_sync")) config.no_meta_sync = root["no_meta_sync"].asBool();
    if (root.isMember("rp_augment_limit")) config.rp_augment_limit = root["rp_augment_limit"].asUInt64();
    if (root.isMember("txn_dp_initial")) config.txn_dp_initial = root["txn_dp_initial"].asUInt64();
    if (root.isMember("dp_reserve_limit")) config.dp_reserve_limit = root["dp_reserve_limit"].asUInt64();
    if (root.isMember("txn_dp_limit_multiplier")) config.txn_dp_limit_multiplier = root["txn_dp_limit_multiplier"].asUInt();
    if (root.isMember("merge_threshold")) config.merge_threshold = root["merge_threshold"].asUInt64();
    if (root.isMember("enable_coalesce")) config.enable_coalesce = root["enable_coalesce"].asBool();
    if (root.isMember("enable_sync_durable")) config.enable_sync_durable = root["enable_sync_durable"].asBool();
    if (root.isMember("enable_notls")) config.enable_notls = root["enable_notls"].asBool();
    if (root.isMember("shared")) config.shared = root["shared"].asBool();
    if (root.isMember("read_ahead")) config.read_ahead = root["read_ahead"].asBool();
    if (root.isMember("write_map")) config.write_map = root["write_map"].asBool();
    if (root.isMember("page_size")) config.page_size = root["page_size"].asUInt64();
    if (root.isMember("max_size")) config.max_size = root["max_size"].asUInt64();
    if (root.isMember("growth_size")) config.growth_size = root["growth_size"].asUInt64();
    if (root.isMember("max_tables")) config.max_tables = root["max_tables"].asUInt();
    if (root.isMember("max_readers")) config.max_readers = root["max_readers"].asUInt();
}

// EnvConfig loader with JSON support
EnvConfig load_env_config(const std::string& config_file) {
    EnvConfig config = create_default_env_config();
    load_json_config_generic(config_file, [&config](const Json::Value& root) {
        load_env_config_from_json(root, config);
    });
    return config;
}

// Configuration loading functions

BenchConfig create_default_bench_config() {
    BenchConfig config;
    config.total_kv_pairs = 1000000;
    config.test_kv_pairs = 100000;
    config.test_rounds = 2;
    config.batch_size = 5000000;
    config.db_path = "/data/mdbx_bench";
    return config;
}

void load_env_var_size_t(const char* env_name, size_t& value) {
    if (const char* env_val = std::getenv(env_name)) {
        try {
            value = std::stoull(env_val);
        } catch (const std::exception& e) {
            fmt::println("⚠ Invalid {}: {}", env_name, env_val);
        }
    }
}

void load_env_var_string(const char* env_name, std::string& value) {
    if (const char* env_val = std::getenv(env_name)) {
        value = env_val;
    }
}

void load_bench_config_from_env(BenchConfig& config) {
    load_env_var_size_t("MDBX_BENCH_TOTAL_KV_PAIRS", config.total_kv_pairs);
    load_env_var_size_t("MDBX_BENCH_TEST_KV_PAIRS", config.test_kv_pairs);
    load_env_var_size_t("MDBX_BENCH_TEST_ROUNDS", config.test_rounds);
    load_env_var_size_t("MDBX_BENCH_BATCH_SIZE", config.batch_size);
    load_env_var_string("MDBX_BENCH_DB_PATH", config.db_path);
}

void load_bench_config_from_json(const Json::Value& root, BenchConfig& config) {
    if (root.isMember("total_kv_pairs")) config.total_kv_pairs = root["total_kv_pairs"].asUInt64();
    if (root.isMember("test_kv_pairs")) config.test_kv_pairs = root["test_kv_pairs"].asUInt64();
    if (root.isMember("test_rounds")) config.test_rounds = root["test_rounds"].asUInt64();
    if (root.isMember("batch_size")) config.batch_size = root["batch_size"].asUInt64();
    if (root.isMember("db_path")) config.db_path = root["db_path"].asString();
    
    if (root.isMember("key_size") || root.isMember("value_size")) {
        fmt::println("⚠ key_size and value_size are fixed at 32 bytes, ignoring config file values");
    }
}

bool load_json_config_generic(const std::string& config_file, const std::function<void(const Json::Value&)>& loader) {
    if (!config_file.empty() && std::filesystem::exists(config_file)) {
        try {
            std::ifstream file(config_file);
            Json::Value root;
            file >> root;
            loader(root);
            fmt::println("✓ Loaded config from: {}", config_file);
            return true;
        } catch (const std::exception& e) {
            fmt::println("⚠ Failed to load config file {}, using defaults: {}", config_file, e.what());
            return false;
        }
    } else if (!config_file.empty()) {
        fmt::println("✓ Using default config (file not found: {})", config_file);
    }
    return false;
}

BenchConfig load_bench_config(const std::string& config_file) {
    BenchConfig config = create_default_bench_config();
    load_bench_config_from_env(config);
    load_json_config_generic(config_file, [&config](const Json::Value& root) {
        load_bench_config_from_json(root, config);
    });
    return config;
}

// Data generation functions

std::string generate_key(size_t index) {
    std::string key = fmt::format("key_{:016x}", index);
    // Pad or truncate to exactly 32 bytes
    key.resize(32, '0');
    return key;
}

std::string generate_value(size_t index) {
    std::string value = fmt::format("value_{:016x}_data", index);
    // Pad or truncate to exactly 32 bytes
    value.resize(32, 'x');
    return value;
}

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

// Latency and statistics functions

void calc_latency_stats(const std::vector<double>& latencies, double& avg, double& tp99) {
    if (latencies.empty()) {
        avg = tp99 = 0.0;
        return;
    }
    
    double sum = 0.0;
    for (double lat : latencies) {
        sum += lat;
    }
    avg = sum / latencies.size();
    
    std::vector<double> sorted_latencies = latencies;
    std::sort(sorted_latencies.begin(), sorted_latencies.end());
    size_t tp99_index = static_cast<size_t>(sorted_latencies.size() * 0.99);
    if (tp99_index >= sorted_latencies.size()) {
        tp99_index = sorted_latencies.size() - 1;
    }
    tp99 = sorted_latencies[tp99_index];
}

void calculate_latency_stats(RoundResult& result) {
    calc_latency_stats(result.read_latencies_us, result.avg_read_latency_us, result.tp99_read_latency_us);
    calc_latency_stats(result.write_latencies_us, result.avg_write_latency_us, result.tp99_write_latency_us);
    calc_latency_stats(result.mixed_latencies_us, result.avg_mixed_latency_us, result.tp99_mixed_latency_us);
}

// Test utility functions

TestContext init_test_context(size_t round_number, const BenchConfig& config, const std::string& test_name) {
    fmt::println("\n=== {} Test Round {} ===", test_name, round_number);
    
    TestContext ctx;
    ctx.result.round_number = round_number;
    ctx.result.test_kv_count = config.test_kv_pairs;
    ctx.result.successful_reads = 0;
    ctx.result.successful_writes = 0;
    ctx.result.successful_mixed = 0;
    
    fmt::println("Generating {} random indices from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    ctx.test_indices = generate_random_indices(config.test_kv_pairs, config.total_kv_pairs);
    
    return ctx;
}

// Summary and output functions

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
    fmt::println("  \"path\": \"/data/mdbx_bench\",");
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
    fmt::println("  \"db_path\": \"/data/mdbx_bench_custom\"");
    fmt::println("  \"Note\": \"key_size and value_size are fixed at 32 bytes\"");
    fmt::println("}}");
}