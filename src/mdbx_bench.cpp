#include "db/mdbx.hpp"
#include "utils/string_utils.hpp"
#include "mdbx_bench_util.hpp"
#include <fmt/format.h>
#include <string>
#include <vector>
#include <chrono>
#include <cassert>
#include <cstring>
#include <filesystem>

using namespace datastore::kvdb;
using namespace utils;

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

// Perform read-only test
RoundResult perform_read_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    auto ctx = init_test_context(round_number, config, "Read");
    
    fmt::println("Reading {} randomly selected KV pairs", config.test_kv_pairs);
    auto read_start = std::chrono::high_resolution_clock::now();
    
    ctx.result.read_latencies_us.reserve(config.test_kv_pairs);
    
    {
        ROTxnManaged ro_txn(env);
        MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = ro_txn.ro_cursor(table_config);
        
        for (size_t index : ctx.test_indices) {
            std::string key = generate_key(index);
            
            double latency_us = measure_operation_us([&]() {
                auto find_result = cursor->find(str_to_slice(key), false);
                if (find_result.done) {
                    ctx.result.successful_reads++;
                }
            });
            
            ctx.result.read_latencies_us.push_back(latency_us);
        }
        
        ro_txn.abort();
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    ctx.result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    calculate_latency_stats(ctx.result);
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", ctx.result.successful_reads, ctx.result.read_time_ms);
    fmt::println("✓ Average read latency: {:.2f} μs", ctx.result.avg_read_latency_us);
    fmt::println("✓ Tp99 read latency: {:.2f} μs", ctx.result.tp99_read_latency_us);
    fmt::println("✓ Read throughput: {:.2f} ops/sec", 
                 static_cast<double>(ctx.result.successful_reads) / (ctx.result.read_time_ms / 1000.0));
    
    return ctx.result;
}

// Perform write-only test
RoundResult perform_write_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    auto ctx = init_test_context(round_number, config, "Write");
    
    fmt::println("Writing {} randomly selected KV pairs", config.test_kv_pairs);
    
    ctx.result.write_latencies_us.reserve(config.test_kv_pairs);
    
    {
        RWTxnManaged rw_txn(env);
        MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = rw_txn.rw_cursor(table_config);
        
        auto write_start = std::chrono::high_resolution_clock::now();
        
        for (size_t i = 0; i < ctx.test_indices.size(); ++i) {
            size_t index = ctx.test_indices[i];
            std::string key = generate_key(index);
            std::string new_value = generate_value(index + round_number * 1000000);
            
            double latency_us = measure_operation_us([&]() {
                cursor->upsert(str_to_slice(key), str_to_slice(new_value));
                ctx.result.successful_writes++;
            });
            
            ctx.result.write_latencies_us.push_back(latency_us);
        }
        
        auto write_end = std::chrono::high_resolution_clock::now();
        ctx.result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start).count();
        
        ctx.result.commit_time_ms = measure_operation_us([&]() {
            rw_txn.commit_and_stop();
        }) / 1000.0;
    }
    
    calculate_latency_stats(ctx.result);
    
    fmt::println("✓ Wrote {} KV pairs in {:.2f} ms", ctx.result.successful_writes, ctx.result.write_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", ctx.result.commit_time_ms);
    fmt::println("✓ Average write latency: {:.2f} μs", ctx.result.avg_write_latency_us);
    fmt::println("✓ Tp99 write latency: {:.2f} μs", ctx.result.tp99_write_latency_us);
    fmt::println("✓ Write throughput: {:.2f} ops/sec", 
                 static_cast<double>(ctx.result.successful_writes) / (ctx.result.write_time_ms / 1000.0));
    
    return ctx.result;
}

// Perform update test (read-then-update pattern like legacy mode)
RoundResult perform_update_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    auto ctx = init_test_context(round_number, config, "Update");
    
    fmt::println("Reading {} randomly selected KV pairs", config.test_kv_pairs);
    auto read_start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::pair<std::string, std::string>> read_data;
    read_data.reserve(config.test_kv_pairs);
    ctx.result.read_latencies_us.reserve(config.test_kv_pairs);
    
    {
        ROTxnManaged ro_txn(env);
        MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = ro_txn.ro_cursor(table_config);
        
        for (size_t index : ctx.test_indices) {
            std::string key = generate_key(index);
            
            double latency_us = measure_operation_us([&]() {
                auto find_result = cursor->find(str_to_slice(key), false);
                if (find_result.done) {
                    std::string value = std::string(find_result.value.as_string());
                    read_data.emplace_back(std::move(key), std::move(value));
                    ctx.result.successful_reads++;
                }
            });
            
            ctx.result.read_latencies_us.push_back(latency_us);
        }
        
        ro_txn.abort();
    }
    
    auto read_end = std::chrono::high_resolution_clock::now();
    ctx.result.read_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        read_end - read_start).count();
    
    fmt::println("✓ Read {} KV pairs in {:.2f} ms", ctx.result.successful_reads, ctx.result.read_time_ms);
    
    fmt::println("Updating and committing {} KV pairs", ctx.result.successful_reads);
    
    ctx.result.write_latencies_us.reserve(ctx.result.successful_reads);
    
    {
        RWTxnManaged rw_txn(env);
        MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = rw_txn.rw_cursor(table_config);
        
        auto write_start = std::chrono::high_resolution_clock::now();
        
        for (size_t i = 0; i < read_data.size(); ++i) {
            const auto& [key, old_value] = read_data[i];
            std::string new_value = generate_value(i + round_number * 1000000);
            
            double latency_us = measure_operation_us([&]() {
                cursor->upsert(str_to_slice(key), str_to_slice(new_value));
                ctx.result.successful_writes++;
            });
            
            ctx.result.write_latencies_us.push_back(latency_us);
        }
        
        auto write_end = std::chrono::high_resolution_clock::now();
        ctx.result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start).count();
        
        ctx.result.commit_time_ms = measure_operation_us([&]() {
            rw_txn.commit_and_stop();
        }) / 1000.0;
    }
    
    // Calculate mixed metrics
    ctx.result.successful_mixed = ctx.result.successful_reads + ctx.result.successful_writes;
    ctx.result.mixed_time_ms = ctx.result.read_time_ms + ctx.result.write_time_ms;
    
    // Combine read and write latencies
    ctx.result.mixed_latencies_us.reserve(ctx.result.read_latencies_us.size() + ctx.result.write_latencies_us.size());
    ctx.result.mixed_latencies_us.insert(ctx.result.mixed_latencies_us.end(), 
                                   ctx.result.read_latencies_us.begin(), ctx.result.read_latencies_us.end());
    ctx.result.mixed_latencies_us.insert(ctx.result.mixed_latencies_us.end(), 
                                   ctx.result.write_latencies_us.begin(), ctx.result.write_latencies_us.end());
    
    calculate_latency_stats(ctx.result);
    
    fmt::println("✓ Updated and committed {} KV pairs", ctx.result.successful_writes);
    fmt::println("✓ Total mixed operations: {} (read: {}, write: {})", 
                 ctx.result.successful_mixed, ctx.result.successful_reads, ctx.result.successful_writes);
    fmt::println("✓ Read time: {:.2f} ms, Write time: {:.2f} ms", ctx.result.read_time_ms, ctx.result.write_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", ctx.result.commit_time_ms);
    fmt::println("✓ Average read latency: {:.2f} μs", ctx.result.avg_read_latency_us);
    fmt::println("✓ Average write latency: {:.2f} μs", ctx.result.avg_write_latency_us);
    fmt::println("✓ Average mixed latency: {:.2f} μs", ctx.result.avg_mixed_latency_us);
    fmt::println("✓ Tp99 mixed latency: {:.2f} μs", ctx.result.tp99_mixed_latency_us);
    fmt::println("✓ Mixed throughput: {:.2f} ops/sec", 
                 static_cast<double>(ctx.result.successful_mixed) / (ctx.result.mixed_time_ms / 1000.0));
    
    return ctx.result;
}

// Perform mixed read-write test with 80:20 ratio
RoundResult perform_mixed_test(::mdbx::env_managed& env, size_t round_number, const BenchConfig& config) {
    auto ctx = init_test_context(round_number, config, "Mixed Read-Write");
    
    fmt::println("Performing {} mixed operations from {} total KV pairs", 
                 config.test_kv_pairs, config.total_kv_pairs);
    
    // Calculate 8:2 ratio (every 8 reads followed by 2 writes)
    size_t batch_size = 10; // 8 reads + 2 writes = 10 operations per batch
    size_t num_batches = config.test_kv_pairs / batch_size;
    size_t remaining_ops = config.test_kv_pairs % batch_size;
    
    size_t read_count = num_batches * 8 + std::min(remaining_ops, static_cast<size_t>(8));
    size_t write_count = num_batches * 2 + std::max(static_cast<int>(remaining_ops) - 8, 0);
    
    fmt::println("Mixed operations: {} reads, {} writes (8:2 pattern)", read_count, write_count);
    
    ctx.result.read_latencies_us.reserve(read_count);
    ctx.result.write_latencies_us.reserve(write_count);
    
    auto test_start = std::chrono::high_resolution_clock::now();
    
    {
        // Use read-write transaction for mixed operations
        RWTxnManaged rw_txn(env);
        MapConfig table_config{"bench_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = rw_txn.rw_cursor(table_config);
        
        // Perform mixed operations with 8:2 pattern
        size_t op_index = 0;
        for (size_t batch = 0; batch < num_batches; ++batch) {
            // 8 reads in this batch
            for (int read_in_batch = 0; read_in_batch < 8 && op_index < config.test_kv_pairs; ++read_in_batch) {
                size_t index = ctx.test_indices[op_index];
                std::string key = generate_key(index);
                
                double latency_us = measure_operation_us([&]() {
                    auto find_result = cursor->find(str_to_slice(key), false);
                    if (find_result.done) {
                        ctx.result.successful_reads++;
                    }
                });
                
                ctx.result.read_latencies_us.push_back(latency_us);
                op_index++;
            }
            
            // 2 writes in this batch
            for (int write_in_batch = 0; write_in_batch < 2 && op_index < config.test_kv_pairs; ++write_in_batch) {
                size_t index = ctx.test_indices[op_index];
                std::string key = generate_key(index);
                std::string new_value = generate_value(index + round_number * 1000000);
                
                double latency_us = measure_operation_us([&]() {
                    cursor->upsert(str_to_slice(key), str_to_slice(new_value));
                    ctx.result.successful_writes++;
                });
                
                ctx.result.write_latencies_us.push_back(latency_us);
                op_index++;
            }
        }
        
        // Handle remaining operations (if any)
        while (op_index < config.test_kv_pairs) {
            size_t index = ctx.test_indices[op_index];
            std::string key = generate_key(index);
            
            if (op_index % batch_size < 8) {
                // Read operation
                double latency_us = measure_operation_us([&]() {
                    auto find_result = cursor->find(str_to_slice(key), false);
                    if (find_result.done) {
                        ctx.result.successful_reads++;
                    }
                });
                ctx.result.read_latencies_us.push_back(latency_us);
            } else {
                // Write operation
                std::string new_value = generate_value(index + round_number * 1000000);
                
                double latency_us = measure_operation_us([&]() {
                    cursor->upsert(str_to_slice(key), str_to_slice(new_value));
                    ctx.result.successful_writes++;
                });
                ctx.result.write_latencies_us.push_back(latency_us);
            }
            
            op_index++;
        }
        
        ctx.result.commit_time_ms = measure_operation_us([&]() {
            rw_txn.commit_and_stop();
        }) / 1000.0;
    }
    
    auto test_end = std::chrono::high_resolution_clock::now();
    ctx.result.mixed_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        test_end - test_start).count();
    
    // Calculate separate read and write times
    ctx.result.read_time_ms = 0; // Individual read timing not tracked in mixed mode
    ctx.result.write_time_ms = 0; // Individual write timing not tracked in mixed mode
    
    ctx.result.successful_mixed = ctx.result.successful_reads + ctx.result.successful_writes;
    
    // Combine read and write latencies for mixed latency stats
    ctx.result.mixed_latencies_us.reserve(ctx.result.read_latencies_us.size() + ctx.result.write_latencies_us.size());
    ctx.result.mixed_latencies_us.insert(ctx.result.mixed_latencies_us.end(), 
                                   ctx.result.read_latencies_us.begin(), ctx.result.read_latencies_us.end());
    ctx.result.mixed_latencies_us.insert(ctx.result.mixed_latencies_us.end(), 
                                   ctx.result.write_latencies_us.begin(), ctx.result.write_latencies_us.end());
    
    calculate_latency_stats(ctx.result);
    
    fmt::println("✓ Completed {} mixed operations (reads: {}, writes: {})", 
                 ctx.result.successful_mixed, ctx.result.successful_reads, ctx.result.successful_writes);
    fmt::println("✓ Total mixed time: {:.2f} ms", ctx.result.mixed_time_ms);
    fmt::println("✓ Commit time: {:.2f} ms", ctx.result.commit_time_ms);
    fmt::println("✓ Average read latency: {:.2f} μs", ctx.result.avg_read_latency_us);
    fmt::println("✓ Average write latency: {:.2f} μs", ctx.result.avg_write_latency_us);
    fmt::println("✓ Average mixed latency: {:.2f} μs", ctx.result.avg_mixed_latency_us);
    fmt::println("✓ Tp99 mixed latency: {:.2f} μs", ctx.result.tp99_mixed_latency_us);
    fmt::println("✓ Mixed throughput: {:.2f} ops/sec", 
                 static_cast<double>(ctx.result.successful_mixed) / (ctx.result.mixed_time_ms / 1000.0));
    
    return ctx.result;
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
    if (bench_config.db_path == "/data/mdbx_bench" && !env_config.path.empty()) {
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