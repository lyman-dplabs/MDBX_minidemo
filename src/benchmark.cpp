#include "core/query_engine.hpp"
#include "db/mdbx_impl.hpp"
#if HAVE_ROCKSDB
#include "db/rocksdb_impl.hpp"
#endif

#include <benchmark/benchmark.h>
#include <fmt/core.h>
#include <filesystem>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <mutex>

// --- Test Data Configuration ---
// Default values (can be overridden by environment variables or command line flags)
size_t get_num_accounts() {
    if (const char* env_val = std::getenv("BENCH_NUM_ACCOUNTS")) {
        return std::stoul(env_val);
    }
    return 10;
}

size_t get_num_blocks_per_account() {
    if (const char* env_val = std::getenv("BENCH_NUM_BLOCKS_PER_ACCOUNT")) {
        return std::stoul(env_val);
    }
    return 100;
}

uint64_t get_max_block_number() {
    if (const char* env_val = std::getenv("BENCH_MAX_BLOCK_NUMBER")) {
        return std::stoull(env_val);
    }
    return 10000;
}

static const size_t NUM_ACCOUNTS = get_num_accounts();
static const size_t NUM_BLOCKS_PER_ACCOUNT = get_num_blocks_per_account();
static const uint64_t MAX_BLOCK_NUMBER = get_max_block_number();

// --- Test Data Generation ---
class BenchmarkDataGenerator {
public:
    struct AccountData {
        std::string account_name;
        std::vector<uint64_t> block_numbers;
        std::vector<std::string> states;
    };

    static auto generate_test_data() -> std::vector<AccountData> {
        const size_t num_accounts = NUM_ACCOUNTS;
        const size_t num_blocks_per_account = NUM_BLOCKS_PER_ACCOUNT;
        const uint64_t max_block_number = MAX_BLOCK_NUMBER;
        
        fmt::print("Generating benchmark data with {} accounts, {} blocks per account, max block number {}\n",
                   num_accounts, num_blocks_per_account, max_block_number);
                   
        std::vector<AccountData> accounts;
        accounts.reserve(num_accounts);

        std::mt19937 gen{42}; // Fixed seed for reproducibility
        std::uniform_int_distribution<uint64_t> block_dist{1, MAX_BLOCK_NUMBER};

        for (size_t i = 0; i < num_accounts; ++i) {
            AccountData account;
            account.account_name = fmt::format("account_{:04d}", i);
            account.block_numbers.reserve(num_blocks_per_account);
            account.states.reserve(num_blocks_per_account);

            // Generate sorted block numbers
            std::set<uint64_t> unique_blocks;
            while (unique_blocks.size() < num_blocks_per_account) {
                unique_blocks.insert(block_dist(gen));
            }

            for (auto block : unique_blocks) {
                account.block_numbers.push_back(block);
                account.states.push_back(fmt::format(
                    R"({{"balance": "{}", "nonce": "{}", "block": "{}"}})",
                    block * 1000, block % 256, block));
            }

            accounts.push_back(std::move(account));
        }

        return accounts;
    }
};

// --- Benchmark Fixture ---
class DatabaseBenchmark : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) override {
        // Generate test data if not already done
        if (test_data_.empty()) {
            test_data_ = BenchmarkDataGenerator::generate_test_data();
        }
        
        // Initialize MDBX data if not already done (but don't keep connection)
        static bool mdbx_data_initialized = false;
        if (!mdbx_data_initialized) {
            cleanup_databases();
            auto temp_mdbx_engine = std::make_unique<QueryEngine>(std::make_unique<MdbxImpl>(mdbx_path_));
            populate_database(*temp_mdbx_engine);
            temp_mdbx_engine.reset(); // Close connection after populating data
            mdbx_data_initialized = true;
        }
        
#if HAVE_ROCKSDB
        // Initialize RocksDB data if not already done (but don't keep connection)
        static bool rocksdb_data_initialized = false;
        if (!rocksdb_data_initialized) {
            auto temp_rocksdb_engine = std::make_unique<QueryEngine>(std::make_unique<RocksDbImpl>(rocksdb_path_));
            populate_database(*temp_rocksdb_engine);
            temp_rocksdb_engine.reset(); // Close connection after populating data
            rocksdb_data_initialized = true;
        }
#endif

        // Prepare query data if not already done
        if (exact_queries_.empty()) {
            prepare_query_data();
        }
    }

    void TearDown(const benchmark::State& state) override {
        // Cleanup is handled by destructors
    }
    
    ~DatabaseBenchmark() {
        // Cleanup will happen at process exit
    }

protected:
    void populate_database(QueryEngine& engine) {
        for (const auto& account : test_data_) {
            for (size_t i = 0; i < account.block_numbers.size(); ++i) {
                engine.set_account_state(account.account_name, account.block_numbers[i], account.states[i]);
            }
        }
    }

    void prepare_query_data() {
        const size_t num_accounts = NUM_ACCOUNTS;
        const uint64_t max_block_number = MAX_BLOCK_NUMBER;
        
        std::mt19937 gen{123};
        std::uniform_int_distribution<size_t> account_dist{0, num_accounts - 1};
        std::uniform_int_distribution<uint64_t> block_dist{1, max_block_number};

        // Prepare exact match queries (query blocks that exist)
        for (size_t i = 0; i < 1000; ++i) {
            const auto& account = test_data_[account_dist(gen)];
            std::uniform_int_distribution<size_t> block_idx_dist{0, account.block_numbers.size() - 1};

            exact_queries_.emplace_back(account.account_name, account.block_numbers[block_idx_dist(gen)]);
        }

        // Prepare lookback queries (query blocks that may not exist, requiring historical lookup)
        for (size_t i = 0; i < 1000; ++i) {
            const auto& account = test_data_[account_dist(gen)];
            lookback_queries_.emplace_back(account.account_name, block_dist(gen));
        }
    }

    void cleanup_databases() {
        std::filesystem::remove_all(mdbx_path_);
        std::filesystem::remove_all(rocksdb_path_);
    }

    // Test data (shared across all tests using static members)
    static inline std::vector<BenchmarkDataGenerator::AccountData> test_data_;
    static inline std::vector<std::pair<std::string, uint64_t>> exact_queries_;
    static inline std::vector<std::pair<std::string, uint64_t>> lookback_queries_;

    // Database paths (shared across all tests)
    static inline const std::filesystem::path mdbx_path_ = std::filesystem::temp_directory_path() / "benchmark_mdbx";
    static inline const std::filesystem::path rocksdb_path_ = std::filesystem::temp_directory_path() / "benchmark_rocksdb";

    // Both MDBX and RocksDB instances are created per-benchmark, no shared instances needed
};

// --- MDBX Benchmarks ---
BENCHMARK_F(DatabaseBenchmark, MDBX_ExactMatch)(benchmark::State& state) {
    // Create independent MDBX connection for this benchmark
    auto mdbx_engine = std::make_unique<QueryEngine>(std::make_unique<MdbxImpl>(mdbx_path_));
    
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = exact_queries_[query_idx % exact_queries_.size()];
        auto result = mdbx_engine->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }

    state.SetItemsProcessed(state.iterations());
    // MDBX connection will be automatically closed when mdbx_engine goes out of scope
}

BENCHMARK_F(DatabaseBenchmark, MDBX_Lookback)(benchmark::State& state) {
    // Create independent MDBX connection for this benchmark
    auto mdbx_engine = std::make_unique<QueryEngine>(std::make_unique<MdbxImpl>(mdbx_path_));
    
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = lookback_queries_[query_idx % lookback_queries_.size()];
        auto result = mdbx_engine->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }

    state.SetItemsProcessed(state.iterations());
    // MDBX connection will be automatically closed when mdbx_engine goes out of scope
}

// --- RocksDB Benchmarks ---
#if HAVE_ROCKSDB
BENCHMARK_F(DatabaseBenchmark, RocksDB_ExactMatch)(benchmark::State& state) {
    // Create independent RocksDB connection for this benchmark
    auto rocksdb_engine = std::make_unique<QueryEngine>(std::make_unique<RocksDbImpl>(rocksdb_path_));
    
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = exact_queries_[query_idx % exact_queries_.size()];
        auto result = rocksdb_engine->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }

    state.SetItemsProcessed(state.iterations());
    // RocksDB connection will be automatically closed when rocksdb_engine goes out of scope
}

BENCHMARK_F(DatabaseBenchmark, RocksDB_Lookback)(benchmark::State& state) {
    // Create independent RocksDB connection for this benchmark
    auto rocksdb_engine = std::make_unique<QueryEngine>(std::make_unique<RocksDbImpl>(rocksdb_path_));
    
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = lookback_queries_[query_idx % lookback_queries_.size()];
        auto result = rocksdb_engine->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }

    state.SetItemsProcessed(state.iterations());
    // RocksDB connection will be automatically closed when rocksdb_engine goes out of scope
}
#endif

// Cleanup function to be called at process exit
void cleanup_at_exit() {
    try {
        const std::filesystem::path mdbx_path = std::filesystem::temp_directory_path() / "benchmark_mdbx";
        const std::filesystem::path rocksdb_path = std::filesystem::temp_directory_path() / "benchmark_rocksdb";
        
        if (std::filesystem::exists(mdbx_path)) {
            std::filesystem::remove_all(mdbx_path);
        }
        if (std::filesystem::exists(rocksdb_path)) {
            std::filesystem::remove_all(rocksdb_path);
        }
    } catch (...) {
        // Ignore cleanup errors during exit
    }
}

// Run the benchmark
int main(int argc, char** argv) {
    // Register cleanup function to run at process exit
    std::atexit(cleanup_at_exit);
    
    // Run benchmarks
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
