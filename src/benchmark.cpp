#include "core/query_engine.hpp"
#if HAVE_MDBX
#include "db/mdbx_impl.hpp"
#endif
#include "db/rocksdb_impl.hpp"

#include <benchmark/benchmark.h>
#include <fmt/core.h>
#include <filesystem>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

// --- Test Data Configuration ---
constexpr size_t NUM_ACCOUNTS = 100;
constexpr size_t NUM_BLOCKS_PER_ACCOUNT = 1000;
constexpr size_t MAX_BLOCK_NUMBER = 100000;

// --- Test Data Generation ---
class BenchmarkDataGenerator {
public:
    struct AccountData {
        std::string account_name;
        std::vector<std::uint64_t> block_numbers;
        std::vector<std::string> states;
    };

    static auto generate_test_data() -> std::vector<AccountData> {
        std::vector<AccountData> accounts;
        accounts.reserve(NUM_ACCOUNTS);

        std::mt19937 gen{42}; // Fixed seed for reproducibility
        std::uniform_int_distribution<std::uint64_t> block_dist{1, MAX_BLOCK_NUMBER};

        for (size_t i = 0; i < NUM_ACCOUNTS; ++i) {
            AccountData account;
            account.account_name = fmt::format("account_{:04d}", i);
            account.block_numbers.reserve(NUM_BLOCKS_PER_ACCOUNT);
            account.states.reserve(NUM_BLOCKS_PER_ACCOUNT);

            // Generate sorted block numbers
            std::set<std::uint64_t> unique_blocks;
            while (unique_blocks.size() < NUM_BLOCKS_PER_ACCOUNT) {
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
        // Clean up any existing databases
        cleanup_databases();

        // Generate test data
        test_data_ = BenchmarkDataGenerator::generate_test_data();

        // Initialize databases
#if HAVE_MDBX
        mdbx_engine_ = std::make_unique<QueryEngine>(std::make_unique<MdbxImpl>(mdbx_path_));
        populate_database(*mdbx_engine_);
#endif
        
        rocksdb_engine_ = std::make_unique<QueryEngine>(std::make_unique<RocksDbImpl>(rocksdb_path_));
        populate_database(*rocksdb_engine_);

        // Prepare query data
        prepare_query_data();
    }

    void TearDown(const benchmark::State& state) override {
#if HAVE_MDBX
        mdbx_engine_.reset();
#endif
        rocksdb_engine_.reset();
        cleanup_databases();
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
        std::mt19937 gen{123};
        std::uniform_int_distribution<size_t> account_dist{0, NUM_ACCOUNTS - 1};
        std::uniform_int_distribution<std::uint64_t> block_dist{1, MAX_BLOCK_NUMBER};

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

    // Test data
    std::vector<BenchmarkDataGenerator::AccountData> test_data_;
    std::vector<std::pair<std::string, std::uint64_t>> exact_queries_;
    std::vector<std::pair<std::string, std::uint64_t>> lookback_queries_;

    // Database paths
    const std::filesystem::path mdbx_path_ = std::filesystem::temp_directory_path() / "benchmark_mdbx";
    const std::filesystem::path rocksdb_path_ = std::filesystem::temp_directory_path() / "benchmark_rocksdb";

    // Database instances
#if HAVE_MDBX
    std::unique_ptr<QueryEngine> mdbx_engine_;
#endif
    std::unique_ptr<QueryEngine> rocksdb_engine_;
};

// --- MDBX Benchmarks ---
#if HAVE_MDBX
BENCHMARK_F(DatabaseBenchmark, MDBX_ExactMatch)(benchmark::State& state) {
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = exact_queries_[query_idx % exact_queries_.size()];
        auto result = mdbx_engine_->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }
    
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_F(DatabaseBenchmark, MDBX_Lookback)(benchmark::State& state) {
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = lookback_queries_[query_idx % lookback_queries_.size()];
        auto result = mdbx_engine_->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }
    
    state.SetItemsProcessed(state.iterations());
}
#endif

// --- RocksDB Benchmarks ---
BENCHMARK_F(DatabaseBenchmark, RocksDB_ExactMatch)(benchmark::State& state) {
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = exact_queries_[query_idx % exact_queries_.size()];
        auto result = rocksdb_engine_->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }
    
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_F(DatabaseBenchmark, RocksDB_Lookback)(benchmark::State& state) {
    size_t query_idx = 0;
    for (auto _ : state) {
        const auto& [account, block] = lookback_queries_[query_idx % lookback_queries_.size()];
        auto result = rocksdb_engine_->find_account_state(account, block);
        benchmark::DoNotOptimize(result);
        ++query_idx;
    }
    
    state.SetItemsProcessed(state.iterations());
}

// Run the benchmark
BENCHMARK_MAIN();