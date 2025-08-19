#include "core/query_engine.hpp"
#if HAVE_MDBX
#include "db/mdbx_impl.hpp"
#endif
#include "db/rocksdb_impl.hpp"

#include <fmt/core.h>
#include <filesystem>
#include <iostream>

// Helper function to perform and print a query.
void perform_query(QueryEngine& engine, std::string_view account, std::uint64_t block) {
    fmt::print("Querying for account '{}' at block {}...\n", account, block);
    auto state = engine.find_account_state(account, block);
    if (state) {
        fmt::print("  -> Found state: '{}'\n\n", *state);
    } else {
        fmt::print("  -> State not found.\n\n");
    }
}

int main() {
    // Create temporary directories for the databases to keep our project clean.
    const std::filesystem::path mdbx_db_path = std::filesystem::temp_directory_path() / "mdbx_demo_db";
    const std::filesystem::path rocksdb_db_path = std::filesystem::temp_directory_path() / "rocksdb_demo_db";
    std::filesystem::remove_all(mdbx_db_path); // Clean up from previous runs
    std::filesystem::remove_all(rocksdb_db_path);

    try {
        // --- Setup ---
        fmt::print("--- DATABASE DEMO ---\n");

#if HAVE_MDBX
        fmt::print("Testing MDBX implementation...\n");
        {
            auto db = std::make_unique<MdbxImpl>(mdbx_db_path);
            QueryEngine engine(std::move(db));
            
            // --- Data Population ---
            fmt::print("Populating MDBX database with sample data for account 'vitalik'...\n");
            engine.set_account_state("vitalik", 1, R"({"balance": "100", "nonce": "0"})");
            engine.set_account_state("vitalik", 5, R"({"balance": "50", "nonce": "1"})");
            engine.set_account_state("vitalik", 100, R"({"balance": "200", "nonce": "2"})");
            fmt::print("MDBX population complete.\n\n");

            // --- Queries ---
            fmt::print("--- MDBX QUERIES ---\n");
            perform_query(engine, "vitalik", 100);
            perform_query(engine, "vitalik", 50);
            perform_query(engine, "vitalik", 80);
            perform_query(engine, "vitalik", 4);
            perform_query(engine, "vitalik", 1);
            perform_query(engine, "vitalik", 0);
            perform_query(engine, "satoshi", 100);
        }
#else
        fmt::print("MDBX not available, skipping MDBX tests...\n");
#endif

        fmt::print("Testing RocksDB implementation...\n");
        {
            auto db = std::make_unique<RocksDbImpl>(rocksdb_db_path);
            QueryEngine engine(std::move(db));

            // --- Data Population ---
            fmt::print("Populating RocksDB database with sample data for account 'vitalik'...\n");
            engine.set_account_state("vitalik", 1, R"({"balance": "100", "nonce": "0"})");
            engine.set_account_state("vitalik", 5, R"({"balance": "50", "nonce": "1"})");
            engine.set_account_state("vitalik", 100, R"({"balance": "200", "nonce": "2"})");
            fmt::print("RocksDB population complete.\n\n");

            // --- Queries ---
            fmt::print("--- ROCKSDB QUERIES ---\n");
            perform_query(engine, "vitalik", 100);
            perform_query(engine, "vitalik", 50);
            perform_query(engine, "vitalik", 80);
            perform_query(engine, "vitalik", 4);
            perform_query(engine, "vitalik", 1);
            perform_query(engine, "vitalik", 0);
            perform_query(engine, "satoshi", 100);
        }

    } catch (const std::exception& e) {
        fmt::print(stderr, "An error occurred: {}\n", e.what());
        return 1;
    }

    // Clean up the temporary database directories
    std::filesystem::remove_all(mdbx_db_path);
    std::filesystem::remove_all(rocksdb_db_path);
    return 0;
}
