#include "core/query_engine.hpp"
#include "db/rocksdb_impl.hpp"

#include <fmt/core.h>
#include <filesystem>

int main() {
    const std::filesystem::path db_path = std::filesystem::temp_directory_path() / "rocksdb_test_db";
    std::filesystem::remove_all(db_path);

    try {
        fmt::print("Creating RocksDB at: {}\n", db_path.string());
        auto db = std::make_unique<RocksDbImpl>(db_path);
        QueryEngine engine(std::move(db));

        fmt::print("Populating database...\n");
        engine.set_account_state("alice", 1, R"({"balance": "100"})");
        engine.set_account_state("alice", 5, R"({"balance": "200"})");
        engine.set_account_state("alice", 10, R"({"balance": "150"})");

        fmt::print("Testing queries...\n");
        
        auto result = engine.find_account_state("alice", 5);
        fmt::print("Query alice at block 5: {}\n", result ? *result : "Not found");

        result = engine.find_account_state("alice", 7);
        fmt::print("Query alice at block 7: {}\n", result ? *result : "Not found");

        result = engine.find_account_state("alice", 0);
        fmt::print("Query alice at block 0: {}\n", result ? *result : "Not found");

        fmt::print("RocksDB test passed!\n");
        
    } catch (const std::exception& e) {
        fmt::print("Error: {}\n", e.what());
        return 1;
    }

    std::filesystem::remove_all(db_path);
    return 0;
}