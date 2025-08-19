#include "src/core/query_engine.hpp"
#include "src/db/rocksdb_impl.hpp"
#include "src/utils/endian.hpp"

#include <fmt/core.h>
#include <filesystem>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>

int main() {
    const std::filesystem::path db_path = std::filesystem::temp_directory_path() / "debug_rocksdb_db";
    std::filesystem::remove_all(db_path);

    try {
        // Test direct RocksDB operations
        rocksdb::Options options;
        options.create_if_missing = true;
        
        rocksdb::DB* raw_db = nullptr;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path.string(), &raw_db);
        if (!status.ok()) {
            fmt::print("Failed to open RocksDB: {}\n", status.ToString());
            return 1;
        }
        
        std::unique_ptr<rocksdb::DB> db(raw_db);
        
        // Manually construct keys
        std::string account = "alice";
        uint64_t block1 = 1, block5 = 5, block10 = 10;
        
        auto construct_key = [](const std::string& account, uint64_t block) {
            std::string key = account;
            auto be_block = utils::to_big_endian_bytes(block);
            key.append(reinterpret_cast<const char*>(be_block.data()), be_block.size());
            return key;
        };
        
        // Insert keys
        std::string key1 = construct_key(account, block1);
        std::string key5 = construct_key(account, block5);
        std::string key10 = construct_key(account, block10);
        
        db->Put(rocksdb::WriteOptions(), key1, R"({"balance": "100"})");
        db->Put(rocksdb::WriteOptions(), key5, R"({"balance": "200"})");
        db->Put(rocksdb::WriteOptions(), key10, R"({"balance": "150"})");
        
        fmt::print("Inserted keys:\n");
        fmt::print("key1 length: {}\n", key1.size());
        fmt::print("key5 length: {}\n", key5.size());
        fmt::print("key10 length: {}\n", key10.size());
        
        // List all keys
        auto iter = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(rocksdb::ReadOptions()));
        fmt::print("\nAll keys in database:\n");
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            auto key = iter->key();
            auto value = iter->value();
            fmt::print("Key: [{}] ({} bytes), Value: {}\n", 
                      std::string(key.data(), key.size()), key.size(), 
                      std::string(value.data(), value.size()));
        }
        
        // Test SeekForPrev with exact keys
        fmt::print("\nTesting SeekForPrev:\n");
        
        std::string target7 = construct_key(account, 7);
        fmt::print("Seeking for target at block 7 (key length: {})\n", target7.size());
        
        iter->SeekForPrev(rocksdb::Slice(target7));
        if (iter->Valid()) {
            auto found_key = iter->key();
            auto found_value = iter->value();
            fmt::print("Found key: [{}] ({} bytes), Value: {}\n", 
                      std::string(found_key.data(), found_key.size()), found_key.size(),
                      std::string(found_value.data(), found_value.size()));
                      
            // Parse the block number
            if (found_key.size() >= account.size() + 8) {
                const auto* block_bytes = reinterpret_cast<const std::byte*>(
                    found_key.data() + account.size());
                std::span<const std::byte, 8> block_span{block_bytes, 8};
                uint64_t found_block = utils::from_big_endian_bytes(block_span);
                fmt::print("Parsed block number: {}\n", found_block);
            }
        } else {
            fmt::print("SeekForPrev returned invalid iterator\n");
        }
        
        fmt::print("\nTest completed.\n");
        
    } catch (const std::exception& e) {
        fmt::print("Error: {}\n", e.what());
        return 1;
    }

    std::filesystem::remove_all(db_path);
    return 0;
}