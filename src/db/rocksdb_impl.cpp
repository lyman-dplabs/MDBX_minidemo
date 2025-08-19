#include "db/rocksdb_impl.hpp"
#include "utils/endian.hpp"

#include <fmt/core.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include <stdexcept>

// --- PImpl Definition ---
struct RocksDbImpl::RocksDbPimpl {
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::Options options;
};

// --- Constructor & Destructor ---
RocksDbImpl::RocksDbImpl(const std::filesystem::path& db_path) : pimpl_{std::make_unique<RocksDbPimpl>()} {
    try {
        if (!std::filesystem::exists(db_path)) {
            std::filesystem::create_directories(db_path);
        }

        // Configure RocksDB options for read-heavy workloads
        pimpl_->options.create_if_missing = true;
        pimpl_->options.max_open_files = 300;
        pimpl_->options.write_buffer_size = 64 << 20; // 64MB
        pimpl_->options.max_write_buffer_number = 3;
        pimpl_->options.target_file_size_base = 64 << 20; // 64MB

        rocksdb::DB* raw_db = nullptr;
        rocksdb::Status status = rocksdb::DB::Open(pimpl_->options, db_path.string(), &raw_db);
        
        if (!status.ok()) {
            throw std::runtime_error(fmt::format("RocksDB initialization failed: {}", status.ToString()));
        }
        
        pimpl_->db.reset(raw_db);
        fmt::print("RocksDB database opened successfully at: {}\n", db_path.string());
    } catch (const std::exception& e) {
        throw std::runtime_error(fmt::format("RocksDB initialization failed: {}", e.what()));
    }
}

RocksDbImpl::~RocksDbImpl() = default;

// --- Public Methods ---
void RocksDbImpl::put(std::span<const std::byte> key, std::span<const std::byte> value) {
    rocksdb::Slice key_slice(reinterpret_cast<const char*>(key.data()), key.size());
    rocksdb::Slice value_slice(reinterpret_cast<const char*>(value.data()), value.size());
    
    rocksdb::Status status = pimpl_->db->Put(rocksdb::WriteOptions(), key_slice, value_slice);
    
    if (!status.ok()) {
        throw std::runtime_error(fmt::format("RocksDB put operation failed: {}", status.ToString()));
    }
}

auto RocksDbImpl::get_state(std::string_view account_name, std::uint64_t block_number)
    -> std::optional<std::vector<std::byte>> {
    
    // 1. Create iterator
    auto iter = std::unique_ptr<rocksdb::Iterator>(pimpl_->db->NewIterator(rocksdb::ReadOptions()));
    
    // 2. Construct the target key: account_name + big_endian(block_number)
    std::vector<std::byte> target_key;
    target_key.reserve(account_name.length() + sizeof(uint64_t));
    target_key.insert(target_key.end(),
                      reinterpret_cast<const std::byte*>(account_name.data()),
                      reinterpret_cast<const std::byte*>(account_name.data()) + account_name.length());

    const auto be_block = utils::to_big_endian_bytes(block_number);
    target_key.insert(target_key.end(), be_block.begin(), be_block.end());
    
    rocksdb::Slice target_slice(reinterpret_cast<const char*>(target_key.data()), target_key.size());
    
    // 3. Use SeekForPrev to find the largest key <= our target key
    iter->SeekForPrev(target_slice);
    
    if (!iter->Valid()) {
        // No key found that is <= target_key, return nullopt
        return std::nullopt;
    }

    // 4. Validate that the found key belongs to the correct account
    rocksdb::Slice found_key = iter->key();
    std::string_view found_key_sv{found_key.data(), found_key.size()};

    // Check if the key starts with our account name and has enough bytes for the block number
    if (found_key_sv.starts_with(account_name) && 
        found_key_sv.length() == account_name.length() + sizeof(uint64_t)) {
        
        // Extract the block number from the found key
        const auto* block_bytes = reinterpret_cast<const std::byte*>(
            found_key_sv.data() + account_name.length());
        std::span<const std::byte, 8> block_span{block_bytes, 8};
        uint64_t found_block = utils::from_big_endian_bytes(block_span);
        
        // Verify that the found block is <= requested block
        if (found_block <= block_number) {
            // 5. Return the value
            rocksdb::Slice found_value = iter->value();
            const auto* value_ptr = reinterpret_cast<const std::byte*>(found_value.data());
            return std::vector<std::byte>{value_ptr, value_ptr + found_value.size()};
        }
    }

    // 6. If no suitable key was found, return nullopt
    return std::nullopt;
}