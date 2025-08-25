#pragma once

#include "db/interface.hpp"

#include <filesystem>
#include <memory>
#include <cstdint>

// Forward declarations to avoid including rocksdb headers in public interface
namespace rocksdb {
class DB;
class Options;
} // namespace rocksdb

class RocksDbImpl final : public IDatabase {
public:
    /**
     * @brief Constructs a RocksDbImpl object and opens/creates the database at the given path.
     * @param db_path The file system path to the directory where the RocksDB database is stored.
     */
    explicit RocksDbImpl(const std::filesystem::path& db_path);

    ~RocksDbImpl() override;

    // Deleted copy and move constructors/assignments to ensure unique ownership of the database.
    RocksDbImpl(const RocksDbImpl&) = delete;
    RocksDbImpl& operator=(const RocksDbImpl&) = delete;
    RocksDbImpl(RocksDbImpl&&) = delete;
    RocksDbImpl& operator=(RocksDbImpl&&) = delete;

    void put(std::span<const std::byte> key, std::span<const std::byte> value) override;

    auto get_state(std::string_view account_name, uint64_t block_number)
        -> std::optional<std::vector<std::byte>> override;

private:
    // PImpl idiom to hide RocksDB implementation details from the header.
    struct RocksDbPimpl;
    std::unique_ptr<RocksDbPimpl> pimpl_;
};
