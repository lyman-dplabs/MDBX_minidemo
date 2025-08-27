#if !HAVE_ROCKSDB
#include "db/rocksdb_impl.hpp"
#include <stdexcept>
#include <cstdint>

// Stub implementation when RocksDB is not available
struct RocksDbImpl::RocksDbPimpl {};

RocksDbImpl::RocksDbImpl(const std::filesystem::path& db_path) {
    throw std::runtime_error("RocksDB support not compiled in");
}

RocksDbImpl::~RocksDbImpl() = default;

void RocksDbImpl::put(std::span<const std::byte> key, std::span<const std::byte> value) {
    throw std::runtime_error("RocksDB support not compiled in");
}

auto RocksDbImpl::get_state(std::string_view account_name, uint64_t block_number)
    -> std::optional<std::vector<std::byte>> {
    throw std::runtime_error("RocksDB support not compiled in");
}

#endif
