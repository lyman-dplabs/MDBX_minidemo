#if !HAVE_MDBX
#include "db/mdbx_impl.hpp"
#include <stdexcept>

// Stub implementation when MDBX is not available
struct MdbxImpl::MdbxPimpl {};

MdbxImpl::MdbxImpl(const std::filesystem::path& db_path) {
    throw std::runtime_error("MDBX support not compiled in");
}

MdbxImpl::~MdbxImpl() = default;

void MdbxImpl::put(std::span<const std::byte> key, std::span<const std::byte> value) {
    throw std::runtime_error("MDBX support not compiled in");
}

auto MdbxImpl::get_state(std::string_view account_name, std::uint64_t block_number)
    -> std::optional<std::vector<std::byte>> {
    throw std::runtime_error("MDBX support not compiled in");
}

#endif