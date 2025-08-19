#pragma once

#include "db/interface.hpp"

#include <filesystem>
#include <memory>

// Forward declarations to avoid including mdbx.hpp in a public header,
// which is good practice to reduce compile times for consumers of this class.
namespace mdbx {
class env_managed;
struct map_handle;
} // namespace mdbx

class MdbxImpl final : public IDatabase {
public:
    /**
     * @brief Constructs an MdbxImpl object and opens/creates the database at the given path.
     * @param db_path The file system path to the directory where the MDBX database is stored.
     */
    explicit MdbxImpl(const std::filesystem::path& db_path);

    ~MdbxImpl() override;

    // Deleted copy and move constructors/assignments to ensure unique ownership of the database environment.
    MdbxImpl(const MdbxImpl&) = delete;
    MdbxImpl& operator=(const MdbxImpl&) = delete;
    MdbxImpl(MdbxImpl&&) = delete;
    MdbxImpl& operator=(MdbxImpl&&) = delete;

    void put(std::span<const std::byte> key, std::span<const std::byte> value) override;

    auto get_state(std::string_view account_name, std::uint64_t block_number)
        -> std::optional<std::vector<std::byte>> override;

private:
    // PImpl idiom to hide MDBX implementation details from the header.
    // This holds the mdbx::env_managed and mdbx::map_handle.
    struct MdbxPimpl;
    std::unique_ptr<MdbxPimpl> pimpl_;
};
