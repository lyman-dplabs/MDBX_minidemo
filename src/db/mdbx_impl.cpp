#include "db/mdbx_impl.hpp"
#include "utils/endian.hpp"

#include <fmt/core.h>
#include <mdbx.h++>

#include <iostream>
#include <stdexcept>
#include <vector>

// --- PImpl Definition ---
// This struct holds the MDBX handles and is hidden from the header file.
struct MdbxImpl::MdbxPimpl {
    mdbx::env_managed env;
    mdbx::map_handle dbi;
};

// --- Constructor & Destructor ---
MdbxImpl::MdbxImpl(const std::filesystem::path& db_path) : pimpl_{std::make_unique<MdbxPimpl>()} {
    try {
        if (!std::filesystem::exists(db_path)) {
            std::filesystem::create_directories(db_path);
        }

        mdbx::env::create env_creator;
        env_creator.set_geometry(mdbx::geometry::from_gib(1)); // 1 GiB initial size
        env_creator.set_max_dbs(16); // Max 16 DBI
        pimpl_->env = env_creator.open(db_path.string().c_str());

        auto txn = pimpl_->env.write_txn();
        pimpl_->dbi = txn.create_map("AccountState", mdbx::key_mode::usual, mdbx::value_mode::single);
        txn.commit();

        fmt::print("MDBX database opened successfully at: {}\n", db_path.string());
    } catch (const mdbx::exception& e) {
        throw std::runtime_error(fmt::format("MDBX initialization failed: {}", e.what()));
    }
}

MdbxImpl::~MdbxImpl() = default; // Needed for the unique_ptr to a forward-declared type

// --- Public Methods ---
void MdbxImpl::put(std::span<const std::byte> key, std::span<const std::byte> value) {
    auto txn = pimpl_->env.write_txn();
    auto cursor = txn.open_cursor(pimpl_->dbi);
    cursor.put({key.data(), key.size()}, {value.data(), value.size()}, mdbx::upsert_flags::in_separate_pages);
    txn.commit();
}

auto MdbxImpl::get_state(std::string_view account_name, std::uint64_t block_number)
    -> std::optional<std::vector<std::byte>> {
    auto txn = pimpl_->env.read_txn();
    auto cursor = txn.open_cursor(pimpl_->dbi);

    // 1. Construct the seek key: account_name + big_endian(block_number + 1)
    std::vector<std::byte> seek_key;
    seek_key.reserve(account_name.length() + sizeof(uint64_t));
    seek_key.insert(seek_key.end(),
                    reinterpret_cast<const std::byte*>(account_name.data()),
                    reinterpret_cast<const std::byte*>(account_name.data()) + account_name.length());

    const auto be_block = utils::to_big_endian_bytes(block_number + 1);
    seek_key.insert(seek_key.end(), be_block.begin(), be_block.end());

    // 2. Find the lower bound, i.e., the first key >= seek_key.
    auto result = cursor.lower_bound({seek_key.data(), seek_key.size()}, /*throw_notfound=*/false);

    // 3. Move to the previous key, which is the largest key <= our target (account, block).
    if (!result.done) {
        // We found a key >= seek_key, which could be for the next account.
        // Or we are at the end. In both cases, we must step back.
        result = cursor.to_previous(/*throw_notfound=*/false);
    } else {
        // We are past the end of the database. The last key might be what we want.
        result = cursor.to_last(/*throw_notfound=*/false);
    }

    if (!result.done) {
        // 4. Validate that the found key belongs to the correct account.
        std::string_view found_key_sv{reinterpret_cast<const char*>(result.key.data()), result.key.size()};

        if (found_key_sv.starts_with(account_name)) {
            // 5. If it matches, return the value.
            const auto* value_ptr = static_cast<const std::byte*>(result.value.data());
            return std::vector<std::byte>{value_ptr, value_ptr + result.value.size()};
        }
    }

    // 6. If no suitable key was found, return nullopt.
    return std::nullopt;
}
