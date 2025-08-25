#pragma once

#include <optional>
#include <span>
#include <string_view>
#include <vector>
#include <cstdint>
class IDatabase {
public:
    virtual ~IDatabase() = default;

    /**
     * @brief Inserts a key-value pair into the database.
     * @param key The key to insert.
     * @param value The value associated with the key.
     */
    virtual void put(std::span<const std::byte> key, std::span<const std::byte> value) = 0;

    /**
     * @brief Retrieves the state for a given account at a specific block number,
     * or the most recent state before that block number if an exact match is not found.
     *
     * @param account_name The name of the account to query.
     * @param block_number The block number to query at.
     * @return An optional containing the state as a vector of bytes if found, otherwise std::nullopt.
     */
    virtual auto get_state(std::string_view account_name, uint64_t block_number) -> std::optional<std::vector<std::byte>> = 0;
};
