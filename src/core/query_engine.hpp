#pragma once

#include "db/interface.hpp"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

class QueryEngine {
public:
    /**
     * @brief Constructs a QueryEngine with a specific database backend.
     * @param db A unique pointer to an object that implements the IDatabase interface.
     */
    explicit QueryEngine(std::unique_ptr<IDatabase> db);

    /**
     * @brief Populates the database with sample data for a given account.
     * @param account_name The name of the account.
     * @param block_number The block number for the state entry.
     * @param state The state data to store.
     */
    void set_account_state(std::string_view account_name, std::uint64_t block_number, std::string_view state);

    /**
     * @brief Finds the state of an account at a specific block, performing a lookback if necessary.
     * @param account_name The name of the account to query.
     * @param block_number The block number to query at.
     * @return An optional containing the state as a string if found, otherwise std::nullopt.
     */
    auto find_account_state(std::string_view account_name, std::uint64_t block_number) -> std::optional<std::string>;

private:
    std::unique_ptr<IDatabase> db_;
};
