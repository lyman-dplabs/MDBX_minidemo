#include "core/query_engine.hpp"
#include "utils/endian.hpp"

#include <utility> // For std::move

QueryEngine::QueryEngine(std::unique_ptr<IDatabase> db) : db_{std::move(db)} {}

void QueryEngine::set_account_state(std::string_view account_name, std::uint64_t block_number, std::string_view state) {
    // Construct the composite key: account_name + big_endian(block_number)
    std::vector<std::byte> key;
    key.reserve(account_name.length() + sizeof(uint64_t));
    key.insert(key.end(),
               reinterpret_cast<const std::byte*>(account_name.data()),
               reinterpret_cast<const std::byte*>(account_name.data()) + account_name.length());

    const auto be_block = utils::to_big_endian_bytes(block_number);
    key.insert(key.end(), be_block.begin(), be_block.end());

    // Convert state to a span of bytes
    std::span<const std::byte> value_span{reinterpret_cast<const std::byte*>(state.data()), state.size()};

    db_->put(key, value_span);
}

auto QueryEngine::find_account_state(std::string_view account_name, std::uint64_t block_number)
    -> std::optional<std::string> {
    auto result_bytes = db_->get_state(account_name, block_number);

    if (result_bytes) {
        // Convert the resulting byte vector back to a string for the application layer.
        return std::string{reinterpret_cast<const char*>(result_bytes->data()), result_bytes->size()};
    }

    return std::nullopt;
}
