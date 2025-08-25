#pragma once

#include <algorithm>  // For std::copy
#include <array>
#include <bit>        // For std::endian and std::byteswap
#include <cstdint>
#include <cstdint>

namespace utils {

/**
 * @brief Converts a 64-bit unsigned integer to a byte array in big-endian order.
 *
 * This is crucial for ensuring correct lexicographical sorting of composite keys in the database.
 *
 * @param value The integer value to convert.
 * @return A std::array of 8 bytes representing the integer in big-endian format.
 */
inline auto to_big_endian_bytes(uint64_t value) -> std::array<std::byte, 8> {
    const uint64_t be_value = (std::endian::native == std::endian::little) ? std::byteswap(value) : value;
    return std::bit_cast<std::array<std::byte, 8>>(be_value);
}

/**
 * @brief Converts a span of 8 bytes in big-endian order back to a 64-bit unsigned integer.
 *
 * @param data A span of exactly 8 bytes representing the integer in big-endian format.
 * @return The converted integer value.
 */
inline auto from_big_endian_bytes(std::span<const std::byte, 8> data) -> uint64_t {
    std::array<std::byte, 8> byte_array;
    std::copy(data.begin(), data.end(), byte_array.begin());
    const auto be_value = std::bit_cast<uint64_t>(byte_array);
    return (std::endian::native == std::endian::little) ? std::byteswap(be_value) : be_value;
}

} // namespace utils
