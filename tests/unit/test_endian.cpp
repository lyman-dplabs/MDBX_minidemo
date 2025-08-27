#include "utils/endian.hpp"
#include <fmt/core.h>

int main() {
    uint64_t block = 5;
    
    fmt::print("Original block: {}\n", block);
    
    auto be_bytes = utils::to_big_endian_bytes(block);
    fmt::print("Big endian bytes (hex): ");
    for (auto byte : be_bytes) {
        fmt::print("{:02x} ", static_cast<uint8_t>(byte));
    }
    fmt::print("\n");
    
    uint64_t restored = utils::from_big_endian_bytes(be_bytes);
    fmt::print("Restored block: {}\n", restored);
    
    // Test with different values
    for (uint64_t test_val : {1, 5, 10, 255, 1024}) {
        auto bytes = utils::to_big_endian_bytes(test_val);
        uint64_t back = utils::from_big_endian_bytes(bytes);
        fmt::print("Test {}: {} -> {} ({})\n", test_val, test_val, back, test_val == back ? "OK" : "FAIL");
    }
    
    return 0;
}