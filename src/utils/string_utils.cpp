#include "string_utils.hpp"

namespace utils {

// 辅助函数：将字符串转换为ByteView
ByteView str_to_byteview(const std::string& str) {
    return ByteView{reinterpret_cast<const std::byte*>(str.data()), str.size()};
}

// 辅助函数：将ByteView转换为字符串
std::string byteview_to_str(const ByteView& bv) {
    return std::string{reinterpret_cast<const char*>(bv.data()), bv.size()};
}

// 辅助函数：将字符串转换为Slice
Slice str_to_slice(const std::string& str) {
    return Slice{str.data(), str.size()};
}

// 辅助函数：将uint64_t转换为hex字符串（左边用0填充到16位）
std::string uint64_to_hex(uint64_t value) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(16) << value;
    return ss.str();
}

// 辅助函数：验证CursorResult
void assert_cursor_result(const CursorResult& result, bool should_exist, 
                         const std::string& expected_key, 
                         const std::string& expected_value) {
    if (should_exist) {
        assert(result.done);
        if (!expected_key.empty()) {
            std::string actual_key{result.key.as_string()};
            fmt::println("actual_key: {}", actual_key);
            fmt::println("expected_key: {}", expected_key);
            assert(actual_key == expected_key);
        }
        if (!expected_value.empty()) {
            std::string actual_value{result.value.as_string()};
            fmt::println("actual_value: {}", actual_value);
            fmt::println("expected_value: {}", expected_value);
            assert(actual_value == expected_value);
        }
    } else {
        assert(!result.done);
    }
}

} // namespace utils
