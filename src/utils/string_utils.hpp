#pragma once

#include <string>
#include <sstream>
#include <iomanip>
#include <cassert>
#include <fmt/format.h>
#include "../db/mdbx.hpp"

using namespace datastore::kvdb;

namespace utils {

// 辅助函数：将字符串转换为ByteView
ByteView str_to_byteview(const std::string& str);

// 辅助函数：将ByteView转换为字符串
std::string byteview_to_str(const ByteView& bv);

// 辅助函数：将字符串转换为Slice
Slice str_to_slice(const std::string& str);

// 辅助函数：将uint64_t转换为hex字符串（左边用0填充到16位）
std::string uint64_to_hex(uint64_t value);

// 辅助函数：将hex字符串转换为uint64_t（支持多种string类型）
template<typename StringType>
uint64_t hex_to_uint64_impl(const StringType& hex_str) {
    std::stringstream ss;
    ss << std::hex << hex_str;
    uint64_t value;
    ss >> value;
    return value;
}

// 辅助函数：将hex字符串转换为uint64_t（支持多种string类型）
template<typename StringType>
uint64_t hex_to_uint64(const StringType& hex_str) {
    return hex_to_uint64_impl(hex_str);
}

// 辅助函数：安全地将任何类型的string转换为std::string
template<typename StringType>
std::string to_std_string(const StringType& str) {
    return std::string(str.data(), str.size());
}

// 辅助函数：验证CursorResult
void assert_cursor_result(const CursorResult& result, bool should_exist, 
                         const std::string& expected_key = "", 
                         const std::string& expected_value = "");

} // namespace utils
