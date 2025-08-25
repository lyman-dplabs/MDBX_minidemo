#include "src/db/mdbx.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <cassert>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <filesystem>

using namespace datastore::kvdb;

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

// 辅助函数：将hex字符串转换为uint64_t（支持多种string类型）
template<typename StringType>
uint64_t hex_to_uint64_impl(const StringType& hex_str) {
    std::stringstream ss;
    ss << std::hex << hex_str;
    uint64_t value;
    ss >> value;
    return value;
}

uint64_t hex_to_uint64(const std::string& hex_str) {
    return hex_to_uint64_impl(hex_str);
}

template<typename CharT, typename Traits, typename Alloc>
uint64_t hex_to_uint64(const std::basic_string<CharT, Traits, Alloc>& hex_str) {
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
                         const std::string& expected_value = "") {
    if (should_exist) {
        assert(result.done);
        if (!expected_key.empty()) {
            std::string actual_key = to_std_string(result.key.as_string());
            assert(actual_key == expected_key);
        }
        if (!expected_value.empty()) {
            std::string actual_value = to_std_string(result.value.as_string());
            assert(actual_value == expected_value);
        }
    } else {
        assert(!result.done);
    }
}

// 全局环境变量
::mdbx::env_managed g_env;

void setup_environment() {
    std::cout << "\n=== 设置测试环境 ===" << std::endl;
    
    // 清理可能存在的旧数据库文件
    std::filesystem::path db_dir = "/tmp/test_mdbx_demand";
    if (std::filesystem::exists(db_dir)) {
        std::filesystem::remove_all(db_dir);
        std::cout << "清理旧的测试数据库文件" << std::endl;
    }
    
    EnvConfig config;
    config.path = "/tmp/test_mdbx_demand";
    config.create = true;
    config.readonly = false;
    config.exclusive = false;
    config.in_memory = false;
    config.shared = false;
    config.read_ahead = false;
    config.write_map = false;
    config.page_size = 4_Kibi;
    config.max_size = 128_Mebi;
    config.growth_size = 16_Mebi;
    config.max_tables = 64;
    config.max_readers = 50;
    
    g_env = open_env(config);
    
    std::cout << "✓ 测试环境设置完成" << std::endl;
}

// ============================================================================
// 基础功能测试
// ============================================================================

void test_basic_1_rw_transaction_commit_abort() {
    std::cout << "\n=== 基础功能1: 读写事务commit和abort测试 ===" << std::endl;
    
    // 测试commit场景
    {
        std::cout << "\n--- 测试读写事务commit ---" << std::endl;
        
        // 创建读写事务
        RWTxnManaged rw_txn(g_env);
        
        // 创建表配置
        MapConfig table_config{"test_commit_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        
        // 通过dbi创建新表并插入数据
        auto cursor = rw_txn.rw_cursor(table_config);
        cursor->insert(str_to_slice("key1"), str_to_slice("value1"));
        
        std::cout << "插入数据到表: test_commit_table" << std::endl;
        
        // commit事务
        rw_txn.commit_and_stop();
        std::cout << "事务已commit" << std::endl;
        
        // 验证表是否正常创建和数据是否存在
        ROTxnManaged ro_txn(g_env);
        bool table_exists = has_map(ro_txn.operator*(), "test_commit_table");
        assert(table_exists == true);
        std::cout << "✓ 表 'test_commit_table' 创建成功" << std::endl;
        
        auto ro_cursor = ro_txn.ro_cursor(table_config);
        auto result = ro_cursor->find(str_to_slice("key1"));
        assert_cursor_result(result, true, "key1", "value1");
        std::cout << "✓ 数据在commit后可读取" << std::endl;
        
        ro_txn.abort();
    }
    
    // 测试abort场景
    {
        std::cout << "\n--- 测试读写事务abort ---" << std::endl;
        
        // 创建读写事务
        RWTxnManaged rw_txn(g_env);
        
        // 创建表配置
        MapConfig table_config{"test_abort_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        
        // 通过dbi创建新表并插入数据
        auto cursor = rw_txn.rw_cursor(table_config);
        cursor->insert(str_to_slice("key1"), str_to_slice("value1"));
        
        std::cout << "插入数据到表: test_abort_table" << std::endl;
        
        // abort事务
        rw_txn.abort();
        std::cout << "事务已abort" << std::endl;
        
        // 验证表不会被创建
        ROTxnManaged ro_txn(g_env);
        bool table_exists = has_map(ro_txn.operator*(), "test_abort_table");
        assert(table_exists == false);
        std::cout << "✓ 表 'test_abort_table' 没有创建（符合预期）" << std::endl;
        
        ro_txn.abort();
    }
    
    std::cout << "✓ 基础功能1测试通过" << std::endl;
}

void test_basic_2_readonly_transaction_restrictions() {
    std::cout << "\n=== 基础功能2: 只读事务操作限制测试 ===" << std::endl;
    
    // 首先用读写事务创建一个表和一些数据用于测试
    {
        RWTxnManaged rw_txn(g_env);
        MapConfig table_config{"readonly_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = rw_txn.rw_cursor(table_config);
        cursor->insert(str_to_slice("existing_key"), str_to_slice("existing_value"));
        rw_txn.commit_and_stop();
    }
    
    // 创建只读事务
    ROTxnManaged ro_txn(g_env);
    MapConfig table_config{"readonly_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    std::cout << "创建只读事务 (MDBX_TXN_RDONLY)" << std::endl;
    
    // 获取只读游标
    auto ro_cursor = ro_txn.ro_cursor(table_config);
    
    // 验证可以读取
    auto result = ro_cursor->find(str_to_slice("existing_key"));
    assert_cursor_result(result, true, "existing_key", "existing_value");
    std::cout << "✓ 只读事务可以正常读取数据" << std::endl;
    
    // 尝试获取读写游标应该会失败（这里我们通过类型系统来保证）
    // 只读事务ROTxn不提供rw_cursor方法，所以在编译时就会阻止写操作
    std::cout << "✓ 只读事务无法获取读写游标（通过类型系统保证）" << std::endl;
    
    // 验证只读事务的ID
    uint64_t txn_id = ro_txn.id();
    std::cout << "只读事务ID: " << txn_id << std::endl;
    
    // 验证事务是打开状态
    assert(ro_txn.is_open() == true);
    std::cout << "✓ 只读事务处于打开状态" << std::endl;
    
    ro_txn.abort();
    std::cout << "✓ 基础功能2测试通过" << std::endl;
}

void test_basic_3_concurrent_transactions() {
    std::cout << "\n=== 基础功能3: 单个env下多事务并发读写测试 ===" << std::endl;
    
    // 准备测试表
    MapConfig table_config{"concurrent_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    // 初始化一些数据
    {
        RWTxnManaged init_txn(g_env);
        auto cursor = init_txn.rw_cursor(table_config);
        cursor->insert(str_to_slice("shared_key1"), str_to_slice("initial_value1"));
        cursor->insert(str_to_slice("shared_key2"), str_to_slice("initial_value2"));
        init_txn.commit_and_stop();
        std::cout << "初始化测试数据完成" << std::endl;
    }
    
    // 并发场景1: 同时存在多个只读事务
    {
        std::cout << "\n--- 并发场景1: 多个只读事务同时存在 ---" << std::endl;
        
        ROTxnManaged ro_txn1(g_env);
        ROTxnManaged ro_txn2(g_env);
        ROTxnManaged ro_txn3(g_env);
        
        auto cursor1 = ro_txn1.ro_cursor(table_config);
        auto cursor2 = ro_txn2.ro_cursor(table_config);
        auto cursor3 = ro_txn3.ro_cursor(table_config);
        
        // 所有只读事务都应该能读取到相同的数据
        auto result1 = cursor1->find(str_to_slice("shared_key1"));
        auto result2 = cursor2->find(str_to_slice("shared_key1"));
        auto result3 = cursor3->find(str_to_slice("shared_key1"));
        
        assert_cursor_result(result1, true, "shared_key1", "initial_value1");
        assert_cursor_result(result2, true, "shared_key1", "initial_value1");
        assert_cursor_result(result3, true, "shared_key1", "initial_value1");
        
        std::cout << "✓ 多个只读事务可以并发读取相同数据" << std::endl;
        
        ro_txn1.abort();
        ro_txn2.abort();
        ro_txn3.abort();
    }
    
    // 并发场景2: 读写事务和只读事务并发（MVCC特性）
    {
        std::cout << "\n--- 并发场景2: 读写事务与只读事务并发 ---" << std::endl;
        
        // 先启动一个只读事务
        ROTxnManaged ro_txn(g_env);
        auto ro_cursor = ro_txn.ro_cursor(table_config);
        
        // 读取初始值
        auto initial_result = ro_cursor->find(str_to_slice("shared_key1"));
        assert_cursor_result(initial_result, true, "shared_key1", "initial_value1");
        std::cout << "只读事务读取到初始值: " << to_std_string(initial_result.value.as_string()) << std::endl;
        
        // 启动读写事务修改数据
        {
            RWTxnManaged rw_txn(g_env);
            auto rw_cursor = rw_txn.rw_cursor(table_config);
            rw_cursor->upsert(str_to_slice("shared_key1"), str_to_slice("modified_value1"));
            rw_cursor->insert(str_to_slice("new_key"), str_to_slice("new_value"));
            rw_txn.commit_and_stop();
            std::cout << "读写事务修改数据并commit" << std::endl;
        }
        
        // 只读事务应该仍然看到原始数据（MVCC特性）
        auto unchanged_result = ro_cursor->find(str_to_slice("shared_key1"));
        assert_cursor_result(unchanged_result, true, "shared_key1", "initial_value1");
        std::cout << "✓ 只读事务仍看到原始数据（MVCC隔离）: " << to_std_string(unchanged_result.value.as_string()) << std::endl;
        
        // 只读事务不应该看到新插入的键
        auto new_key_result = ro_cursor->find(str_to_slice("new_key"), false);
        assert_cursor_result(new_key_result, false);
        std::cout << "✓ 只读事务看不到后插入的键" << std::endl;
        
        ro_txn.abort();
        
        // 新的只读事务应该能看到修改后的数据
        ROTxnManaged new_ro_txn(g_env);
        auto new_cursor = new_ro_txn.ro_cursor(table_config);
        auto updated_result = new_cursor->find(str_to_slice("shared_key1"));
        assert_cursor_result(updated_result, true, "shared_key1", "modified_value1");
        std::cout << "✓ 新只读事务能看到修改后的数据: " << to_std_string(updated_result.value.as_string()) << std::endl;
        
        auto new_key_result2 = new_cursor->find(str_to_slice("new_key"));
        assert_cursor_result(new_key_result2, true, "new_key", "new_value");
        std::cout << "✓ 新只读事务能看到新插入的键" << std::endl;
        
        new_ro_txn.abort();
    }
    
    std::cout << "✓ 基础功能3测试通过" << std::endl;
}

void test_basic_4_dupsort_table_operations() {
    std::cout << "\n=== 基础功能4: MDBX_DUPSORT表操作测试 ===" << std::endl;
    
    // 创建支持DUPSORT的表（多值表）
    MapConfig dupsort_config{"dupsort_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
    
    RWTxnManaged rw_txn(g_env);
    auto cursor = rw_txn.rw_cursor_dup_sort(dupsort_config);
    
    std::cout << "创建支持DUPSORT的表" << std::endl;
    
    // 测试同一key写入多个不同value（使用MDBX_APPENDDUP）
    std::cout << "\n--- 测试同一key写入多个不同value ---" << std::endl;
    
    std::string test_key = "user123";
    std::vector<std::string> different_values = {"role_admin", "role_editor", "role_viewer"};
    
    for (const auto& value : different_values) {
        cursor->append(str_to_slice(test_key), str_to_slice(value));
        std::cout << "添加 " << test_key << " -> " << value << std::endl;
    }
    
    // 验证所有不同value都被存储
    cursor->find(str_to_slice(test_key));
    size_t value_count = cursor->count_multivalue();
    assert(value_count == 3);
    std::cout << "✓ 同一key下存储了 " << value_count << " 个不同值" << std::endl;
    
    // 遍历所有值验证
    cursor->to_current_first_multi();
    std::vector<std::string> found_values;
    do {
        auto current = cursor->current();
        found_values.push_back(to_std_string(current.value.as_string()));
        auto next_result = cursor->to_current_next_multi(false);
        if (!next_result.done) break;
    } while (true);
    
    assert(found_values.size() == 3);
    std::cout << "所有存储的值: ";
    for (const auto& val : found_values) {
        std::cout << val << " ";
    }
    std::cout << std::endl;
    
    // 测试同一key写入相同value不会重复存储
    std::cout << "\n--- 测试同一key写入相同value ---" << std::endl;
    
    // 尝试再次添加已存在的值
    try {
        cursor->append(str_to_slice(test_key), str_to_slice("role_admin"));
        std::cout << "再次尝试添加已存在的值: role_admin" << std::endl;
    } catch (const mdbx::key_exists&) {
        std::cout << "尝试添加重复值被阻止（符合预期）: role_admin" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "添加重复值时出现异常（符合预期）: " << e.what() << std::endl;
    }
    
    // 验证值的数量没有增加
    cursor->find(str_to_slice(test_key));
    size_t count_after_dup = cursor->count_multivalue();
    assert(count_after_dup == 3);
    std::cout << "✓ 相同值不会重复存储，数量仍为: " << count_after_dup << std::endl;
    
    // 测试精确查找特定的key-value组合
    auto exact_result = cursor->find_multivalue(str_to_slice(test_key), str_to_slice("role_editor"));
    assert_cursor_result(exact_result, true, test_key, "role_editor");
    std::cout << "✓ 可以精确查找特定的key-value组合" << std::endl;
    
    // 测试查找不存在的value
    auto not_found_result = cursor->find_multivalue(str_to_slice(test_key), str_to_slice("role_nonexistent"), false);
    assert_cursor_result(not_found_result, false);
    std::cout << "✓ 查找不存在的value正确返回未找到" << std::endl;
    
    rw_txn.commit_and_stop();
    std::cout << "✓ 基础功能4测试通过" << std::endl;
}

// ============================================================================
// 业务功能测试
// ============================================================================

void test_business_1_get_both_range_equivalent() {
    std::cout << "\n=== 业务功能1: MDBX_GET_BOTH_RANGE等价功能测试 ===" << std::endl;
    
    MapConfig addr_height_config{"address_height_mapping", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
    
    RWTxnManaged rw_txn(g_env);
    auto cursor = rw_txn.rw_cursor_dup_sort(addr_height_config);
    
    // 构造测试数据：address -> blocknumberhex
    std::string addr = "0x1234567890abcdef";
    std::vector<uint64_t> block_numbers = {100, 150, 200};
    
    std::cout << "构造测试数据: " << addr << " -> hex(100), hex(150), hex(200)" << std::endl;
    
    // 插入数据，使用hex格式
    for (uint64_t block_num : block_numbers) {
        std::string hex_value = uint64_to_hex(block_num);
        cursor->append(str_to_slice(addr), str_to_slice(hex_value));
        std::cout << "插入: " << addr << " -> " << hex_value << " (十进制: " << block_num << ")" << std::endl;
    }
    
    // 测试MDBX_GET_BOTH_RANGE等价功能：查找kv为addr->hex(175)的对应value
    std::cout << "\n--- 测试GET_BOTH_RANGE: 查找 >= hex(175) 的第一个值 ---" << std::endl;
    
    std::string search_hex = uint64_to_hex(175);
    std::cout << "查找条件: " << addr << " -> >= " << search_hex << " (十进制: 175)" << std::endl;
    
    // 使用lower_bound_multivalue实现GET_BOTH_RANGE功能
    auto range_result = cursor->lower_bound_multivalue(str_to_slice(addr), str_to_slice(search_hex));
    
    if (range_result.done) {
        std::string found_hex = to_std_string(range_result.value.as_string());
        uint64_t found_value = hex_to_uint64(found_hex);
        std::cout << "找到值: " << found_hex << " (十进制: " << found_value << ")" << std::endl;
        
        // 应该找到200，因为它是 >= 175的第一个值
        assert(found_value == 200);
        std::cout << "✓ GET_BOTH_RANGE功能正确：找到的值是hex(200)" << std::endl;
    } else {
        assert(false);  // 应该找到结果
    }
    
    // 额外测试：查找边界值
    std::cout << "\n--- 边界值测试 ---" << std::endl;
    
    // 查找 >= hex(100)，应该找到hex(100)
    auto boundary1 = cursor->lower_bound_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(100)));
    assert(boundary1.done);
    assert(hex_to_uint64(to_std_string(boundary1.value.as_string())) == 100);
    std::cout << "✓ 查找 >= hex(100) 正确找到 hex(100)" << std::endl;
    
    // 查找 >= hex(250)，应该找不到
    auto boundary2 = cursor->lower_bound_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(250)), false);
    assert(!boundary2.done);
    std::cout << "✓ 查找 >= hex(250) 正确返回未找到" << std::endl;
    
    rw_txn.commit_and_stop();
    std::cout << "✓ 业务功能1测试通过" << std::endl;
}

void test_business_2_prev_dup_equivalent() {
    std::cout << "\n=== 业务功能2: MDBX_PREV_DUP等价功能测试 ===" << std::endl;
    
    MapConfig addr_height_config{"address_height_mapping", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
    
    // 使用现有数据进行测试
    ROTxnManaged ro_txn(g_env);
    auto cursor = ro_txn.ro_cursor_dup_sort(addr_height_config);
    
    std::string addr = "0x1234567890abcdef";
    
    // 定位到addr->hex(200)
    std::string target_hex = uint64_to_hex(200);
    std::cout << "定位到 " << addr << " -> " << target_hex << " (十进制: 200)" << std::endl;
    
    auto find_result = cursor->find_multivalue(str_to_slice(addr), str_to_slice(target_hex));
    assert_cursor_result(find_result, true, addr, target_hex);
    std::cout << "成功定位到目标位置" << std::endl;
    
    // 使用MDBX_PREV_DUP等价功能：to_current_prev_multi
    std::cout << "\n--- 测试PREV_DUP: 查找前一个值 ---" << std::endl;
    
    auto prev_result = cursor->to_current_prev_multi(false);
    
    if (prev_result.done) {
        std::string prev_hex = to_std_string(prev_result.value.as_string());
        uint64_t prev_value = hex_to_uint64(prev_hex);
        std::cout << "前一个值: " << prev_hex << " (十进制: " << prev_value << ")" << std::endl;
        
        // 应该找到150，因为它是200的前一个值
        assert(prev_value == 150);
        std::cout << "✓ PREV_DUP功能正确：前一个值是hex(150)" << std::endl;
    } else {
        assert(false);  // 应该找到结果
    }
    
    // 额外测试：从最小值开始应该没有前一个值
    std::cout << "\n--- 边界测试：从最小值查找前一个 ---" << std::endl;
    
    cursor->find_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(100)));
    auto no_prev_result = cursor->to_current_prev_multi(false);
    assert(!no_prev_result.done);
    std::cout << "✓ 从最小值查找前一个正确返回未找到" << std::endl;
    
    // 测试完整的前后导航
    std::cout << "\n--- 完整导航测试 ---" << std::endl;
    
    // 定位到中间值150
    cursor->find_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(150)));
    
    // 向前导航到100
    auto prev_to_100 = cursor->to_current_prev_multi();
    assert(hex_to_uint64(to_std_string(prev_to_100.value.as_string())) == 100);
    std::cout << "从150向前到100: ✓" << std::endl;
    
    // 向后导航回150
    auto next_to_150 = cursor->to_current_next_multi();
    assert(hex_to_uint64(to_std_string(next_to_150.value.as_string())) == 150);
    std::cout << "从100向后到150: ✓" << std::endl;
    
    // 再向后到200
    auto next_to_200 = cursor->to_current_next_multi();
    assert(hex_to_uint64(to_std_string(next_to_200.value.as_string())) == 200);
    std::cout << "从150向后到200: ✓" << std::endl;
    
    ro_txn.abort();
    std::cout << "✓ 业务功能2测试通过" << std::endl;
}

void test_business_3_atomic_multi_table_transaction() {
    std::cout << "\n=== 业务功能3: 同一事务内多表原子性写入测试 ===" << std::endl;
    
    // 创建两个不同的表
    MapConfig table1_config{"atomic_test_table1", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    MapConfig table2_config{"atomic_test_table2", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    std::string addr = "0xabcdef1234567890";
    std::string storage_key = "storage_slot_001";
    std::string combined_key = addr + "+" + storage_key;
    
    std::cout << "测试地址: " << addr << std::endl;
    std::cout << "存储键: " << storage_key << std::endl;
    std::cout << "组合键: " << combined_key << std::endl;
    
    // 清理可能存在的旧数据
    {
        RWTxnManaged cleanup_txn(g_env);
        auto cursor1 = cleanup_txn.rw_cursor(table1_config);
        auto cursor2 = cleanup_txn.rw_cursor(table2_config);
        cursor1->erase(str_to_slice(combined_key), false);
        cursor2->erase(str_to_slice(combined_key), false);
        cleanup_txn.commit_and_stop();
    }
    
    // 在同一事务内向两个表插入数据
    std::cout << "\n--- 同一事务内向两个表插入数据 ---" << std::endl;
    
    {
        RWTxnManaged atomic_txn(g_env);
        
        // 获取两个表的游标
        auto cursor1 = atomic_txn.rw_cursor(table1_config);
        auto cursor2 = atomic_txn.rw_cursor(table2_config);
        
        // 向table1插入: addr+storagekey -> hex(100)
        std::string value1 = uint64_to_hex(100);
        cursor1->insert(str_to_slice(combined_key), str_to_slice(value1));
        std::cout << "table1插入: " << combined_key << " -> " << value1 << std::endl;
        
        // 向table2插入: addr+storagekey -> hex(100)+storagevalue
        std::string storage_value = "storage_data_xyz";
        std::string value2 = uint64_to_hex(100) + "+" + storage_value;
        cursor2->insert(str_to_slice(combined_key), str_to_slice(value2));
        std::cout << "table2插入: " << combined_key << " -> " << value2 << std::endl;
        
        // 在提交前，验证数据在事务内可见
        auto result1 = cursor1->find(str_to_slice(combined_key));
        auto result2 = cursor2->find(str_to_slice(combined_key));
        
        assert_cursor_result(result1, true, combined_key, value1);
        assert_cursor_result(result2, true, combined_key, value2);
        std::cout << "✓ 事务内两个表的数据都可见" << std::endl;
        
        // 提交事务 - 这应该原子性地提交两个插入操作
        atomic_txn.commit_and_stop();
        std::cout << "事务已提交" << std::endl;
    }
    
    // 验证事务提交后，两个表的数据都存在
    std::cout << "\n--- 验证原子性提交结果 ---" << std::endl;
    
    {
        ROTxnManaged verify_txn(g_env);
        
        auto cursor1 = verify_txn.ro_cursor(table1_config);
        auto cursor2 = verify_txn.ro_cursor(table2_config);
        
        // 验证table1的数据
        auto result1 = cursor1->find(str_to_slice(combined_key));
        assert_cursor_result(result1, true, combined_key, uint64_to_hex(100));
        std::cout << "✓ table1数据提交成功: " << to_std_string(result1.value.as_string()) << std::endl;
        
        // 验证table2的数据
        auto result2 = cursor2->find(str_to_slice(combined_key));
        std::string expected_value2 = uint64_to_hex(100) + "+storage_data_xyz";
        assert_cursor_result(result2, true, combined_key, expected_value2);
        std::cout << "✓ table2数据提交成功: " << to_std_string(result2.value.as_string()) << std::endl;
        
        verify_txn.abort();
    }
    
    // 测试原子性回滚场景
    std::cout << "\n--- 测试原子性回滚场景 ---" << std::endl;
    
    std::string test_key_rollback = combined_key + "_rollback";
    
    {
        RWTxnManaged rollback_txn(g_env);
        
        auto cursor1 = rollback_txn.rw_cursor(table1_config);
        auto cursor2 = rollback_txn.rw_cursor(table2_config);
        
        // 向两个表插入数据
        cursor1->insert(str_to_slice(test_key_rollback), str_to_slice("rollback_value1"));
        cursor2->insert(str_to_slice(test_key_rollback), str_to_slice("rollback_value2"));
        
        std::cout << "插入回滚测试数据到两个表" << std::endl;
        
        // 验证数据在事务内可见
        auto result1 = cursor1->find(str_to_slice(test_key_rollback));
        auto result2 = cursor2->find(str_to_slice(test_key_rollback));
        assert(result1.done && result2.done);
        std::cout << "事务内数据可见" << std::endl;
        
        // 回滚事务
        rollback_txn.abort();
        std::cout << "事务已回滚" << std::endl;
    }
    
    // 验证回滚后数据不存在
    {
        ROTxnManaged verify_rollback_txn(g_env);
        
        auto cursor1 = verify_rollback_txn.ro_cursor(table1_config);
        auto cursor2 = verify_rollback_txn.ro_cursor(table2_config);
        
        auto result1 = cursor1->find(str_to_slice(test_key_rollback), false);
        auto result2 = cursor2->find(str_to_slice(test_key_rollback), false);
        
        assert_cursor_result(result1, false);
        assert_cursor_result(result2, false);
        std::cout << "✓ 回滚后两个表的数据都不存在（原子性保证）" << std::endl;
        
        verify_rollback_txn.abort();
    }
    
    std::cout << "✓ 业务功能3测试通过" << std::endl;
}

// ============================================================================
// 主测试函数
// ============================================================================

int main() {
    std::cout << "开始MDBX需求测试 - 基于test_demand.md" << std::endl;
    
    try {
        // 设置测试环境
        setup_environment();
        
        // 基础功能测试
        std::cout << "\n" << std::string(60, '=') << std::endl;
        std::cout << "基础功能测试" << std::endl;
        std::cout << std::string(60, '=') << std::endl;
        
        test_basic_1_rw_transaction_commit_abort();
        test_basic_2_readonly_transaction_restrictions();
        test_basic_3_concurrent_transactions();
        test_basic_4_dupsort_table_operations();
        
        // 业务功能测试
        std::cout << "\n" << std::string(60, '=') << std::endl;
        std::cout << "业务功能测试" << std::endl;
        std::cout << std::string(60, '=') << std::endl;
        
        test_business_1_get_both_range_equivalent();
        test_business_2_prev_dup_equivalent();
        test_business_3_atomic_multi_table_transaction();
        
        std::cout << "\n" << std::string(60, '=') << std::endl;
        std::cout << "🎉 所有测试通过！MDBX需求功能验证完成。" << std::endl;
        std::cout << std::string(60, '=') << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "\n❌ 测试失败: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}