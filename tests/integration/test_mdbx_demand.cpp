#include "db/mdbx.hpp"
#include "../../src/utils/string_utils.hpp"
#include <fmt/format.h>
#include <string>
#include <vector>
#include <cassert>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <filesystem>

using namespace datastore::kvdb;
using namespace utils;

// 全局环境变量
::mdbx::env_managed g_env;

void setup_environment() {
    fmt::println("\n=== 设置测试环境 ===");
    
    // 清理可能存在的旧数据库文件
    std::filesystem::path db_dir = "/tmp/test_mdbx_demand";
    if (std::filesystem::exists(db_dir)) {
        std::filesystem::remove_all(db_dir);
        fmt::println("清理旧的测试数据库文件");
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
    
    fmt::println("✓ 测试环境设置完成");
}

// ============================================================================
// 基础功能测试
// ============================================================================

void test_basic_1_rw_transaction_commit_abort() {
    fmt::println("\n=== 基础功能1: 读写事务commit和abort测试 ===");
    
    // 测试commit场景
    {
        fmt::println("\n--- 测试读写事务commit ---");
        
        // 创建读写事务
        RWTxnManaged rw_txn(g_env);
        
        // 创建表配置
        MapConfig table_config{"test_commit_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        
        // 通过dbi创建新表并插入数据
        auto cursor = rw_txn.rw_cursor(table_config);
        std::string key1 = "key1";
        std::string value1 = "value1";
        cursor->insert(str_to_slice(key1), str_to_slice(value1));
        
        fmt::println("插入数据到表: test_commit_table");
        
        // commit事务
        rw_txn.commit_and_stop();
        fmt::println("事务已commit");
        
        // 验证表是否正常创建和数据是否存在
        ROTxnManaged ro_txn(g_env);
        bool table_exists = has_map(ro_txn.operator*(), "test_commit_table");
        assert(table_exists == true);
        fmt::println("✓ 表 'test_commit_table' 创建成功");
        
        auto ro_cursor = ro_txn.ro_cursor(table_config);
        auto result = ro_cursor->find(str_to_slice(key1));
        assert_cursor_result(result, true, "key1", "value1");
        fmt::println("✓ 数据在commit后可读取");
        
        ro_txn.abort();
    }
    
    // 测试abort场景
    {
        fmt::println("\n--- 测试读写事务abort ---");
        
        // 创建读写事务
        RWTxnManaged rw_txn(g_env);
        
        // 创建表配置
        MapConfig table_config{"test_abort_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        
        // 通过dbi创建新表并插入数据
        auto cursor = rw_txn.rw_cursor(table_config);
        std::string key1 = "key1";
        std::string value1 = "value1";
        cursor->insert(str_to_slice(key1), str_to_slice(value1));
        
        fmt::println("插入数据到表: test_abort_table");
        
        // abort事务
        rw_txn.abort();
        fmt::println("事务已abort");
        
        // 验证表不会被创建
        ROTxnManaged ro_txn(g_env);
        bool table_exists = has_map(ro_txn.operator*(), "test_abort_table");
        assert(table_exists == false);
        fmt::println("✓ 表 'test_abort_table' 没有创建（符合预期）");
        
        ro_txn.abort();
    }
    
    fmt::println("✓ 基础功能1测试通过");
}

void test_basic_2_readonly_transaction_restrictions() {
    fmt::println("\n=== 基础功能2: 只读事务操作限制测试 ===");
    
    // 首先用读写事务创建一个表和一些数据用于测试
    {
        RWTxnManaged rw_txn(g_env);
        MapConfig table_config{"readonly_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
        auto cursor = rw_txn.rw_cursor(table_config);
        std::string existing_key = "existing_key";
        std::string existing_value = "existing_value";
        cursor->insert(str_to_slice(existing_key), str_to_slice(existing_value));
        rw_txn.commit_and_stop();
    }
    
    // 创建只读事务
    ROTxnManaged ro_txn(g_env);
    MapConfig table_config{"readonly_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    fmt::println("创建只读事务 (MDBX_TXN_RDONLY)");
    
    // 获取只读游标
    auto ro_cursor = ro_txn.ro_cursor(table_config);
    
    // 验证可以读取
    std::string existing_key = "existing_key";
    std::string existing_value = "existing_value";
    auto result = ro_cursor->find(str_to_slice(existing_key));
    assert_cursor_result(result, true, "existing_key", "existing_value");
    fmt::println("✓ 只读事务可以正常读取数据");
    
    // 尝试获取读写游标应该会失败（这里我们通过类型系统来保证）
    // 只读事务ROTxn不提供rw_cursor方法，所以在编译时就会阻止写操作
    fmt::println("✓ 只读事务无法获取读写游标（通过类型系统保证）");
    
    // 验证只读事务的ID
    uint64_t txn_id = ro_txn.id();
    fmt::println("只读事务ID: {}", txn_id);
    
    // 验证事务是打开状态
    assert(ro_txn.is_open() == true);
    fmt::println("✓ 只读事务处于打开状态");
    
    ro_txn.abort();
    fmt::println("✓ 基础功能2测试通过");
}

void test_basic_3_concurrent_transactions() {
    fmt::println("\n=== 基础功能3: 单个env下多事务并发读写测试 ===");
    
    // 准备测试表
    MapConfig table_config{"concurrent_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    // 初始化一些数据
    {
        RWTxnManaged init_txn(g_env);
        auto cursor = init_txn.rw_cursor(table_config);
        std::string shared_key1 = "shared_key1";
        std::string initial_value1 = "initial_value1";
        std::string shared_key2 = "shared_key2";
        std::string initial_value2 = "initial_value2";
        cursor->insert(str_to_slice(shared_key1), str_to_slice(initial_value1));
        cursor->insert(str_to_slice(shared_key2), str_to_slice(initial_value2));
        init_txn.commit_and_stop();
        fmt::println("初始化测试数据完成");
    }
    
    // 并发场景1: 同时存在多个只读事务
    {
        fmt::println("\n--- 并发场景1: 多个只读事务同时存在 ---");
        
        ROTxnManaged ro_txn1(g_env);
        ROTxnManaged ro_txn2(g_env);
        ROTxnManaged ro_txn3(g_env);
        
        auto cursor1 = ro_txn1.ro_cursor(table_config);
        auto cursor2 = ro_txn2.ro_cursor(table_config);
        auto cursor3 = ro_txn3.ro_cursor(table_config);
        
        // 所有只读事务都应该能读取到相同的数据
        std::string shared_key1 = "shared_key1";
        auto result1 = cursor1->find(str_to_slice(shared_key1));
        auto result2 = cursor2->find(str_to_slice(shared_key1));
        auto result3 = cursor3->find(str_to_slice(shared_key1));
        
        assert_cursor_result(result1, true, "shared_key1", "initial_value1");
        assert_cursor_result(result2, true, "shared_key1", "initial_value1");
        assert_cursor_result(result3, true, "shared_key1", "initial_value1");
        
        fmt::println("✓ 多个只读事务可以并发读取相同数据");
        
        ro_txn1.abort();
        ro_txn2.abort();
        ro_txn3.abort();
    }
    
    // 并发场景2: 读写事务和只读事务并发（MVCC特性）
    {
        fmt::println("\n--- 并发场景2: 读写事务与只读事务并发 ---");
        
        // 先启动一个只读事务
        ROTxnManaged ro_txn(g_env);
        auto ro_cursor = ro_txn.ro_cursor(table_config);
        
        // 读取初始值
        std::string shared_key1 = "shared_key1";
        auto initial_result = ro_cursor->find(str_to_slice(shared_key1));
        assert_cursor_result(initial_result, true, "shared_key1", "initial_value1");
        fmt::println("只读事务读取到初始值: {}", to_std_string(initial_result.value.as_string()));
        
        // 启动读写事务修改数据
        {
            RWTxnManaged rw_txn(g_env);
            auto rw_cursor = rw_txn.rw_cursor(table_config);
            std::string modified_value1 = "modified_value1";
            std::string new_key = "new_key";
            std::string new_value = "new_value";
            rw_cursor->upsert(str_to_slice(shared_key1), str_to_slice(modified_value1));
            rw_cursor->insert(str_to_slice(new_key), str_to_slice(new_value));
            rw_txn.commit_and_stop();
            fmt::println("读写事务修改数据并commit");
        }
        
        // 只读事务应该仍然看到原始数据（MVCC特性）
        auto unchanged_result = ro_cursor->find(str_to_slice(shared_key1));
        assert_cursor_result(unchanged_result, true, "shared_key1", "initial_value1");
        fmt::println("✓ 只读事务仍看到原始数据（MVCC隔离）: {}", to_std_string(unchanged_result.value.as_string()));
        
        // 只读事务不应该看到新插入的键
        std::string new_key = "new_key";
        auto new_key_result = ro_cursor->find(str_to_slice(new_key), false);
        assert_cursor_result(new_key_result, false);
        fmt::println("✓ 只读事务看不到后插入的键");
        
        ro_txn.abort();
        
        // 新的只读事务应该能看到修改后的数据
        ROTxnManaged new_ro_txn(g_env);
        auto new_cursor = new_ro_txn.ro_cursor(table_config);
        auto updated_result = new_cursor->find(str_to_slice(shared_key1));
        assert_cursor_result(updated_result, true, "shared_key1", "modified_value1");
        fmt::println("✓ 新只读事务能看到修改后的数据: {}", to_std_string(updated_result.value.as_string()));
        
        auto new_key_result2 = new_cursor->find(str_to_slice(new_key));
        assert_cursor_result(new_key_result2, true, "new_key", "new_value");
        fmt::println("✓ 新只读事务能看到新插入的键");
        
        new_ro_txn.abort();
    }
    
    fmt::println("✓ 基础功能3测试通过");
}

void test_basic_4_dupsort_table_operations() {
    fmt::println("\n=== 基础功能4: MDBX_DUPSORT表操作测试 ===");
    
    // 创建支持DUPSORT的表（多值表）
    MapConfig dupsort_config{"dupsort_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
    
    RWTxnManaged rw_txn(g_env);
    auto cursor = rw_txn.rw_cursor_dup_sort(dupsort_config);
    
    fmt::println("创建支持DUPSORT的表");
    
    // 测试同一key写入多个不同value（使用MDBX_APPENDDUP）
    fmt::println("\n--- 测试同一key写入多个不同value ---");
    
    std::string test_key = "user123";
    std::vector<std::string> different_values = {"role_admin", "role_editor", "role_viewer"};
    
    for (const auto& value : different_values) {
        cursor->append(str_to_slice(test_key), str_to_slice(value));
        fmt::println("添加 {} -> {}", test_key, value);
    }
    
    // 验证所有不同value都被存储
    cursor->find(str_to_slice(test_key));
    size_t value_count = cursor->count_multivalue();
    assert(value_count == 3);
    fmt::println("✓ 同一key下存储了 {} 个不同值", value_count);
    
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
    fmt::print("所有存储的值: ");
    for (const auto& val : found_values) {
        fmt::print("{} ", val);
    }
    fmt::println("");
    
    // 测试同一key写入相同value不会重复存储
    fmt::println("\n--- 测试同一key写入相同value ---");
    
    // 尝试再次添加已存在的值
    try {
        std::string role_admin = "role_admin";
        cursor->append(str_to_slice(test_key), str_to_slice(role_admin));
        fmt::println("再次尝试添加已存在的值: role_admin");
    } catch (const mdbx::key_exists&) {
        fmt::println("尝试添加重复值被阻止（符合预期）: role_admin");
    } catch (const std::exception& e) {
        fmt::println("添加重复值时出现异常（符合预期）: {}", e.what());
    }
    
    // 验证值的数量没有增加
    cursor->find(str_to_slice(test_key));
    size_t count_after_dup = cursor->count_multivalue();
    assert(count_after_dup == 3);
    fmt::println("✓ 相同值不会重复存储，数量仍为: {}", count_after_dup);
    
    // 测试精确查找特定的key-value组合
    std::string role_editor = "role_editor";
    auto exact_result = cursor->find_multivalue(str_to_slice(test_key), str_to_slice(role_editor));
    assert_cursor_result(exact_result, true, test_key, "role_editor");
    fmt::println("✓ 可以精确查找特定的key-value组合");
    
    // 测试查找不存在的value
    std::string role_nonexistent = "role_nonexistent";
    auto not_found_result = cursor->find_multivalue(str_to_slice(test_key), str_to_slice(role_nonexistent), false);
    assert_cursor_result(not_found_result, false);
    fmt::println("✓ 查找不存在的value正确返回未找到");
    
    rw_txn.commit_and_stop();
    fmt::println("✓ 基础功能4测试通过");
}

// ============================================================================
// 业务功能测试
// ============================================================================

void test_business_1_get_both_range_equivalent() {
    fmt::println("\n=== 业务功能1: MDBX_GET_BOTH_RANGE等价功能测试 ===");
    
    MapConfig addr_height_config{"address_height_mapping", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
    
    RWTxnManaged rw_txn(g_env);
    auto cursor = rw_txn.rw_cursor_dup_sort(addr_height_config);
    
    // 构造测试数据：address -> blocknumberhex
    std::string addr = "0x1234567890abcdef";
    std::vector<uint64_t> block_numbers = {100, 150, 200};
    
    fmt::println("构造测试数据: {} -> hex(100), hex(150), hex(200)", addr);
    
    // 插入数据，使用hex格式
    for (uint64_t block_num : block_numbers) {
        std::string hex_value = uint64_to_hex(block_num);
        cursor->append(str_to_slice(addr), str_to_slice(hex_value));
        fmt::println("插入: {} -> {} (十进制: {})", addr, hex_value, block_num);
    }
    
    // 测试MDBX_GET_BOTH_RANGE等价功能：查找kv为addr->hex(175)的对应value
    fmt::println("\n--- 测试GET_BOTH_RANGE: 查找 >= hex(175) 的第一个值 ---");
    
    std::string search_hex = uint64_to_hex(175);
    fmt::println("查找条件: {} -> >= {} (十进制: 175)", addr, search_hex);
    
    // 使用lower_bound_multivalue实现GET_BOTH_RANGE功能
    auto range_result = cursor->lower_bound_multivalue(str_to_slice(addr), str_to_slice(search_hex));
    
    if (range_result.done) {
        std::string found_hex = to_std_string(range_result.value.as_string());
        uint64_t found_value = hex_to_uint64(found_hex);
        fmt::println("找到值: {} (十进制: {})", found_hex, found_value);
        
        // 应该找到200，因为它是 >= 175的第一个值
        assert(found_value == 200);
        fmt::println("✓ GET_BOTH_RANGE功能正确：找到的值是hex(200)");
    } else {
        assert(false);  // 应该找到结果
    }
    
    // 额外测试：查找边界值
    fmt::println("\n--- 边界值测试 ---");
    
    // 查找 >= hex(100)，应该找到hex(100)
    auto boundary1 = cursor->lower_bound_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(100)));
    assert(boundary1.done);
    assert(hex_to_uint64(to_std_string(boundary1.value.as_string())) == 100);
    fmt::println("✓ 查找 >= hex(100) 正确找到 hex(100)");
    
    // 查找 >= hex(250)，应该找不到
    auto boundary2 = cursor->lower_bound_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(250)), false);
    assert(!boundary2.done);
    fmt::println("✓ 查找 >= hex(250) 正确返回未找到");
    
    rw_txn.commit_and_stop();
    fmt::println("✓ 业务功能1测试通过");
}

void test_business_2_prev_dup_equivalent() {
    fmt::println("\n=== 业务功能2: MDBX_PREV_DUP等价功能测试 ===");
    
    MapConfig addr_height_config{"address_height_mapping", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
    
    // 使用现有数据进行测试
    ROTxnManaged ro_txn(g_env);
    auto cursor = ro_txn.ro_cursor_dup_sort(addr_height_config);
    
    std::string addr = "0x1234567890abcdef";
    
    // 定位到addr->hex(200)
    std::string target_hex = uint64_to_hex(200);
    fmt::println("定位到 {} -> {} (十进制: 200)", addr, target_hex);
    
    auto find_result = cursor->find_multivalue(str_to_slice(addr), str_to_slice(target_hex));
    assert_cursor_result(find_result, true, addr, target_hex);
    fmt::println("成功定位到目标位置");
    
    // 使用MDBX_PREV_DUP等价功能：to_current_prev_multi
    fmt::println("\n--- 测试PREV_DUP: 查找前一个值 ---");
    
    auto prev_result = cursor->to_current_prev_multi(false);
    
    if (prev_result.done) {
        std::string prev_hex = to_std_string(prev_result.value.as_string());
        uint64_t prev_value = hex_to_uint64(prev_hex);
        fmt::println("前一个值: {} (十进制: {})", prev_hex, prev_value);
        
        // 应该找到150，因为它是200的前一个值
        assert(prev_value == 150);
        fmt::println("✓ PREV_DUP功能正确：前一个值是hex(150)");
    } else {
        assert(false);  // 应该找到结果
    }
    
    // 额外测试：从最小值开始应该没有前一个值
    fmt::println("\n--- 边界测试：从最小值查找前一个 ---");
    
    cursor->find_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(100)));
    auto no_prev_result = cursor->to_current_prev_multi(false);
    assert(!no_prev_result.done);
    fmt::println("✓ 从最小值查找前一个正确返回未找到");
    
    // 测试完整的前后导航
    fmt::println("\n--- 完整导航测试 ---");
    
    // 定位到中间值150
    cursor->find_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(150)));
    
    // 向前导航到100
    auto prev_to_100 = cursor->to_current_prev_multi();
    assert(hex_to_uint64(to_std_string(prev_to_100.value.as_string())) == 100);
    fmt::println("从150向前到100: ✓");
    
    // 向后导航回150
    auto next_to_150 = cursor->to_current_next_multi();
    assert(hex_to_uint64(to_std_string(next_to_150.value.as_string())) == 150);
    fmt::println("从100向后到150: ✓");
    
    // 再向后到200
    auto next_to_200 = cursor->to_current_next_multi();
    assert(hex_to_uint64(to_std_string(next_to_200.value.as_string())) == 200);
    fmt::println("从150向后到200: ✓");
    
    ro_txn.abort();
    fmt::println("✓ 业务功能2测试通过");
}

void test_business_3_atomic_multi_table_transaction() {
    fmt::println("\n=== 业务功能3: 同一事务内多表原子性写入测试 ===");
    
    // 创建两个不同的表
    MapConfig table1_config{"atomic_test_table1", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    MapConfig table2_config{"atomic_test_table2", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    
    std::string addr = "0xabcdef1234567890";
    std::string storage_key = "storage_slot_001";
    std::string combined_key = addr + "+" + storage_key;
    
    fmt::println("测试地址: {}", addr);
    fmt::println("存储键: {}", storage_key);
    fmt::println("组合键: {}", combined_key);
    
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
    fmt::println("\n--- 同一事务内向两个表插入数据 ---");
    
    {
        RWTxnManaged atomic_txn(g_env);
        
        // 获取两个表的游标
        auto cursor1 = atomic_txn.rw_cursor(table1_config);
        auto cursor2 = atomic_txn.rw_cursor(table2_config);
        
        // 向table1插入: addr+storagekey -> hex(100)
        std::string value1 = uint64_to_hex(100);
        cursor1->insert(str_to_slice(combined_key), str_to_slice(value1));
        fmt::println("table1插入: {} -> {}", combined_key, value1);
        
        // 向table2插入: addr+storagekey -> hex(100)+storagevalue
        std::string storage_value = "storage_data_xyz";
        std::string value2 = uint64_to_hex(100) + "+" + storage_value;
        cursor2->insert(str_to_slice(combined_key), str_to_slice(value2));
        fmt::println("table2插入: {} -> {}", combined_key, value2);
        
        // 在提交前，验证数据在事务内可见
        auto result1 = cursor1->find(str_to_slice(combined_key));
        auto result2 = cursor2->find(str_to_slice(combined_key));
        
        assert_cursor_result(result1, true, combined_key, value1);
        assert_cursor_result(result2, true, combined_key, value2);
        fmt::println("✓ 事务内两个表的数据都可见");
        
        // 提交事务 - 这应该原子性地提交两个插入操作
        atomic_txn.commit_and_stop();
        fmt::println("事务已提交");
    }
    
    // 验证事务提交后，两个表的数据都存在
    fmt::println("\n--- 验证原子性提交结果 ---");
    
    {
        ROTxnManaged verify_txn(g_env);
        
        auto cursor1 = verify_txn.ro_cursor(table1_config);
        auto cursor2 = verify_txn.ro_cursor(table2_config);
        
        // 验证table1的数据
        auto result1 = cursor1->find(str_to_slice(combined_key));
        assert_cursor_result(result1, true, combined_key, uint64_to_hex(100));
        fmt::println("✓ table1数据提交成功: {}", to_std_string(result1.value.as_string()));
        
        // 验证table2的数据
        auto result2 = cursor2->find(str_to_slice(combined_key));
        std::string expected_value2 = uint64_to_hex(100) + "+storage_data_xyz";
        assert_cursor_result(result2, true, combined_key, expected_value2);
        fmt::println("✓ table2数据提交成功: {}", to_std_string(result2.value.as_string()));
        
        verify_txn.abort();
    }
    
    // 测试原子性回滚场景
    fmt::println("\n--- 测试原子性回滚场景 ---");
    
    std::string test_key_rollback = combined_key + "_rollback";
    
    {
        RWTxnManaged rollback_txn(g_env);
        
        auto cursor1 = rollback_txn.rw_cursor(table1_config);
        auto cursor2 = rollback_txn.rw_cursor(table2_config);
        
        // 向两个表插入数据
        std::string rollback_value1 = "rollback_value1";
        std::string rollback_value2 = "rollback_value2";
        cursor1->insert(str_to_slice(test_key_rollback), str_to_slice(rollback_value1));
        cursor2->insert(str_to_slice(test_key_rollback), str_to_slice(rollback_value2));
        
        fmt::println("插入回滚测试数据到两个表");
        
        // 验证数据在事务内可见
        auto result1 = cursor1->find(str_to_slice(test_key_rollback));
        auto result2 = cursor2->find(str_to_slice(test_key_rollback));
        assert(result1.done && result2.done);
        fmt::println("事务内数据可见");
        
        // 回滚事务
        rollback_txn.abort();
        fmt::println("事务已回滚");
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
        fmt::println("✓ 回滚后两个表的数据都不存在（原子性保证）");
        
        verify_rollback_txn.abort();
    }
    
    fmt::println("✓ 业务功能3测试通过");
}

// ============================================================================
// 主测试函数
// ============================================================================

int main() {
    fmt::println("开始MDBX需求测试 - 基于test_demand.md");
    
    try {
        // 设置测试环境
        setup_environment();
        
        // 基础功能测试
        fmt::println("\n{}", std::string(60, '='));
        fmt::println("基础功能测试");
        fmt::println("{}", std::string(60, '='));
        
        test_basic_1_rw_transaction_commit_abort();
        test_basic_2_readonly_transaction_restrictions();
        test_basic_3_concurrent_transactions();
        test_basic_4_dupsort_table_operations();
        
        // 业务功能测试
        fmt::println("\n{}", std::string(60, '='));
        fmt::println("业务功能测试");
        fmt::println("{}", std::string(60, '='));
        
        test_business_1_get_both_range_equivalent();
        test_business_2_prev_dup_equivalent();
        test_business_3_atomic_multi_table_transaction();
        
        fmt::println("\n{}", std::string(60, '='));
        fmt::println("🎉 所有测试通过！MDBX需求功能验证完成。");
        fmt::println("{}", std::string(60, '='));
        
    } catch (const std::exception& e) {
        fmt::println(stderr, "\n❌ 测试失败: {}", e.what());
        return 1;
    }
    
    return 0;
}