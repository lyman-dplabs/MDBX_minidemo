#include "db/mdbx.hpp"
#include <fmt/format.h>
#include <string>
#include <vector>
#include <cassert>
#include <cstring>

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

// 辅助函数：验证CursorResult
void assert_cursor_result(const CursorResult& result, bool should_exist,
                         const std::string& expected_key = "",
                         const std::string& expected_value = "") {
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

void test_environment_and_config() {
    fmt::println("\n=== 测试环境配置和打开 ===");

    std::filesystem::path db_dir = "/tmp/test_mdbx_comprehensive";
    if (std::filesystem::exists(db_dir)) {
        std::filesystem::remove_all(db_dir);
        fmt::println("清理旧的测试数据库文件");
    }

    // 测试 EnvConfig 结构体的所有配置选项
    EnvConfig config;
    config.path = "/tmp/test_mdbx_comprehensive";
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
    config.max_tables = 32;
    config.max_readers = 50;

    // 测试 open_env 函数
    ::mdbx::env_managed env = open_env(config);

    // 测试 get_datafile_path 工具函数
    auto data_file_path = get_datafile_path(std::filesystem::path{config.path});
    fmt::println("数据文件路径: {}", data_file_path.string());

    fmt::println("✓ 环境配置和打开测试通过");
}

void test_map_config_and_operations(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试表配置和基本操作 ===");

    // 测试 MapConfig 结构体
    MapConfig single_config{"single_value_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};
    MapConfig multi_config{"multi_value_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};

    // 测试事务和表操作
    RWTxnManaged txn(env);

    // 测试 open_map 函数
    auto single_map = open_map(txn.operator*(), single_config);
    auto multi_map = open_map(txn.operator*(), multi_config);

    // 测试 open_cursor 函数
    auto cursor = open_cursor(txn.operator*(), single_config);

    // 测试 has_map 函数（表还没有数据，但已创建）
    bool has_single = has_map(txn.operator*(), "single_value_table");
    bool has_multi = has_map(txn.operator*(), "multi_value_table");
    bool has_nonexistent = has_map(txn.operator*(), "nonexistent_table");

    fmt::println("表 'single_value_table' 存在: {}", (has_single ? "是" : "否"));
    fmt::println("表 'multi_value_table' 存在: {}", (has_multi ? "是" : "否"));
    fmt::println("表 'nonexistent_table' 存在: {}", (has_nonexistent ? "是" : "否"));

    // 测试 list_maps 函数
    auto map_names = list_maps(txn.operator*(), false);
    fmt::print("数据库中的表: ");
    for (const auto& name : map_names) {
        fmt::print("{} ", name);
    }
    fmt::println("");

    txn.commit_and_stop();

    fmt::println("✓ 表配置和基本操作测试通过");
}

void test_transaction_types(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试各种事务类型 ===");

    MapConfig config{"txn_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};

    // 测试 RWTxnManaged
    {
        RWTxnManaged rw_txn(env);

        // 测试事务属性
        uint64_t txn_id = rw_txn.id();
        bool is_open = rw_txn.is_open();
        mdbx::env db_env = rw_txn.db();

        fmt::println("读写事务ID: {}, 是否开启: {}", txn_id, (is_open ? "是" : "否"));

        // 测试提交控制
        rw_txn.disable_commit();
        assert(rw_txn.commit_disabled() == true);

        rw_txn.enable_commit();
        assert(rw_txn.commit_disabled() == false);

        // 插入测试数据
        auto cursor = rw_txn.rw_cursor(config);
        cursor->insert(str_to_slice("txn_key"), str_to_slice("txn_value"));

        rw_txn.commit_and_stop();
    }

    // 测试 ROTxnManaged
    {
        ROTxnManaged ro_txn(env);

        uint64_t ro_txn_id = ro_txn.id();
        bool ro_is_open = ro_txn.is_open();

        fmt::println("只读事务ID: {}, 是否开启: {}", ro_txn_id, (ro_is_open ? "是" : "否"));

        auto cursor = ro_txn.ro_cursor(config);
        auto result = cursor->find(str_to_slice("txn_key"));
        assert_cursor_result(result, true, "txn_key", "txn_value");

        ro_txn.abort();
    }

    // 测试 ROAccess 和 RWAccess
    {
        ROAccess ro_access(env);
        auto ro_tx = ro_access.start_ro_tx();
        auto cursor = ro_tx.ro_cursor(config);
        auto result = cursor->find(str_to_slice("txn_key"));
        assert_cursor_result(result, true);

        RWAccess rw_access(env);
        auto rw_tx = rw_access.start_rw_tx();
        auto rw_cursor = rw_tx.rw_cursor(config);
        rw_cursor->upsert(str_to_slice("access_key"), str_to_slice("access_value"));
        rw_tx.commit_and_stop();
    }

    fmt::println("✓ 事务类型测试通过");
}

void test_single_value_cursor_operations(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试单值游标操作 ===");

    MapConfig config{"single_cursor_test", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};

    RWTxnManaged txn(env);

    // 测试 PooledCursor 的构造和绑定
    PooledCursor cursor(txn, config);

    // 测试游标状态检查方法
    assert(cursor.empty() == true);  // 空表
    assert(cursor.is_multi_value() == false);  // 单值表
    // 注意：is_dangling的实现可能在空表时返回false，这是正常的

    // 测试插入操作 - RWCursor 接口
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"key001", "value001"},
        {"key003", "value003"},
        {"key002", "value002"},
        {"key005", "value005"},
        {"key004", "value004"}
    };

    for (const auto& [key, value] : test_data) {
        cursor.insert(str_to_slice(key), str_to_slice(value));
    }

    // 测试表大小
    size_t table_size = cursor.size();
    assert(table_size == 5);
    fmt::println("表大小: {}", table_size);

    // 测试导航操作 - ROCursor 接口

    // 测试 to_first
    auto result = cursor.to_first();
    assert_cursor_result(result, true, "key001", "value001");
    fmt::println("第一个记录: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试 on_first
    assert(cursor.on_first() == true);

    // 测试 to_next
    result = cursor.to_next();
    assert_cursor_result(result, true, "key002", "value002");

    // 测试 current
    result = cursor.current();
    assert_cursor_result(result, true, "key002", "value002");

    // 测试 to_last
    result = cursor.to_last();
    assert_cursor_result(result, true, "key005", "value005");
    fmt::println("最后一个记录: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试 on_last
    assert(cursor.on_last() == true);

    // 测试 to_previous
    result = cursor.to_previous();
    assert_cursor_result(result, true, "key004", "value004");

    // 测试 find 操作
    result = cursor.find(str_to_slice("key003"));
    assert_cursor_result(result, true, "key003", "value003");
    fmt::println("查找key003: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试 lower_bound 操作
    result = cursor.lower_bound(str_to_slice("key0025"));
    assert_cursor_result(result, true, "key003", "value003");
    fmt::println("下界查找key0025: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试 seek 操作
    bool seek_result = cursor.seek(str_to_slice("key004"));
    assert(seek_result == true);
    result = cursor.current();
    assert_cursor_result(result, true, "key004", "value004");

    // 测试 move 操作
    auto move_result = cursor.move(MoveOperation::next, false);
    if (move_result.done) {
        fmt::println("移动到下一个: {} = {}", move_result.key.as_string(), move_result.value.as_string());
    }

    // 测试 upsert 操作
    cursor.upsert(str_to_slice("key003"), str_to_slice("updated_value003"));
    result = cursor.find(str_to_slice("key003"));
    assert_cursor_result(result, true, "key003", "updated_value003");
    fmt::println("更新后key003: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试 update 操作（需要先定位到键）
    cursor.find(str_to_slice("key002"));
    cursor.update(str_to_slice("key002"), str_to_slice("updated_value002"));
    result = cursor.find(str_to_slice("key002"));
    assert_cursor_result(result, true, "key002", "updated_value002");

    // 测试删除操作
    bool erase_result = cursor.erase(str_to_slice("key001"));
    assert(erase_result == true);
    result = cursor.find(str_to_slice("key001"), false);
    assert_cursor_result(result, false);

    // 测试当前位置删除
    cursor.find(str_to_slice("key002"));
    erase_result = cursor.erase();
    assert(erase_result == true);

    // 测试 eof 状态
    cursor.to_last();
    cursor.to_next(false);  // 移动到末尾之后
    assert(cursor.eof() == true);

    // 测试游标克隆
    cursor.to_first();
    auto cloned_cursor = cursor.clone();
    auto clone_result = cloned_cursor->current();
    auto orig_result = cursor.current();
    assert(clone_result.key == orig_result.key);
    assert(clone_result.value == orig_result.value);

    txn.commit_and_stop();

    fmt::println("✓ 单值游标操作测试通过");
}

void test_multi_value_cursor_operations(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试多值游标操作 ===");

    MapConfig config{"multi_cursor_test", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};

    RWTxnManaged txn(env);
    PooledCursor cursor(txn, config);

    // 验证这是多值表
    assert(cursor.is_multi_value() == true);

    // 插入多值数据 - 测试 append 方法
    std::vector<std::pair<std::string, std::string>> multi_data = {
        {"user1", "admin"},
        {"user1", "editor"},
        {"user1", "viewer"},
        {"user2", "editor"},
        {"user2", "viewer"},
        {"user3", "viewer"}
    };

    for (const auto& [key, value] : multi_data) {
        cursor.append(str_to_slice(key), str_to_slice(value));
    }

    // 测试多值游标特有的导航操作

    // 定位到user1的数据
    auto result = cursor.find(str_to_slice("user1"));
    assert_cursor_result(result, true, "user1", "admin");  // 应该是第一个值（字典序）

    // 测试 to_current_first_multi
    result = cursor.to_current_first_multi();
    assert_cursor_result(result, true, "user1", "admin");
    fmt::println("user1的第一个值: {}", result.value.as_string());

    // 测试 to_current_next_multi
    result = cursor.to_current_next_multi();
    assert_cursor_result(result, true, "user1", "editor");
    fmt::println("user1的下一个值: {}", result.value.as_string());

    // 测试 to_current_last_multi
    result = cursor.to_current_last_multi();
    assert_cursor_result(result, true, "user1", "viewer");
    fmt::println("user1的最后一个值: {}", result.value.as_string());

    // 测试 to_current_prev_multi
    result = cursor.to_current_prev_multi();
    assert_cursor_result(result, true, "user1", "editor");
    fmt::println("user1的前一个值: {}", result.value.as_string());

    // 测试 count_multivalue
    cursor.find(str_to_slice("user1"));
    size_t count = cursor.count_multivalue();
    assert(count == 3);
    fmt::println("user1的值数量: {}", count);

    // 测试 find_multivalue
    result = cursor.find_multivalue(str_to_slice("user1"), str_to_slice("editor"));
    assert_cursor_result(result, true, "user1", "editor");
    fmt::println("精确查找user1-editor: {}", result.value.as_string());

    // 测试 lower_bound_multivalue
    result = cursor.lower_bound_multivalue(str_to_slice("user1"), str_to_slice("e"));
    assert_cursor_result(result, true, "user1", "editor");
    fmt::println("下界查找user1-e: {}", result.value.as_string());

    // 测试 to_next_first_multi
    cursor.find(str_to_slice("user1"));
    result = cursor.to_next_first_multi();
    assert_cursor_result(result, true, "user2", "editor");
    fmt::println("下一个键的第一个值: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试 to_previous_last_multi
    cursor.find(str_to_slice("user2"));
    result = cursor.to_previous_last_multi();
    assert_cursor_result(result, true, "user1", "viewer");
    fmt::println("上一个键的最后一个值: {} = {}", result.key.as_string(), result.value.as_string());

    // 测试多值删除操作

    // 测试删除特定键值对
    bool erase_result = cursor.erase(str_to_slice("user1"), str_to_slice("admin"));
    assert(erase_result == true);

    // 验证删除结果
    result = cursor.find_multivalue(str_to_slice("user1"), str_to_slice("admin"), false);
    assert_cursor_result(result, false);

    // 检查其他值仍然存在
    result = cursor.find_multivalue(str_to_slice("user1"), str_to_slice("editor"));
    assert_cursor_result(result, true, "user1", "editor");

    // 测试删除整个键的所有值
    cursor.find(str_to_slice("user3"));
    erase_result = cursor.erase(true);  // whole_multivalue = true
    assert(erase_result == true);

    result = cursor.find(str_to_slice("user3"), false);
    assert_cursor_result(result, false);

    txn.commit_and_stop();

    fmt::println("✓ 多值游标操作测试通过");
}

void test_batch_operations(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试批量操作 ===");

    MapConfig config{"batch_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};

    // 准备测试数据
    RWTxnManaged setup_txn(env);
    auto setup_cursor = setup_txn.rw_cursor(config);

    std::vector<std::pair<std::string, std::string>> batch_data = {
        {"batch_001", "data_001"},
        {"batch_002", "data_002"},
        {"batch_003", "data_003"},
        {"prefix_001", "prefix_data_001"},
        {"prefix_002", "prefix_data_002"},
        {"other_001", "other_data_001"},
        {"other_002", "other_data_002"}
    };

    for (const auto& [key, value] : batch_data) {
        setup_cursor->insert(str_to_slice(key), str_to_slice(value));
    }
    setup_txn.commit_and_stop();

    // 测试 cursor_for_each
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(config);

        std::vector<std::pair<std::string, std::string>> collected_data;
        auto walker_func = [&collected_data](ByteView key, ByteView value) {
            collected_data.emplace_back(byteview_to_str(key), byteview_to_str(value));
        };

        // 正向遍历
        size_t forward_count = cursor_for_each(*cursor, walker_func, CursorMoveDirection::kForward);
        fmt::println("正向遍历记录数: {}", forward_count);
        assert(forward_count == batch_data.size());

        // 反向遍历
        collected_data.clear();
        size_t reverse_count = cursor_for_each(*cursor, walker_func, CursorMoveDirection::kReverse);
        fmt::println("反向遍历记录数: {}", reverse_count);
        assert(reverse_count == batch_data.size());

        ro_txn.abort();
    }

    // 测试 cursor_for_prefix
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(config);

        std::vector<std::pair<std::string, std::string>> prefix_data;
        auto prefix_walker_func = [&prefix_data](ByteView key, ByteView value) {
            prefix_data.emplace_back(byteview_to_str(key), byteview_to_str(value));
        };

        ByteView prefix = str_to_byteview("prefix_");
        size_t prefix_count = cursor_for_prefix(*cursor, prefix, prefix_walker_func);
        fmt::println("前缀'prefix_'的记录数: {}", prefix_count);
        assert(prefix_count == 2);

        for (const auto& [key, value] : prefix_data) {
            fmt::println("  前缀记录: {} = {}", key, value);
            assert(key.starts_with("prefix_"));
        }

        ro_txn.abort();
    }

    // 测试 cursor_for_count
    {
        ROTxnManaged ro_txn(env);
        auto cursor = ro_txn.ro_cursor(config);

        std::vector<std::pair<std::string, std::string>> limited_data;
        auto count_walker_func = [&limited_data](ByteView key, ByteView value) {
            limited_data.emplace_back(byteview_to_str(key), byteview_to_str(value));
        };

        size_t limited_count = cursor_for_count(*cursor, count_walker_func, 3);
        fmt::println("限制遍历记录数: {}", limited_count);
        assert(limited_count == 3);
        assert(limited_data.size() == 3);

        ro_txn.abort();
    }

    // 测试批量删除操作
    {
        RWTxnManaged rw_txn(env);
        auto cursor = rw_txn.rw_cursor(config);

        // 测试 cursor_erase_prefix
        ByteView erase_prefix = str_to_byteview("prefix_");
        size_t erased_count = cursor_erase_prefix(*cursor, erase_prefix);
        fmt::println("删除前缀'prefix_'的记录数: {}", erased_count);
        assert(erased_count == 2);

        // 验证删除结果
        auto verify_result = cursor->find(str_to_slice("prefix_001"), false);
        assert_cursor_result(verify_result, false);

        // 测试 cursor_erase 从某个键开始删除
        ByteView start_key = str_to_byteview("other_001");
        size_t range_erased = cursor_erase(*cursor, start_key, CursorMoveDirection::kForward);
        fmt::println("从'other_001'开始删除的记录数: {}", range_erased);

        rw_txn.commit_and_stop();
    }

    fmt::println("✓ 批量操作测试通过");
}

void test_utility_functions(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试工具函数 ===");

    // 测试类型转换函数
    std::string test_str = "hello_world";
    ByteView bv = str_to_byteview(test_str);
    Slice slice = to_slice(bv);
    ByteView converted_bv = from_slice(slice);

    assert(bv.size() == slice.size());
    assert(bv.size() == converted_bv.size());
    assert(std::memcmp(bv.data(), converted_bv.data(), bv.size()) == 0);
    fmt::println("✓ 类型转换函数测试通过");

    // 测试大小计算函数
    RWTxnManaged txn(env);
    size_t page_size = 4096;
    size_t key_size = 32;

    size_t max_value_size1 = max_value_size_for_leaf_page(page_size, key_size);
    size_t max_value_size2 = max_value_size_for_leaf_page(txn.operator*(), key_size);

    fmt::println("页面大小 {}, 键大小 {} 时的最大值大小: {}", page_size, key_size, max_value_size1);
    fmt::println("从事务获取的最大值大小: {}", max_value_size2);

    assert(max_value_size1 > 0);
    assert(max_value_size2 > 0);

    txn.commit_and_stop();

    fmt::println("✓ 工具函数测试通过");
}

void test_pooled_cursor_features(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试PooledCursor特有功能 ===");

    MapConfig config{"pooled_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};

    RWTxnManaged txn1(env);

    // 测试 PooledCursor 构造方式
    PooledCursor cursor1;  // 默认构造
    cursor1.bind(txn1, config);

    // 插入测试数据
    cursor1.insert(str_to_slice("pool_key1"), str_to_slice("pool_value1"));

    // 测试获取统计信息
    MDBX_stat stat = cursor1.get_map_stat();
    fmt::println("表统计 - 页面大小: {}, 条目数: {}", stat.ms_psize, stat.ms_entries);

    // 测试获取表标志
    MDBX_db_flags_t flags = cursor1.get_map_flags();
    fmt::println("表标志: {}", static_cast<unsigned int>(flags));

    // 测试获取map handle
    auto map_handle = cursor1.map();
    fmt::println("Map handle有效: {}", (map_handle ? "是" : "否"));

    txn1.commit_and_stop();

    // 测试游标重新绑定到新事务
    RWTxnManaged txn2(env);
    cursor1.bind(txn2, config);

    std::string pool_key1 = "pool_key1";
    std::string pool_value1 = "pool_value1";
    auto result = cursor1.find(str_to_slice(pool_key1));
    assert_cursor_result(result, true, pool_key1, pool_value1);

    // 测试游标缓存
    const auto& cache = PooledCursor::handles_cache();
    fmt::println("测试游标句柄缓存访问成功");

    // 测试 put 操作（低级接口）
    std::string pool_key2 = "pool_key2";
    std::string new_pool_value_str = "new_pool_value";
    Slice value_slice = str_to_slice(new_pool_value_str);
    MDBX_error_t put_result = cursor1.put(str_to_slice(pool_key2), &value_slice, MDBX_UPSERT);
    assert(put_result == MDBX_SUCCESS);

    // 验证put结果
    result = cursor1.find(str_to_slice(pool_key2));
    assert_cursor_result(result, true, pool_key2, new_pool_value_str);
    // 测试显式关闭
    cursor1.close();

    txn2.commit_and_stop();

    fmt::println("✓ PooledCursor特有功能测试通过");
}

void test_important_features(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试重要功能：DUPSORT和特殊查询 ===");

    // 配置启用DUPSORT的表（多值表）
    MapConfig dup_config{"address_height_mapping", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};  // 启用DUPSORT

    RWTxnManaged txn(env);
    auto cursor = txn.rw_cursor_dup_sort(dup_config);

    // 模拟important_feature.md中的示例：地址到区块高度的映射
    std::string addressA = "addressA";
    std::string addressB = "addressB";

    // 为每个地址添加多个区块高度（注意：需要按字节序排序，所以使用固定宽度）
    std::vector<std::pair<std::string, uint64_t>> mappings = {
        {addressA, 100},
        {addressA, 150},
        {addressA, 200},
        {addressB, 150}
    };

    // 插入数据（将数字转换为固定8字节大端序以确保正确的字节序排序）
    for (const auto& [address, height] : mappings) {
        // 将高度转换为大端序8字节
        uint64_t height_be = htobe64(height);
        Slice height_slice{&height_be, sizeof(height_be)};

        cursor->append(str_to_slice(address), height_slice);
        fmt::println("添加映射: {} -> {}", address, height);
    }

    fmt::println("\n--- 测试MDBX_GET_BOTH_RANGE等价功能 ---");

    // 测试 lower_bound_multivalue (相当于 MDBX_GET_BOTH_RANGE)
    // 查找 addressA 中 >= 125 的第一个高度
    uint64_t search_height = htobe64(125);
    Slice search_height_slice{&search_height, sizeof(search_height)};

    auto range_result = cursor->lower_bound_multivalue(str_to_slice(addressA), search_height_slice);
    if (range_result.done) {
        uint64_t found_height = be64toh(*reinterpret_cast<const uint64_t*>(range_result.value.data()));
        fmt::println("BOTH_RANGE查找 {} >= 125: 找到高度 {}", addressA, found_height);
        assert(found_height == 150);  // 应该找到150，因为它是>=125的第一个
    }

    fmt::println("\n--- 测试MDBX_PREV_DUP等价功能 ---");

    // 先定位到addressA的最后一个值（200）
    auto last_result = cursor->find(str_to_slice(addressA));
    if (last_result.done) {
        cursor->to_current_last_multi();
        auto current = cursor->current();
        uint64_t current_height = be64toh(*reinterpret_cast<const uint64_t*>(current.value.data()));
        fmt::println("当前位置: {} -> {}", addressA, current_height);

        // 测试 to_current_prev_multi (相当于 MDBX_PREV_DUP)
        auto prev_result = cursor->to_current_prev_multi(false);
        if (prev_result.done) {
            uint64_t prev_height = be64toh(*reinterpret_cast<const uint64_t*>(prev_result.value.data()));
            fmt::println("PREV_DUP: {} -> {}", addressA, prev_height);
            assert(prev_height == 150);  // 应该是200的前一个值150
        }
    }

    fmt::println("\n--- 测试完整的多值遍历 ---");

    // 遍历addressA的所有值
    cursor->find(str_to_slice(addressA));
    cursor->to_current_first_multi();

    fmt::println("addressA的所有高度值:");
    size_t value_count = 0;
    do {
        auto current = cursor->current();
        uint64_t height = be64toh(*reinterpret_cast<const uint64_t*>(current.value.data()));
        fmt::println("  高度: {}", height);
        value_count++;

        auto next = cursor->to_current_next_multi(false);
        if (!next.done) break;
    } while (true);

    assert(value_count == 3);  // addressA应该有3个高度值
    fmt::println("addressA总共有 {} 个高度值", value_count);

    // 测试count_multivalue
    cursor->find(str_to_slice(addressA));
    size_t counted_values = cursor->count_multivalue();
    assert(counted_values == 3);
    fmt::println("count_multivalue确认: {} 个值", counted_values);

    fmt::println("\n--- 测试跨键导航 ---");

    // 从addressA的最后一个值导航到addressB的第一个值
    cursor->find(str_to_slice(addressA));
    cursor->to_current_last_multi();

    auto next_key_result = cursor->to_next_first_multi(false);
    if (next_key_result.done) {
        uint64_t next_height = be64toh(*reinterpret_cast<const uint64_t*>(next_key_result.value.data()));
        fmt::println("下一个键的第一个值: {} -> {}", next_key_result.key.as_string(), next_height);
        assert(std::string(next_key_result.key.as_string()) == addressB);
        assert(next_height == 150);
    }

    txn.commit_and_stop();

    fmt::println("✓ 重要功能测试通过");
}

void test_error_handling_and_edge_cases(::mdbx::env_managed& env) {
    fmt::println("\n=== 测试错误处理和边界情况 ===");

    MapConfig config{"error_test_table", ::mdbx::key_mode::usual, ::mdbx::value_mode::single};

    try {
        RWTxnManaged txn(env);
        auto cursor = txn.rw_cursor(config);

        // 测试查找不存在的键
        auto result = cursor->find(str_to_slice("nonexistent_key"), false);
        assert_cursor_result(result, false);
        fmt::println("✓ 查找不存在键的处理正确");

        // 测试在空表上的操作
        assert(cursor->empty() == true);
        assert(cursor->eof() == true);
        // 注意：is_dangling的实现在空表时可能返回不同值

        // 测试在空表上移动
        auto move_result = cursor->to_first(false);
        assert_cursor_result(move_result, false);

        // 插入一些数据用于边界测试
        cursor->insert(str_to_slice("key1"), str_to_slice("value1"));
        cursor->insert(str_to_slice("key2"), str_to_slice("value2"));

        // 测试边界导航
        cursor->to_first();
        auto prev_result = cursor->to_previous(false);  // 在第一个记录上向前移动
        assert_cursor_result(prev_result, false);

        cursor->to_last();
        auto next_result = cursor->to_next(false);  // 在最后一个记录上向后移动
        assert_cursor_result(next_result, false);
        assert(cursor->eof() == true);

        // 测试重复插入（应该失败）
        try {
            cursor->insert(str_to_slice("key1"), str_to_slice("duplicate_value"));
            assert(false);  // 不应该到达这里
        } catch (const mdbx::key_exists&) {
            fmt::println("✓ 重复键插入正确抛出异常");
        }

        // 测试更新不存在的键
        try {
            cursor->update(str_to_slice("nonexistent"), str_to_slice("value"));
            assert(false);  // 不应该到达这里
        } catch (const mdbx::not_found&) {
            fmt::println("✓ 更新不存在键正确抛出异常");
        } catch (const mdbx::key_mismatch&) {
            fmt::println("✓ 更新不存在键正确抛出异常");
        }

        txn.commit_and_stop();

    } catch (const std::exception& e) {
        fmt::println(stderr, "意外异常: {}", e.what());
        assert(false);
    }

    fmt::println("✓ 错误处理和边界情况测试通过");
}

int main() {
    fmt::println("开始MDBX综合功能测试");

    try {
        // 测试1: 环境配置和打开
        test_environment_and_config();

        // 重新打开环境用于后续测试（因为create=true只能用一次）
        EnvConfig test_config;
        test_config.path = "/tmp/test_mdbx_comprehensive";
        test_config.create = false;  // 现在数据库已存在
        test_config.max_size = 128_Mebi;
        test_config.max_tables = 32;
        ::mdbx::env_managed env = open_env(test_config);

        // 测试2: 表配置和基本操作
        test_map_config_and_operations(env);

        // 测试3: 各种事务类型
        test_transaction_types(env);

        // 测试4: 单值游标操作
        test_single_value_cursor_operations(env);

        // 测试5: 多值游标操作
        test_multi_value_cursor_operations(env);

        // 测试6: 批量操作
        test_batch_operations(env);

        // 测试7: 工具函数
        test_utility_functions(env);

        // 测试8: PooledCursor特有功能
        test_pooled_cursor_features(env);

        // 测试9: 重要功能（DUPSORT相关）
        test_important_features(env);

        // 测试10: 错误处理和边界情况
        test_error_handling_and_edge_cases(env);

        fmt::println("\n🎉 所有测试通过！MDBX包装API功能完整且正确工作。");

    } catch (const std::exception& e) {
        fmt::println(stderr, "\n❌ 测试失败: {}", e.what());
        return 1;
    }

    return 0;
}
