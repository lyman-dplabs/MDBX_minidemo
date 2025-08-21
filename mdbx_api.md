# MDBX API 使用文档

## 概述

这个文档描述了 `src/db/mdbx.hpp` 和 `src/db/mdbx.cpp` 中提供的MDBX数据库包装API的使用方法。该API提供了对MDBX数据库的高级抽象，支持事务管理、游标操作和批量处理。

## 命名空间

所有API都在 `datastore::kvdb` 命名空间中。

```cpp
using namespace datastore::kvdb;
```

## 核心类型

### 基本类型别名

```cpp
using ByteView = std::span<const std::byte>;           // 字节视图
using Slice = ::mdbx::slice;                           // MDBX切片
using CursorResult = ::mdbx::pair_result;              // 游标结果
using MoveResult = ::mdbx::cursor::move_result;        // 移动结果
using WalkFuncRef = function_ref<void(ByteView key, ByteView value)>; // 遍历函数引用
```

### 字面量后缀

```cpp
size_t size1 = 4_Tebi;   // 4 TiB
size_t size2 = 2_Gibi;   // 2 GiB  
size_t size3 = 128_Mebi; // 128 MiB
size_t size4 = 16_Kibi;  // 16 KiB
```

## 环境配置

### EnvConfig 结构体

用于配置MDBX数据库环境：

```cpp
EnvConfig config;
config.path = "/path/to/database";     // 数据库路径
config.create = true;                  // 是否创建新数据库
config.readonly = false;               // 是否只读模式
config.exclusive = false;              // 是否独占访问
config.in_memory = false;              // 是否内存数据库
config.shared = false;                 // 是否共享访问
config.read_ahead = false;             // 是否启用预读
config.write_map = false;              // 是否启用写映射
config.page_size = 4096;               // 页面大小
config.max_size = 4_Tebi;              // 最大数据库大小
config.growth_size = 2_Gibi;           // 增长步长
config.max_tables = 256;               // 最大表数量
config.max_readers = 100;              // 最大读者数量
```

### 打开数据库环境

```cpp
// 创建新的数据库环境
EnvConfig config;
config.path = "/tmp/my_database";
config.create = true;
config.max_size = 1_Gibi;

::mdbx::env_managed env = open_env(config);
```

## 事务管理

### 只读事务 (ROTxn)

#### ROTxnManaged - 托管只读事务

```cpp
// 创建托管只读事务
ROTxnManaged txn(env);

// 使用事务
auto cursor = txn.ro_cursor(map_config);

// 事务会自动在析构时提交
```

#### ROAccess - 只读访问包装器

```cpp
// 创建只读访问包装器
ROAccess ro_access(env);

// 启动只读事务
auto txn = ro_access.start_ro_tx();
```

### 读写事务 (RWTxn)

#### RWTxnManaged - 托管读写事务

```cpp
// 创建托管读写事务
RWTxnManaged txn(env);

// 禁用自动提交（用于批量操作）
txn.disable_commit();

// 执行多个操作
auto cursor = txn.rw_cursor(map_config);
cursor->insert(key, value);

// 手动提交并续期事务
txn.commit_and_renew();

// 或者提交并停止
txn.commit_and_stop();
```

#### RWAccess - 读写访问包装器

```cpp
// 创建读写访问包装器
RWAccess rw_access(env);

// 启动读写事务
auto txn = rw_access.start_rw_tx();
```

## 表配置

### MapConfig 结构体

```cpp
MapConfig config;
config.name = "my_table";                           // 表名
config.key_mode = ::mdbx::key_mode::usual;         // 键模式
config.value_mode = ::mdbx::value_mode::single;    // 值模式（单值或多值）
```

### 打开表

```cpp
// 在事务中打开表
::mdbx::map_handle map = open_map(txn, config);

// 或者直接打开游标
::mdbx::cursor_managed cursor = open_cursor(txn, config);
```

## 游标操作

### 游标类型

#### ROCursor - 只读游标

```cpp
// 获取只读游标
auto cursor = txn.ro_cursor(map_config);

// 导航操作
auto result = cursor->to_first();           // 移动到第一个记录
auto result = cursor->to_last();            // 移动到最后一个记录
auto result = cursor->to_next();            // 移动到下一个记录
auto result = cursor->to_previous();        // 移动到上一个记录

// 查找操作
auto result = cursor->find(key);            // 精确查找
auto result = cursor->lower_bound(key);     // 下界查找

// 状态检查
bool is_empty = cursor->empty();            // 检查表是否为空
bool is_multi = cursor->is_multi_value();   // 检查是否多值表
bool is_eof = cursor->eof();                // 检查是否到达末尾
```

#### RWCursor - 读写游标

```cpp
// 获取读写游标
auto cursor = txn.rw_cursor(map_config);

// 插入操作
cursor->insert(key, value);                 // 插入（键不存在）
cursor->upsert(key, value);                 // 插入或更新
cursor->update(key, value);                 // 更新（键必须存在）

// 删除操作
bool deleted = cursor->erase();             // 删除当前记录
bool deleted = cursor->erase(key);          // 删除指定键的记录
```

#### ROCursorDupSort - 多值只读游标

```cpp
// 获取多值只读游标
auto cursor = txn.ro_cursor_dup_sort(map_config);

// 多值操作
auto result = cursor->to_current_first_multi();  // 移动到当前键的第一个值
auto result = cursor->to_current_last_multi();   // 移动到当前键的最后一个值
auto result = cursor->to_current_next_multi();   // 移动到当前键的下一个值
auto result = cursor->to_current_prev_multi();   // 移动到当前键的上一个值

// 多值查找
auto result = cursor->find_multivalue(key, value);        // 精确查找键值对
auto result = cursor->lower_bound_multivalue(key, value); // 下界查找键值对

size_t count = cursor->count_multivalue();  // 获取当前键的值数量
```

#### RWCursorDupSort - 多值读写游标

```cpp
// 获取多值读写游标
auto cursor = txn.rw_cursor_dup_sort(map_config);

// 多值操作
cursor->append(key, value);                 // 追加值到键

// 多值删除
bool deleted = cursor->erase(key, value);   // 删除特定键值对
bool deleted = cursor->erase(key, true);    // 删除键的所有值
```

### PooledCursor - 池化游标

```cpp
// 创建池化游标（自动管理MDBX游标句柄）
PooledCursor cursor(txn, map_config);

// 重新绑定到不同的事务和表
cursor.bind(new_txn, new_map_config);

// 克隆游标
auto cloned = cursor.clone();

// 关闭游标（句柄返回池中）
cursor.close();
```

## 批量操作

### 遍历所有记录

```cpp
// 定义遍历函数
auto walker = [](ByteView key, ByteView value) {
    std::cout << "Key: " << std::string_view(reinterpret_cast<const char*>(key.data()), key.size()) << std::endl;
    std::cout << "Value: " << std::string_view(reinterpret_cast<const char*>(value.data()), value.size()) << std::endl;
};

// 正向遍历
size_t count = cursor_for_each(*cursor, walker, CursorMoveDirection::kForward);

// 反向遍历
size_t count = cursor_for_each(*cursor, walker, CursorMoveDirection::kReverse);
```

### 前缀遍历

```cpp
// 遍历以特定前缀开头的键
ByteView prefix = {reinterpret_cast<const std::byte*>("user_"), 5};
size_t count = cursor_for_prefix(*cursor, prefix, walker);
```

### 限制遍历数量

```cpp
// 最多遍历10条记录
size_t count = cursor_for_count(*cursor, walker, 10);
```

### 批量删除

```cpp
// 删除从指定键开始的所有记录
ByteView start_key = {reinterpret_cast<const std::byte*>("user_100"), 8};
size_t deleted = cursor_erase(*cursor, start_key, CursorMoveDirection::kForward);

// 删除以特定前缀开头的所有记录
ByteView prefix = {reinterpret_cast<const std::byte*>("temp_"), 5};
size_t deleted = cursor_erase_prefix(*cursor, prefix);
```

## 工具函数

### 类型转换

```cpp
// ByteView 转 Slice
ByteView byte_view = {reinterpret_cast<const std::byte*>("hello"), 5};
Slice slice = to_slice(byte_view);

// Slice 转 ByteView
ByteView converted = from_slice(slice);
```

### 数据库管理

```cpp
// 检查表是否存在
bool exists = has_map(txn, "my_table");

// 列出所有表
std::vector<std::string> tables = list_maps(txn);

// 获取数据文件路径
std::filesystem::path data_file = get_datafile_path("/path/to/db");
```

### 性能优化

```cpp
// 计算叶子页面最大值大小
size_t max_value_size = max_value_size_for_leaf_page(4096, 32);
size_t max_value_size = max_value_size_for_leaf_page(txn, 32);
```

## 完整示例

### 基本CRUD操作

```cpp
#include "db/mdbx.hpp"
using namespace datastore::kvdb;

// 配置数据库
EnvConfig config;
config.path = "/tmp/example_db";
config.create = true;
config.max_size = 1_Gibi;

// 打开数据库
::mdbx::env_managed env = open_env(config);

// 配置表
MapConfig table_config;
table_config.name = "users";
table_config.value_mode = ::mdbx::value_mode::single;

// 读写事务
RWTxnManaged txn(env);
auto cursor = txn.rw_cursor(table_config);

// 插入数据
std::string key = "user:1";
std::string value = "John Doe";
Slice key_slice{key.data(), key.size()};
Slice value_slice{value.data(), value.size()};
cursor->insert(key_slice, value_slice);

// 提交事务
txn.commit_and_stop();

// 只读事务查询
ROTxnManaged ro_txn(env);
auto ro_cursor = ro_txn.ro_cursor(table_config);

// 查找数据
auto result = ro_cursor->find(key_slice);
if (result) {
    std::string found_value{result.value.data(), result.value.size()};
    std::cout << "Found: " << found_value << std::endl;
}
```

### 批量处理示例

```cpp
// 批量插入
RWTxnManaged batch_txn(env);
batch_txn.disable_commit();

auto batch_cursor = batch_txn.rw_cursor(table_config);

for (int i = 0; i < 1000; ++i) {
    std::string key = "user:" + std::to_string(i);
    std::string value = "User " + std::to_string(i);
    
    Slice key_slice{key.data(), key.size()};
    Slice value_slice{value.data(), value.size()};
    
    batch_cursor->upsert(key_slice, value_slice);
    
    if (i % 100 == 0) {
        batch_txn.commit_and_renew();
        batch_cursor = batch_txn.rw_cursor(table_config);
    }
}

batch_txn.commit_and_stop();

// 批量查询
ROTxnManaged query_txn(env);
auto query_cursor = query_txn.ro_cursor(table_config);

auto walker = [](ByteView key, ByteView value) {
    std::string key_str{reinterpret_cast<const char*>(key.data()), key.size()};
    std::string value_str{reinterpret_cast<const char*>(value.data()), value.size()};
    std::cout << key_str << " = " << value_str << std::endl;
};

size_t processed = cursor_for_each(*query_cursor, walker);
std::cout << "Processed " << processed << " records" << std::endl;
```

### 多值表示例

```cpp
// 配置多值表
MapConfig multi_config;
multi_config.name = "user_tags";
multi_config.value_mode = ::mdbx::value_mode::multi;

RWTxnManaged txn(env);
auto cursor = txn.rw_cursor_dup_sort(multi_config);

// 为用户添加多个标签
std::string user_key = "user:1";
Slice user_slice{user_key.data(), user_key.size()};

std::vector<std::string> tags = {"admin", "moderator", "verified"};
for (const auto& tag : tags) {
    Slice tag_slice{tag.data(), tag.size()};
    cursor->append(user_slice, tag_slice);
}

txn.commit_and_stop();

// 查询用户的所有标签
ROTxnManaged ro_txn(env);
auto ro_cursor = ro_txn.ro_cursor_dup_sort(multi_config);

auto result = ro_cursor->find(user_slice);
if (result) {
    auto tag_walker = [](ByteView key, ByteView value) {
        std::string tag{reinterpret_cast<const char*>(value.data()), value.size()};
        std::cout << "Tag: " << tag << std::endl;
    };
    
    // 遍历当前键的所有值
    auto first_result = ro_cursor->to_current_first_multi();
    if (first_result) {
        do {
            tag_walker(first_result.key, first_result.value);
            first_result = ro_cursor->to_current_next_multi();
        } while (first_result);
    }
}
```

## 错误处理

```cpp
try {
    EnvConfig config;
    config.path = "/invalid/path";
    config.create = true;
    
    ::mdbx::env_managed env = open_env(config);
} catch (const std::runtime_error& e) {
    std::cerr << "Database error: " << e.what() << std::endl;
}

try {
    RWTxnManaged txn(env);
    auto cursor = txn.rw_cursor(map_config);
    
    // 尝试更新不存在的键
    cursor->update(non_existent_key, value);
} catch (const mdbx::not_found& e) {
    std::cerr << "Key not found: " << e.what() << std::endl;
}
```

## 性能建议

1. **使用池化游标**: `PooledCursor` 自动管理MDBX游标句柄，减少内存分配开销。

2. **批量操作**: 对于大量操作，使用 `disable_commit()` 和 `commit_and_renew()` 进行批量处理。

3. **适当的事务大小**: 避免单个事务包含过多操作，定期提交事务。

4. **使用前缀遍历**: 对于范围查询，使用 `cursor_for_prefix()` 而不是手动遍历。

5. **合理配置**: 根据数据大小和访问模式调整 `EnvConfig` 中的参数。

## 注意事项

1. **事务生命周期**: 托管事务在析构时自动提交或回滚，非托管事务需要手动管理。

2. **游标绑定**: 游标绑定到特定事务，不能在事务外使用。

3. **内存管理**: `ByteView` 和 `Slice` 是轻量级引用，不拥有数据。

4. **并发安全**: 多个进程可以同时访问数据库，但单个进程内的并发访问需要适当的同步。

5. **错误处理**: 始终检查函数返回值，使用异常处理机制捕获错误。

