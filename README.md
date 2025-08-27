# MDBX Blockchain State Read Accelerator

这是一个用于区块链状态读取加速的演示项目，以 MDBX 为主要后端，可选支持 RocksDB。

## 项目概述

本项目实现了一个区块链状态查询引擎，支持：
- **MDBX**: 轻量级的嵌入式数据库（默认必需）
- **RocksDB**: 高性能的键值存储数据库（可选）
- 性能基准测试
- 单元测试

## 快速开始

### 前置要求

- **CMake 3.21+**
- **C++23 兼容的编译器** (GCC 12+, Clang 14+)
- **Git** (用于管理子模块)
- **MDBX** (通过 CPM 自动下载构建，无需手动安装)

### 安装依赖

#### Ubuntu/Debian
```bash
# 安装基础依赖
sudo apt update
sudo apt install cmake build-essential git

# 安装 RocksDB (可选，仅在使用 --rocksdb 选项时需要)
sudo apt install librocksdb-dev
```

#### CentOS/RHEL
```bash
# 安装基础依赖
sudo yum install cmake gcc-c++ git

# 安装 RocksDB (可选，仅在使用 --rocksdb 选项时需要)
sudo yum install rocksdb-devel
```

### 构建和运行

项目提供了一个自动化构建脚本 `run.sh`，支持多种构建和运行选项。

#### 基本用法

```bash
# 构建项目（默认仅 MDBX）
./run.sh

# 构建并运行演示程序
./run.sh --demo

# 构建并运行性能基准测试
./run.sh --benchmark

# 构建并运行单元测试
./run.sh --tests

# 构建并运行MDBX需求验证测试
./run.sh --test-demand

# 构建并运行所有组件
./run.sh --demo --benchmark --tests
```

#### 高级选项

```bash
# 启用 RocksDB 支持（可选）
./run.sh --rocksdb --demo

# 发布模式构建
./run.sh --release --benchmark

# 清理构建目录
./run.sh --clean --demo

# 指定并行构建任务数
./run.sh --jobs 8 --demo

# 详细输出
./run.sh --verbose --demo
```

#### 基准测试选项

```bash
# 运行特定模式的基准测试
./run.sh --benchmark --filter "MDBX*"

# 自定义时间单位和最小运行时间
./run.sh --benchmark --time-unit us --min-time 1s

# 输出为 JSON 格式
./run.sh --benchmark --format json

# 自定义基准测试数据量
./run.sh --benchmark --accounts 50 --blocks 200 --max-block 50000

# 使用环境变量配置数据量
export BENCH_NUM_ACCOUNTS=20
export BENCH_NUM_BLOCKS_PER_ACCOUNT=500
export BENCH_MAX_BLOCK_NUMBER=100000
./run.sh --benchmark
```

## 脚本选项详解

### 构建选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `--rocksdb` | 启用 RocksDB 支持 | 禁用 |
| `--release` | 发布模式构建 | Debug |
| `--clean` | 清理构建目录 | 否 |
| `--jobs N` | 并行构建任务数 | CPU 核心数 |
| `--verbose` | 详细输出 | 否 |

**注意**: MDBX 是默认启用的，无需额外选项。

### 运行选项

| 选项 | 描述 |
|------|------|
| `--demo` | 运行主演示程序 |
| `--benchmark` | 运行性能基准测试 |
| `--tests` | 运行单元测试 |
| `--test-demand` | 运行MDBX需求验证测试 |

### 基准测试选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `--filter PATTERN` | 基准测试过滤模式 | "*" |
| `--time-unit UNIT` | 时间单位 (ns/us/ms/s) | ms |
| `--min-time TIME` | 最小运行时间 | 0.1s |
| `--format FORMAT` | 输出格式 (console/json/csv) | console |

### 基准测试数据配置

| 选项 | 描述 | 默认值 | 环境变量 |
|------|------|--------|----------|
| `--accounts NUM` | 测试账户数量 | 10 | `BENCH_NUM_ACCOUNTS` |
| `--blocks NUM` | 每个账户的区块数量 | 100 | `BENCH_NUM_BLOCKS_PER_ACCOUNT` |
| `--max-block NUM` | 最大区块号 | 10000 | `BENCH_MAX_BLOCK_NUMBER` |

**注意**: 增加数据量会显著增加基准测试的初始化时间。建议根据测试目标调整这些参数：
- **快速测试**: 使用默认值 (10账户 × 100区块 = 1,000条记录)
- **中等规模**: `--accounts 50 --blocks 200` (10,000条记录)
- **大规模测试**: `--accounts 100 --blocks 1000` (100,000条记录)

## 项目结构

```
mdbx_demo/
├── run.sh                 # 主构建脚本
├── CMakeLists.txt         # CMake 配置
├── vcpkg.json            # vcpkg 依赖配置
├── src/                  # 源代码
│   ├── main.cpp          # 主程序入口
│   ├── benchmark.cpp     # 基准测试
│   ├── core/             # 核心逻辑
│   ├── db/               # 数据库实现
│   └── utils/            # 工具函数
├── cmake/                # CMake 模块
│   └── CPM.cmake         # CPM 包管理器
├── third_party/          # 第三方依赖
│   └── vcpkg/           # vcpkg 包管理器
├── build/                # 构建输出目录
├── test_*.cpp           # 测试文件
├── test_mdbx_demand.cpp # MDBX需求验证测试
└── test_demand.md       # 测试需求规格说明
```

## 常见问题

### 1. CMake 错误：Unknown CMake command "CPMAddPackage"

**问题**: 构建时出现 `CPMAddPackage` 命令未找到的错误。

**解决方案**: 
- 确保 `cmake/CPM.cmake` 文件存在
- 检查 CMake 版本是否支持 (需要 3.21+)
- 如果问题持续，可以手动下载 CPM.cmake 文件

### 2. RocksDB 依赖问题

**问题**: 使用 `--rocksdb` 选项时构建失败。

**解决方案**:
- 安装系统级 RocksDB 库：`sudo apt install librocksdb-dev`
- 或者通过 vcpkg 管理 RocksDB 依赖

### 3. MDBX 构建问题

**问题**: MDBX 通过 CPM 下载或编译失败。

**解决方案**:
- 确保网络连接正常，可以访问 GitHub
- 检查 CMake 版本是否支持 CPM (需要 3.21+)
- 清理构建目录后重试：`./run.sh --clean --demo`

### 4. vcpkg 相关问题

**问题**: vcpkg 依赖安装失败。

**解决方案**:
- 确保网络连接正常
- 检查 `vcpkg.json` 文件格式是否正确
- 手动运行 `./third_party/vcpkg/vcpkg install`

### 5. 编译器版本问题

**问题**: 编译器不支持 C++23 特性。

**解决方案**:
- 升级到 GCC 12+ 或 Clang 14+
- 或者修改 `CMakeLists.txt` 中的 C++ 标准版本

## 开发指南

### 添加新的数据库后端

1. 在 `src/db/` 目录下创建新的实现文件
2. 实现数据库接口
3. 在 `CMakeLists.txt` 中添加条件编译
4. 更新 `run.sh` 脚本的选项

### 添加新的基准测试

1. 在 `src/benchmark.cpp` 中添加新的测试用例
2. 使用 Google Benchmark 框架
3. 运行 `./run.sh --benchmark` 验证

### 添加新的单元测试

1. 创建 `test_*.cpp` 文件
2. 使用 Google Test 框架
3. 在 `CMakeLists.txt` 中添加测试目标
4. 运行 `./run.sh --tests` 验证

## 性能基准测试

项目包含多个性能基准测试，比较不同数据库后端的性能：

### 测试类型

- **MDBX_ExactMatch**: MDBX 精确匹配查询（查询存在的键值对）
- **MDBX_Lookback**: MDBX 历史状态查询（查询可能不存在的键，需要回溯到最近的历史状态）
- **RocksDB_ExactMatch**: RocksDB 精确匹配查询（仅在启用 `--rocksdb` 时可用）
- **RocksDB_Lookback**: RocksDB 历史状态查询（仅在启用 `--rocksdb` 时可用）

### 基准测试架构设计

基准测试采用了独特的**一次初始化，多次独立连接**的设计模式：

#### 数据初始化阶段
1. **单次数据生成**: 在 `SetUp` 阶段只生成一次测试数据，避免重复创建
2. **预填充数据库**: 为每个数据库（MDBX/RocksDB）创建临时连接，填充测试数据后立即关闭连接
3. **查询数据预处理**: 预生成 1000 个精确匹配查询和 1000 个历史状态查询，确保基准测试结果的可重复性

#### 基准测试执行阶段
1. **独立数据库连接**: 每个 `BENCHMARK_F` 测试都创建自己独立的数据库连接实例
2. **线程安全**: 避免多线程环境下的共享状态问题，特别是 RocksDB 的 pthread 锁冲突
3. **公平性比较**: MDBX 和 RocksDB 使用相同的连接模式，确保性能比较的公平性
4. **自动清理**: 连接在测试结束时自动关闭，数据库文件在进程退出时统一清理

#### 设计优势

```cpp
// 传统的共享连接模式（已弃用，有并发问题）
static std::unique_ptr<QueryEngine> shared_engine; // ❌ 多线程不安全

// 当前的独立连接模式（推荐）
BENCHMARK_F(DatabaseBenchmark, MDBX_ExactMatch)(benchmark::State& state) {
    // ✅ 每个测试创建独立连接
    auto mdbx_engine = std::make_unique<QueryEngine>(std::make_unique<MdbxImpl>(mdbx_path_));
    // ... 执行基准测试 ...
    // 连接在作用域结束时自动关闭
}
```

**优势**：
- **线程安全**: 消除了共享状态导致的竞态条件
- **隔离性**: 每个测试相互独立，不会互相干扰
- **可靠性**: 避免了 RocksDB 的 pthread 锁错误
- **一致性**: MDBX 和 RocksDB 使用相同的测试模式
- **效率**: 数据只初始化一次，但连接按需创建

### 运行基准测试

```bash
# 运行所有基准测试（仅 MDBX）
./run.sh --benchmark

# 包含 RocksDB 的完整性能对比
./run.sh --rocksdb --benchmark

# 运行特定类型的测试
./run.sh --benchmark --filter "MDBX*"
./run.sh --benchmark --filter "*ExactMatch*"
./run.sh --benchmark --filter "*Lookback*"

# 自定义时间单位和输出格式
./run.sh --benchmark --time-unit us --format json
```

## MDBX需求验证测试

项目包含专门的MDBX功能验证测试 (`test_mdbx_demand.cpp`)，用于验证MDBX数据库的关键功能需求：

### 基础功能测试

1. **事务提交和回滚测试**
   - 验证读写事务commit后表和数据的正确创建
   - 验证读写事务abort后表不会被创建

2. **只读事务限制测试**
   - 验证只读事务无法执行写操作
   - 通过类型系统确保只读事务的安全性

3. **并发事务测试**
   - 多个只读事务并发读取
   - 读写事务与只读事务的MVCC隔离特性

4. **DUPSORT表功能测试**
   - 同一键多个不同值的存储
   - 重复值的自动去重
   - 精确的键值对查找

### 业务功能测试

1. **GET_BOTH_RANGE等价功能**
   - 地址到区块高度的范围查询
   - 使用`lower_bound_multivalue`实现范围查找

2. **PREV_DUP等价功能**
   - 多值表中的前序值查找
   - 使用`to_current_prev_multi`实现前序导航

3. **多表原子性事务**
   - 单一事务中对多个表的原子写入
   - 事务回滚时的一致性保证

### 运行测试

```bash
# 运行完整的MDBX需求验证测试
./run.sh --test-demand

# 包含在所有测试中
./run.sh --tests
```

该测试确保MDBX封装API满足所有指定的功能需求，为区块链状态存储提供可靠的数据库操作接口。