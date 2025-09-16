# MDBX Blockchain State Read Accelerator

这是一个用于区块链状态读取加速的演示项目，以 MDBX 为主要后端，可选支持 RocksDB。

## 项目概述

本项目实现了一个区块链状态查询引擎，支持：
- **MDBX**: 轻量级的嵌入式数据库（默认必需）
- **RocksDB**: 高性能的键值存储数据库（可选）
- 性能基准测试和专用基准测试工具
- 单元测试和需求验证测试

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
```

#### CentOS/RHEL
```bash
# 安装基础依赖
sudo yum install cmake gcc-c++ git
```

**注意**: RocksDB 依赖会在启用 `--rocksdb` 选项时自动通过 vcpkg 安装，无需手动安装系统包。

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

# 构建并运行MDBX性能基准测试
./run.sh --mdbx-bench

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
| `--mdbx-bench` | 运行MDBX性能基准测试 |
| `--rocksdb-bench` | 运行RocksDB性能基准测试（需要 --rocksdb） |

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
├── run_mdbx_bench.sh      # MDBX性能基准测试专用脚本
├── run_rocksdb_bench.sh   # RocksDB性能基准测试专用脚本
├── configs/               # 基准测试配置文件目录
│   ├── README.md          # 配置文件使用说明
│   ├── bench_*.json       # 通用基准测试配置文件
│   ├── mdbx_env_*.json    # MDBX 环境配置文件
│   └── rocksdb_*.json     # RocksDB 配置文件
├── CMakeLists.txt         # CMake 配置
├── vcpkg.json            # vcpkg 依赖配置
├── src/                  # 源代码
│   ├── main.cpp          # 主程序入口
│   ├── benchmark.cpp     # 基准测试
│   ├── mdbx_bench.cpp    # MDBX性能基准测试工具
│   ├── rocksdb_bench.cpp # RocksDB性能基准测试工具
│   ├── core/             # 核心逻辑
│   ├── db/               # 数据库实现
│   └── utils/            # 工具函数
├── cmake/                # CMake 模块
│   └── CPM.cmake         # CPM 包管理器
├── third_party/          # 第三方依赖
│   └── vcpkg/           # vcpkg 包管理器
├── build/                # 构建输出目录
│   ├── mdbx_demo         # 主演示程序
│   ├── benchmark_runner  # 性能基准测试
│   ├── mdbx_bench        # MDBX性能基准测试工具
│   └── rocksdb_bench     # RocksDB性能基准测试工具
├── tests/                # 测试目录
│   ├── integration/      # 集成测试
│   │   └── test_mdbx_demand.cpp # MDBX需求验证测试
│   └── unit/            # 单元测试
├── bench_demand.md       # 基准测试需求规格
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
- RocksDB 会自动通过 vcpkg 安装，请确保网络连接正常
- 如果自动安装失败，手动运行：`./third_party/vcpkg/vcpkg install rocksdb`
- 确保 vcpkg 已正确初始化：`git submodule update --init --recursive`

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

## 专用基准测试工具

项目包含两个专门的数据库基准测试工具，用于深入评估数据库性能：

### MDBX 性能基准测试工具 (mdbx_bench.cpp)

专门的 MDBX 性能基准测试工具，模拟真实的读写工作负载。

#### 核心功能
- **大规模数据初始化**: 创建包含百万级 KV 对的数据库实例
- **多轮随机测试**: 每轮从数据库中随机选择样本进行读写测试
- **精确性能测量**: 分别测量读取时间和提交时间
- **统计分析**: 提供完整的性能统计（平均值、最值、吞吐量等）
- **灵活配置**: 支持命令行参数和配置文件自定义测试参数

#### 使用方法

```bash
# 通过主脚本运行
./run.sh --mdbx-bench

# 通过专用脚本运行（推荐，支持更多选项）
./run_mdbx_bench.sh

# 使用预设配置文件
./run_mdbx_bench.sh --config configs/mdbx_env_performance.json --bench-config configs/bench_large.json

# 快速小规模测试
./run_mdbx_bench.sh --bench-config configs/bench_small.json

# 使用自定义配置
./build/mdbx_bench -c config.json -b bench_config.json

# 查看所有选项
./build/mdbx_bench --help
```

#### 配置选项

**命令行参数**:
- `-c, --config FILE`: EnvConfig JSON 配置文件
- `-b, --bench-config FILE`: BenchConfig JSON 配置文件  
- `-h, --help`: 显示帮助信息

**环境变量**:
- `MDBX_BENCH_TOTAL_KV_PAIRS`: 数据库总KV对数（默认: 1,000,000）
- `MDBX_BENCH_TEST_KV_PAIRS`: 每轮测试KV对数（默认: 100,000）
- `MDBX_BENCH_TEST_ROUNDS`: 测试轮次（默认: 2）
- `MDBX_BENCH_DB_PATH`: 数据库路径

### RocksDB 性能基准测试工具 (rocksdb_bench.cpp)

与 MDBX 工具完全对应的 RocksDB 性能基准测试工具，采用相同的测试逻辑和度量标准。

#### 核心功能
- **相同测试架构**: 与 mdbx_bench 完全相同的测试流程和统计方法
- **RocksDB 优化**: 针对 RocksDB 特性优化的配置和批处理操作
- **性能对比**: 便于与 MDBX 进行直接性能比较
- **配置兼容**: 支持相同的配置文件格式和环境变量

#### 使用方法

```bash
# 通过主脚本运行（需要启用RocksDB支持）
./run.sh --rocksdb --rocksdb-bench

# 通过专用脚本运行（推荐，支持更多选项）
./run_rocksdb_bench.sh

# 使用预设配置文件
./run_rocksdb_bench.sh --config configs/rocksdb_high_throughput.json --bench-config configs/bench_large.json

# 快速小规模测试
./run_rocksdb_bench.sh --bench-config configs/bench_small.json

# 使用自定义配置
./build/rocksdb_bench -c rocksdb_config.json -b bench_config.json

# 查看所有选项
./build/rocksdb_bench --help
```

#### 配置选项

**命令行参数**:
- `-c, --config FILE`: RocksDBConfig JSON 配置文件
- `-b, --bench-config FILE`: BenchConfig JSON 配置文件
- `-h, --help`: 显示帮助信息

**环境变量**:
- `ROCKSDB_BENCH_TOTAL_KV_PAIRS`: 数据库总KV对数（默认: 1,000,000）
- `ROCKSDB_BENCH_TEST_KV_PAIRS`: 每轮测试KV对数（默认: 100,000）
- `ROCKSDB_BENCH_TEST_ROUNDS`: 测试轮次（默认: 2）
- `ROCKSDB_BENCH_DB_PATH`: 数据库路径

#### 预设配置文件

项目提供了多种预设配置文件，位于 `configs/` 目录中：

**MDBX 环境配置**:
- `mdbx_env_default.json` - 默认配置 (4KB页面，8GB数据库)
- `mdbx_env_performance.json` - 性能优化配置 (16KB页面，启用写映射)
- `mdbx_env_memory.json` - 内存数据库配置

**RocksDB 配置**:
- `rocksdb_default.json` - 默认配置 (64MB写缓冲区)
- `rocksdb_performance.json` - 性能优化配置 (128MB写缓冲区)
- `rocksdb_memory_optimized.json` - 内存优化配置
- `rocksdb_high_throughput.json` - 高吞吐量配置 (256MB写缓冲区)

**基准测试配置**（两个工具通用）:
- `bench_default.json` - 默认测试 (100万KV对，测试10万对)
- `bench_small.json` - 快速测试 (10万KV对，测试1万对)
- `bench_large.json` - 大规模测试 (500万KV对，测试50万对)
- `bench_stress.json` - 极限测试 (1000万KV对，测试100万对)

#### 测试逻辑

两个基准测试工具采用完全相同的测试流程：

1. **数据库初始化阶段**
   - 创建包含指定数量（默认100万）个32字节KV对的数据库
   - 批量写入数据并测量初始化时间

2. **多轮性能测试阶段**
   - 每轮随机选择指定数量（默认10万）个KV对进行测试
   - **读取测试**: 测量随机读取操作的时间和成功率
   - **更新测试**: 对读取的数据进行更新并测量提交时间
   - 记录每轮的详细性能指标

3. **统计分析阶段**
   - 计算所有轮次的平均、最小、最大性能指标
   - 输出读取吞吐量、提交延迟等关键性能数据
   - 提供完整的性能分析报告

#### 性能对比优势

通过使用相同的测试逻辑和参数，可以获得：
- **公平对比**: 相同的数据规模、测试模式和测量方法
- **一致性**: 统一的性能指标和统计分析
- **可重现**: 相同的配置系统和环境变量支持
- **全面性**: 涵盖初始化、读取、更新等完整数据库操作
- **预设场景**: 丰富的配置文件支持不同测试场景（小规模、大规模、高性能等）

#### 快速开始示例

```bash
# 快速测试（开发调试）
./run_mdbx_bench.sh --bench-config configs/bench_small.json
./run_rocksdb_bench.sh --bench-config configs/bench_small.json

# 标准性能测试
./run_mdbx_bench.sh --config configs/mdbx_env_default.json --bench-config configs/bench_default.json
./run_rocksdb_bench.sh --config configs/rocksdb_default.json --bench-config configs/bench_default.json

# 高性能对比测试
./run_mdbx_bench.sh --config configs/mdbx_env_performance.json --bench-config configs/bench_large.json
./run_rocksdb_bench.sh --config configs/rocksdb_high_throughput.json --bench-config configs/bench_large.json

# 极限压力测试
./run_mdbx_bench.sh --config configs/mdbx_env_performance.json --bench-config configs/bench_stress.json
./run_rocksdb_bench.sh --config configs/rocksdb_high_throughput.json --bench-config configs/bench_stress.json
```

详细的配置文件说明请参考 `configs/README.md`。

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

项目包含专门的MDBX功能验证测试 (`tests/integration/test_mdbx_demand.cpp`)，基于 `test_demand.md` 的需求规格，全面验证MDBX数据库的关键功能。该测试确保MDBX封装API满足区块链状态存储的所有功能需求。

### 基础功能测试

#### 1. 事务提交和回滚测试 (`test_basic_1_rw_transaction_commit_abort`)
```cpp
// 测试读写事务的生命周期管理
RWTxnManaged rw_txn(g_env);
auto cursor = rw_txn.rw_cursor(table_config);
cursor->insert(str_to_slice("key1"), str_to_slice("value1"));

// commit场景：数据持久化
rw_txn.commit_and_stop();  // ✅ 表和数据被创建

// abort场景：数据回滚
rw_txn.abort();            // ✅ 表不会被创建
```

**验证内容**：
- 读写事务 (`MDBX_TXN_READWRITE`) commit后表和数据正确创建
- 读写事务 abort后表不会被创建
- 事务状态管理和资源清理

#### 2. 只读事务操作限制测试 (`test_basic_2_readonly_transaction_restrictions`)
```cpp
// 只读事务只能读取，不能写入
ROTxnManaged ro_txn(g_env);  // MDBX_TXN_RDONLY
auto ro_cursor = ro_txn.ro_cursor(table_config);

// ✅ 可以读取数据
auto result = ro_cursor->find(str_to_slice("key"));

// ❌ 无法获取写游标（编译时阻止）
// ro_txn.rw_cursor() 方法不存在
```

**验证内容**：
- 只读事务类型安全：通过类型系统在编译时阻止写操作
- 只读事务可以正常读取现有数据
- 事务ID和状态查询功能

#### 3. 并发事务测试 (`test_basic_3_concurrent_transactions`)
```cpp
// 场景1：多个只读事务并发
ROTxnManaged ro_txn1(g_env), ro_txn2(g_env), ro_txn3(g_env);
// 所有事务都能读取相同数据

// 场景2：MVCC隔离测试
ROTxnManaged old_txn(g_env);              // 先启动只读事务
auto initial_value = old_txn.read("key"); // 读取初始值

{
    RWTxnManaged rw_txn(g_env);
    rw_txn.modify("key", "new_value");     // 修改数据
    rw_txn.commit_and_stop();              // 提交修改
}

auto unchanged = old_txn.read("key");      // ✅ 仍读取到初始值（MVCC）
```

**验证内容**：
- 多个只读事务可以并发访问相同数据
- MVCC（多版本并发控制）隔离特性：老事务看不到新提交的修改
- 新事务能看到最新的修改结果

#### 4. DUPSORT表功能测试 (`test_basic_4_dupsort_table_operations`)
```cpp
// 创建支持多值的表
MapConfig dupsort_config{"test", ::mdbx::key_mode::usual, ::mdbx::value_mode::multi};
auto cursor = rw_txn.rw_cursor_dup_sort(dupsort_config);

// 同一key存储多个不同value
cursor->append(str_to_slice("user123"), str_to_slice("role_admin"));
cursor->append(str_to_slice("user123"), str_to_slice("role_editor"));
cursor->append(str_to_slice("user123"), str_to_slice("role_viewer"));

// ✅ 存储了3个不同值
assert(cursor->count_multivalue() == 3);

// ❌ 重复值不会被重复存储
cursor->append(str_to_slice("user123"), str_to_slice("role_admin")); // 异常或被忽略
```

**验证内容**：
- DUPSORT模式允许同一key存储多个不同value
- 重复value不会被重复存储（自动去重）
- 精确查找特定的key-value组合
- 多值计数和遍历功能

### 业务功能测试

#### 1. GET_BOTH_RANGE等价功能测试 (`test_business_1_get_both_range_equivalent`)
```cpp
// 场景：区块链地址到区块高度的映射
// 数据：addr -> hex(100), hex(150), hex(200)

// 查询：找到 >= hex(175) 的第一个值
auto result = cursor->lower_bound_multivalue(
    str_to_slice(addr), 
    str_to_slice(uint64_to_hex(175))
);

// ✅ 应该找到 hex(200)
assert(hex_to_uint64(result.value.as_string()) == 200);
```

**验证内容**：
- 使用 `lower_bound_multivalue` 实现 MDBX_GET_BOTH_RANGE 功能
- 区块链地址到区块高度的范围查询场景
- 边界条件测试（精确匹配、超出范围）

#### 2. PREV_DUP等价功能测试 (`test_business_2_prev_dup_equivalent`)
```cpp
// 定位到特定的key-value：addr -> hex(200)
cursor->find_multivalue(str_to_slice(addr), str_to_slice(uint64_to_hex(200)));

// 查找前一个value
auto prev_result = cursor->to_current_prev_multi();

// ✅ 应该找到 hex(150)
assert(hex_to_uint64(prev_result.value.as_string()) == 150);
```

**验证内容**：
- 使用 `to_current_prev_multi` 实现 MDBX_PREV_DUP 功能
- 多值表中的前后导航功能
- 边界情况：从最小值查找前一个值

#### 3. 多表原子性事务测试 (`test_business_3_atomic_multi_table_transaction`)
```cpp
// 区块链场景：同时更新状态表和存储表
{
    RWTxnManaged atomic_txn(g_env);
    
    // 表1：地址状态 addr+storagekey -> hex(100)
    auto cursor1 = atomic_txn.rw_cursor(table1_config);
    cursor1->insert(str_to_slice(combined_key), str_to_slice(uint64_to_hex(100)));
    
    // 表2：存储数据 addr+storagekey -> hex(100)+storagevalue
    auto cursor2 = atomic_txn.rw_cursor(table2_config);
    cursor2->insert(str_to_slice(combined_key), str_to_slice(value_with_storage));
    
    // 原子提交：要么都成功，要么都失败
    atomic_txn.commit_and_stop();
}

// 验证两个表的数据都存在
assert(table1_has_data && table2_has_data);
```

**验证内容**：
- 单一事务内对多个表的原子写入操作
- 事务提交时的原子性保证：所有操作同时生效
- 事务回滚时的一致性保证：所有操作都被撤销
- 区块链状态更新的典型使用场景

### 测试架构特点

#### 环境设置和清理
```cpp
void setup_environment() {
    // 自动清理旧测试数据
    std::filesystem::remove_all("/tmp/test_mdbx_demand");
    
    // 配置测试环境
    EnvConfig config{
        .path = "/tmp/test_mdbx_demand",
        .max_size = 128_Mebi,
        .max_tables = 64,
        .max_readers = 50
    };
    
    g_env = open_env(config);
}
```

#### 辅助工具函数
- **类型转换**: `str_to_byteview`, `byteview_to_str`, `str_to_slice`
- **数据格式化**: `uint64_to_hex`, `hex_to_uint64` （区块链十六进制格式）
- **结果验证**: `assert_cursor_result` （统一的断言验证）
- **字符串处理**: `to_std_string` （跨类型字符串转换）

#### 测试覆盖范围
- ✅ **事务管理**: 提交、回滚、并发、隔离
- ✅ **数据操作**: 插入、查询、更新、删除
- ✅ **高级功能**: 多值表、范围查询、导航操作
- ✅ **原子性**: 多表事务、一致性保证
- ✅ **边界条件**: 空结果、重复数据、并发访问
- ✅ **实际场景**: 区块链地址映射、状态存储

### 运行测试

```bash
# 运行完整的MDBX需求验证测试
./run.sh --test-demand

# 包含在所有测试中
./run.sh --tests

# 仅运行需求验证测试（直接执行）
./build/tests/test_mdbx_demand
```

### 测试输出示例
```
=== 设置测试环境 ===
✓ 测试环境设置完成

=== 基础功能1: 读写事务commit和abort测试 ===
✓ 表 'test_commit_table' 创建成功
✓ 数据在commit后可读取
✓ 表 'test_abort_table' 没有创建（符合预期）

... [详细测试输出] ...

🎉 所有测试通过！MDBX需求功能验证完成。
```

该测试套件确保MDBX封装完全满足 `test_demand.md` 中定义的所有功能需求，为区块链状态存储提供可靠、高性能的数据库操作接口。

