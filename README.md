# MDBX Blockchain State Read Accelerator

这是一个用于区块链状态读取加速的演示项目，支持 RocksDB 和 MDBX 两种数据库后端。

## 项目概述

本项目实现了一个区块链状态查询引擎，支持：
- **RocksDB**: 高性能的键值存储数据库
- **MDBX**: 轻量级的嵌入式数据库（可选）
- 性能基准测试
- 单元测试

## 快速开始

### 前置要求

- **CMake 3.21+**
- **C++23 兼容的编译器** (GCC 12+, Clang 14+)
- **Git** (用于管理子模块)
- **libmdbx** (可选，仅在使用 MDBX 功能时需要)

### 安装依赖

#### Ubuntu/Debian
```bash
# 安装基础依赖
sudo apt update
sudo apt install cmake build-essential git

# 安装 MDBX (可选)
sudo apt install libmdbx-dev
```

#### CentOS/RHEL
```bash
# 安装基础依赖
sudo yum install cmake gcc-c++ git

# 安装 MDBX (可选)
sudo yum install libmdbx-devel
```

### 构建和运行

项目提供了一个自动化构建脚本 `run.sh`，支持多种构建和运行选项。

#### 基本用法

```bash
# 构建项目（仅 RocksDB）
./run.sh

# 构建并运行演示程序
./run.sh --demo

# 构建并运行性能基准测试
./run.sh --benchmark

# 构建并运行单元测试
./run.sh --tests

# 构建并运行MDBX需求验证测试
./run.sh --mdbx --test-demand

# 构建并运行所有组件
./run.sh --demo --benchmark --tests
```

#### 高级选项

```bash
# 启用 MDBX 支持
./run.sh --mdbx --demo

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
./run.sh --benchmark --filter "RocksDB*"

# 自定义时间单位和最小运行时间
./run.sh --benchmark --time-unit us --min-time 1s

# 输出为 JSON 格式
./run.sh --benchmark --format json
```

## 脚本选项详解

### 构建选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `--mdbx` | 启用 MDBX 支持 | 禁用 |
| `--release` | 发布模式构建 | Debug |
| `--clean` | 清理构建目录 | 否 |
| `--jobs N` | 并行构建任务数 | CPU 核心数 |
| `--verbose` | 详细输出 | 否 |

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

### 2. MDBX 依赖问题

**问题**: 使用 `--mdbx` 选项时构建失败。

**解决方案**:
- 安装系统级 MDBX 库：`sudo apt install libmdbx-dev`
- 或者让脚本通过 CPM 自动下载编译 MDBX

### 3. vcpkg 相关问题

**问题**: vcpkg 依赖安装失败。

**解决方案**:
- 确保网络连接正常
- 检查 `vcpkg.json` 文件格式是否正确
- 手动运行 `./third_party/vcpkg/vcpkg install`

### 4. 编译器版本问题

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

- **插入性能**: 批量插入键值对
- **查询性能**: 随机和顺序查询
- **范围查询**: 前缀和范围扫描
- **并发性能**: 多线程访问

运行基准测试：
```bash
./run.sh --benchmark --filter "Insert*"
./run.sh --benchmark --filter "Query*"
./run.sh --benchmark --filter "Range*"
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
./run.sh --mdbx --test-demand

# 包含在所有测试中
./run.sh --mdbx --tests
```

该测试确保MDBX封装API满足所有指定的功能需求，为区块链状态存储提供可靠的数据库操作接口。