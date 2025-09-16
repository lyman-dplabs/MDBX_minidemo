# 基准测试配置文件

此目录包含各种预配置的基准测试配置文件，用于测试不同场景下的数据库性能。

## 配置文件类型

### 基准测试配置文件 (BenchConfig)
这些配置文件定义测试参数，适用于 MDBX 和 RocksDB 两个基准测试工具：

- **`bench_default.json`** - 默认测试配置
  - 100万个KV对数据库，每轮测试10万个KV对，2轮测试
  - 适用于快速性能验证

- **`bench_small.json`** - 小规模测试配置
  - 10万个KV对数据库，每轮测试1万个KV对，3轮测试
  - 适用于开发调试和快速测试

- **`bench_large.json`** - 大规模测试配置
  - 500万个KV对数据库，每轮测试50万个KV对，5轮测试
  - 适用于生产环境性能评估

- **`bench_stress.json`** - 压力测试配置
  - 1000万个KV对数据库，每轮测试100万个KV对，10轮测试
  - 适用于极限性能测试

### MDBX 环境配置文件 (EnvConfig)

- **`mdbx_env_default.json`** - MDBX 默认配置
  - 4KB页面大小，8GB最大数据库大小
  - 适用于一般性能测试

- **`mdbx_env_performance.json`** - MDBX 性能优化配置
  - 16KB页面大小，启用写映射，16GB最大数据库大小
  - 适用于高性能场景

- **`mdbx_env_memory.json`** - MDBX 内存数据库配置
  - 内存数据库模式，4GB最大大小
  - 适用于内存性能测试

### RocksDB 配置文件

- **`rocksdb_default.json`** - RocksDB 默认配置
  - 64MB写缓冲区，标准压缩设置
  - 适用于一般性能测试

- **`rocksdb_performance.json`** - RocksDB 性能优化配置
  - 128MB写缓冲区，更激进的压缩参数
  - 适用于高性能写入场景

- **`rocksdb_memory_optimized.json`** - RocksDB 内存优化配置
  - 32MB写缓冲区，减少内存使用
  - 适用于内存受限环境

- **`rocksdb_high_throughput.json`** - RocksDB 高吞吐量配置
  - 256MB写缓冲区，优化批处理性能
  - 适用于大批量写入场景

## 使用方法

### 使用专用脚本运行

```bash
# MDBX 基准测试 - 使用性能优化配置
./run_mdbx_bench.sh --config configs/mdbx_env_performance.json --bench-config configs/bench_large.json

# RocksDB 基准测试 - 使用高吞吐量配置
./run_rocksdb_bench.sh --config configs/rocksdb_high_throughput.json --bench-config configs/bench_large.json
```

### 使用主脚本运行

```bash
# MDBX 测试
./run.sh --mdbx-bench

# RocksDB 测试（需要启用RocksDB支持）
./run.sh --rocksdb --rocksdb-bench
```

### 直接运行可执行文件

```bash
# 编译后直接运行
./build/mdbx_bench --config configs/mdbx_env_default.json --bench-config configs/bench_default.json
./build/rocksdb_bench --config configs/rocksdb_default.json --bench-config configs/bench_default.json
```

## 配置文件参数说明

### BenchConfig 参数

| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `total_kv_pairs` | number | 数据库中总的KV对数量 | 1,000,000 |
| `test_kv_pairs` | number | 每轮测试的KV对数量 | 100,000 |
| `test_rounds` | number | 测试轮次数 | 2 |
| `db_path` | string | 数据库文件路径 | "/tmp/bench_default" |

### MDBX EnvConfig 参数

| 参数 | 类型 | 说明 | 推荐值 |
|------|------|------|--------|
| `page_size` | number | MDBX页面大小(字节) | 4096/8192/16384 |
| `max_size` | number | 最大数据库大小(字节) | 根据数据量调整 |
| `write_map` | boolean | 启用写映射模式 | 高性能场景设为true |
| `growth_size` | number | 自动扩展大小(字节) | 1GB-2GB |

### RocksDB Config 参数

| 参数 | 类型 | 说明 | 推荐值 |
|------|------|------|--------|
| `write_buffer_size` | number | 写缓冲区大小(字节) | 64MB-256MB |
| `max_write_buffer_number` | number | 最大写缓冲区数量 | 3-6 |
| `target_file_size_base` | number | 目标SST文件大小(字节) | 64MB-256MB |
| `level0_file_num_compaction_trigger` | number | L0压缩触发文件数 | 2-4 |

## 性能测试建议

### 快速测试 (开发调试)
```bash
./run_mdbx_bench.sh --bench-config configs/bench_small.json
./run_rocksdb_bench.sh --bench-config configs/bench_small.json
```

### 标准性能测试
```bash
./run_mdbx_bench.sh --config configs/mdbx_env_default.json --bench-config configs/bench_default.json
./run_rocksdb_bench.sh --config configs/rocksdb_default.json --bench-config configs/bench_default.json
```

### 高性能对比测试
```bash
./run_mdbx_bench.sh --config configs/mdbx_env_performance.json --bench-config configs/bench_large.json
./run_rocksdb_bench.sh --config configs/rocksdb_performance.json --bench-config configs/bench_large.json
```

### 极限压力测试
```bash
./run_mdbx_bench.sh --config configs/mdbx_env_performance.json --bench-config configs/bench_stress.json
./run_rocksdb_bench.sh --config configs/rocksdb_high_throughput.json --bench-config configs/bench_stress.json
```

## 注意事项

1. **存储空间**: 大规模测试需要足够的磁盘空间
2. **内存需求**: 高性能配置需要更多内存
3. **测试环境**: 建议在SSD上运行测试以获得最佳性能
4. **并发测试**: 同时运行多个基准测试时注意资源争用