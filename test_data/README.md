# 基准测试分析工具

本目录包含用于解析数据库基准测试日志并比较 MDBX 和 RocksDB 性能的工具。

## 概述

工具包包含三个主要的 Python 脚本：
- `parse_mdbx_log.py` - 解析 MDBX 基准测试日志并生成 CSV 数据
- `parse_rocksdb_log.py` - 解析 RocksDB 基准测试日志并生成 CSV 数据  
- `compare_benchmarks.py` - 比较性能数据并生成分析报告

## 前置要求

安装所需的 Python 包：
```bash
pip install pandas numpy
```

## 文件结构

```
test_data/
├── README.md                              # 本文件
├── parse_mdbx_log.py                      # MDBX 日志解析器
├── parse_rocksdb_log.py                   # RocksDB 日志解析器
├── compare_benchmarks.py                  # 对比分析工具
├── mdbx_bench_2billion_20250916_231124.log.bak    # MDBX 基准测试日志
├── rocksdb_bench_2billion_20250916_221346.log.bak # RocksDB 基准测试日志
├── mdbx_sample.md                         # MDBX 日志格式示例
├── rocksdb_sampl.md                       # RocksDB 日志格式示例
├── mdbx_benchmark_results.csv             # 生成的 MDBX CSV 数据
├── rocksdb_benchmark_results.csv          # 生成的 RocksDB CSV 数据
├── benchmark_analysis.json                # 详细分析结果
└── mdbx_vs_rocksdb_comparison.md          # 最终对比报告
```

## 使用指南

### 1. 解析 MDBX 基准测试日志

`parse_mdbx_log.py` 脚本从 MDBX 基准测试日志中提取性能数据。

#### 使用方法：
```bash
python3 parse_mdbx_log.py
```

#### 功能：
- 读取 `mdbx_bench_2billion_20250916_231124.log.bak`
- 从 5 个测试阶段提取数据：
  - 数据准备（数据库填充）
  - 只读测试（100 轮）
  - 只写测试（100 轮）
  - 更新测试（100 轮）
  - 混合读写测试（100 轮）
- 生成 `mdbx_benchmark_results.csv`

#### 输出格式：
CSV 包含 16 列性能指标：
- `test_type` - 测试类型（read、write、update、mixed）
- `round` - 测试轮次（1-100）
- `total_time_ms` - 总操作时间
- `commit_time_ms` - 数据库提交时间
- `read_time_ms`/`write_time_ms` - 单独的读/写时间
- `avg_latency_us` - 平均延迟（微秒）
- `avg_read_latency_us`/`avg_write_latency_us`/`avg_mixed_latency_us`
- `tp99_latency_us` - 99分位延迟
- `throughput_ops_sec` - 每秒操作数
- `read_ops`/`write_ops` - 操作数量

#### 示例输出：
```
解析 MDBX 基准测试日志: mdbx_bench_2billion_20250916_231124.log.bak
生成 CSV 输出: mdbx_benchmark_results.csv

数据准备摘要:
  total_kv_pairs: 2000000000
  total_time_seconds: 966
  total_commit_time_ms: 95945
  avg_commit_time_per_batch_ms: 479.7

测试结果摘要:
  读取测试: 100 轮
  写入测试: 100 轮
  更新测试: 100 轮
  混合测试: 100 轮
  总测试轮次: 400
```

### 2. 解析 RocksDB 基准测试日志

`parse_rocksdb_log.py` 脚本对 RocksDB 日志的工作方式类似。

#### 使用方法：
```bash
python3 parse_rocksdb_log.py
```

#### 功能：
- 读取 `rocksdb_bench_2billion_20250916_221346.log.bak`
- 提取与 MDBX 相同的 5 个测试阶段
- 生成具有相同架构的 `rocksdb_benchmark_results.csv`

#### 输出格式：
与 MDBX 相同的 16 列 CSV 架构，便于直接比较。

#### 示例输出：
```
解析 RocksDB 基准测试日志: rocksdb_bench_2billion_20250916_221346.log.bak
生成 CSV 输出: rocksdb_benchmark_results.csv

数据准备摘要:
  total_kv_pairs: 2000000000
  total_time_seconds: 1858
  total_commit_time_ms: 108853
  avg_commit_time_per_batch_ms: 5.44

测试结果摘要:
  读取测试: 100 轮
  写入测试: 100 轮
  更新测试: 100 轮
  混合测试: 100 轮
  总测试轮次: 400
```

### 3. 比较基准测试结果

`compare_benchmarks.py` 脚本执行全面的分析和比较。

#### 使用方法：
```bash
python3 compare_benchmarks.py
```

#### 前置要求：
- 两个 CSV 文件必须存在（`mdbx_benchmark_results.csv` 和 `rocksdb_benchmark_results.csv`）
- 首先运行解析脚本

#### 功能：
1. **加载数据**: 读取两个 CSV 文件
2. **聚合指标**: 计算平均值、标准差、最小/最大值
3. **执行比较**: 计算改进因子和百分比
4. **生成报告**: 创建详细分析文件

#### 输出文件：

1. **`benchmark_analysis.json`** - JSON 格式的详细指标：
   ```json
   {
     "mdbx_metrics": {...},
     "rocksdb_metrics": {...},
     "comparison": {...},
     "summary": {...}
   }
   ```

2. **`mdbx_vs_rocksdb_comparison.md`** - 包含以下内容的专业 Markdown 报告：
   - 执行摘要
   - 详细性能表格
   - 按测试类型分析
   - 建议

#### 示例输出：
```
=== 基准测试比较摘要 ===
MDBX - 总体平均延迟: 259.49 μs
RocksDB - 总体平均延迟: 23.75 μs
RocksDB 延迟优势: 90.8%

MDBX - 总体平均吞吐量: 4195.13 ops/sec
RocksDB - 总体平均吞吐量: 669989.62 ops/sec
RocksDB 吞吐量优势: 15870.6%

生成的文件:
- benchmark_analysis.json (详细指标)
- mdbx_vs_rocksdb_comparison.md (对比报告)
```

## 完整工作流程

运行完整的分析流水线：

```bash
# 步骤 1: 解析 MDBX 日志
python3 parse_mdbx_log.py

# 步骤 2: 解析 RocksDB 日志  
python3 parse_rocksdb_log.py

# 步骤 3: 比较和分析
python3 compare_benchmarks.py

# 查看最终报告
cat mdbx_vs_rocksdb_comparison.md
```

## 日志格式要求

### MDBX 日志格式
解析器期望日志包含以下部分：
- 带有批次完成日志的数据库填充
- 带有延迟和吞吐量指标的读取测试轮次
- 带有提交时间的写入测试轮次
- 带有单独读/写指标的更新测试轮次
- 带有操作分解的混合测试轮次

### RocksDB 日志格式
与 MDBX 类似的结构，但具有 RocksDB 特定格式：
- 不同的批次提交日志格式
- LSM-tree 特定的性能特征
- 显示近零延迟的写入操作指标

## 自定义

### 修改输入文件
要使用不同的日志文件，编辑每个解析器中的 `log_file` 变量：

```python
# 在 parse_mdbx_log.py 中
log_file = 'your_mdbx_log_file.log'

# 在 parse_rocksdb_log.py 中  
log_file = 'your_rocksdb_log_file.log'
```

### 更改输出文件
修改 `output_file` 变量：

```python
output_file = 'your_custom_output.csv'
```

### 添加自定义指标
要添加新指标，扩展解析函数并更新 CSV 标题数组。

## 故障排除

### 常见问题：

1. **ModuleNotFoundError**: 安装所需包
   ```bash
   pip install pandas numpy
   ```

2. **文件未找到**: 确保日志文件在正确目录中
   ```bash
   ls -la *.log.bak
   ```

3. **解析错误**: 检查日志文件格式是否符合预期结构

4. **空结果**: 验证日志文件包含预期的测试部分

### 日志格式验证
检查您的日志是否包含预期模式：
```bash
# 检查测试部分
grep "=== .* Test Round" your_log_file.log

# 检查完成标记
grep "✓.*completed\|✓.*ms" your_log_file.log
```

## 性能注意事项

- 解析大型日志文件（>5000 行）可能需要几秒钟
- CSV 生成是内存高效的，可以处理大型数据集
- 比较分析与数据集大小呈线性扩展

## 支持

如有问题或疑问：
1. 对照 `*_sample.md` 中的示例检查日志文件格式
2. 验证所有前置文件存在
3. 查看错误消息以了解特定解析问题
4. 确保 Python 环境具有所需包

---

*为 MDBX vs RocksDB 基准测试分析工具包生成*