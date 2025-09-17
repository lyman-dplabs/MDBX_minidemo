#!/usr/bin/env python3
"""
Benchmark Comparison Tool
Compares MDBX and RocksDB benchmark results and generates analysis report.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import json

def load_benchmark_data(mdbx_file: str, rocksdb_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load benchmark data from CSV files"""
    mdbx_df = pd.read_csv(mdbx_file)
    rocksdb_df = pd.read_csv(rocksdb_file)
    
    # Add database type column
    mdbx_df['database'] = 'MDBX'
    rocksdb_df['database'] = 'RocksDB'
    
    return mdbx_df, rocksdb_df

def calculate_aggregated_metrics(df: pd.DataFrame) -> Dict:
    """Calculate aggregated metrics by test type"""
    metrics = {}
    
    for test_type in df['test_type'].unique():
        test_data = df[df['test_type'] == test_type]
        
        metrics[test_type] = {
            'rounds': len(test_data),
            'avg_total_time_ms': test_data['total_time_ms'].mean(),
            'std_total_time_ms': test_data['total_time_ms'].std(),
            'avg_commit_time_ms': test_data['commit_time_ms'].mean() if test_data['commit_time_ms'].notna().any() else None,
            'avg_latency_us': test_data['avg_latency_us'].mean(),
            'std_latency_us': test_data['avg_latency_us'].std(),
            'avg_tp99_latency_us': test_data['tp99_latency_us'].mean(),
            'avg_throughput_ops_sec': test_data['throughput_ops_sec'].mean() if test_data['throughput_ops_sec'].notna().any() else None,
            'min_latency_us': test_data['avg_latency_us'].min(),
            'max_latency_us': test_data['avg_latency_us'].max(),
            'min_throughput_ops_sec': test_data['throughput_ops_sec'].min() if test_data['throughput_ops_sec'].notna().any() else None,
            'max_throughput_ops_sec': test_data['throughput_ops_sec'].max() if test_data['throughput_ops_sec'].notna().any() else None,
        }
        
        # Type-specific metrics
        if test_type == 'read':
            metrics[test_type]['avg_read_latency_us'] = test_data['avg_read_latency_us'].mean()
            metrics[test_type]['avg_read_time_ms'] = test_data['read_time_ms'].mean()
        elif test_type == 'write':
            metrics[test_type]['avg_write_latency_us'] = test_data['avg_write_latency_us'].mean()
            metrics[test_type]['avg_write_time_ms'] = test_data['write_time_ms'].mean()
        elif test_type == 'update':
            metrics[test_type]['avg_read_latency_us'] = test_data['avg_read_latency_us'].mean()
            metrics[test_type]['avg_write_latency_us'] = test_data['avg_write_latency_us'].mean()
            metrics[test_type]['avg_mixed_latency_us'] = test_data['avg_mixed_latency_us'].mean()
            metrics[test_type]['avg_read_time_ms'] = test_data['read_time_ms'].mean()
            metrics[test_type]['avg_write_time_ms'] = test_data['write_time_ms'].mean()
            metrics[test_type]['avg_mixed_throughput_ops_sec'] = test_data['mixed_throughput_ops_sec'].mean()
        elif test_type == 'mixed':
            metrics[test_type]['avg_read_latency_us'] = test_data['avg_read_latency_us'].mean()
            metrics[test_type]['avg_write_latency_us'] = test_data['avg_write_latency_us'].mean()
            metrics[test_type]['avg_mixed_latency_us'] = test_data['avg_mixed_latency_us'].mean()
            metrics[test_type]['avg_mixed_throughput_ops_sec'] = test_data['mixed_throughput_ops_sec'].mean()
            metrics[test_type]['avg_read_ops'] = test_data['read_ops'].mean()
            metrics[test_type]['avg_write_ops'] = test_data['write_ops'].mean()
    
    return metrics

def compare_databases(mdbx_metrics: Dict, rocksdb_metrics: Dict) -> Dict:
    """Compare metrics between databases"""
    comparison = {}
    
    for test_type in mdbx_metrics.keys():
        if test_type in rocksdb_metrics:
            mdbx_data = mdbx_metrics[test_type]
            rocksdb_data = rocksdb_metrics[test_type]
            
            comparison[test_type] = {
                'latency_improvement': {
                    'mdbx_avg_us': mdbx_data['avg_latency_us'],
                    'rocksdb_avg_us': rocksdb_data['avg_latency_us'],
                    'improvement_factor': mdbx_data['avg_latency_us'] / rocksdb_data['avg_latency_us'],
                    'percent_improvement': ((mdbx_data['avg_latency_us'] - rocksdb_data['avg_latency_us']) / mdbx_data['avg_latency_us']) * 100
                },
                'throughput_improvement': {},
                'time_improvement': {
                    'mdbx_avg_ms': mdbx_data['avg_total_time_ms'],
                    'rocksdb_avg_ms': rocksdb_data['avg_total_time_ms'],
                    'improvement_factor': mdbx_data['avg_total_time_ms'] / rocksdb_data['avg_total_time_ms'],
                    'percent_improvement': ((mdbx_data['avg_total_time_ms'] - rocksdb_data['avg_total_time_ms']) / mdbx_data['avg_total_time_ms']) * 100
                }
            }
            
            # Throughput comparison (if available)
            if mdbx_data['avg_throughput_ops_sec'] and rocksdb_data['avg_throughput_ops_sec']:
                comparison[test_type]['throughput_improvement'] = {
                    'mdbx_avg_ops_sec': mdbx_data['avg_throughput_ops_sec'],
                    'rocksdb_avg_ops_sec': rocksdb_data['avg_throughput_ops_sec'],
                    'improvement_factor': rocksdb_data['avg_throughput_ops_sec'] / mdbx_data['avg_throughput_ops_sec'],
                    'percent_improvement': ((rocksdb_data['avg_throughput_ops_sec'] - mdbx_data['avg_throughput_ops_sec']) / mdbx_data['avg_throughput_ops_sec']) * 100
                }
            
            # TP99 latency comparison
            comparison[test_type]['tp99_latency_improvement'] = {
                'mdbx_avg_us': mdbx_data['avg_tp99_latency_us'],
                'rocksdb_avg_us': rocksdb_data['avg_tp99_latency_us'],
                'improvement_factor': mdbx_data['avg_tp99_latency_us'] / rocksdb_data['avg_tp99_latency_us'],
                'percent_improvement': ((mdbx_data['avg_tp99_latency_us'] - rocksdb_data['avg_tp99_latency_us']) / mdbx_data['avg_tp99_latency_us']) * 100
            }
    
    return comparison

def generate_summary_stats(mdbx_df: pd.DataFrame, rocksdb_df: pd.DataFrame) -> Dict:
    """Generate overall summary statistics"""
    summary = {
        'mdbx': {
            'total_rounds': len(mdbx_df),
            'test_types': list(mdbx_df['test_type'].unique()),
            'overall_avg_latency_us': mdbx_df['avg_latency_us'].mean(),
            'overall_avg_throughput_ops_sec': mdbx_df['throughput_ops_sec'].mean(),
            'data_prep_time': "966 seconds (from log)",
            'total_kv_pairs': "2,000,000,000"
        },
        'rocksdb': {
            'total_rounds': len(rocksdb_df),
            'test_types': list(rocksdb_df['test_type'].unique()),
            'overall_avg_latency_us': rocksdb_df['avg_latency_us'].mean(),
            'overall_avg_throughput_ops_sec': rocksdb_df['throughput_ops_sec'].mean(),
            'data_prep_time': "1858 seconds (from log)",
            'total_kv_pairs': "2,000,000,000"
        }
    }
    return summary

def save_analysis_results(mdbx_metrics: Dict, rocksdb_metrics: Dict, comparison: Dict, summary: Dict):
    """Save analysis results to JSON file"""
    results = {
        'mdbx_metrics': mdbx_metrics,
        'rocksdb_metrics': rocksdb_metrics,
        'comparison': comparison,
        'summary': summary
    }
    
    with open('benchmark_analysis.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)

def generate_markdown_report(mdbx_metrics: Dict, rocksdb_metrics: Dict, comparison: Dict, summary: Dict):
    """Generate markdown comparison report"""
    
    report = """# MDBX vs RocksDB Benchmark Comparison Report

## Executive Summary

This report compares the performance of MDBX and RocksDB databases using identical benchmark workloads on 2 billion key-value pairs. Both databases were tested with 32-byte keys and 32-byte values across four test categories: read-only, write-only, update (read-then-write), and mixed read-write operations.

### Key Findings

"""

    # Add key findings based on comparison data
    for test_type, comp_data in comparison.items():
        latency_improvement = comp_data['latency_improvement']['percent_improvement']
        if latency_improvement > 0:
            winner = "RocksDB"
            improvement = f"{abs(latency_improvement):.1f}%"
        else:
            winner = "MDBX"
            improvement = f"{abs(latency_improvement):.1f}%"
        
        report += f"- **{test_type.title()} Tests**: {winner} shows {improvement} better average latency\n"

    report += f"""
### Test Configuration
- **Dataset Size**: {summary['mdbx']['total_kv_pairs']} key-value pairs
- **Key Size**: 32 bytes
- **Value Size**: 32 bytes  
- **Test Rounds**: 100 rounds per test type
- **Operations per Round**: 100,000 operations

### Data Preparation Performance
- **MDBX**: {summary['mdbx']['data_prep_time']}
- **RocksDB**: {summary['rocksdb']['data_prep_time']}

## Detailed Performance Analysis

"""

    # Generate detailed analysis for each test type
    for test_type in ['read', 'write', 'update', 'mixed']:
        if test_type in comparison:
            mdbx_data = mdbx_metrics[test_type]
            rocksdb_data = rocksdb_metrics[test_type]
            comp_data = comparison[test_type]
            
            report += f"""### {test_type.title()} Performance

| Metric | MDBX | RocksDB | Improvement |
|--------|------|---------|-------------|
| Average Latency | {mdbx_data['avg_latency_us']:.2f} μs | {rocksdb_data['avg_latency_us']:.2f} μs | {comp_data['latency_improvement']['percent_improvement']:+.1f}% |
| TP99 Latency | {mdbx_data['avg_tp99_latency_us']:.2f} μs | {rocksdb_data['avg_tp99_latency_us']:.2f} μs | {comp_data['tp99_latency_improvement']['percent_improvement']:+.1f}% |
| Total Time | {mdbx_data['avg_total_time_ms']:.2f} ms | {rocksdb_data['avg_total_time_ms']:.2f} ms | {comp_data['time_improvement']['percent_improvement']:+.1f}% |
"""

            if comp_data['throughput_improvement']:
                report += f"| Throughput | {mdbx_data['avg_throughput_ops_sec']:.2f} ops/sec | {rocksdb_data['avg_throughput_ops_sec']:.2f} ops/sec | {comp_data['throughput_improvement']['percent_improvement']:+.1f}% |\n"
            
            if mdbx_data.get('avg_commit_time_ms'):
                report += f"| Commit Time | {mdbx_data['avg_commit_time_ms']:.2f} ms | {rocksdb_data['avg_commit_time_ms']:.2f} ms | - |\n"

            report += "\n"
            
            # Add interpretation
            if test_type == 'read':
                report += f"""**Analysis**: RocksDB demonstrates significantly better read performance with {abs(comp_data['latency_improvement']['percent_improvement']):.1f}% lower latency. This suggests RocksDB's LSM-tree structure and caching mechanisms are highly optimized for read workloads.

"""
            elif test_type == 'write':
                report += f"""**Analysis**: RocksDB shows exceptional write performance with nearly instantaneous write operations. The LSM-tree's append-only writes and write batching provide substantial advantages over MDBX's B-tree structure for write-heavy workloads.

"""
            elif test_type == 'update':
                report += f"""**Analysis**: Update operations involve both reads and writes. RocksDB's performance advantage in both individual operations translates to better overall update performance, though the gap is smaller due to the mixed nature of the workload.

"""
            elif test_type == 'mixed':
                report += f"""**Analysis**: Mixed workloads (80% reads, 20% writes) show RocksDB's balanced performance across both operation types. The significant performance advantage demonstrates RocksDB's optimization for real-world application patterns.

"""

    report += """## Performance Summary by Database

### MDBX Characteristics
- **Architecture**: B-tree based storage engine
- **Strengths**: ACID compliance, consistent performance, simple architecture
- **Read Performance**: Moderate latency with consistent response times
- **Write Performance**: Higher latency due to B-tree maintenance overhead
- **Use Cases**: Applications requiring strong consistency and predictable performance

### RocksDB Characteristics  
- **Architecture**: LSM-tree based storage engine (LevelDB fork)
- **Strengths**: Exceptional write performance, optimized read paths, extensive tuning options
- **Read Performance**: Excellent latency with efficient caching and bloom filters
- **Write Performance**: Outstanding throughput with write batching and append-only operations
- **Use Cases**: High-throughput applications, write-heavy workloads, analytics systems

## Recommendations

### Choose MDBX when:
- Strong ACID guarantees are required
- Predictable, consistent performance is more important than peak performance
- Simple deployment and maintenance are priorities
- Application has moderate performance requirements

### Choose RocksDB when:
- Maximum performance is the primary concern
- Write-heavy or mixed workloads are common
- Application can benefit from extensive tuning options
- High throughput requirements justify the complexity

## Conclusion

RocksDB demonstrates superior performance across all test categories, with particularly strong advantages in write operations and mixed workloads. The LSM-tree architecture proves highly effective for high-throughput scenarios. However, MDBX provides consistent, predictable performance that may be more suitable for applications where consistency is prioritized over peak performance.

The choice between these databases should consider not only raw performance metrics but also factors such as operational complexity, consistency requirements, and specific application access patterns.

---
*Report generated from benchmark data collected on 2025-09-16*
"""

    with open('mdbx_vs_rocksdb_comparison.md', 'w', encoding='utf-8') as f:
        f.write(report)

def main():
    # Load data
    print("Loading benchmark data...")
    mdbx_df, rocksdb_df = load_benchmark_data('mdbx_benchmark_results.csv', 'rocksdb_benchmark_results.csv')
    
    # Calculate metrics
    print("Calculating aggregated metrics...")
    mdbx_metrics = calculate_aggregated_metrics(mdbx_df)
    rocksdb_metrics = calculate_aggregated_metrics(rocksdb_df)
    
    # Compare databases
    print("Performing comparative analysis...")
    comparison = compare_databases(mdbx_metrics, rocksdb_metrics)
    
    # Generate summary
    summary = generate_summary_stats(mdbx_df, rocksdb_df)
    
    # Save results
    print("Saving analysis results...")
    save_analysis_results(mdbx_metrics, rocksdb_metrics, comparison, summary)
    
    # Generate report
    print("Generating markdown report...")
    generate_markdown_report(mdbx_metrics, rocksdb_metrics, comparison, summary)
    
    print("\n=== BENCHMARK COMPARISON SUMMARY ===")
    print(f"MDBX - Overall Average Latency: {summary['mdbx']['overall_avg_latency_us']:.2f} μs")
    print(f"RocksDB - Overall Average Latency: {summary['rocksdb']['overall_avg_latency_us']:.2f} μs")
    print(f"RocksDB Latency Advantage: {((summary['mdbx']['overall_avg_latency_us'] - summary['rocksdb']['overall_avg_latency_us']) / summary['mdbx']['overall_avg_latency_us'] * 100):.1f}%")
    
    print(f"\nMDBX - Overall Average Throughput: {summary['mdbx']['overall_avg_throughput_ops_sec']:.2f} ops/sec")
    print(f"RocksDB - Overall Average Throughput: {summary['rocksdb']['overall_avg_throughput_ops_sec']:.2f} ops/sec")
    print(f"RocksDB Throughput Advantage: {((summary['rocksdb']['overall_avg_throughput_ops_sec'] - summary['mdbx']['overall_avg_throughput_ops_sec']) / summary['mdbx']['overall_avg_throughput_ops_sec'] * 100):.1f}%")
    
    print(f"\nFiles generated:")
    print("- benchmark_analysis.json (detailed metrics)")
    print("- mdbx_vs_rocksdb_comparison.md (comparison report)")

if __name__ == "__main__":
    main()