#!/usr/bin/env python3
"""
RocksDB Benchmark Log Parser
Parses RocksDB benchmark logs and generates a structured CSV output with the same schema as MDBX.
"""

import re
import csv
from typing import Dict, List, Optional

def parse_data_preparation(log_content: str) -> Dict:
    """Parse data preparation phase metrics"""
    data_prep = {}
    
    # Find database population summary
    pop_pattern = r'✓ Database populated with (\d+) KV pairs in (\d+) seconds'
    pop_match = re.search(pop_pattern, log_content)
    if pop_match:
        data_prep['total_kv_pairs'] = int(pop_match.group(1))
        data_prep['total_time_seconds'] = int(pop_match.group(2))
    
    # For RocksDB, calculate total commit time from individual batch commits
    batch_commits = re.findall(r'batch commit: (\d+) ms', log_content)
    if batch_commits:
        total_commit_time = sum(int(commit) for commit in batch_commits)
        data_prep['total_commit_time_ms'] = total_commit_time
        data_prep['avg_commit_time_per_batch_ms'] = total_commit_time / len(batch_commits)
    
    return data_prep

def parse_read_tests(log_content: str) -> List[Dict]:
    """Parse read-only test results"""
    read_tests = []
    
    # Pattern for read test rounds
    pattern = r'=== Read Test Round (\d+) ===.*?'
    pattern += r'✓ Read \d+ KV pairs in ([\d.]+) ms.*?'
    pattern += r'✓ Average read latency: ([\d.]+) μs.*?'
    pattern += r'✓ Tp99 read latency: ([\d.]+) μs.*?'
    pattern += r'✓ Read throughput: ([\d.]+) ops/sec'
    
    matches = re.findall(pattern, log_content, re.DOTALL)
    
    for match in matches:
        round_num, total_time, avg_latency, tp99_latency, throughput = match
        read_tests.append({
            'test_type': 'read',
            'round': int(round_num),
            'total_time_ms': float(total_time),
            'avg_latency_us': float(avg_latency),
            'tp99_latency_us': float(tp99_latency),
            'throughput_ops_sec': float(throughput),
            'commit_time_ms': None,
            'read_time_ms': float(total_time),
            'write_time_ms': None,
            'avg_read_latency_us': float(avg_latency),
            'avg_write_latency_us': None,
            'avg_mixed_latency_us': None,
            'tp99_mixed_latency_us': float(tp99_latency),
            'mixed_throughput_ops_sec': None,
            'read_ops': 100000,
            'write_ops': 0
        })
    
    return read_tests

def parse_write_tests(log_content: str) -> List[Dict]:
    """Parse write-only test results"""
    write_tests = []
    
    # Pattern for write test rounds
    pattern = r'=== Write Test Round (\d+) ===.*?'
    pattern += r'✓ Wrote \d+ KV pairs in ([\d.]+) ms.*?'
    pattern += r'✓ Commit time: ([\d.]+) ms.*?'
    pattern += r'✓ Average write latency: ([\d.]+) μs.*?'
    pattern += r'✓ Tp99 write latency: ([\d.]+) μs.*?'
    pattern += r'✓ Write throughput: ([\d.]+) ops/sec'
    
    matches = re.findall(pattern, log_content, re.DOTALL)
    
    for match in matches:
        round_num, total_time, commit_time, avg_latency, tp99_latency, throughput = match
        write_tests.append({
            'test_type': 'write',
            'round': int(round_num),
            'total_time_ms': float(total_time),
            'avg_latency_us': float(avg_latency),
            'tp99_latency_us': float(tp99_latency),
            'throughput_ops_sec': float(throughput),
            'commit_time_ms': float(commit_time),
            'read_time_ms': None,
            'write_time_ms': float(total_time),
            'avg_read_latency_us': None,
            'avg_write_latency_us': float(avg_latency),
            'avg_mixed_latency_us': None,
            'tp99_mixed_latency_us': float(tp99_latency),
            'mixed_throughput_ops_sec': None,
            'read_ops': 0,
            'write_ops': 100000
        })
    
    return write_tests

def parse_update_tests(log_content: str) -> List[Dict]:
    """Parse update test results"""
    update_tests = []
    
    # Pattern for update test rounds
    pattern = r'=== Update Test Round (\d+) ===.*?'
    pattern += r'✓ Read \d+ KV pairs in ([\d.]+) ms.*?'
    pattern += r'✓ Read time: ([\d.]+) ms, Write time: ([\d.]+) ms.*?'
    pattern += r'✓ Commit time: ([\d.]+) ms.*?'
    pattern += r'✓ Average read latency: ([\d.]+) μs.*?'
    pattern += r'✓ Average write latency: ([\d.]+) μs.*?'
    pattern += r'✓ Average mixed latency: ([\d.]+) μs.*?'
    pattern += r'✓ Tp99 mixed latency: ([\d.]+) μs.*?'
    pattern += r'✓ Mixed throughput: ([\d.]+) ops/sec'
    
    matches = re.findall(pattern, log_content, re.DOTALL)
    
    for match in matches:
        (round_num, total_read_time, read_time, write_time, commit_time, 
         avg_read_latency, avg_write_latency, avg_mixed_latency, 
         tp99_mixed_latency, mixed_throughput) = match
        
        update_tests.append({
            'test_type': 'update',
            'round': int(round_num),
            'total_time_ms': float(read_time) + float(write_time),
            'avg_latency_us': float(avg_mixed_latency),
            'tp99_latency_us': float(tp99_mixed_latency),
            'throughput_ops_sec': float(mixed_throughput),
            'commit_time_ms': float(commit_time),
            'read_time_ms': float(read_time),
            'write_time_ms': float(write_time),
            'avg_read_latency_us': float(avg_read_latency),
            'avg_write_latency_us': float(avg_write_latency),
            'avg_mixed_latency_us': float(avg_mixed_latency),
            'tp99_mixed_latency_us': float(tp99_mixed_latency),
            'mixed_throughput_ops_sec': float(mixed_throughput),
            'read_ops': 100000,
            'write_ops': 100000
        })
    
    return update_tests

def parse_mixed_tests(log_content: str) -> List[Dict]:
    """Parse mixed read-write test results"""
    mixed_tests = []
    
    # Pattern for mixed test rounds
    pattern = r'=== Mixed Read-Write Test Round (\d+) ===.*?'
    pattern += r'Mixed operations: (\d+) reads, (\d+) writes.*?'
    pattern += r'✓ Total mixed time: ([\d.]+) ms.*?'
    pattern += r'✓ Commit time: ([\d.]+) ms.*?'
    pattern += r'✓ Average read latency: ([\d.]+) μs.*?'
    pattern += r'✓ Average write latency: ([\d.]+) μs.*?'
    pattern += r'✓ Average mixed latency: ([\d.]+) μs.*?'
    pattern += r'✓ Tp99 mixed latency: ([\d.]+) μs.*?'
    pattern += r'✓ Mixed throughput: ([\d.]+) ops/sec'
    
    matches = re.findall(pattern, log_content, re.DOTALL)
    
    for match in matches:
        (round_num, read_ops, write_ops, total_time, commit_time,
         avg_read_latency, avg_write_latency, avg_mixed_latency,
         tp99_mixed_latency, mixed_throughput) = match
        
        mixed_tests.append({
            'test_type': 'mixed',
            'round': int(round_num),
            'total_time_ms': float(total_time),
            'avg_latency_us': float(avg_mixed_latency),
            'tp99_latency_us': float(tp99_mixed_latency),
            'throughput_ops_sec': float(mixed_throughput),
            'commit_time_ms': float(commit_time),
            'read_time_ms': None,  # Not separately measured in mixed tests
            'write_time_ms': None,  # Not separately measured in mixed tests
            'avg_read_latency_us': float(avg_read_latency),
            'avg_write_latency_us': float(avg_write_latency),
            'avg_mixed_latency_us': float(avg_mixed_latency),
            'tp99_mixed_latency_us': float(tp99_mixed_latency),
            'mixed_throughput_ops_sec': float(mixed_throughput),
            'read_ops': int(read_ops),
            'write_ops': int(write_ops)
        })
    
    return mixed_tests

def main():
    log_file = 'rocksdb_bench_2billion_20250916_221346.log.bak'
    output_file = 'rocksdb_benchmark_results.csv'
    
    # Read log file
    with open(log_file, 'r', encoding='utf-8') as f:
        log_content = f.read()
    
    # Parse all test types
    data_prep = parse_data_preparation(log_content)
    read_tests = parse_read_tests(log_content)
    write_tests = parse_write_tests(log_content)
    update_tests = parse_update_tests(log_content)
    mixed_tests = parse_mixed_tests(log_content)
    
    # Combine all test results
    all_tests = read_tests + write_tests + update_tests + mixed_tests
    
    # Define CSV headers with readable names (same as MDBX)
    headers = [
        'test_type',
        'round',
        'total_time_ms',
        'commit_time_ms',
        'read_time_ms',
        'write_time_ms',
        'avg_latency_us',
        'avg_read_latency_us',
        'avg_write_latency_us',
        'avg_mixed_latency_us',
        'tp99_latency_us',
        'tp99_mixed_latency_us',
        'throughput_ops_sec',
        'mixed_throughput_ops_sec',
        'read_ops',
        'write_ops'
    ]
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        for test in all_tests:
            writer.writerow(test)
    
    # Print summary
    print(f"Parsed RocksDB benchmark log: {log_file}")
    print(f"Generated CSV output: {output_file}")
    print(f"\nData preparation summary:")
    for key, value in data_prep.items():
        print(f"  {key}: {value}")
    print(f"\nTest results summary:")
    print(f"  Read tests: {len(read_tests)} rounds")
    print(f"  Write tests: {len(write_tests)} rounds")
    print(f"  Update tests: {len(update_tests)} rounds")
    print(f"  Mixed tests: {len(mixed_tests)} rounds")
    print(f"  Total test rounds: {len(all_tests)}")

if __name__ == "__main__":
    main()