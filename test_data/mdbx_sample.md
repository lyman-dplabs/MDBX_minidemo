# MDBX的日志

## 数据准备阶段

日志格式
有200条这样的日志
```
Using batch size: 10000000 KV pairs per transaction
  Processing batch 1/200: KV pairs 0 to 9999999 (10000000 pairs)
    ✓ Batch 1 completed: 10000000 pairs in 4711 ms (commit: 439 ms)
  Processing batch 2/200: KV pairs 10000000 to 19999999 (10000000 pairs)
    ✓ Batch 2 completed: 10000000 pairs in 4817 ms (commit: 435 ms)
```

准备完成后
```
✓ Database populated with 2000000000 KV pairs in 966 seconds
  Key size: 32 bytes
  Value size: 32 bytes
  Batch size: 10000000 KV pairs
  Total batches: 200
  Total commit time: 95945 ms
  Average commit time per batch: 479.7 ms
```

## Read Test

日志格式
```
=== Read Test Round 1 ===
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 36710.00 ms
✓ Average read latency: 366.10 μs
✓ Tp99 read latency: 884.00 μs
✓ Read throughput: 2724.05 ops/sec
```

## Write only test

日志格式
```
--- WRITE-ONLY TESTS ---

=== Write Test Round 1 ===
Generating 100000 random indices from 2000000000 total KV pairs
Writing 100000 randomly selected KV pairs
✓ Wrote 100000 KV pairs in 34630.00 ms
✓ Commit time: 1055.00 ms
✓ Average write latency: 345.08 μs
✓ Tp99 write latency: 782.00 μs
✓ Write throughput: 2887.67 ops/sec
```

## update test

日志格式

```
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 25012.00 ms
Updating and committing 100000 KV pairs
✓ Updated and committed 100000 KV pairs
✓ Total mixed operations: 200000 (read: 100000, write: 100000)
✓ Read time: 25012.00 ms, Write time: 5900.00 ms
✓ Commit time: 2097.00 ms
✓ Average read latency: 248.95 μs
✓ Average write latency: 58.21 μs
✓ Average mixed latency: 153.58 μs
✓ Tp99 mixed latency: 492.00 μs
✓ Mixed throughput: 6469.98 ops/sec

=== Update Test Round 2 ===
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 26922.00 ms
Updating and committing 100000 KV pairs
✓ Updated and committed 100000 KV pairs
✓ Total mixed operations: 200000 (read: 100000, write: 100000)
✓ Read time: 26922.00 ms, Write time: 5893.00 ms
✓ Commit time: 1785.00 ms
✓ Average read latency: 268.10 μs
✓ Average write latency: 58.14 μs
✓ Average mixed latency: 163.12 μs
✓ Tp99 mixed latency: 510.00 μs
✓ Mixed throughput: 6094.77 ops/sec
```

## Mixed

日志格式
```
=== Mixed Read-Write Test Round 1 ===
Generating 100000 mixed operations from 2000000000 total KV pairs
Mixed operations: 80000 reads, 20000 writes (8:2 pattern)
✓ Completed 100000 mixed operations (reads: 80000, writes: 20000)
✓ Total mixed time: 27793.00 ms
✓ Commit time: 22.00 ms
✓ Average read latency: 258.52 μs
✓ Average write latency: 349.75 μs
✓ Average mixed latency: 276.77 μs
✓ Tp99 mixed latency: 652.00 μs
✓ Mixed throughput: 3598.03 ops/sec

=== Mixed Read-Write Test Round 2 ===
Generating 100000 mixed operations from 2000000000 total KV pairs
Mixed operations: 80000 reads, 20000 writes (8:2 pattern)
✓ Completed 100000 mixed operations (reads: 80000, writes: 20000)
✓ Total mixed time: 25928.00 ms
✓ Commit time: 487.00 ms
✓ Average read latency: 235.52 μs
✓ Average write latency: 325.39 μs
✓ Average mixed latency: 253.49 μs
✓ Tp99 mixed latency: 543.00 μs
✓ Mixed throughput: 3856.83 ops/sec
```