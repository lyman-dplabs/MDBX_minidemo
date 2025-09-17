# MDBX的日志

## 数据准备阶段

日志格式
```
=== Populating Database ===
Inserting 2000000000 KV pairs into database
  Inserted 100000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 200000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 300000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 400000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 500000/2000000000 KV pairs, batch commit: 6 ms
  Inserted 600000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 700000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 800000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 900000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 1000000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 1100000/2000000000 KV pairs, batch commit: 6 ms
  Inserted 1200000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 1300000/2000000000 KV pairs, batch commit: 5 ms
  Inserted 1400000/2000000000 KV pairs, batch commit: 6 ms
  Inserted 1500000/2000000000 KV pairs, batch commit: 6 ms
  Inserted 1600000/2000000000 KV pairs, batch commit: 6 ms
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
--- READ-ONLY TESTS ---

=== Read Test Round 1 ===
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 7069.00 ms
✓ Average read latency: 69.88 μs
✓ Tp99 read latency: 185.00 μs
✓ Read throughput: 14146.27 ops/sec

=== Read Test Round 2 ===
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 7352.00 ms
✓ Average read latency: 72.69 μs
✓ Tp99 read latency: 189.00 μs
✓ Read throughput: 13601.74 ops/sec
```

## Write only test

日志格式
```
=== Write Test Round 1 ===
Generating 100000 random indices from 2000000000 total KV pairs
Writing 100000 randomly selected KV pairs
✓ Wrote 100000 KV pairs in 40.00 ms
✓ Commit time: 189.00 ms
✓ Average write latency: 0.03 μs
✓ Tp99 write latency: 0.00 μs
✓ Write throughput: 2500000.00 ops/sec

=== Write Test Round 2 ===
Generating 100000 random indices from 2000000000 total KV pairs
Writing 100000 randomly selected KV pairs
✓ Wrote 100000 KV pairs in 41.00 ms
✓ Commit time: 226.00 ms
✓ Average write latency: 0.04 μs
✓ Tp99 write latency: 0.00 μs
✓ Write throughput: 2439024.39 ops/sec
```

## update test

日志格式

```
=== Update Test Round 1 ===
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 6737.00 ms
Updating and committing 100000 KV pairs
✓ Updated and committed 100000 KV pairs
✓ Total mixed operations: 200000 (read: 100000, write: 100000)
✓ Read time: 6737.00 ms, Write time: 32.00 ms
✓ Commit time: 371.00 ms
✓ Average read latency: 66.33 μs
✓ Average write latency: 0.02 μs
✓ Average mixed latency: 33.17 μs
✓ Tp99 mixed latency: 172.00 μs
✓ Mixed throughput: 29546.46 ops/sec

=== Update Test Round 2 ===
Generating 100000 random indices from 2000000000 total KV pairs
Reading 100000 randomly selected KV pairs
✓ Read 100000 KV pairs in 6875.00 ms
Updating and committing 100000 KV pairs
✓ Updated and committed 100000 KV pairs
✓ Total mixed operations: 200000 (read: 100000, write: 100000)
✓ Read time: 6875.00 ms, Write time: 31.00 ms
✓ Commit time: 377.00 ms
✓ Average read latency: 67.71 μs
✓ Average write latency: 0.01 μs
✓ Average mixed latency: 33.86 μs
✓ Tp99 mixed latency: 178.00 μs
✓ Mixed throughput: 28960.32 ops/sec
```

## Mixed

日志格式
```
--- MIXED READ-WRITE TESTS ---

=== Mixed Read-Write Test Round 1 ===
Generating 100000 mixed operations from 2000000000 total KV pairs
Mixed operations: 80000 reads, 20000 writes (8:2 pattern)
✓ Completed 100000 mixed operations (reads: 80000, writes: 20000)
✓ Total mixed time: 2829.00 ms
✓ Commit time: 63.00 ms
✓ Average read latency: 33.71 μs
✓ Average write latency: 0.01 μs
✓ Average mixed latency: 26.97 μs
✓ Tp99 mixed latency: 134.00 μs
✓ Mixed throughput: 35348.18 ops/sec

=== Mixed Read-Write Test Round 2 ===
Generating 100000 mixed operations from 2000000000 total KV pairs
Mixed operations: 80000 reads, 20000 writes (8:2 pattern)
✓ Completed 100000 mixed operations (reads: 80000, writes: 20000)
✓ Total mixed time: 2824.00 ms
✓ Commit time: 60.00 ms
✓ Average read latency: 33.68 μs
✓ Average write latency: 0.01 μs
✓ Average mixed latency: 26.95 μs
✓ Tp99 mixed latency: 134.00 μs
✓ Mixed throughput: 35410.76 ops/sec

=== Mixed Read-Write Test Round 3 ===
Generating 100000 mixed operations from 2000000000 total KV pairs
Mixed operations: 80000 reads, 20000 writes (8:2 pattern)
✓ Completed 100000 mixed operations (reads: 80000, writes: 20000)
✓ Total mixed time: 2819.00 ms
✓ Commit time: 63.00 ms
✓ Average read latency: 33.59 μs
✓ Average write latency: 0.01 μs
✓ Average mixed latency: 26.87 μs
✓ Tp99 mixed latency: 134.00 μs
✓ Mixed throughput: 35473.57 ops/sec

=== Mixed Read-Write Test Round 4 ===
Generating 100000 mixed operations from 2000000000 total KV pairs
Mixed operations: 80000 reads, 20000 writes (8:2 pattern)
✓ Completed 100000 mixed operations (reads: 80000, writes: 20000)
✓ Total mixed time: 2845.00 ms
✓ Commit time: 61.00 ms
✓ Average read latency: 33.93 μs
✓ Average write latency: 0.01 μs
✓ Average mixed latency: 27.14 μs
✓ Tp99 mixed latency: 136.00 μs
✓ Mixed throughput: 35149.38 ops/sec
```