# ChronoCask

> **ChronoCask**: A High-Performance Persistent Key-Value Storage Engine in Java with Append-Only Log Segments, Off-Heap Indexing, Crash Recovery, Safe Segment Compaction, JMH Benchmarking, and Standalone Redis/RESP TCP Server.

---

## 1. Overview

ChronoCask is an embedded and standalone persistent key-value storage engine inspired by the Bitcask model. It is designed to explore systems-level data storage, low Java heap allocation via the **Foreign Function & Memory (FFM) API**, sequential log-structured disk I/O, deterministic crash recovery, crash-safe segment compaction, and high-concurrency client-server networking with **Java 25 Virtual Threads**.

### Key Architectural Highlights

- **Append-Only Disk Storage**: Eliminates random disk writes during mutations by writing sequential records to append-only log segments.
- **In-Memory KeyDir Index**: Maps keys to exact segment and byte offsets for $O(1)$ read lookups without scanning disk logs.
- **Native Memory Management**: Implements an off-heap hash-table index using Java's FFM API (`MemorySegment`, `Arena`) to eliminate Java GC overhead for large key spaces.
- **Authoritative Storage Manifest**: Persistently describes the authoritative generation of active and immutable segments (`MANIFEST`), ensuring deterministic crash consistency without directory file heuristics.
- **Fast Startup via Hint Files**: Generates durable `.hint` files for sealed segments, enabling sub-millisecond recovery by reading index metadata without loading full payload values.
- **Safe Prefix Compaction**: Enforces a strict contiguous-prefix boundary rule starting from the oldest segment to eliminate superseded records and tombstones without risking deleted key resurrection.
- **Standalone Redis-Compatible Server**: Speaks the standard Redis Serialization Protocol (RESP2), allowing any Redis client (`redis-cli`, Python, Jedis, Go, Node.js) to interact directly with ChronoCask.
- **Virtual Thread Concurrency**: Powered by Java 25 virtual threads (`Thread.ofVirtual()`), handling thousands of concurrent client connections with near-zero memory footprint and zero thread-pool exhaustion.
- **Rigorous JMH Performance Benchmarking**: Built-in microbenchmark harness evaluating Heap vs. Off-Heap index performance, record framing throughput, and end-to-end storage engine I/O.

---

## 2. Standalone Redis/RESP Server (Milestone 8)

ChronoCask includes a standalone high-concurrency TCP server speaking the standard Redis protocol:

### Starting the Server

```bash
# Launch ChronoCask server on port 7379 with off-heap index enabled
java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     io.chronocask.server.ChronoCaskServerMain --port 7379 --dir ./chronocask-data
```

### Interacting via `redis-cli` or `nc`

Any standard Redis client can connect seamlessly:

```bash
$ redis-cli -p 7379
127.0.0.1:7379> PING
PONG
127.0.0.1:7379> SET user:101 "Rishabh"
OK
127.0.0.1:7379> GET user:101
"Rishabh"
127.0.0.1:7379> EXISTS user:101
(integer) 1
127.0.0.1:7379> DBSIZE
(integer) 1
127.0.0.1:7379> INFO
# Server
chronocask_version:0.1.0
java_version:25
os_name:Linux
uptime_in_seconds:12
# Storage
index_type:off-heap
total_keys:1
active_segment_id:1
127.0.0.1:7379> COMPACT
OK compacted 0 segments into 0 segment(s), reclaimed 0 bytes
127.0.0.1:7379> DEL user:101
(integer) 1
127.0.0.1:7379> GET user:101
(nil)
127.0.0.1:7379> QUIT
OK
```

---

## 3. Embedded Java Client API

ChronoCask can also be used directly as an embedded database library:

```java
import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.api.WriteOptions;
import io.chronocask.compaction.CompactionResult;

import java.nio.file.Path;

// Open store with default settings (off-heap index, ASYNC durability)
try (ChronoCask store = ChronoCask.open(Path.of("./data"))) {

    // PUT operations
    store.put("user:101", "Rishabh");
    store.put("user:102", "Alice", WriteOptions.sync()); // Force fsync

    // GET operations (O(1) direct offset lookup)
    String user = store.get("user:101");
    System.out.println("Found: " + user);

    // DELETE operation (appends tombstone to log segment)
    store.delete("user:102");

    // Check existence and size
    boolean exists = store.containsKey("user:101"); // true
    long totalKeys = store.size();                   // 1

    // Scan all active entries
    store.forEach((key, val) -> System.out.println(key + " -> " + val));

    // Run synchronous log compaction (reclaims space from old PUTs and tombstones)
    CompactionResult stats = store.compact();
    System.out.println("Reclaimed bytes: " + stats.bytesReclaimed());
}
```

---

## 4. Directory Layout & Persistence Model

```text
chronocask-data/
├── MANIFEST
├── segment-000001.log
├── segment-000002.log
└── hints/
    ├── segment-000001.hint
    └── segment-000002.hint
```

- **`MANIFEST`**: Authoritative record of the active database generation, tracking active segment ID, immutable segment list, and hint file status. Commits are atomic via `MANIFEST.tmp` + `ATOMIC_MOVE` + `force(true)`.
- **`segment-*.log`**: Append-only log files. Mutations are strictly sequential.
- **`hints/segment-*.hint`**: Compact index files containing key and record offset metadata (no values) to enable near-instantaneous startup recovery.

---

## 5. Performance Benchmarks (Milestone 7)

Microbenchmarks run on **OpenJDK 25 (Ubuntu x86_64)** via the OpenJDK JMH harness:

### A. KeyDir In-Memory Lookups (Heap vs. Off-Heap FFM)

| Benchmark                | Index Type                     | Key Count | Mode       | Throughput                           |
| ------------------------ | ------------------------------ | --------- | ---------- | ------------------------------------ |
| `KeyDir.get`             | **Off-Heap (FFM)**             | 10,000    | Throughput | **6,954,950 ops/sec** (~6.95M ops/s) |
| `KeyDir.get`             | **Heap (`ConcurrentHashMap`)** | 10,000    | Throughput | **7,267,104 ops/sec** (~7.27M ops/s) |
| `KeyDir.put` (overwrite) | **Off-Heap (FFM)**             | 10,000    | Throughput | **4,087,846 ops/sec** (~4.09M ops/s) |
| `KeyDir.put` (overwrite) | **Heap (`ConcurrentHashMap`)** | 10,000    | Throughput | **5,863,082 ops/sec** (~5.86M ops/s) |

> **Key Finding**: Off-Heap native FFM achieves **~95.7% of the raw lookup throughput** of JVM on-heap HashMaps while completely eliminating GC pause overhead and heap memory footprint for large key directories.

### B. End-to-End Engine I/O Throughput

| Benchmark           | Durability | Index Mode         | Throughput                           |
| ------------------- | ---------- | ------------------ | ------------------------------------ |
| `StorageEngine.get` | `ASYNC`    | **Off-Heap (FFM)** | **607,351 ops/sec** (~607k reads/s)  |
| `StorageEngine.get` | `ASYNC`    | **Heap**           | **599,172 ops/sec** (~599k reads/s)  |
| `StorageEngine.put` | `ASYNC`    | **Off-Heap (FFM)** | **254,286 ops/sec** (~254k writes/s) |
| `StorageEngine.put` | `ASYNC`    | **Heap**           | **313,702 ops/sec** (~314k writes/s) |

---

## 6. Requirements & Build Verification

- **Java Development Kit (JDK)**: Java 25
- **Build Tool**: Apache Maven 3.8+

```bash
# Clean and run full unit/integration test suite (75 tests)
mvn clean test

# Run the JMH benchmark suite
java -cp "target/classes:$(mvn dependency:build-classpath | grep -v '\[INFO\]' | tr '\n' ':')" io.chronocask.benchmark.BenchmarkRunner
```
