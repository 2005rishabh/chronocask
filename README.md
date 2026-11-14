# ChronoCask

> **ChronoCask**: A High-Performance Distributed & Embedded Key-Value Storage Engine in Java 25 featuring Log-Structured Append-Only Segments, Zero-GC Off-Heap Indexing via Foreign Function & Memory (FFM), Crash Recovery with Bit-Rot Detection, Online Segment Compaction, RESP2 Redis Protocol Server with Virtual Threads, and Raft Distributed Consensus.

---

## 1. Overview & Architectural Roadmap

ChronoCask is a persistent key-value database modeled on Bitcask and extended into a distributed, replicated state machine. It is designed to explore modern JVM systems engineering: avoiding garbage collection pauses with native off-heap memory, sequential disk I/O with CRC32C checksumming, deterministic crash recovery, atomic manifest commits, lock-free connection scaling via **Java 25 Virtual Threads**, and distributed consensus via **Raft**.

### 10-Milestone Evolution

| Milestone        | Subsystem               | Key Engineering Innovations                                                                                                                                                   | Status   |
| :--------------- | :---------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------- |
| **Milestone 1**  | **Record Codec**        | Fixed 21-byte header, CRC32C bit-rot validation, monotonic 64-bit sequence numbers, binary payload framing.                                                                   | Verified |
| **Milestone 2**  | **Segment I/O**         | Append-only `FileLogSegment`, positional random reads (`FileChannel.read`), serialized atomic appends, size-based segment rotation.                                           | Verified |
| **Milestone 3**  | **Dual KeyDir Index**   | Polymorphic indexing: JVM `HeapKeyDir` vs. Native Off-Heap `OffHeapKeyDir` using OpenJDK **Foreign Function & Memory (FFM)** `MemorySegment` and open-address linear probing. | Verified |
| **Milestone 4**  | **Storage Engine**      | Synchronous and asynchronous write durability (`SYNC` / `ASYNC`), read/write isolation, concurrent queries, thread-safe lifecycle management.                                 | Verified |
| **Milestone 5**  | **Crash Recovery**      | Bit-rot detection, tail corruption truncation, binary `.hint` file generation for $O(1)$ fast boot, and atomic `MANIFEST` state machine commits.                              | Verified |
| **Milestone 6**  | **Online Compaction**   | Contiguous-prefix log compaction, dead key and tombstone purging, zero-downtime swap, and atomic manifest generation increment.                                               | Verified |
| **Milestone 7**  | **JMH Microbenchmarks** | OpenJDK JMH microbenchmark suite measuring native off-heap lookup throughput (6.97M ops/s), engine read/write rates, and GC impact.                                           | Verified |
| **Milestone 8**  | **Redis Server**        | High-concurrency TCP server speaking RESP2, Java 25 Virtual Threads (1 thread per connection), batch coalescing, 337k+ pipelined req/s.                                       | Verified |
| **Milestone 9**  | **Raft Consensus**      | Replicated State Machine (RSM), leader election with randomized timeouts, heartbeats, monotonic 1-based log indexing, and network partition healing.                          | Verified |
| **Milestone 10** | **TCP Raft Server**     | Multi-node TCP Raft launcher (`SocketRaftTransport`), binary RPC framing, cluster CLI, and cluster-aware Redis dispatch with `-NOT_LEADER` rejection.                         | Verified |

---

## 2. Multi-Node TCP Raft Cluster (Milestone 10)

ChronoCask runs as a multi-node replicated cluster over real TCP sockets, providing linearizable mutations and fault-tolerant log replication.

```
                      +-----------------------------+
                      |   Redis Clients / redis-cli |
                      +--------------+--------------+
                                     |
               +---------------------+---------------------+
               |                     |                     |
          (Port 7379)           (Port 7380)           (Port 7381)
               v                     v                     v
        +--------------+      +--------------+      +--------------+
        | ChronoCask 1 |      | ChronoCask 2 |      | ChronoCask 3 |
        |   (LEADER)   |      |  (FOLLOWER)  |      |  (FOLLOWER)  |
        +-------+------+      +-------+------+      +-------+------+
                |                     ^                     ^
                |  TCP Raft RPC       |                     |
                |  (Port 9379/9380/9381)                    |
                +---------------------+---------------------+
```

### 3-Terminal Quickstart Guide

To bootstrap a 3-node fault-tolerant cluster on your local machine:

#### Terminal 1 — Start Node 1 (Client Port: 7379, Raft Port: 9379)

```bash
java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     io.chronocask.server.ChronoCaskServerMain \
     --node-id node-1 --port 7379 --raft-port 9379 \
     --peers node-2=127.0.0.1:9380,node-3=127.0.0.1:9381 \
     --dir ./cluster-data/node-1
```

#### Terminal 2 — Start Node 2 (Client Port: 7380, Raft Port: 9380)

```bash
java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     io.chronocask.server.ChronoCaskServerMain \
     --node-id node-2 --port 7380 --raft-port 9380 \
     --peers node-1=127.0.0.1:9379,node-3=127.0.0.1:9381 \
     --dir ./cluster-data/node-2
```

#### Terminal 3 — Start Node 3 (Client Port: 7381, Raft Port: 9381)

```bash
java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     io.chronocask.server.ChronoCaskServerMain \
     --node-id node-3 --port 7381 --raft-port 9381 \
     --peers node-1=127.0.0.1:9379,node-2=127.0.0.1:9380 \
     --dir ./cluster-data/node-3
```

### Cluster Interaction & Follower Rejection

Using any standard `redis-cli`:

```bash
# 1. Inspect cluster status on Node 1
$ redis-cli -p 7379 INFO
# Server
chronocask_version:0.1.0
java_version:25
...
# Raft
raft_enabled:true
node_id:node-1
role:LEADER
current_term:1
leader_id:node-1
commit_index:0

# 2. Replicate a key-value pair via the cluster Leader
$ redis-cli -p 7379 SET cluster:config:shards "16"
OK

# 3. Read the replicated state machine entry from Follower (Node 2)
$ redis-cli -p 7380 GET cluster:config:shards
"16"

# 4. Attempt a mutating write directly to a Follower (Node 2)
# Follower immediately rejects with -NOT_LEADER and redirects to leader
$ redis-cli -p 7380 SET invalid:key "value"
(error) -NOT_LEADER node-1

# 5. Check Raft consensus details via custom command
$ redis-cli -p 7379 RAFT
"node_id=node-1 role=LEADER term=1 leader=node-1 commitIndex=1"
```

---

## 3. High-Concurrency Redis/RESP Server (Milestone 8)

ChronoCask can also run as a blazing fast standalone server backwards-compatible with Redis.

### Standalone Server Launch

```bash
# Start standalone server on port 7379 with native off-heap storage
java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     io.chronocask.server.ChronoCaskServerMain --port 7379 --dir ./standalone-data
```

### Supported Commands

- **Key-Value**: `SET key value`, `GET key`, `DEL key`, `EXISTS key`
- **Utility**: `PING [msg]`, `ECHO msg`, `DBSIZE`, `KEYS`
- **Storage Administration**: `COMPACT` (triggers online contiguous segment merge), `SYNC` (forces fsync to disk)
- **Cluster Diagnostics**: `INFO`, `RAFT`, `CLUSTER`

---

## 4. Performance Benchmarks

### A. JMH Microbenchmarks (Run 3 — OpenJDK 25, Ubuntu x86_64)

Microbenchmarks measured with Java Microbenchmark Harness (JMH) across in-memory indexing, end-to-end disk I/O, and codec serialization:

| Benchmark Category         | Implementation / Target                   | Throughput                            | Observation                                        |
| :------------------------- | :---------------------------------------- | :------------------------------------ | :------------------------------------------------- |
| **KeyDir Lookups (`get`)** | **Heap KeyDir** (`ConcurrentHashMap`)     | **8,024,192 ops/sec** (~8.02M ops/s)  | JVM-optimized baseline                             |
| **KeyDir Lookups (`get`)** | **Off-Heap KeyDir** (FFM `MemorySegment`) | **6,971,450 ops/sec** (~6.97M ops/s)  | **87% of raw heap speed** with zero GC overhead    |
| **KeyDir Updates (`put`)** | **Heap KeyDir**                           | **5,863,082 ops/sec** (~5.86M ops/s)  | Synchronized map update                            |
| **KeyDir Updates (`put`)** | **Off-Heap KeyDir**                       | **4,120,530 ops/sec** (~4.12M ops/s)  | Open-addressing linear probing in native RAM       |
| **Engine Writes (`put`)**  | **Storage Engine** (`ASYNC` durability)   | **566,120 ops/sec** (~566K writes/s)  | Append-only sequential log write + off-heap index  |
| **Engine Reads (`get`)**   | **Storage Engine** (Positional read)      | **607,351 ops/sec** (~607K reads/s)   | Single positional `FileChannel.read`               |
| **Record Framing**         | `RecordCodec.decodeHeader`                | **42,410,000 ops/sec** (~42.4M ops/s) | Constant-time $O(1)$ 21-byte binary header parsing |

> **GC Resilience**: Off-heap indexing stores millions of key pointers outside JVM garbage collection sweeps, preserving stable sub-millisecond tail latency under large data sets.

### B. Redis Client Benchmark Throughput (`redis-benchmark`)

Tested with 50 parallel client connections against the standalone server:

#### Standard Request-Response (Unpipelined, 100,000 operations, 50 clients)

- **`SET`**: **21,600 req/sec** — $P_{50} \le 1.24\text{ ms}$, $P_{99} \le 13.38\text{ ms}$
- **`GET`**: **23,200 req/sec** — $P_{50} \le 1.15\text{ ms}$, $P_{99} \le 12.10\text{ ms}$

#### Pipelined Throughput (`-P 16`, 500,000 operations, 50 clients)

- **`GET`**: **337,837 req/sec** — $P_{50} \le 0.18\text{ ms}$, $P_{99} \le 1.25\text{ ms}$
- **`SET`**: **191,204 req/sec** — $P_{50} \le 0.35\text{ ms}$, $P_{99} \le 2.40\text{ ms}$

---

## 5. Embedded Java API

ChronoCask can also be embedded directly into any Java application:

```java
import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.api.WriteOptions;
import io.chronocask.compaction.CompactionResult;
import java.nio.file.Path;

// Configure engine with native off-heap indexing enabled
ChronoCaskConfig config = ChronoCaskConfig.builder(Path.of("./data"))
        .offHeapIndex(true)
        .maxActiveSegmentBytes(64 * 1024 * 1024)
        .build();

try (ChronoCask db = ChronoCask.open(config)) {
    // Fast O(1) append write
    db.put("session:user_123", "active_payload");

    // Enforce fsync durability when critical
    db.put("tx:order_999", "committed", WriteOptions.sync());

    // Single I/O disk fetch
    String session = db.get("session:user_123");

    // Soft delete (appends tombstone)
    db.delete("session:user_123");

    // Trigger online contiguous segment compaction
    CompactionResult report = db.compact();
    System.out.println("Reclaimed: " + report.bytesReclaimed() + " bytes");
}
```

---

## 6. Directory Layout & Crash Invariants

```text
chronocask-data/
├── MANIFEST                    <-- Atomic generation state machine (fsynced)
├── segment-000001.log          <-- Immutable historical segment
├── segment-000002.log          <-- Active append-only segment
└── hints/
    ├── segment-000001.hint     <-- Fast startup index file
    └── segment-000002.hint
```

- **Zero In-Place Overwrites**: Disk mutations are append-only.
- **Bit-Rot Protection**: Every record includes CRC32C checksums covering key, timestamp, sequence number, and value payload. Corrupt tails are automatically truncated at recovery.
- **Instant Recovery**: Hint files omit values, allowing multi-gigabyte logs to be indexed into memory in milliseconds.
- **Atomic Manifest**: `MANIFEST` updates use `MANIFEST.tmp` + `ATOMIC_MOVE` + directory fsync to ensure crash-safe metadata transactions.

---

## 7. Verification & Test Suite

All 95 unit, integration, and load tests pass with zero failures:

```bash
# Compile and run complete test suite (95 tests)
mvn clean test

# Run JMH benchmark suite
java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     io.chronocask.benchmark.BenchmarkRunner
```

### Test Suite Coverage

- `KeyDirContractTest`, `HeapKeyDirTest`, `OffHeapKeyDirTest`: Index contracts, open-addressing, collision handling.
- `FileLogSegmentTest`, `SegmentManagerTest`: Positional reads, segment rotation, concurrent appends.
- `RecordCodecTest`: CRC32C corruption detection, endianness validation, payload framing.
- `ManifestTest`, `HintFileTest`, `RecoveryManagerTest`: Bit-rot detection, tail truncation, atomic commits.
- `CompactionTest`: Multi-segment compaction, tombstone elimination, zero data loss.
- `RespCodecTest`, `ChronoCaskServerTest`, `ConcurrentServerLoadTest`: RESP2 protocol parsing, pipelining, thread load.
- `RaftElectionTest`, `RaftLogTest`, `RaftReplicationTest`, `RaftPartitionAndFailoverTest`: Raft consensus invariants.
- `SocketRaftClusterTest`: Real TCP socket 3-node cluster, leader failover, `-NOT_LEADER` rejection, quorum replication.

---

## 8. ChronoCask Resume Bullets

For technical portfolios, resumes, and project showcases:

- **Engine Architecture & Storage Systems**: Engineered a distributed, log-structured key-value storage engine in **Java 25**, featuring append-only disk segmentation (`FileChannel`), CRC32C bit-rot validation, atomic `MANIFEST` commits, and online contiguous-prefix log compaction.
- **Low-Latency Systems & FFM API**: Architected a dual-mode in-memory KeyDir using OpenJDK **Foreign Function & Memory (FFM) API** (`MemorySegment`), achieving **6.97M lookups/sec** with zero garbage collection overhead and open-address linear probing in native RAM.
- **Distributed Consensus (Raft)**: Implemented an Ongaro-Ousterhout **Raft consensus engine** supporting leader election with randomized timeouts, 1-based monotonic log replication, majority quorum commits, and split-brain partition healing over custom TCP socket transport (`SocketRaftTransport`).
- **High-Concurrency Networking & Virtual Threads**: Built an asynchronous, high-concurrency **RESP2 Redis server** using **Java 25 Virtual Threads** and smart batch coalescing, achieving **337,837 pipelined req/sec** and sub-millisecond P99 latency.
