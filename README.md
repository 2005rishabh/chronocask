# ChronoCask

> **ChronoCask**: A High-Performance Persistent Key-Value Storage Engine in Java with Append-Only Log Segments, Off-Heap Indexing, and Crash Recovery.

---

## 1. Overview

ChronoCask is an embedded persistent key-value storage engine inspired by the Bitcask model. It is designed to explore systems-level data storage, low Java heap allocation via the **Foreign Function & Memory (FFM) API**, sequential log-structured disk I/O, and crash-consistent recovery.

### Key Architectural Highlights

- **Append-Only Disk Storage**: Eliminates random disk writes during mutations by writing sequential records to append-only log segments.
- **In-Memory KeyDir Index**: Maps keys to exact segment and byte offsets for $O(1)$ read lookups without scanning disk logs.
- **Native Memory Management**: Implements an off-heap hash-table index using Java's FFM API (`MemorySegment`, `Arena`) to eliminate Java GC overhead for large key spaces.
- **Crash Durability & Recovery**: Strict record framing with CRC32C checksums to identify bit rot, crash truncation, and partial writes during recovery.
- **Space Reclamation**: Segment compaction protocol that safely purges superseded records and tombstones into fresh immutable segments.

---

## 2. Quickstart & Client API (Milestone 5)

ChronoCask provides an intuitive, high-performance embedded Java API:

```java
import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.api.WriteOptions;

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
}
```

### Custom Engine Configuration

```java
ChronoCaskConfig config = ChronoCaskConfig.builder(Path.of("./data"))
    .maxSegmentSizeBytes(64 * 1024 * 1024L) // 64 MB segment rotation threshold
    .offHeapIndex(true)                     // Use native FFM off-heap index
    .initialIndexCapacity(2048)
    .initialKeySlabBytes(2 * 1024 * 1024L)
    .build();

try (ChronoCask store = ChronoCask.open(config)) {
    // ...
}
```

---

## 3. Binary Record Format (Milestone 1)

Every log entry on disk is framed in big-endian byte order:

```text
+----------------------+----------------+
| Field                | Size           |
+----------------------+----------------+
| Magic Number         | 4 bytes        | (0x4341534B -> 'CASK')
| Format Version       | 1 byte         | (Current: 1)
| Record Type          | 1 byte         | (1 = PUT, 2 = DELETE)
| Reserved             | 2 bytes        | (0x0000)
| Sequence Number      | 8 bytes        | (Monotonic logical timestamp)
| Key Length           | 4 bytes        | (Bytes length, >= 0)
| Value Length         | 4 bytes        | (Bytes length, 0 for DELETE)
| CRC32C Checksum      | 4 bytes        | (Checksum of metadata + key + value)
+----------------------+----------------+
| Key Bytes            | Variable       |
| Value Bytes          | Variable       |
+----------------------+----------------+
```

- **Fixed Header Size**: 28 bytes
- **Checksum Coverage**: Covers Magic, Version, Type, Reserved, Sequence, Key Length, Value Length, Key Bytes, and Value Bytes.

---

## 4. Storage & Index Architecture

### Append-Only Log Segments (Milestone 2)

- Managed by `SegmentManager` and backed by `FileLogSegment`.
- Concurrently readable via positional `FileChannel.read(dst, offset)` (`pread`).
- Automatic rotation when active segment exceeds configured size threshold.

### KeyDir Index (Milestone 3)

ChronoCask provides two implementations conforming to the same `KeyDir` interface:

1. **`HeapKeyDir`**: Reference implementation backed by `ConcurrentHashMap` with `ByteArrayKey`.
2. **`OffHeapKeyDir`**: High-performance native memory implementation powered by Java FFM (`Arena`, `MemorySegment`):
   - Cache-line aligned 64-byte slots in contiguous off-heap memory.
   - Open addressing with linear probing and 64-bit hash fingerprints (`HashUtil`).
   - Dynamic off-heap key slab storage for variable-length keys.
   - Deterministic reclamation with zero JVM garbage collection pause impact.

#### 64-Byte Slot Layout

```text
+--------+----------+---------+-----------------+------------+------------+---------------+--------------+--------------+
| Offset | Size (B) | Type    | Field           | Offset     | Size (B)   | Type          | Field        | Description  |
+--------+----------+---------+-----------------+------------+------------+---------------+--------------+--------------+
| 0      | 1        | byte    | State           | 24         | 4          | int           | SegmentId    | Segment file |
| 1      | 3        | padding | Reserved        | 28         | 4          | int           | RecordSize   | Header+Data  |
| 4      | 4        | int     | KeyLength       | 32         | 8          | long          | RecordOffset | Disk offset  |
| 8      | 8        | long    | HashFingerprint | 40         | 4          | int           | ValueLength  | Value size   |
| 16     | 8        | long    | KeyOffset       | 48         | 8          | long          | SequenceNum  | Logical seq  |
+--------+----------+---------+-----------------+------------+------------+---------------+--------------+--------------+
```

### Crash Recovery & Replay (Milestone 4)

- Executed by `RecoveryManager` on startup.
- Inspects and validates segments sequentially, verifying CRC32C checksums.
- Gracefully truncates partial crash tails in the active segment while strictly flagging corruption within earlier records.

---

## 5. Requirements & Build

- **Java Development Kit (JDK)**: Java 25
- **Build Tool**: Apache Maven 3.8+

```bash
# Clean and run full test suite
mvn clean test
```
