# ChronoCask

> **ChronoCask**: A High-Performance Persistent Key-Value Storage Engine in Java with Append-Only Log Segments, Off-Heap Indexing, Crash Recovery, and Safe Segment Compaction.

---

## 1. Overview

ChronoCask is an embedded persistent key-value storage engine inspired by the Bitcask model. It is designed to explore systems-level data storage, low Java heap allocation via the **Foreign Function & Memory (FFM) API**, sequential log-structured disk I/O, deterministic crash recovery, and crash-safe segment compaction.

### Key Architectural Highlights

- **Append-Only Disk Storage**: Eliminates random disk writes during mutations by writing sequential records to append-only log segments.
- **In-Memory KeyDir Index**: Maps keys to exact segment and byte offsets for $O(1)$ read lookups without scanning disk logs.
- **Native Memory Management**: Implements an off-heap hash-table index using Java's FFM API (`MemorySegment`, `Arena`) to eliminate Java GC overhead for large key spaces.
- **Authoritative Storage Manifest**: Persistently describes the authoritative generation of active and immutable segments (`MANIFEST`), ensuring deterministic crash consistency without directory file heuristics.
- **Fast Startup via Hint Files**: Generates durable `.hint` files for sealed segments, enabling sub-millisecond recovery by reading index metadata without loading full payload values.
- **Safe Prefix Compaction**: Enforces a strict contiguous-prefix boundary rule starting from the oldest segment to eliminate superseded records and tombstones without risking deleted key resurrection.

---

## 2. Quickstart & Client API

ChronoCask provides an intuitive, high-performance embedded Java API:

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

## 3. Directory Layout & Persistence Model

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

## 4. Binary Record & File Framing

### Log Record Framing (28-Byte Fixed Header)

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

### Hint File Framing (20-Byte Header + 32-Byte Entry Headers)

```text
File Header:
| Magic (0x48494E54) | Version (1) | Reserved (3B) | SegmentID (4B) | TotalEntries (4B) | Checksum (4B) |

Hint Record:
| SeqNum (8B) | Offset (8B) | RecordSize (4B) | ValueLen (4B) | KeyLen (4B) | Checksum (4B) | Key Bytes |
```

---

## 5. Storage, Indexing & Compaction Architecture

### Append-Only Log Segments

- Managed by `SegmentManager` and backed by `FileLogSegment`.
- Concurrently readable via positional `FileChannel.read(dst, offset)` (`pread`).
- Automatic rotation when active segment exceeds configured size threshold.

### KeyDir Index

ChronoCask provides two implementations conforming to the same `KeyDir` interface:

1. **`HeapKeyDir`**: Reference implementation backed by `ConcurrentHashMap` with `ByteArrayKey`.
2. **`OffHeapKeyDir`**: High-performance native memory implementation powered by Java FFM (`Arena`, `MemorySegment`):
   - Cache-line aligned 64-byte slots in contiguous off-heap memory.
   - Open addressing with linear probing and 64-bit hash fingerprints (`HashUtil`).
   - Dynamic off-heap key slab storage for variable-length keys.
   - Deterministic reclamation with zero JVM garbage collection pause impact.

### Safe Prefix Compaction Protocol

1. **Contiguous Prefix Selection**: `CompactionPlanner` selects immutable segments strictly starting from index 0 (oldest). This guarantees tombstones are only dropped when no older segment can contain obsolete versions of that key.
2. **Preserving Logical History**: `CompactionWorker` rewrites live records preserving their original sequence numbers and timestamps.
3. **Atomic Generation Switch**: `SegmentManager.replaceSegments` flushes replacement segments, writes hint files, commits the new `StorageManifest` atomically, swaps the in-memory registry, and only then purges obsolete files.

---

## 6. Requirements & Build

- **Java Development Kit (JDK)**: Java 25
- **Build Tool**: Apache Maven 3.8+

```bash
# Clean and run full test suite
mvn clean test
```
