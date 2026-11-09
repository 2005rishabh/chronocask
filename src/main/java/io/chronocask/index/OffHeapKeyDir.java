package io.chronocask.index;

import io.chronocask.exception.StorageException;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

/**
 * High-performance off-heap KeyDir index implemented using the Java Foreign Function & Memory (FFM) API.
 *
 * Architecture:
 * - Allocates fixed-size 64-byte slots in contiguous native off-heap memory via {@link Arena}.
 * - Open addressing with linear probing and 64-bit hash fingerprints for fast collision rejection.
 * - Stores variable-length key bytes in an off-heap key slab, avoiding Java heap object allocations.
 * - Concurrent reads via {@link ReentrantReadWriteLock}.
 * - Deterministic native memory reclamation upon calling {@link #close()}.
 *
 * 64-Byte Slot Layout (cache-line aligned):
 * <pre>
 * +--------+----------+---------+-----------------+------------+------------+---------------+--------------+--------------+
 * | Offset | Size (B) | Type    | Field           | Offset     | Size (B)   | Type          | Field        | Description  |
 * +--------+----------+---------+-----------------+------------+------------+---------------+--------------+--------------+
 * | 0      | 1        | byte    | State           | 24         | 4          | int           | SegmentId    | Segment file |
 * | 1      | 3        | padding | Reserved        | 28         | 4          | int           | RecordSize   | Header+Data  |
 * | 4      | 4        | int     | KeyLength       | 32         | 8          | long          | RecordOffset | Disk offset  |
 * | 8      | 8        | long    | HashFingerprint | 40         | 4          | int           | ValueLength  | Value size   |
 * | 16     | 8        | long    | KeyOffset       | 48         | 8          | long          | SequenceNum  | Logical seq  |
 * +--------+----------+---------+-----------------+------------+------------+---------------+--------------+--------------+
 * </pre>
 */
public class OffHeapKeyDir implements KeyDir {

    // Slot layout offsets
    private static final long OFFSET_STATE = 0;
    private static final long OFFSET_KEY_LENGTH = 4;
    private static final long OFFSET_HASH = 8;
    private static final long OFFSET_KEY_OFFSET = 16;
    private static final long OFFSET_SEGMENT_ID = 24;
    private static final long OFFSET_RECORD_SIZE = 28;
    private static final long OFFSET_RECORD_OFFSET = 32;
    private static final long OFFSET_VALUE_LENGTH = 40;
    private static final long OFFSET_SEQUENCE_NUM = 48;
    public static final int SLOT_SIZE = 64; // 64 bytes per slot

    // Slot states
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_OCCUPIED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private static final int DEFAULT_INITIAL_CAPACITY = 1024;
    private static final long DEFAULT_KEY_SLAB_SIZE = 1024 * 1024L; // 1 MB
    private static final float LOAD_FACTOR = 0.70f;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private Arena arena;
    private MemorySegment tableSegment;
    private MemorySegment keySlabSegment;

    private int capacity;
    private int mask;
    private long size;
    private long tombstoneCount;
    private long keySlabHead;
    private long keySlabCapacity;
    private volatile boolean closed = false;

    public OffHeapKeyDir() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_KEY_SLAB_SIZE);
    }

    public OffHeapKeyDir(int initialCapacity) {
        this(initialCapacity, DEFAULT_KEY_SLAB_SIZE);
    }

    public OffHeapKeyDir(int initialCapacity, long initialKeySlabBytes) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
        }
        if (initialKeySlabBytes <= 0) {
            throw new IllegalArgumentException("initialKeySlabBytes must be positive: " + initialKeySlabBytes);
        }

        this.capacity = tableSizeFor(initialCapacity);
        this.mask = this.capacity - 1;
        this.size = 0;
        this.tombstoneCount = 0;
        this.keySlabHead = 0;
        this.keySlabCapacity = initialKeySlabBytes;

        this.arena = Arena.ofShared();
        this.tableSegment = arena.allocate((long) this.capacity * SLOT_SIZE, 64);
        this.keySlabSegment = arena.allocate(this.keySlabCapacity, 8);
    }

    @Override
    public IndexEntry put(byte[] key, IndexEntry entry) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(entry, "entry cannot be null");

        writeLock.lock();
        try {
            ensureOpen();

            if ((size + tombstoneCount + 1) >= (capacity * LOAD_FACTOR)) {
                resizeAndRehash();
            }

            long hash = HashUtil.hash64(key);
            int idx = hashToIndex(hash, mask);

            int firstTombstoneIdx = -1;
            MemorySegment keySegment = MemorySegment.ofArray(key);

            while (true) {
                long slotOffset = (long) idx * SLOT_SIZE;
                byte state = tableSegment.get(ValueLayout.JAVA_BYTE, slotOffset + OFFSET_STATE);

                if (state == STATE_EMPTY) {
                    int targetIdx = (firstTombstoneIdx != -1) ? firstTombstoneIdx : idx;
                    long targetSlotOffset = (long) targetIdx * SLOT_SIZE;

                    if (targetIdx == firstTombstoneIdx) {
                        tombstoneCount--;
                    }

                    long keyOffset = appendKeyToSlab(keySegment, key.length);
                    writeSlot(targetSlotOffset, STATE_OCCUPIED, hash, keyOffset, key.length, entry);
                    size++;
                    return null;
                }

                if (state == STATE_TOMBSTONE) {
                    if (firstTombstoneIdx == -1) {
                        firstTombstoneIdx = idx;
                    }
                } else if (state == STATE_OCCUPIED) {
                    long slotHash = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_HASH);
                    int slotKeyLen = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_KEY_LENGTH);

                    if (slotHash == hash && slotKeyLen == key.length) {
                        long slotKeyOffset = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_KEY_OFFSET);
                        if (keysMatch(slotKeyOffset, keySegment, key.length)) {
                            // Key exists: overwrite metadata and return previous entry
                            IndexEntry previous = readEntry(slotOffset);
                            updateSlotEntry(slotOffset, entry);
                            return previous;
                        }
                    }
                }

                idx = (idx + 1) & mask;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public IndexEntry get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");

        readLock.lock();
        try {
            ensureOpen();
            if (size == 0) {
                return null;
            }

            long hash = HashUtil.hash64(key);
            int idx = hashToIndex(hash, mask);
            MemorySegment keySegment = MemorySegment.ofArray(key);

            while (true) {
                long slotOffset = (long) idx * SLOT_SIZE;
                byte state = tableSegment.get(ValueLayout.JAVA_BYTE, slotOffset + OFFSET_STATE);

                if (state == STATE_EMPTY) {
                    return null; // Key absent
                }

                if (state == STATE_OCCUPIED) {
                    long slotHash = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_HASH);
                    int slotKeyLen = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_KEY_LENGTH);

                    if (slotHash == hash && slotKeyLen == key.length) {
                        long slotKeyOffset = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_KEY_OFFSET);
                        if (keysMatch(slotKeyOffset, keySegment, key.length)) {
                            return readEntry(slotOffset);
                        }
                    }
                }

                idx = (idx + 1) & mask;
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public IndexEntry remove(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");

        writeLock.lock();
        try {
            ensureOpen();
            if (size == 0) {
                return null;
            }

            long hash = HashUtil.hash64(key);
            int idx = hashToIndex(hash, mask);
            MemorySegment keySegment = MemorySegment.ofArray(key);

            while (true) {
                long slotOffset = (long) idx * SLOT_SIZE;
                byte state = tableSegment.get(ValueLayout.JAVA_BYTE, slotOffset + OFFSET_STATE);

                if (state == STATE_EMPTY) {
                    return null; // Not found
                }

                if (state == STATE_OCCUPIED) {
                    long slotHash = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_HASH);
                    int slotKeyLen = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_KEY_LENGTH);

                    if (slotHash == hash && slotKeyLen == key.length) {
                        long slotKeyOffset = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_KEY_OFFSET);
                        if (keysMatch(slotKeyOffset, keySegment, key.length)) {
                            IndexEntry removed = readEntry(slotOffset);
                            tableSegment.set(ValueLayout.JAVA_BYTE, slotOffset + OFFSET_STATE, STATE_TOMBSTONE);
                            size--;
                            tombstoneCount++;
                            return removed;
                        }
                    }
                }

                idx = (idx + 1) & mask;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean containsKey(byte[] key) {
        return get(key) != null;
    }

    @Override
    public long size() {
        readLock.lock();
        try {
            ensureOpen();
            return size;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            ensureOpen();
            tableSegment.fill((byte) 0);
            size = 0;
            tombstoneCount = 0;
            keySlabHead = 0;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void forEach(BiConsumer<byte[], IndexEntry> action) {
        Objects.requireNonNull(action, "action cannot be null");

        readLock.lock();
        try {
            ensureOpen();
            for (int i = 0; i < capacity; i++) {
                long slotOffset = (long) i * SLOT_SIZE;
                byte state = tableSegment.get(ValueLayout.JAVA_BYTE, slotOffset + OFFSET_STATE);
                if (state == STATE_OCCUPIED) {
                    int keyLen = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_KEY_LENGTH);
                    long keyOffset = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_KEY_OFFSET);

                    byte[] keyBytes = new byte[keyLen];
                    MemorySegment.copy(keySlabSegment, ValueLayout.JAVA_BYTE, keyOffset, keyBytes, 0, keyLen);

                    IndexEntry entry = readEntry(slotOffset);
                    action.accept(keyBytes, entry);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isOffHeap() {
        return true;
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            size = 0;
            tombstoneCount = 0;
            if (arena != null && arena.scope().isAlive()) {
                arena.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public int capacity() {
        readLock.lock();
        try {
            return capacity;
        } finally {
            readLock.unlock();
        }
    }

    public long offHeapMemoryUsed() {
        readLock.lock();
        try {
            return ((long) capacity * SLOT_SIZE) + keySlabCapacity;
        } finally {
            readLock.unlock();
        }
    }

    // --- Internal Helpers ---

    private boolean keysMatch(long slotKeyOffset, MemorySegment targetKeySegment, int length) {
        long mismatch = MemorySegment.mismatch(keySlabSegment, slotKeyOffset, slotKeyOffset + length,
                targetKeySegment, 0, length);
        return mismatch == -1;
    }

    private long appendKeyToSlab(MemorySegment keySegment, int keyLen) {
        if (keySlabHead + keyLen > keySlabCapacity) {
            expandKeySlab(keyLen);
        }
        long offset = keySlabHead;
        MemorySegment.copy(keySegment, 0, keySlabSegment, offset, keyLen);
        keySlabHead += keyLen;
        return offset;
    }

    private void expandKeySlab(int neededBytes) {
        long newCapacity = Math.max(keySlabCapacity * 2, keySlabCapacity + neededBytes + 1024 * 1024L);
        MemorySegment newSlab = arena.allocate(newCapacity, 8);
        MemorySegment.copy(keySlabSegment, 0, newSlab, 0, keySlabHead);
        this.keySlabSegment = newSlab;
        this.keySlabCapacity = newCapacity;
    }

    private void writeSlot(long slotOffset, byte state, long hash, long keyOffset, int keyLen, IndexEntry entry) {
        tableSegment.set(ValueLayout.JAVA_BYTE, slotOffset + OFFSET_STATE, state);
        tableSegment.set(ValueLayout.JAVA_INT, slotOffset + OFFSET_KEY_LENGTH, keyLen);
        tableSegment.set(ValueLayout.JAVA_LONG, slotOffset + OFFSET_HASH, hash);
        tableSegment.set(ValueLayout.JAVA_LONG, slotOffset + OFFSET_KEY_OFFSET, keyOffset);
        updateSlotEntry(slotOffset, entry);
    }

    private void updateSlotEntry(long slotOffset, IndexEntry entry) {
        tableSegment.set(ValueLayout.JAVA_INT, slotOffset + OFFSET_SEGMENT_ID, entry.segmentId());
        tableSegment.set(ValueLayout.JAVA_INT, slotOffset + OFFSET_RECORD_SIZE, entry.recordSize());
        tableSegment.set(ValueLayout.JAVA_LONG, slotOffset + OFFSET_RECORD_OFFSET, entry.offset());
        tableSegment.set(ValueLayout.JAVA_INT, slotOffset + OFFSET_VALUE_LENGTH, entry.valueLength());
        tableSegment.set(ValueLayout.JAVA_LONG, slotOffset + OFFSET_SEQUENCE_NUM, entry.sequenceNumber());
    }

    private IndexEntry readEntry(long slotOffset) {
        int segmentId = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_SEGMENT_ID);
        int recordSize = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_RECORD_SIZE);
        long recordOffset = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_RECORD_OFFSET);
        int valueLength = tableSegment.get(ValueLayout.JAVA_INT, slotOffset + OFFSET_VALUE_LENGTH);
        long sequenceNumber = tableSegment.get(ValueLayout.JAVA_LONG, slotOffset + OFFSET_SEQUENCE_NUM);
        return new IndexEntry(segmentId, recordOffset, recordSize, valueLength, sequenceNumber);
    }

    private void resizeAndRehash() {
        int newCapacity = capacity << 1;
        int newMask = newCapacity - 1;
        MemorySegment newTable = arena.allocate((long) newCapacity * SLOT_SIZE, 64);

        for (int i = 0; i < capacity; i++) {
            long oldSlotOffset = (long) i * SLOT_SIZE;
            byte state = tableSegment.get(ValueLayout.JAVA_BYTE, oldSlotOffset + OFFSET_STATE);

            if (state == STATE_OCCUPIED) {
                long hash = tableSegment.get(ValueLayout.JAVA_LONG, oldSlotOffset + OFFSET_HASH);
                int keyLen = tableSegment.get(ValueLayout.JAVA_INT, oldSlotOffset + OFFSET_KEY_LENGTH);
                long keyOffset = tableSegment.get(ValueLayout.JAVA_LONG, oldSlotOffset + OFFSET_KEY_OFFSET);
                IndexEntry entry = readEntry(oldSlotOffset);

                int newIdx = hashToIndex(hash, newMask);
                while (true) {
                    long newSlotOffset = (long) newIdx * SLOT_SIZE;
                    byte destState = newTable.get(ValueLayout.JAVA_BYTE, newSlotOffset + OFFSET_STATE);
                    if (destState == STATE_EMPTY) {
                        newTable.set(ValueLayout.JAVA_BYTE, newSlotOffset + OFFSET_STATE, STATE_OCCUPIED);
                        newTable.set(ValueLayout.JAVA_INT, newSlotOffset + OFFSET_KEY_LENGTH, keyLen);
                        newTable.set(ValueLayout.JAVA_LONG, newSlotOffset + OFFSET_HASH, hash);
                        newTable.set(ValueLayout.JAVA_LONG, newSlotOffset + OFFSET_KEY_OFFSET, keyOffset);
                        newTable.set(ValueLayout.JAVA_INT, newSlotOffset + OFFSET_SEGMENT_ID, entry.segmentId());
                        newTable.set(ValueLayout.JAVA_INT, newSlotOffset + OFFSET_RECORD_SIZE, entry.recordSize());
                        newTable.set(ValueLayout.JAVA_LONG, newSlotOffset + OFFSET_RECORD_OFFSET, entry.offset());
                        newTable.set(ValueLayout.JAVA_INT, newSlotOffset + OFFSET_VALUE_LENGTH, entry.valueLength());
                        newTable.set(ValueLayout.JAVA_LONG, newSlotOffset + OFFSET_SEQUENCE_NUM, entry.sequenceNumber());
                        break;
                    }
                    newIdx = (newIdx + 1) & newMask;
                }
            }
        }

        this.tableSegment = newTable;
        this.capacity = newCapacity;
        this.mask = newMask;
        this.tombstoneCount = 0; // Tombstones purged upon rehash
    }

    private static int hashToIndex(long hash, int mask) {
        return (int) ((hash ^ (hash >>> 32)) & mask);
    }

    private static int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        return (n < 0) ? 1 : (n >= (1 << 30)) ? (1 << 30) : n + 1;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("OffHeapKeyDir is closed");
        }
    }
}
