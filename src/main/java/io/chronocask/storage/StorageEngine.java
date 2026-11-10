package io.chronocask.storage;

import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.api.WriteOptions;
import io.chronocask.exception.StorageException;
import io.chronocask.index.HeapKeyDir;
import io.chronocask.index.IndexEntry;
import io.chronocask.index.KeyDir;
import io.chronocask.index.OffHeapKeyDir;
import io.chronocask.record.Record;
import io.chronocask.recovery.RecoveryManager;
import io.chronocask.recovery.RecoveryResult;
import io.chronocask.segment.DurabilityMode;
import io.chronocask.segment.RecordLocation;
import io.chronocask.segment.SegmentManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

/**
 * StorageEngine orchestrates the segment manager, KeyDir index, and recovery manager.
 *
 * It serves as the core engine for all read/write operations, coordinating concurrency,
 * monotonic sequence numbering, and lifecycle management.
 */
public class StorageEngine implements Closeable {

    private final ChronoCaskConfig config;
    private final SegmentManager segmentManager;
    private final KeyDir keyDir;
    private final RecoveryResult recoveryResult;
    private final AtomicLong sequenceNumber;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private volatile boolean closed = false;

    private StorageEngine(ChronoCaskConfig config, SegmentManager segmentManager, KeyDir keyDir, RecoveryResult recoveryResult) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.segmentManager = Objects.requireNonNull(segmentManager, "segmentManager cannot be null");
        this.keyDir = Objects.requireNonNull(keyDir, "keyDir cannot be null");
        this.recoveryResult = Objects.requireNonNull(recoveryResult, "recoveryResult cannot be null");
        this.sequenceNumber = new AtomicLong(recoveryResult.highestSequenceNumber());
    }

    /**
     * Initializes and starts a StorageEngine according to the given configuration.
     * Replays existing disk segments to restore index state before returning.
     */
    public static StorageEngine open(ChronoCaskConfig config) throws IOException {
        Objects.requireNonNull(config, "config cannot be null");

        SegmentManager segmentManager = SegmentManager.open(
                config.dataDir(),
                config.maxSegmentSizeBytes(),
                config.defaultDurability()
        );

        KeyDir keyDir = config.offHeapIndex()
                ? new OffHeapKeyDir(config.initialIndexCapacity(), config.initialKeySlabBytes())
                : new HeapKeyDir(config.initialIndexCapacity());

        try {
            RecoveryManager recoveryManager = new RecoveryManager(segmentManager);
            RecoveryResult recoveryResult = recoveryManager.recover(keyDir);
            return new StorageEngine(config, segmentManager, keyDir, recoveryResult);
        } catch (Exception e) {
            try {
                keyDir.close();
            } catch (Exception ignored) {
            }
            try {
                segmentManager.close();
            } catch (Exception ignored) {
            }
            throw new StorageException("Failed to open StorageEngine: " + e.getMessage(), e);
        }
    }

    /**
     * Puts a key-value pair into the store.
     */
    public void put(byte[] key, byte[] value, WriteOptions options) throws IOException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");

        DurabilityMode durability = resolveDurability(options);

        writeLock.lock();
        try {
            ensureOpen();
            long seq = sequenceNumber.incrementAndGet();

            Record record = Record.put(seq, key, value);
            RecordLocation location = segmentManager.append(record, durability);

            IndexEntry entry = IndexEntry.of(location, value.length, seq);
            keyDir.put(key, entry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @return value bytes or null if the key does not exist
     */
    public byte[] get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key cannot be null");

        readLock.lock();
        try {
            ensureOpen();
            IndexEntry entry = keyDir.get(key);
            if (entry == null) {
                return null;
            }
            Record record = segmentManager.read(entry.location());
            return record.value();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Deletes the key from the store by appending a tombstone record.
     *
     * @return true if the key was present in the index before deletion
     */
    public boolean delete(byte[] key, WriteOptions options) throws IOException {
        Objects.requireNonNull(key, "key cannot be null");

        DurabilityMode durability = resolveDurability(options);

        writeLock.lock();
        try {
            ensureOpen();
            long seq = sequenceNumber.incrementAndGet();

            Record record = Record.delete(seq, key);
            segmentManager.append(record, durability);

            IndexEntry prev = keyDir.remove(key);
            return prev != null;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns true if the key exists in the active index.
     */
    public boolean containsKey(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");

        readLock.lock();
        try {
            ensureOpen();
            return keyDir.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the total count of active keys in the store.
     */
    public long size() {
        readLock.lock();
        try {
            ensureOpen();
            return keyDir.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns true if the store contains no active keys.
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Forces all unwritten segment data to stable storage.
     */
    public void sync() throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            segmentManager.flush();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Iterates over all active key-value entries in the store.
     */
    public void forEach(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");

        readLock.lock();
        try {
            ensureOpen();
            keyDir.forEach((key, entry) -> {
                try {
                    Record record = segmentManager.read(entry.location());
                    action.accept(key, record.value());
                } catch (IOException e) {
                    throw new StorageException("Failed to read record for key during forEach", e);
                }
            });
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            try {
                keyDir.close();
            } finally {
                segmentManager.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public ChronoCaskConfig getConfig() {
        return config;
    }

    public RecoveryResult getRecoveryResult() {
        return recoveryResult;
    }

    public SegmentManager getSegmentManager() {
        return segmentManager;
    }

    public KeyDir getKeyDir() {
        return keyDir;
    }

    public long currentSequenceNumber() {
        return sequenceNumber.get();
    }

    private DurabilityMode resolveDurability(WriteOptions options) {
        if (options != null && options.durability() != null) {
            return options.durability();
        }
        return config.defaultDurability();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("StorageEngine is closed");
        }
    }
}
