package io.chronocask.api;

import io.chronocask.compaction.CompactionResult;
import io.chronocask.exception.StorageException;
import io.chronocask.recovery.RecoveryResult;
import io.chronocask.storage.StorageEngine;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Public embedded Java API for ChronoCask key-value storage engine.
 *
 * Example usage:
 * <pre>{@code
 * try (ChronoCask store = ChronoCask.open(Path.of("./data"))) {
 *     store.put("user:101", "Rishabh");
 *     String val = store.get("user:101");
 *     store.delete("user:101");
 *     store.compact();
 * }
 * }</pre>
 */
public final class ChronoCask implements Closeable {

    private final StorageEngine engine;

    private ChronoCask(StorageEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine cannot be null");
    }

    /**
     * Opens or creates a ChronoCask instance at the specified directory path using default configuration.
     */
    public static ChronoCask open(Path dataDir) {
        return open(ChronoCaskConfig.of(dataDir));
    }

    /**
     * Opens or creates a ChronoCask instance with custom configuration.
     */
    public static ChronoCask open(ChronoCaskConfig config) {
        try {
            StorageEngine engine = StorageEngine.open(config);
            return new ChronoCask(engine);
        } catch (IOException e) {
            throw new StorageException(
                "Failed to open ChronoCask: " + e.getMessage(),
                e
            );
        }
    }

    // --- String API ---

    /**
     * Inserts or updates a key-value mapping using UTF-8 strings.
     */
    public void put(String key, String value) {
        put(key, value, WriteOptions.DEFAULT);
    }

    /**
     * Inserts or updates a key-value mapping using UTF-8 strings with specific write options.
     */
    public void put(String key, String value, WriteOptions options) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        put(
            key.getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8),
            options
        );
    }

    /**
     * Retrieves the string value for the specified key, or null if not found.
     */
    public String get(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        byte[] valueBytes = get(key.getBytes(StandardCharsets.UTF_8));
        return valueBytes != null
            ? new String(valueBytes, StandardCharsets.UTF_8)
            : null;
    }

    /**
     * Retrieves an Optional containing the string value for the key if present.
     */
    public Optional<String> find(String key) {
        return Optional.ofNullable(get(key));
    }

    /**
     * Deletes the mapping for the specified string key.
     *
     * @return true if the key was present in the index before deletion
     */
    public boolean delete(String key) {
        return delete(key, WriteOptions.DEFAULT);
    }

    /**
     * Deletes the mapping for the specified string key with specific write options.
     */
    public boolean delete(String key, WriteOptions options) {
        Objects.requireNonNull(key, "key cannot be null");
        return delete(key.getBytes(StandardCharsets.UTF_8), options);
    }

    /**
     * Returns true if the string key is present in the database.
     */
    public boolean containsKey(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return containsKey(key.getBytes(StandardCharsets.UTF_8));
    }

    // --- Binary byte[] API ---

    /**
     * Inserts or updates a binary key-value pair using default write options.
     */
    public void put(byte[] key, byte[] value) {
        put(key, value, WriteOptions.DEFAULT);
    }

    /**
     * Inserts or updates a binary key-value pair with specific write options.
     */
    public void put(byte[] key, byte[] value, WriteOptions options) {
        try {
            engine.put(key, value, options);
        } catch (IOException e) {
            throw new StorageException("Failed to put key into store", e);
        }
    }

    /**
     * Retrieves the raw value bytes for the specified binary key, or null if not found.
     */
    public byte[] get(byte[] key) {
        try {
            return engine.get(key);
        } catch (IOException e) {
            throw new StorageException("Failed to read key from store", e);
        }
    }

    /**
     * Deletes the specified binary key.
     *
     * @return true if the key was present in the index before deletion
     */
    public boolean delete(byte[] key) {
        return delete(key, WriteOptions.DEFAULT);
    }

    /**
     * Deletes the specified binary key with specific write options.
     */
    public boolean delete(byte[] key, WriteOptions options) {
        try {
            return engine.delete(key, options);
        } catch (IOException e) {
            throw new StorageException("Failed to delete key from store", e);
        }
    }

    /**
     * Returns true if the binary key is present in the database.
     */
    public boolean containsKey(byte[] key) {
        return engine.containsKey(key);
    }

    // --- Management & Diagnostics ---

    /**
     * Executes a synchronous compaction cycle on immutable log segments,
     * reclaiming storage space occupied by outdated records and tombstones.
     *
     * @return summary statistics of the compaction cycle
     */
    public CompactionResult compact() {
        try {
            return engine.compact();
        } catch (IOException e) {
            throw new StorageException(
                "Failed to run compaction: " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Total number of active keys in the store.
     */
    public long size() {
        return engine.size();
    }

    /**
     * Returns true if the store contains zero active keys.
     */
    public boolean isEmpty() {
        return engine.isEmpty();
    }

    /**
     * Forces all unwritten segment data to stable storage.
     */
    public void sync() {
        try {
            engine.sync();
        } catch (IOException e) {
            throw new StorageException("Failed to sync storage engine", e);
        }
    }

    /**
     * Iterates over all active key-value pairs decoded as UTF-8 strings.
     */
    public void forEach(BiConsumer<String, String> action) {
        Objects.requireNonNull(action, "action cannot be null");
        engine.forEach((k, v) ->
            action.accept(
                new String(k, StandardCharsets.UTF_8),
                new String(v, StandardCharsets.UTF_8)
            )
        );
    }

    /**
     * Iterates over all active raw binary key-value pairs.
     */
    public void forEachBytes(BiConsumer<byte[], byte[]> action) {
        engine.forEach(action);
    }

    /**
     * Returns recovery statistics obtained during database startup.
     */
    public RecoveryResult getRecoveryResult() {
        return engine.getRecoveryResult();
    }

    /**
     * Returns the engine configuration.
     */
    public ChronoCaskConfig getConfig() {
        return engine.getConfig();
    }

    /**
     * Returns true if the database index is stored off-heap in native memory.
     */
    public boolean isOffHeap() {
        return engine.getKeyDir().isOffHeap();
    }

    /**
     * Returns the current monotonically incrementing sequence number.
     */
    public long currentSequenceNumber() {
        return engine.currentSequenceNumber();
    }

    /**
     * Direct access to underlying StorageEngine for testing or advanced operations.
     */
    public StorageEngine getEngine() {
        return engine;
    }

    @Override
    public void close() {
        try {
            engine.close();
        } catch (IOException e) {
            throw new StorageException("Failed to close ChronoCask engine", e);
        }
    }
}
