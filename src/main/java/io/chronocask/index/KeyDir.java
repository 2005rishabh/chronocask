package io.chronocask.index;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Common contract for KeyDir implementations in ChronoCask.
 *
 * Maps binary keys to their latest {@link IndexEntry} on persistent storage.
 * Both on-heap ({@link HeapKeyDir}) and off-heap ({@link OffHeapKeyDir}) implementations
 * adhere strictly to this contract.
 */
public interface KeyDir extends Closeable {

    /**
     * Associates the specified key with the given IndexEntry.
     * If the key was previously present, the previous entry is replaced and returned.
     *
     * @param key   binary key bytes
     * @param entry location and metadata for the key
     * @return previous IndexEntry associated with key, or null if none
     */
    IndexEntry put(byte[] key, IndexEntry entry);

    /**
     * Convenience put using UTF-8 string key.
     */
    default IndexEntry put(String key, IndexEntry entry) {
        Objects.requireNonNull(key, "key cannot be null");
        return put(key.getBytes(StandardCharsets.UTF_8), entry);
    }

    /**
     * Looks up the latest IndexEntry for the given key.
     *
     * @param key binary key bytes
     * @return matching IndexEntry, or null if key is not present
     */
    IndexEntry get(byte[] key);

    /**
     * Convenience get using UTF-8 string key.
     */
    default IndexEntry get(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Removes the entry for the specified key.
     *
     * @param key binary key bytes
     * @return previous IndexEntry associated with key, or null if not present
     */
    IndexEntry remove(byte[] key);

    /**
     * Convenience remove using UTF-8 string key.
     */
    default IndexEntry remove(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return remove(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns true if this index contains an entry for the specified key.
     */
    default boolean containsKey(byte[] key) {
        return get(key) != null;
    }

    /**
     * Returns true if this index contains an entry for the specified string key.
     */
    default boolean containsKey(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return containsKey(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns the total number of key-value mappings currently in this index.
     */
    long size();

    /**
     * Returns true if this index contains no mappings.
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Removes all entries from this index.
     */
    void clear();

    /**
     * Performs the given action for each key-value entry in this index.
     *
     * @param action action receiving key bytes and IndexEntry
     */
    void forEach(BiConsumer<byte[], IndexEntry> action);

    /**
     * Returns true if this KeyDir stores entries in off-heap native memory.
     */
    boolean isOffHeap();

    /**
     * Releases any resources or native memory associated with this index.
     */
    @Override
    void close();
}
