package io.chronocask.index;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * In-memory reference KeyDir implementation storing mappings entirely on the Java heap.
 *
 * Backed by {@link ConcurrentHashMap} with {@link ByteArrayKey} wrappers.
 */
public class HeapKeyDir implements KeyDir {

    private final ConcurrentMap<ByteArrayKey, IndexEntry> map;
    private volatile boolean closed = false;

    public HeapKeyDir() {
        this.map = new ConcurrentHashMap<>();
    }

    public HeapKeyDir(int initialCapacity) {
        this.map = new ConcurrentHashMap<>(initialCapacity);
    }

    @Override
    public IndexEntry put(byte[] key, IndexEntry entry) {
        ensureOpen();
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(entry, "entry cannot be null");
        return map.put(new ByteArrayKey(key), entry);
    }

    @Override
    public IndexEntry get(byte[] key) {
        ensureOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return map.get(new ByteArrayKey(key));
    }

    @Override
    public IndexEntry remove(byte[] key) {
        ensureOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return map.remove(new ByteArrayKey(key));
    }

    @Override
    public boolean containsKey(byte[] key) {
        ensureOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return map.containsKey(new ByteArrayKey(key));
    }

    @Override
    public long size() {
        ensureOpen();
        return map.size();
    }

    @Override
    public void clear() {
        ensureOpen();
        map.clear();
    }

    @Override
    public void forEach(BiConsumer<byte[], IndexEntry> action) {
        ensureOpen();
        Objects.requireNonNull(action, "action cannot be null");
        map.forEach((k, v) -> action.accept(k.getBytes(), v));
    }

    @Override
    public boolean isOffHeap() {
        return false;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            map.clear();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("HeapKeyDir is closed");
        }
    }
}
