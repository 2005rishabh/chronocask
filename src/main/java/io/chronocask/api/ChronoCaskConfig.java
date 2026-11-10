package io.chronocask.api;

import io.chronocask.segment.DurabilityMode;
import io.chronocask.segment.SegmentManager;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Immutable configuration for a ChronoCask storage engine instance.
 */
public record ChronoCaskConfig(
        Path dataDir,
        long maxSegmentSizeBytes,
        DurabilityMode defaultDurability,
        boolean offHeapIndex,
        int initialIndexCapacity,
        long initialKeySlabBytes
) {
    public static final long DEFAULT_MAX_SEGMENT_SIZE = SegmentManager.DEFAULT_MAX_SEGMENT_SIZE_BYTES; // 64MB
    public static final DurabilityMode DEFAULT_DURABILITY = DurabilityMode.ASYNC;
    public static final boolean DEFAULT_OFF_HEAP = true;
    public static final int DEFAULT_INITIAL_CAPACITY = 1024;
    public static final long DEFAULT_KEY_SLAB_BYTES = 1024 * 1024L; // 1MB

    public ChronoCaskConfig {
        Objects.requireNonNull(dataDir, "dataDir cannot be null");
        Objects.requireNonNull(defaultDurability, "defaultDurability cannot be null");
        if (maxSegmentSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSegmentSizeBytes must be positive: " + maxSegmentSizeBytes);
        }
        if (initialIndexCapacity <= 0) {
            throw new IllegalArgumentException("initialIndexCapacity must be positive: " + initialIndexCapacity);
        }
        if (initialKeySlabBytes <= 0) {
            throw new IllegalArgumentException("initialKeySlabBytes must be positive: " + initialKeySlabBytes);
        }
    }

    public static ChronoCaskConfig of(Path dataDir) {
        return builder(dataDir).build();
    }

    public static Builder builder(Path dataDir) {
        return new Builder(dataDir);
    }

    public static final class Builder {
        private final Path dataDir;
        private long maxSegmentSizeBytes = DEFAULT_MAX_SEGMENT_SIZE;
        private DurabilityMode defaultDurability = DEFAULT_DURABILITY;
        private boolean offHeapIndex = DEFAULT_OFF_HEAP;
        private int initialIndexCapacity = DEFAULT_INITIAL_CAPACITY;
        private long initialKeySlabBytes = DEFAULT_KEY_SLAB_BYTES;

        public Builder(Path dataDir) {
            this.dataDir = Objects.requireNonNull(dataDir, "dataDir cannot be null");
        }

        public Builder maxSegmentSizeBytes(long maxSegmentSizeBytes) {
            if (maxSegmentSizeBytes <= 0) {
                throw new IllegalArgumentException("maxSegmentSizeBytes must be positive: " + maxSegmentSizeBytes);
            }
            this.maxSegmentSizeBytes = maxSegmentSizeBytes;
            return this;
        }

        public Builder defaultDurability(DurabilityMode durability) {
            this.defaultDurability = Objects.requireNonNull(durability, "durability cannot be null");
            return this;
        }

        public Builder offHeapIndex(boolean offHeapIndex) {
            this.offHeapIndex = offHeapIndex;
            return this;
        }

        public Builder initialIndexCapacity(int initialIndexCapacity) {
            if (initialIndexCapacity <= 0) {
                throw new IllegalArgumentException("initialIndexCapacity must be positive: " + initialIndexCapacity);
            }
            this.initialIndexCapacity = initialIndexCapacity;
            return this;
        }

        public Builder initialKeySlabBytes(long initialKeySlabBytes) {
            if (initialKeySlabBytes <= 0) {
                throw new IllegalArgumentException("initialKeySlabBytes must be positive: " + initialKeySlabBytes);
            }
            this.initialKeySlabBytes = initialKeySlabBytes;
            return this;
        }

        public ChronoCaskConfig build() {
            return new ChronoCaskConfig(
                    dataDir,
                    maxSegmentSizeBytes,
                    defaultDurability,
                    offHeapIndex,
                    initialIndexCapacity,
                    initialKeySlabBytes
            );
        }
    }
}
