package io.chronocask.segment;

/**
 * Durability modes for write operations.
 */
public enum DurabilityMode {
    /**
     * Appends data to OS page cache without forcing every write to physical disk.
     * Higher throughput, but recent writes may be lost on OS crash or power failure.
     */
    ASYNC,

    /**
     * Forces the FileChannel to flush data to stable storage before reporting success.
     * Strong crash durability guarantee with higher I/O latency.
     */
    SYNC
}
