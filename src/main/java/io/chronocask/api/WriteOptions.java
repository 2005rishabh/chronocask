package io.chronocask.api;

import io.chronocask.segment.DurabilityMode;

import java.util.Objects;

/**
 * Options controlling write operations in ChronoCask.
 */
public record WriteOptions(DurabilityMode durability) {

    public static final WriteOptions DEFAULT = new WriteOptions(null);
    public static final WriteOptions SYNC = new WriteOptions(DurabilityMode.SYNC);
    public static final WriteOptions ASYNC = new WriteOptions(DurabilityMode.ASYNC);

    public static WriteOptions of(DurabilityMode durability) {
        return new WriteOptions(durability);
    }

    public static WriteOptions sync() {
        return SYNC;
    }

    public static WriteOptions async() {
        return ASYNC;
    }
}
