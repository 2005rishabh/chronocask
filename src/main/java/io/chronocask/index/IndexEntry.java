package io.chronocask.index;

import io.chronocask.segment.RecordLocation;

import java.util.Objects;

/**
 * Metadata stored in KeyDir pointing to the latest version of a record on disk.
 *
 * @param segmentId      the log segment containing this record
 * @param offset         the byte offset where the record begins
 * @param recordSize     total serialized byte length of the record (header + payload)
 * @param valueLength    length of the value payload in bytes (0 for tombstones)
 * @param sequenceNumber monotonic sequence number assigned to this record
 */
public record IndexEntry(
        int segmentId,
        long offset,
        int recordSize,
        int valueLength,
        long sequenceNumber
) {
    public IndexEntry {
        if (segmentId < 0) {
            throw new IllegalArgumentException("segmentId cannot be negative: " + segmentId);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative: " + offset);
        }
        if (recordSize <= 0) {
            throw new IllegalArgumentException("recordSize must be positive: " + recordSize);
        }
        if (valueLength < 0) {
            throw new IllegalArgumentException("valueLength cannot be negative: " + valueLength);
        }
    }

    /**
     * Creates an IndexEntry from a RecordLocation, valueLength, and sequenceNumber.
     */
    public static IndexEntry of(RecordLocation location, int valueLength, long sequenceNumber) {
        Objects.requireNonNull(location, "location cannot be null");
        return new IndexEntry(location.segmentId(), location.offset(), location.recordSize(), valueLength, sequenceNumber);
    }

    /**
     * Returns the physical RecordLocation corresponding to this entry.
     */
    public RecordLocation location() {
        return new RecordLocation(segmentId, offset, recordSize);
    }
}
