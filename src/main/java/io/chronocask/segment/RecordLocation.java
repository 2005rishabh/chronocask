package io.chronocask.segment;

/**
 * Identifies the exact location and physical extent of a record on disk.
 *
 * @param segmentId  unique identifier for the log segment file
 * @param offset     byte offset in the segment where the 28-byte header begins
 * @param recordSize total serialized byte length of the record (header + payload)
 */
public record RecordLocation(int segmentId, long offset, int recordSize) {

    public RecordLocation {
        if (segmentId < 0) {
            throw new IllegalArgumentException("segmentId cannot be negative: " + segmentId);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative: " + offset);
        }
        if (recordSize <= 0) {
            throw new IllegalArgumentException("recordSize must be positive: " + recordSize);
        }
    }
}
