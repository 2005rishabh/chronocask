package io.chronocask.hint;

import io.chronocask.index.IndexEntry;
import io.chronocask.segment.RecordLocation;

import java.util.Arrays;
import java.util.Objects;

/**
 * In-memory representation of a single hint record pointing to a live record in a segment.
 *
 * @param segmentId      the log segment ID this hint corresponds to
 * @param sequenceNumber monotonic sequence number of the original record
 * @param recordOffset   byte offset where the record begins in the segment
 * @param recordSize     total serialized size of the record (header + payload)
 * @param valueLength    length of the value payload (0 for tombstones)
 * @param key            binary key bytes
 */
public record HintEntry(
        int segmentId,
        long sequenceNumber,
        long recordOffset,
        int recordSize,
        int valueLength,
        byte[] key
) {
    public HintEntry {
        if (segmentId < 0) throw new IllegalArgumentException("segmentId < 0");
        if (sequenceNumber < 0) throw new IllegalArgumentException("sequenceNumber < 0");
        if (recordOffset < 0) throw new IllegalArgumentException("recordOffset < 0");
        if (recordSize <= 0) throw new IllegalArgumentException("recordSize <= 0");
        if (valueLength < 0) throw new IllegalArgumentException("valueLength < 0");
        Objects.requireNonNull(key, "key cannot be null");
        key = key.clone();
    }

    public static HintEntry of(int segmentId, byte[] key, IndexEntry entry) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(entry, "entry cannot be null");
        return new HintEntry(
                segmentId,
                entry.sequenceNumber(),
                entry.offset(),
                entry.recordSize(),
                entry.valueLength(),
                key
        );
    }

    public IndexEntry toIndexEntry() {
        return new IndexEntry(segmentId, recordOffset, recordSize, valueLength, sequenceNumber);
    }

    public RecordLocation toRecordLocation() {
        return new RecordLocation(segmentId, recordOffset, recordSize);
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HintEntry hintEntry = (HintEntry) o;
        return segmentId == hintEntry.segmentId &&
                sequenceNumber == hintEntry.sequenceNumber &&
                recordOffset == hintEntry.recordOffset &&
                recordSize == hintEntry.recordSize &&
                valueLength == hintEntry.valueLength &&
                Arrays.equals(key, hintEntry.key);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(segmentId, sequenceNumber, recordOffset, recordSize, valueLength);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
