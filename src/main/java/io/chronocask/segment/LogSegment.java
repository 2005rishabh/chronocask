package io.chronocask.segment;

import io.chronocask.record.Record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Common contract for an append-only disk segment in ChronoCask.
 */
public interface LogSegment extends Closeable {

    /**
     * Unique integer identifier for this segment (e.g. 1 for segment-000001.log).
     */
    int segmentId();

    /**
     * Absolute filesystem path to the underlying segment file.
     */
    Path path();

    /**
     * Current byte size of the segment file.
     */
    long size();

    /**
     * Returns true if this segment is sealed (immutable, read-only).
     */
    boolean isSealed();

    /**
     * Seals this segment, preventing further appends and flushing pending writes.
     */
    void seal() throws IOException;

    /**
     * Appends a record using the specified durability mode.
     *
     * @param record     the record to serialize and append
     * @param durability ASYNC or SYNC
     * @return the physical disk location of the appended record
     * @throws IOException           on I/O failure
     * @throws IllegalStateException if this segment is sealed or closed
     */
    RecordLocation append(Record record, DurabilityMode durability) throws IOException;

    /**
     * Appends a record using the default ASYNC durability mode.
     */
    default RecordLocation append(Record record) throws IOException {
        return append(record, DurabilityMode.ASYNC);
    }

    /**
     * Reads and validates a record from the specified byte offset.
     *
     * @param offset byte offset where the record begins
     * @param size   total byte length of the record (header + payload)
     * @return deserialized and checksum-validated Record
     * @throws IOException                      on I/O failure
     * @throws io.chronocask.exception.CorruptRecordException if checksum or framing fails
     */
    Record read(long offset, int size) throws IOException;

    /**
     * Convenience read method taking a RecordLocation.
     */
    default Record read(RecordLocation location) throws IOException {
        if (location.segmentId() != segmentId()) {
            throw new IllegalArgumentException("RecordLocation segmentId " + location.segmentId() +
                    " does not match this segment " + segmentId());
        }
        return read(location.offset(), location.recordSize());
    }

    /**
     * Flushes unwritten buffered data to underlying storage.
     *
     * @param forceMetadata if true, forces filesystem metadata updates as well
     */
    void flush(boolean forceMetadata) throws IOException;

    /**
     * Deletes the underlying segment file from the filesystem.
     * The segment must be closed before calling delete.
     */
    void delete() throws IOException;

    /**
     * Truncates the segment file to the given target length in bytes.
     * Primarily used during recovery to discard an uncommitted or corrupted tail record.
     */
    void truncate(long targetLength) throws IOException;

    /**
     * Returns an iterator that scans records sequentially from the beginning of this segment.
     */
    Iterator<LocatedRecord> iterator() throws IOException;

    /**
     * Sequentially scans and passes each valid record to the consumer.
     */
    default void scan(Consumer<LocatedRecord> consumer) throws IOException {
        Iterator<LocatedRecord> it = iterator();
        while (it.hasNext()) {
            consumer.accept(it.next());
        }
    }
}
