package io.chronocask.segment;

import io.chronocask.record.Record;

import java.util.Objects;

/**
 * Combines an in-memory Record with its persistent RecordLocation on disk.
 */
public record LocatedRecord(RecordLocation location, Record record) {

    public LocatedRecord {
        Objects.requireNonNull(location, "location cannot be null");
        Objects.requireNonNull(record, "record cannot be null");
    }
}
