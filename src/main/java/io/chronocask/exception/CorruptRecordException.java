package io.chronocask.exception;

/**
 * Thrown when a record has invalid framing, unrecognized magic/version,
 * corrupted fields, or a failing CRC32C checksum verification.
 */
public class CorruptRecordException extends StorageException {

    private final long offset;

    public CorruptRecordException(String message) {
        this(message, -1);
    }

    public CorruptRecordException(String message, long offset) {
        super(offset >= 0 ? message + " at offset " + offset : message);
        this.offset = offset;
    }

    public CorruptRecordException(String message, Throwable cause) {
        this(message, -1, cause);
    }

    public CorruptRecordException(String message, long offset, Throwable cause) {
        super(offset >= 0 ? message + " at offset " + offset : message, cause);
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }
}
