package io.chronocask.record;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * An immutable in-memory representation of a ChronoCask log entry.
 */
public final class Record {

    private final RecordHeader header;
    private final byte[] key;
    private final byte[] value;

    public Record(RecordHeader header, byte[] key, byte[] value) {
        this.header = Objects.requireNonNull(header, "header cannot be null");
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.value = value == null ? new byte[0] : value;

        if (this.key.length != header.keyLength()) {
            throw new IllegalArgumentException("Key array length " + this.key.length +
                    " does not match header keyLength " + header.keyLength());
        }
        if (this.value.length != header.valueLength()) {
            throw new IllegalArgumentException("Value array length " + this.value.length +
                    " does not match header valueLength " + header.valueLength());
        }
    }

    /**
     * Creates a PUT record with automatically computed CRC32C checksum.
     */
    public static Record put(long sequenceNumber, byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        byte[] val = value == null ? new byte[0] : value;
        RecordHeader baseHeader = RecordHeader.forPut(sequenceNumber, key.length, val.length);
        int checksum = RecordChecksum.calculate(baseHeader, key, val);
        return new Record(baseHeader.withChecksum(checksum), key, val);
    }

    /**
     * Creates a PUT record from UTF-8 strings.
     */
    public static Record put(long sequenceNumber, String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return put(sequenceNumber, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a DELETE (tombstone) record with automatically computed CRC32C checksum.
     */
    public static Record delete(long sequenceNumber, byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        byte[] emptyValue = new byte[0];
        RecordHeader baseHeader = RecordHeader.forDelete(sequenceNumber, key.length);
        int checksum = RecordChecksum.calculate(baseHeader, key, emptyValue);
        return new Record(baseHeader.withChecksum(checksum), key, emptyValue);
    }

    /**
     * Creates a DELETE (tombstone) record from a UTF-8 string key.
     */
    public static Record delete(long sequenceNumber, String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return delete(sequenceNumber, key.getBytes(StandardCharsets.UTF_8));
    }

    public RecordHeader header() {
        return header;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public RecordType type() {
        return header.type();
    }

    public long sequenceNumber() {
        return header.sequenceNumber();
    }

    public boolean isTombstone() {
        return header.type() == RecordType.DELETE;
    }

    public String keyAsString() {
        return new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    public int totalSize() {
        return header.totalRecordSize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(header, record.header) &&
                Arrays.equals(key, record.key) &&
                Arrays.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(header);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "type=" + header.type() +
                ", seq=" + header.sequenceNumber() +
                ", key='" + keyAsString() + '\'' +
                ", valLen=" + header.valueLength() +
                ", checksum=" + Long.toHexString(header.checksumAsUnsignedLong()) +
                '}';
    }
}
