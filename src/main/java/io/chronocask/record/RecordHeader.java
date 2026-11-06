package io.chronocask.record;

import java.util.Objects;

/**
 * Fixed 28-byte header for each record in an append-only segment.
 *
 * Binary layout (big-endian):
 * <pre>
 * +----------------------+----------------+
 * | Field                | Size           |
 * +----------------------+----------------+
 * | Magic Number         | 4 bytes        | (0x4341534B = 'CASK')
 * | Format Version       | 1 byte         | (Current: 1)
 * | Record Type          | 1 byte         | (1 = PUT, 2 = DELETE)
 * | Reserved             | 2 bytes        | (0x0000)
 * | Sequence Number      | 8 bytes        |
 * | Key Length           | 4 bytes        |
 * | Value Length         | 4 bytes        |
 * | CRC32C Checksum      | 4 bytes        |
 * +----------------------+----------------+
 * </pre>
 */
public record RecordHeader(
        int magic,
        byte version,
        RecordType type,
        short reserved,
        long sequenceNumber,
        int keyLength,
        int valueLength,
        int checksum
) {
    public static final int MAGIC = 0x4341534B; // ASCII for 'C', 'A', 'S', 'K'
    public static final byte CURRENT_VERSION = 1;
    public static final short DEFAULT_RESERVED = 0;
    public static final int HEADER_SIZE = 28;
    public static final int METADATA_SIZE = 24; // Header bytes preceding the checksum field

    public RecordHeader {
        Objects.requireNonNull(type, "type cannot be null");
        if (keyLength < 0) {
            throw new IllegalArgumentException("keyLength cannot be negative: " + keyLength);
        }
        if (valueLength < 0) {
            throw new IllegalArgumentException("valueLength cannot be negative: " + valueLength);
        }
    }

    /**
     * Creates a header for a PUT record with zero checksum (checksum to be computed later).
     */
    public static RecordHeader forPut(long sequenceNumber, int keyLength, int valueLength) {
        return new RecordHeader(MAGIC, CURRENT_VERSION, RecordType.PUT, DEFAULT_RESERVED, sequenceNumber, keyLength, valueLength, 0);
    }

    /**
     * Creates a header for a DELETE record with zero valueLength and zero checksum.
     */
    public static RecordHeader forDelete(long sequenceNumber, int keyLength) {
        return new RecordHeader(MAGIC, CURRENT_VERSION, RecordType.DELETE, DEFAULT_RESERVED, sequenceNumber, keyLength, 0, 0);
    }

    /**
     * Returns a copy of this header with the given calculated CRC32C checksum.
     */
    public RecordHeader withChecksum(int computedChecksum) {
        return new RecordHeader(magic, version, type, reserved, sequenceNumber, keyLength, valueLength, computedChecksum);
    }

    /**
     * Returns the checksum as an unsigned 32-bit integer in a long.
     */
    public long checksumAsUnsignedLong() {
        return Integer.toUnsignedLong(checksum);
    }

    /**
     * Total serialized length of this record, including header and key/value payload.
     */
    public int totalRecordSize() {
        return HEADER_SIZE + keyLength + valueLength;
    }

    /**
     * Total payload size (key bytes + value bytes).
     */
    public int payloadSize() {
        return keyLength + valueLength;
    }
}
