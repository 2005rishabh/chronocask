package io.chronocask.record;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * Utility for computing and verifying CRC32C checksums over ChronoCask records.
 *
 * Checksum covers:
 * 1. The 24-byte header metadata (magic, version, type, reserved, sequenceNumber, keyLength, valueLength)
 * 2. Key bytes
 * 3. Value bytes
 *
 * The 4-byte checksum field itself is excluded.
 */
public final class RecordChecksum {

    private RecordChecksum() {
    }

    /**
     * Calculates the CRC32C checksum for the given record header and key/value byte arrays.
     */
    public static int calculate(RecordHeader header, byte[] key, byte[] value) {
        CRC32C crc = new CRC32C();
        ByteBuffer metaBuf = ByteBuffer.allocate(RecordHeader.METADATA_SIZE);
        writeMetadata(metaBuf, header.magic(), header.version(), header.type().getCode(),
                header.reserved(), header.sequenceNumber(), header.keyLength(), header.valueLength());
        metaBuf.flip();
        crc.update(metaBuf);

        if (key != null && key.length > 0) {
            crc.update(key, 0, key.length);
        }
        if (value != null && value.length > 0) {
            crc.update(value, 0, value.length);
        }
        return (int) crc.getValue();
    }

    /**
     * Calculates the CRC32C checksum from a ByteBuffer containing a serialized record.
     *
     * @param buffer ByteBuffer containing the record
     * @param recordStart offset in the buffer where the record begins
     * @param keyLength key length in bytes
     * @param valueLength value length in bytes
     * @return 32-bit CRC32C checksum
     */
    public static int calculate(ByteBuffer buffer, int recordStart, int keyLength, int valueLength) {
        CRC32C crc = new CRC32C();

        // 1. Update with 24-byte metadata (from recordStart to recordStart + METADATA_SIZE)
        ByteBuffer metaSlice = buffer.duplicate();
        metaSlice.position(recordStart);
        metaSlice.limit(recordStart + RecordHeader.METADATA_SIZE);
        crc.update(metaSlice);

        // 2. Update with key and value payload (skipping the 4-byte checksum at offset 24..28)
        int payloadStart = recordStart + RecordHeader.HEADER_SIZE;
        int payloadLength = keyLength + valueLength;
        if (payloadLength > 0) {
            ByteBuffer payloadSlice = buffer.duplicate();
            payloadSlice.position(payloadStart);
            payloadSlice.limit(payloadStart + payloadLength);
            crc.update(payloadSlice);
        }

        return (int) crc.getValue();
    }

    /**
     * Writes the 24-byte metadata into the destination buffer (big-endian).
     */
    public static void writeMetadata(ByteBuffer buffer, int magic, byte version, byte typeCode,
                                     short reserved, long sequenceNumber, int keyLength, int valueLength) {
        buffer.putInt(magic);
        buffer.put(version);
        buffer.put(typeCode);
        buffer.putShort(reserved);
        buffer.putLong(sequenceNumber);
        buffer.putInt(keyLength);
        buffer.putInt(valueLength);
    }
}
