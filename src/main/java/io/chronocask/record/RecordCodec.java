package io.chronocask.record;

import io.chronocask.exception.CorruptRecordException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Encoder and decoder for ChronoCask binary record format.
 *
 * All operations use big-endian byte ordering.
 */
public final class RecordCodec {

    private RecordCodec() {
    }

    /**
     * Serializes a Record into a newly allocated heap ByteBuffer.
     * The returned buffer is flipped and ready for reading/writing to channel.
     */
    public static ByteBuffer encode(Record record) {
        Objects.requireNonNull(record, "record cannot be null");
        ByteBuffer buffer = ByteBuffer.allocate(record.totalSize()).order(ByteOrder.BIG_ENDIAN);
        encode(record, buffer);
        buffer.flip();
        return buffer;
    }

    /**
     * Serializes a Record into the provided destination ByteBuffer at its current position.
     */
    public static void encode(Record record, ByteBuffer destination) {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(destination, "destination cannot be null");

        ByteOrder originalOrder = destination.order();
        destination.order(ByteOrder.BIG_ENDIAN);
        try {
            RecordHeader header = record.header();

            // Calculate checksum if it was not already computed
            int checksum = header.checksum();
            if (checksum == 0) {
                checksum = RecordChecksum.calculate(header, record.key(), record.value());
                header = header.withChecksum(checksum);
            }

            // 1. Write 24-byte metadata
            RecordChecksum.writeMetadata(
                    destination,
                    header.magic(),
                    header.version(),
                    header.type().getCode(),
                    header.reserved(),
                    header.sequenceNumber(),
                    header.keyLength(),
                    header.valueLength()
            );

            // 2. Write 4-byte CRC32C checksum
            destination.putInt(checksum);

            // 3. Write key and value payload
            if (record.key().length > 0) {
                destination.put(record.key());
            }
            if (record.value().length > 0) {
                destination.put(record.value());
            }
        } finally {
            destination.order(originalOrder);
        }
    }

    /**
     * Checks if the buffer contains at least a full 28-byte header.
     */
    public static boolean hasCompleteHeader(ByteBuffer source) {
        return source != null && source.remaining() >= RecordHeader.HEADER_SIZE;
    }

    /**
     * Decodes and validates only the 28-byte header from the buffer without advancing
     * the buffer's position beyond the header.
     *
     * @param source ByteBuffer positioned at the start of a record
     * @return validated RecordHeader
     * @throws CorruptRecordException if the header is malformed or invalid
     */
    public static RecordHeader decodeHeader(ByteBuffer source) {
        Objects.requireNonNull(source, "source buffer cannot be null");
        if (source.remaining() < RecordHeader.HEADER_SIZE) {
            throw new CorruptRecordException("Incomplete record header: expected at least "
                    + RecordHeader.HEADER_SIZE + " bytes, but only " + source.remaining() + " available");
        }

        ByteOrder originalOrder = source.order();
        source.order(ByteOrder.BIG_ENDIAN);
        try {
            int magic = source.getInt();
            if (magic != RecordHeader.MAGIC) {
                throw new CorruptRecordException(String.format(
                        "Invalid magic number: expected 0x%08X, got 0x%08X", RecordHeader.MAGIC, magic));
            }

            byte version = source.get();
            if (version != RecordHeader.CURRENT_VERSION) {
                throw new CorruptRecordException(String.format(
                        "Unsupported format version: %d (expected %d)", version, RecordHeader.CURRENT_VERSION));
            }

            byte typeCode = source.get();
            RecordType type = RecordType.fromCode(typeCode);

            short reserved = source.getShort();
            long sequenceNumber = source.getLong();

            int keyLength = source.getInt();
            if (keyLength < 0) {
                throw new CorruptRecordException("Negative key length: " + keyLength);
            }

            int valueLength = source.getInt();
            if (valueLength < 0) {
                throw new CorruptRecordException("Negative value length: " + valueLength);
            }
            if (type == RecordType.DELETE && valueLength != 0) {
                throw new CorruptRecordException(
                        "DELETE record must have valueLength = 0, got " + valueLength);
            }

            int checksum = source.getInt();

            return new RecordHeader(magic, version, type, reserved, sequenceNumber, keyLength, valueLength, checksum);
        } finally {
            source.order(originalOrder);
        }
    }

    /**
     * Decodes a complete Record from the buffer, verifying framing and CRC32C checksum.
     * Advances the buffer position past the record upon success.
     *
     * @param source ByteBuffer positioned at the start of a record
     * @return validated Record
     * @throws CorruptRecordException if corrupted or checksum verification fails
     */
    public static Record decode(ByteBuffer source) {
        Objects.requireNonNull(source, "source buffer cannot be null");
        int recordStart = source.position();

        RecordHeader header = decodeHeader(source);

        int payloadLength = header.payloadSize();
        if (source.remaining() < payloadLength) {
            source.position(recordStart); // rewind
            throw new CorruptRecordException("Incomplete record payload: expected "
                    + payloadLength + " bytes, but only " + source.remaining() + " available");
        }

        byte[] key = new byte[header.keyLength()];
        if (header.keyLength() > 0) {
            source.get(key);
        }

        byte[] value = new byte[header.valueLength()];
        if (header.valueLength() > 0) {
            source.get(value);
        }

        // Verify CRC32C checksum
        int computedChecksum = RecordChecksum.calculate(source, recordStart, header.keyLength(), header.valueLength());
        if (computedChecksum != header.checksum()) {
            source.position(recordStart); // rewind on error
            throw new CorruptRecordException(String.format(
                    "CRC32C checksum mismatch: expected 0x%08X, computed 0x%08X",
                    header.checksum(), computedChecksum), recordStart);
        }

        return new Record(header, key, value);
    }
}
