package io.chronocask.hint;

import io.chronocask.exception.CorruptRecordException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Binary framing and codec for ChronoCask hint files.
 *
 * Header Layout (20 bytes):
 * <pre>
 * +----------------------+----------------+
 * | Field                | Size           |
 * +----------------------+----------------+
 * | Magic Number         | 4 bytes        | (0x48494E54 = 'HINT')
 * | Format Version       | 1 byte         | (1)
 * | Reserved             | 3 bytes        | (0x000000)
 * | Segment ID           | 4 bytes        | (int)
 * | Total Entries        | 4 bytes        | (int)
 * | Header Checksum      | 4 bytes        | (CRC32C over first 16 bytes)
 * +----------------------+----------------+
 * </pre>
 *
 * Record Layout (32 bytes fixed + key length):
 * <pre>
 * +----------------------+----------------+
 * | Sequence Number      | 8 bytes        |
 * | Record Offset        | 8 bytes        |
 * | Record Size          | 4 bytes        |
 * | Value Length         | 4 bytes        |
 * | Key Length           | 4 bytes        |
 * | Record Checksum      | 4 bytes        | (CRC32C over fields + key bytes)
 * +----------------------+----------------+
 * | Key Bytes            | Variable       |
 * +----------------------+----------------+
 * </pre>
 */
public final class HintFileCodec {

    public static final int MAGIC = 0x48494E54; // 'HINT'
    public static final byte CURRENT_VERSION = 1;
    public static final int FILE_HEADER_SIZE = 20;
    public static final int RECORD_HEADER_SIZE = 32;

    private HintFileCodec() {
    }

    public static ByteBuffer encodeFileHeader(int segmentId, int totalEntries) {
        ByteBuffer buf = ByteBuffer.allocate(FILE_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(MAGIC);
        buf.put(CURRENT_VERSION);
        buf.put((byte) 0);
        buf.putShort((short) 0);
        buf.putInt(segmentId);
        buf.putInt(totalEntries);

        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 0, 16);
        buf.putInt((int) crc.getValue());

        buf.flip();
        return buf;
    }

    public static HintFileHeader decodeFileHeader(ByteBuffer buf) {
        Objects.requireNonNull(buf, "buffer cannot be null");
        if (buf.remaining() < FILE_HEADER_SIZE) {
            throw new CorruptRecordException("Hint file header too small: " + buf.remaining() + " bytes");
        }

        ByteOrder original = buf.order();
        buf.order(ByteOrder.BIG_ENDIAN);
        int start = buf.position();

        try {
            int magic = buf.getInt();
            if (magic != MAGIC) {
                throw new CorruptRecordException(String.format("Invalid hint magic: 0x%08X", magic));
            }
            byte version = buf.get();
            if (version != CURRENT_VERSION) {
                throw new CorruptRecordException("Unsupported hint version: " + version);
            }
            buf.get(); // reserved
            buf.getShort(); // reserved
            int segmentId = buf.getInt();
            int totalEntries = buf.getInt();
            int checksum = buf.getInt();

            CRC32C crc = new CRC32C();
            ByteBuffer slice = buf.duplicate().order(ByteOrder.BIG_ENDIAN);
            slice.position(start);
            slice.limit(start + 16);
            crc.update(slice);

            int computed = (int) crc.getValue();
            if (computed != checksum) {
                throw new CorruptRecordException(String.format(
                        "Hint header CRC32C mismatch: expected 0x%08X, got 0x%08X", checksum, computed));
            }

            return new HintFileHeader(segmentId, totalEntries);
        } finally {
            buf.order(original);
        }
    }

    public static ByteBuffer encodeEntry(HintEntry entry) {
        Objects.requireNonNull(entry, "entry cannot be null");
        byte[] key = entry.key();
        int totalSize = RECORD_HEADER_SIZE + key.length;

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(entry.sequenceNumber());
        buf.putLong(entry.recordOffset());
        buf.putInt(entry.recordSize());
        buf.putInt(entry.valueLength());
        buf.putInt(key.length);

        // Checksum over the 28 metadata bytes + key bytes
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 0, 28);
        crc.update(key, 0, key.length);
        buf.putInt((int) crc.getValue());

        buf.put(key);

        buf.flip();
        return buf;
    }

    public static HintEntry decodeEntry(ByteBuffer buf, int segmentId) {
        Objects.requireNonNull(buf, "buffer cannot be null");
        if (buf.remaining() < RECORD_HEADER_SIZE) {
            throw new CorruptRecordException("Incomplete hint record header: " + buf.remaining() + " bytes");
        }

        ByteOrder original = buf.order();
        buf.order(ByteOrder.BIG_ENDIAN);
        int start = buf.position();

        try {
            long sequenceNumber = buf.getLong();
            long recordOffset = buf.getLong();
            int recordSize = buf.getInt();
            int valueLength = buf.getInt();
            int keyLength = buf.getInt();
            int checksum = buf.getInt();

            if (keyLength < 0 || buf.remaining() < keyLength) {
                buf.position(start);
                throw new CorruptRecordException("Truncated hint record key: expected " + keyLength + " bytes");
            }

            byte[] key = new byte[keyLength];
            buf.get(key);

            // Verify checksum
            CRC32C crc = new CRC32C();
            ByteBuffer metaSlice = buf.duplicate().order(ByteOrder.BIG_ENDIAN);
            metaSlice.position(start);
            metaSlice.limit(start + 28);
            crc.update(metaSlice);
            crc.update(key, 0, keyLength);

            int computed = (int) crc.getValue();
            if (computed != checksum) {
                buf.position(start);
                throw new CorruptRecordException(String.format(
                        "Hint record CRC32C mismatch: expected 0x%08X, got 0x%08X", checksum, computed));
            }

            return new HintEntry(segmentId, sequenceNumber, recordOffset, recordSize, valueLength, key);
        } finally {
            buf.order(original);
        }
    }

    public record HintFileHeader(int segmentId, int totalEntries) {}
}
