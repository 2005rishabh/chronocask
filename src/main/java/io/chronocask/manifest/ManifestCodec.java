package io.chronocask.manifest;

import io.chronocask.exception.CorruptRecordException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Binary encoder and decoder for StorageManifest.
 *
 * Header Layout (28 bytes):
 * <pre>
 * +----------------------+----------------+
 * | Magic Number         | 4 bytes        | (0x4D414E46 = 'MANF')
 * | Format Version       | 1 byte         | (1)
 * | Reserved             | 3 bytes        | (0x000000)
 * | Generation           | 8 bytes        |
 * | Active Segment ID    | 4 bytes        |
 * | Immutable Count      | 4 bytes        |
 * | CRC32C Checksum      | 4 bytes        | (checksum over metadata + all entries)
 * +----------------------+----------------+
 * </pre>
 *
 * Each immutable entry is fixed 8 bytes:
 * <pre>
 * | Segment ID (4B) | Type (1B) | HasHintFile (1B) | Reserved (2B) |
 * </pre>
 */
public final class ManifestCodec {

    public static final int MAGIC = 0x4D414E46; // 'MANF'
    public static final byte CURRENT_VERSION = 1;
    public static final int HEADER_SIZE = 28;
    public static final int ENTRY_SIZE = 8;

    private ManifestCodec() {
    }

    public static ByteBuffer encode(StorageManifest manifest) {
        Objects.requireNonNull(manifest, "manifest cannot be null");

        int entryCount = manifest.immutableSegments().size();
        int totalSize = HEADER_SIZE + (entryCount * ENTRY_SIZE);

        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);

        // 1. Write header fields (checksum set to 0 initially)
        buffer.putInt(MAGIC);
        buffer.put(CURRENT_VERSION);
        buffer.put((byte) 0); // reserved
        buffer.putShort((short) 0); // reserved
        buffer.putLong(manifest.generation());
        buffer.putInt(manifest.activeSegmentId());
        buffer.putInt(entryCount);
        buffer.putInt(0); // Checksum placeholder at offset 24

        // 2. Write entries
        for (ManifestSegmentEntry entry : manifest.immutableSegments()) {
            buffer.putInt(entry.segmentId());
            buffer.put(entry.type().getCode());
            buffer.put((byte) (entry.hasHintFile() ? 1 : 0));
            buffer.putShort((short) 0); // reserved
        }

        // 3. Compute CRC32C over everything except the 4-byte checksum field
        CRC32C crc = new CRC32C();
        // Bytes 0..24 (metadata before checksum)
        crc.update(buffer.array(), 0, 24);
        // Bytes 28..totalSize (entries)
        if (entryCount > 0) {
            crc.update(buffer.array(), HEADER_SIZE, entryCount * ENTRY_SIZE);
        }

        int checksum = (int) crc.getValue();
        buffer.putInt(24, checksum);

        buffer.flip();
        return buffer;
    }

    public static StorageManifest decode(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer cannot be null");

        if (buffer.remaining() < HEADER_SIZE) {
            throw new CorruptRecordException("Manifest file too small: " + buffer.remaining() + " bytes");
        }

        ByteOrder originalOrder = buffer.order();
        buffer.order(ByteOrder.BIG_ENDIAN);
        int startPos = buffer.position();

        try {
            int magic = buffer.getInt();
            if (magic != MAGIC) {
                throw new CorruptRecordException(String.format("Invalid manifest magic: 0x%08X", magic));
            }

            byte version = buffer.get();
            if (version != CURRENT_VERSION) {
                throw new CorruptRecordException("Unsupported manifest version: " + version);
            }

            buffer.get(); // reserved
            buffer.getShort(); // reserved
            long generation = buffer.getLong();
            int activeSegmentId = buffer.getInt();
            int entryCount = buffer.getInt();

            if (entryCount < 0) {
                throw new CorruptRecordException("Negative entry count in manifest: " + entryCount);
            }

            int checksum = buffer.getInt();

            int expectedRemaining = entryCount * ENTRY_SIZE;
            if (buffer.remaining() < expectedRemaining) {
                throw new CorruptRecordException("Manifest truncated: expected " + expectedRemaining +
                        " entry bytes, got " + buffer.remaining());
            }

            // Verify checksum
            CRC32C crc = new CRC32C();
            ByteBuffer duplicate = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
            duplicate.position(startPos);
            duplicate.limit(startPos + 24);
            crc.update(duplicate);

            if (entryCount > 0) {
                ByteBuffer entriesSlice = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
                entriesSlice.position(startPos + HEADER_SIZE);
                entriesSlice.limit(startPos + HEADER_SIZE + (entryCount * ENTRY_SIZE));
                crc.update(entriesSlice);
            }

            int computed = (int) crc.getValue();
            if (computed != checksum) {
                throw new CorruptRecordException(String.format(
                        "Manifest CRC32C mismatch: expected 0x%08X, computed 0x%08X", checksum, computed));
            }

            List<ManifestSegmentEntry> entries = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                int segId = buffer.getInt();
                byte typeCode = buffer.get();
                boolean hasHint = buffer.get() == 1;
                buffer.getShort(); // reserved
                entries.add(new ManifestSegmentEntry(segId, SegmentType.fromCode(typeCode), hasHint));
            }

            return new StorageManifest(generation, activeSegmentId, entries);
        } finally {
            buffer.order(originalOrder);
        }
    }
}
