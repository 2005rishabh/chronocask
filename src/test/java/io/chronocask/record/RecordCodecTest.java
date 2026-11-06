package io.chronocask.record;

import io.chronocask.exception.CorruptRecordException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RecordCodecTest {

    @Test
    @DisplayName("Encode and decode a PUT record matching specification example")
    void testEncodeDecodePutRecord() {
        Record record = Record.put(1L, "user:101", "Rishabh");

        assertEquals(8, record.header().keyLength());
        assertEquals(7, record.header().valueLength());
        assertEquals(1L, record.header().sequenceNumber());
        assertEquals(RecordType.PUT, record.type());

        // Header (28) + Key (8) + Value (7) = 43 bytes
        assertEquals(43, record.totalSize());

        ByteBuffer encoded = RecordCodec.encode(record);
        assertEquals(43, encoded.remaining());

        // Verify exact byte layout and fields in Big-Endian order
        assertEquals(0x4341534B, encoded.getInt(0), "Magic number must be 0x4341534B ('CASK')");
        assertEquals((byte) 1, encoded.get(4), "Version must be 1");
        assertEquals((byte) 1, encoded.get(5), "PUT type code must be 1");
        assertEquals((short) 0, encoded.getShort(6), "Reserved must be 0");
        assertEquals(1L, encoded.getLong(8), "Sequence number must be 1");
        assertEquals(8, encoded.getInt(16), "Key length must be 8");
        assertEquals(7, encoded.getInt(20), "Value length must be 7");

        int checksum = encoded.getInt(24);
        assertNotEquals(0, checksum, "Checksum should be calculated and non-zero");

        byte[] keyBytes = new byte[8];
        encoded.position(28);
        encoded.get(keyBytes);
        assertEquals("user:101", new String(keyBytes, StandardCharsets.UTF_8));

        byte[] valueBytes = new byte[7];
        encoded.get(valueBytes);
        assertEquals("Rishabh", new String(valueBytes, StandardCharsets.UTF_8));

        // Rewind and decode
        encoded.rewind();
        Record decoded = RecordCodec.decode(encoded);

        assertEquals(record.header(), decoded.header());
        assertArrayEquals(record.key(), decoded.key());
        assertArrayEquals(record.value(), decoded.value());
        assertEquals("user:101", decoded.keyAsString());
        assertEquals("Rishabh", decoded.valueAsString());
    }

    @Test
    @DisplayName("Encode and decode a DELETE (tombstone) record")
    void testEncodeDecodeDeleteRecord() {
        Record tombstone = Record.delete(42L, "user:101");

        assertEquals(RecordType.DELETE, tombstone.type());
        assertTrue(tombstone.isTombstone());
        assertEquals(0, tombstone.header().valueLength());
        assertEquals(0, tombstone.value().length);

        ByteBuffer encoded = RecordCodec.encode(tombstone);
        assertEquals(28 + 8, encoded.remaining()); // 28 header + 8 key

        Record decoded = RecordCodec.decode(encoded);
        assertEquals(RecordType.DELETE, decoded.type());
        assertEquals(42L, decoded.sequenceNumber());
        assertEquals("user:101", decoded.keyAsString());
        assertEquals(0, decoded.value().length);
    }

    @Test
    @DisplayName("Detect corrupted magic number")
    void testCorruptedMagicNumber() {
        Record record = Record.put(1L, "k", "v");
        ByteBuffer encoded = RecordCodec.encode(record);
        encoded.putInt(0, 0x12345678); // Corrupt magic

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("Invalid magic number"));
    }

    @Test
    @DisplayName("Detect unsupported format version")
    void testUnsupportedVersion() {
        Record record = Record.put(1L, "k", "v");
        ByteBuffer encoded = RecordCodec.encode(record);
        encoded.put(4, (byte) 99); // Corrupt version

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("Unsupported format version"));
    }

    @Test
    @DisplayName("Detect unknown record type")
    void testUnknownRecordType() {
        Record record = Record.put(1L, "k", "v");
        ByteBuffer encoded = RecordCodec.encode(record);
        encoded.put(5, (byte) 15); // Unknown type code

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("Unknown record type"));
    }

    @Test
    @DisplayName("Reject DELETE record with non-zero value length")
    void testDeleteWithNonZeroValueLength() {
        Record record = Record.delete(1L, "k");
        ByteBuffer encoded = RecordCodec.encode(record);
        encoded.putInt(20, 10); // Set value length to 10 for DELETE

        assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
    }

    @Test
    @DisplayName("Detect single-bit corruption in payload key")
    void testDetectChecksumMismatchInKey() {
        Record record = Record.put(10L, "my-test-key", "my-test-value");
        ByteBuffer encoded = RecordCodec.encode(record);

        // Mutate one byte in the key (key starts at offset 28)
        byte originalByte = encoded.get(28);
        encoded.put(28, (byte) (originalByte ^ 0x01));

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("CRC32C checksum mismatch"));
    }

    @Test
    @DisplayName("Detect single-bit corruption in payload value")
    void testDetectChecksumMismatchInValue() {
        Record record = Record.put(10L, "my-key", "my-value");
        ByteBuffer encoded = RecordCodec.encode(record);

        // Header is 28, key is 6 ("my-key"), value starts at 34
        byte valByte = encoded.get(34);
        encoded.put(34, (byte) (valByte ^ 0x01));

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("CRC32C checksum mismatch"));
    }

    @Test
    @DisplayName("Detect checksum mismatch when sequence number is altered")
    void testDetectChecksumMismatchInHeaderField() {
        Record record = Record.put(10L, "key", "val");
        ByteBuffer encoded = RecordCodec.encode(record);

        // Mutate sequence number at offset 8
        long originalSeq = encoded.getLong(8);
        encoded.putLong(8, originalSeq + 1);

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(encoded));
        assertTrue(ex.getMessage().contains("CRC32C checksum mismatch"));
    }

    @Test
    @DisplayName("Handle incomplete buffer for header decoding")
    void testIncompleteHeader() {
        ByteBuffer shortBuffer = ByteBuffer.allocate(10);
        assertThrows(CorruptRecordException.class, () -> RecordCodec.decodeHeader(shortBuffer));
        assertFalse(RecordCodec.hasCompleteHeader(shortBuffer));
    }

    @Test
    @DisplayName("Handle incomplete buffer for payload decoding")
    void testIncompletePayload() {
        Record record = Record.put(1L, "long-key-name", "long-value-data");
        ByteBuffer encoded = RecordCodec.encode(record);

        // Truncate by 5 bytes
        ByteBuffer truncated = ByteBuffer.allocate(encoded.remaining() - 5);
        encoded.limit(encoded.remaining() - 5);
        truncated.put(encoded);
        truncated.flip();

        CorruptRecordException ex = assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(truncated));
        assertTrue(ex.getMessage().contains("Incomplete record payload"));
    }

    @Test
    @DisplayName("Encode and decode using DirectByteBuffer")
    void testDirectByteBuffer() {
        Record record = Record.put(99L, "direct-key", "direct-value");
        ByteBuffer direct = ByteBuffer.allocateDirect(record.totalSize()).order(ByteOrder.BIG_ENDIAN);

        RecordCodec.encode(record, direct);
        direct.flip();

        Record decoded = RecordCodec.decode(direct);
        assertEquals(record, decoded);
    }

    @Test
    @DisplayName("Large record encoding and decoding")
    void testLargePayload() {
        byte[] largeValue = new byte[65536]; // 64 KB
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 251);
        }

        Record record = Record.put(500L, "large-blob".getBytes(StandardCharsets.UTF_8), largeValue);
        ByteBuffer encoded = RecordCodec.encode(record);

        Record decoded = RecordCodec.decode(encoded);
        assertEquals(500L, decoded.sequenceNumber());
        assertEquals("large-blob", decoded.keyAsString());
        assertArrayEquals(largeValue, decoded.value());
    }
}
