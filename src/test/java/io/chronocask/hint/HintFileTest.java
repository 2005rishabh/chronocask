package io.chronocask.hint;

import io.chronocask.exception.CorruptRecordException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HintFileTest {

    @Test
    @DisplayName("Encode and decode individual hint entries with checksum verification")
    void testEncodeDecodeHintEntry() {
        byte[] key = "user:session:101".getBytes(StandardCharsets.UTF_8);
        HintEntry entry = new HintEntry(1, 42L, 1024L, 88, 24, key);

        ByteBuffer encoded = HintFileCodec.encodeEntry(entry);
        assertEquals(HintFileCodec.RECORD_HEADER_SIZE + key.length, encoded.remaining());

        HintEntry decoded = HintFileCodec.decodeEntry(encoded, 1);
        assertEquals(entry, decoded);
        assertEquals("user:session:101", new String(decoded.key(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Detect checksum mismatch in hint entry")
    void testCorruptedHintEntry() {
        byte[] key = "my-key".getBytes(StandardCharsets.UTF_8);
        HintEntry entry = new HintEntry(1, 10L, 500L, 60, 10, key);
        ByteBuffer encoded = HintFileCodec.encodeEntry(entry);

        // Mutate offset at position 8
        encoded.putLong(8, 9999L);

        assertThrows(CorruptRecordException.class, () -> HintFileCodec.decodeEntry(encoded, 1));
    }

    @Test
    @DisplayName("Write and read complete hint file")
    void testWriteAndReadHintFile(@TempDir Path tempDir) throws IOException {
        List<HintEntry> entries = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            entries.add(new HintEntry(
                    5,
                    i + 1,
                    i * 120L,
                    120,
                    30,
                    ("key:" + i).getBytes(StandardCharsets.UTF_8)
            ));
        }

        Path writtenPath = HintFileWriter.write(tempDir, 5, entries);
        assertTrue(Files.exists(writtenPath));
        assertEquals(HintFileWriter.getHintFilePath(tempDir, 5), writtenPath);

        List<HintEntry> readBack = HintFileReader.readAll(writtenPath);
        assertEquals(entries.size(), readBack.size());
        assertEquals(entries, readBack);
    }
}
