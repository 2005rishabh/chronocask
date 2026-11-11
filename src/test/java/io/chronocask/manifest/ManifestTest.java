package io.chronocask.manifest;

import io.chronocask.exception.CorruptRecordException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ManifestTest {

    @Test
    @DisplayName("Encode and decode StorageManifest with checksum verification")
    void testEncodeDecodeManifest() {
        List<ManifestSegmentEntry> entries = List.of(
                ManifestSegmentEntry.normal(1, true),
                ManifestSegmentEntry.compacted(2, false),
                ManifestSegmentEntry.compacted(3, true)
        );

        StorageManifest manifest = new StorageManifest(42L, 4, entries);

        ByteBuffer encoded = ManifestCodec.encode(manifest);
        assertNotNull(encoded);
        assertTrue(encoded.remaining() >= ManifestCodec.HEADER_SIZE + (3 * ManifestCodec.ENTRY_SIZE));

        StorageManifest decoded = ManifestCodec.decode(encoded);
        assertEquals(42L, decoded.generation());
        assertEquals(4, decoded.activeSegmentId());
        assertEquals(3, decoded.immutableSegments().size());
        assertEquals(entries, decoded.immutableSegments());
        assertEquals(Set.of(1, 2, 3, 4), decoded.allSegmentIds());
    }

    @Test
    @DisplayName("Detect corrupted checksum in manifest")
    void testCorruptedChecksum() {
        StorageManifest manifest = StorageManifest.initial(1);
        ByteBuffer encoded = ManifestCodec.encode(manifest);

        // Corrupt generation at offset 8
        encoded.putLong(8, 999L);

        assertThrows(CorruptRecordException.class, () -> ManifestCodec.decode(encoded));
    }

    @Test
    @DisplayName("ManifestManager atomic commit and reload")
    void testManifestManagerCommitAndLoad(@TempDir Path tempDir) throws IOException {
        ManifestManager manager = new ManifestManager(tempDir);
        assertNull(manager.load());

        StorageManifest initial = StorageManifest.initial(1);
        manager.commit(initial);

        assertTrue(Files.exists(manager.getManifestPath()));
        assertFalse(Files.exists(tempDir.resolve(ManifestManager.TEMP_MANIFEST_FILE_NAME)));

        StorageManifest loaded = manager.load();
        assertEquals(initial, loaded);

        // Rotation
        StorageManifest rotated = loaded.withActiveSegmentRotation(2);
        manager.commit(rotated);

        StorageManifest reloaded = manager.load();
        assertEquals(2L, reloaded.generation());
        assertEquals(2, reloaded.activeSegmentId());
        assertEquals(1, reloaded.immutableSegments().size());
        assertEquals(1, reloaded.immutableSegments().get(0).segmentId());

        // Replacement
        StorageManifest compacted = reloaded.withReplacements(
                Set.of(1),
                List.of(ManifestSegmentEntry.compacted(3, true))
        );
        manager.commit(compacted);

        StorageManifest finalLoaded = manager.load();
        assertEquals(3L, finalLoaded.generation());
        assertEquals(2, finalLoaded.activeSegmentId());
        assertEquals(1, finalLoaded.immutableSegments().size());
        assertEquals(3, finalLoaded.immutableSegments().get(0).segmentId());
        assertEquals(SegmentType.COMPACTED, finalLoaded.immutableSegments().get(0).type());
        assertTrue(finalLoaded.immutableSegments().get(0).hasHintFile());
    }
}
