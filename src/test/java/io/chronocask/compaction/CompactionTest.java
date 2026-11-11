package io.chronocask.compaction;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.hint.HintFileWriter;
import io.chronocask.manifest.ManifestManager;
import io.chronocask.manifest.StorageManifest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CompactionTest {

    @Test
    @DisplayName("Compact multiple segments: purges dead records, frees disk space, and maintains read consistency")
    void testBasicCompaction(@TempDir Path tempDir) {
        // Small segment size (120B) to force rotation after every 2-3 records
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .maxSegmentSizeBytes(120)
                .offHeapIndex(true)
                .build();

        try (ChronoCask store = ChronoCask.open(config)) {
            // Write multiple updates and tombstones
            store.put("user:1", "Alice");
            store.put("user:2", "Bob");
            store.put("user:3", "Charlie");

            store.put("user:1", "Alice-v2"); // supersedes Alice
            store.put("user:2", "Bob-v2");   // supersedes Bob
            store.delete("user:3");          // tombstone

            store.put("user:4", "David");
            store.put("user:1", "Alice-final");

            // We should have multiple immutable segments
            assertTrue(store.getEngine().getSegmentManager().getImmutableSegments().size() >= 2);
            assertEquals(3, store.size()); // user:1, user:2, user:4

            // Run compaction
            CompactionResult result = store.compact();
            assertTrue(result.segmentsExamined() >= 2);
            assertTrue(result.obsoleteRecordsReclaimed() > 0);
            assertTrue(result.bytesReclaimed() > 0);
            assertEquals(3, store.size());

            // Verify live data integrity
            assertEquals("Alice-final", store.get("user:1"));
            assertEquals("Bob-v2", store.get("user:2"));
            assertEquals("David", store.get("user:4"));
            assertNull(store.get("user:3"));
        }

        // Reopen database and verify persistent state after compaction
        try (ChronoCask store = ChronoCask.open(config)) {
            assertEquals(3, store.size());
            assertEquals("Alice-final", store.get("user:1"));
            assertEquals("Bob-v2", store.get("user:2"));
            assertEquals("David", store.get("user:4"));
            assertNull(store.get("user:3"));

            // Verify that hint files were generated and used
            StorageManifest manifest = store.getEngine().getSegmentManager().getManifest();
            assertNotNull(manifest);
            assertTrue(manifest.immutableSegments().stream().allMatch(s ->
                    Files.exists(HintFileWriter.getHintFilePath(tempDir, s.segmentId()))));
        }
    }

    @Test
    @DisplayName("Issue 1 verification: Tombstone removal boundary prevents deleted key resurrection")
    void testTombstoneRemovalBoundaryNoResurrection(@TempDir Path tempDir) {
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .maxSegmentSizeBytes(80)
                .offHeapIndex(false)
                .build();

        try (ChronoCask store = ChronoCask.open(config)) {
            // Segment 1: PUT account:1 = 100
            store.put("account:1", "100");
            store.put("account:2", "200"); // forces rotation

            // Segment 2: DELETE account:1
            store.delete("account:1");
            store.put("account:3", "300"); // forces rotation

            // Segment 3: active segment with other writes
            store.put("account:4", "400");

            assertEquals(3, store.size()); // account:2, account:3, account:4
            assertNull(store.get("account:1"));

            // Compact immutable prefix (segments 1 and 2)
            CompactionResult result = store.compact();
            assertTrue(result.segmentsReplaced() >= 2);

            // account:1 must remain absent
            assertNull(store.get("account:1"));
            assertEquals(3, store.size());
        }

        // Reopen from disk: account:1 MUST NOT resurrect
        try (ChronoCask store = ChronoCask.open(config)) {
            assertNull(store.get("account:1"), "Deleted key must NOT reappear after compaction and restart!");
            assertEquals("200", store.get("account:2"));
            assertEquals("300", store.get("account:3"));
            assertEquals("400", store.get("account:4"));
            assertEquals(3, store.size());
        }
    }

    @Test
    @DisplayName("Original record sequence numbers are preserved across compaction")
    void testPreserveOriginalSequenceNumbers(@TempDir Path tempDir) {
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .maxSegmentSizeBytes(90)
                .build();

        long seqBeforeCompaction;
        try (ChronoCask store = ChronoCask.open(config)) {
            store.put("k1", "v1");
            store.put("k2", "v2");
            store.put("k3", "v3");
            store.put("k1", "v1-updated");
            store.put("k4", "v4");

            seqBeforeCompaction = store.currentSequenceNumber();
            assertEquals(5, seqBeforeCompaction);

            // Compaction should not allocate new logical sequence numbers
            CompactionResult result = store.compact();
            assertTrue(result.liveRecordsRetained() > 0);
            assertEquals(seqBeforeCompaction, store.currentSequenceNumber());
        }

        // On restart, the restored sequence number must match seqBeforeCompaction
        try (ChronoCask store = ChronoCask.open(config)) {
            assertEquals(seqBeforeCompaction, store.currentSequenceNumber());
            // New append continues monotonically
            store.put("k5", "v5");
            assertEquals(seqBeforeCompaction + 1, store.currentSequenceNumber());
        }
    }

    @Test
    @DisplayName("Fault tolerance: Unreferenced files from interrupted compaction are safely cleaned on restart")
    void testInterruptedCompactionRecovery(@TempDir Path tempDir) throws IOException {
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .maxSegmentSizeBytes(100)
                .build();

        try (ChronoCask store = ChronoCask.open(config)) {
            store.put("k1", "v1");
            store.put("k2", "v2");
            store.put("k3", "v3");
        }

        // Simulate crash during compaction: an orphaned compacted segment exists on disk
        // but was never published to the MANIFEST
        Path orphanSegment = tempDir.resolve("segment-000099.log");
        Files.writeString(orphanSegment, "abandoned-compacted-data");
        assertTrue(Files.exists(orphanSegment));

        // Reopen database: manifest is authoritative, orphan segment must be cleaned up
        try (ChronoCask store = ChronoCask.open(config)) {
            assertEquals(3, store.size());
            assertEquals("v1", store.get("k1"));
            assertEquals("v2", store.get("k2"));
            assertEquals("v3", store.get("k3"));

            // Orphan file was cleaned up
            assertFalse(Files.exists(orphanSegment), "Orphan segment not in manifest should be cleaned on startup");
        }
    }
}
