package io.chronocask.segment;

import static org.junit.jupiter.api.Assertions.*;

import io.chronocask.record.Record;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentManagerTest {

    @Test
    @DisplayName(
        "Initializes empty directory with segment-000001.log as active"
    )
    void testInitialCreation(@TempDir Path tempDir) throws IOException {
        try (SegmentManager manager = SegmentManager.open(tempDir)) {
            LogSegment active = manager.getActiveSegment();
            assertNotNull(active);
            assertEquals(1, active.segmentId());
            assertTrue(manager.getImmutableSegments().isEmpty());
            assertTrue(Files.exists(tempDir.resolve("segment-000001.log")));
        }
    }

    @Test
    @DisplayName(
        "Rotates active segment when max segment size threshold is exceeded"
    )
    void testSegmentRotation(@TempDir Path tempDir) throws IOException {
        // Record size for "k" -> "v" is 28 (header) + 1 + 1 = 30 bytes
        // With max size 70 bytes, 2 records fit (60 bytes), 3rd record triggers rotation
        long maxSegmentSize = 70;
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                maxSegmentSize,
                DurabilityMode.ASYNC
            )
        ) {
            Record r1 = Record.put(1L, "k1", "v1");
            Record r2 = Record.put(2L, "k2", "v2");
            Record r3 = Record.put(3L, "k3", "v3");

            RecordLocation loc1 = manager.append(r1);
            RecordLocation loc2 = manager.append(r2);
            assertEquals(1, loc1.segmentId());
            assertEquals(1, loc2.segmentId());

            // 3rd record exceeds 70 bytes, triggers rotation to segment 2
            RecordLocation loc3 = manager.append(r3);
            assertEquals(2, loc3.segmentId());
            assertEquals(0, loc3.offset());

            // Segment 1 should now be immutable
            assertEquals(1, manager.getImmutableSegments().size());
            assertEquals(1, manager.getImmutableSegments().get(0).segmentId());
            assertTrue(manager.getImmutableSegments().get(0).isSealed());

            // Segment 2 should be active
            assertEquals(2, manager.getActiveSegment().segmentId());
            assertFalse(manager.getActiveSegment().isSealed());

            // Reading all records through manager should succeed seamlessly
            assertEquals(r1, manager.read(loc1));
            assertEquals(r2, manager.read(loc2));
            assertEquals(r3, manager.read(loc3));
        }
    }

    @Test
    @DisplayName(
        "Reopens existing directory and discovers immutable and active segments"
    )
    void testReopenExistingSegments(@TempDir Path tempDir) throws IOException {
        // Record size: 28 (header) + 2 (key) + 2 (value) = 32 bytes
        Record r1 = Record.put(1L, "k1", "v1");
        Record r2 = Record.put(2L, "k2", "v2");
        RecordLocation loc1;
        RecordLocation loc2;

        // Step 1: Open with maxSegmentSize = 50 so r2 (32+32=64 > 50) rotates to segment 2
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                50,
                DurabilityMode.ASYNC
            )
        ) {
            loc1 = manager.append(r1);
            loc2 = manager.append(r2);
            assertEquals(1, loc1.segmentId());
            assertEquals(2, loc2.segmentId());
        }

        // Step 2: Reopen from disk with maxSegmentSize = 100 so segment 2 can hold r2 (32) + r3 (32) = 64 <= 100
        try (
            SegmentManager reopened = SegmentManager.open(
                tempDir,
                100,
                DurabilityMode.ASYNC
            )
        ) {
            assertEquals(1, reopened.getImmutableSegments().size());
            assertEquals(1, reopened.getImmutableSegments().get(0).segmentId());
            assertTrue(reopened.getImmutableSegments().get(0).isSealed());

            assertEquals(2, reopened.getActiveSegment().segmentId());
            assertFalse(reopened.getActiveSegment().isSealed());

            // Verify reads across both segments
            assertEquals(r1, reopened.read(loc1));
            assertEquals(r2, reopened.read(loc2));

            // Append new record into reopened active segment (32 + 32 = 64 <= 100 bytes)
            Record r3 = Record.put(3L, "k3", "v3");
            RecordLocation loc3 = reopened.append(r3);
            assertEquals(2, loc3.segmentId());
            assertEquals(r3, reopened.read(loc3));
        }
    }

    @Test
    @DisplayName(
        "Deletes immutable segment during compaction and prevents deleting active segment"
    )
    void testDeleteSegment(@TempDir Path tempDir) throws IOException {
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                50,
                DurabilityMode.ASYNC
            )
        ) {
            Record r1 = Record.put(1L, "k1", "v1");
            Record r2 = Record.put(2L, "k2", "v2");
            RecordLocation loc1 = manager.append(r1);
            RecordLocation loc2 = manager.append(r2); // Triggers rotation

            assertEquals(1, loc1.segmentId());
            assertEquals(2, loc2.segmentId());

            // Attempting to delete active segment (2) must fail
            assertThrows(IllegalStateException.class, () ->
                manager.deleteSegment(2)
            );

            // Deleting immutable segment (1) succeeds
            manager.deleteSegment(1);
            assertTrue(manager.getImmutableSegments().isEmpty());
            assertFalse(Files.exists(tempDir.resolve("segment-000001.log")));

            // Active segment remains intact and writable
            Record r3 = Record.put(3L, "k3", "v3");
            RecordLocation loc3 = manager.append(r3);
            assertEquals(r3, manager.read(loc3));
        }
    }

    @Test
    @DisplayName(
        "Allows appending a record larger than maxSegmentSizeBytes when active segment is empty"
    )
    void testRecordLargerThanMaxSegmentSize(@TempDir Path tempDir)
        throws IOException {
        // Max segment size is small (50 bytes), but record is 100 bytes
        byte[] largeValue = new byte[70];
        Record largeRecord = Record.put(1L, "key", new String(largeValue));
        assertTrue(largeRecord.totalSize() > 50);

        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                50,
                DurabilityMode.ASYNC
            )
        ) {
            RecordLocation loc = manager.append(largeRecord);
            assertEquals(1, loc.segmentId());
            assertEquals(largeRecord, manager.read(loc));

            // Subsequent append should immediately rotate since segment 1 is now over 50 bytes
            Record next = Record.put(2L, "k", "v");
            RecordLocation loc2 = manager.append(next);
            assertEquals(2, loc2.segmentId());
            assertEquals(next, manager.read(loc2));
        }
    }
}
