package io.chronocask.recovery;

import static org.junit.jupiter.api.Assertions.*;

import io.chronocask.exception.CorruptRecordException;
import io.chronocask.index.HeapKeyDir;
import io.chronocask.index.IndexEntry;
import io.chronocask.index.KeyDir;
import io.chronocask.index.OffHeapKeyDir;
import io.chronocask.record.Record;
import io.chronocask.segment.DurabilityMode;
import io.chronocask.segment.RecordLocation;
import io.chronocask.segment.SegmentManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryManagerTest {

    @Test
    @DisplayName(
        "Recover multiple segments into KeyDir with PUTs, overwrites, and DELETE tombstones"
    )
    void testStandardRecoveryAcrossSegments(@TempDir Path tempDir)
        throws IOException {
        // Record sizes: ~34 to 41 bytes each.
        // With max size = 90 bytes:
        // seg 1: r1 (39) + r2 (37) = 76 bytes. Adding r3 (41) would exceed 90 -> rotates to seg 2
        // seg 2: r3 (41) + r4 (38) = 79 bytes. Adding r5 (39) would exceed 90 -> rotates to seg 3
        // seg 3: r5 (39) + r6 (34) = 73 bytes. Total = 3 segments.
        long maxSegmentSize = 90;
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                maxSegmentSize,
                DurabilityMode.ASYNC
            )
        ) {
            manager.append(Record.put(1L, "user:1", "Alice")); // seg 1
            manager.append(Record.put(2L, "user:2", "Bob")); // seg 1
            manager.append(Record.put(3L, "user:3", "Charlie")); // seg 2 (rotation)
            manager.append(Record.put(4L, "user:1", "Alex")); // seg 2 (overwrite)
            manager.append(Record.put(5L, "user:4", "David")); // seg 3 (rotation)
            manager.append(Record.delete(6L, "user:2")); // seg 3 (tombstone)
        }

        // Step 2: Open fresh SegmentManager and recover into HeapKeyDir
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                maxSegmentSize,
                DurabilityMode.ASYNC
            );
            KeyDir heapIndex = new HeapKeyDir()
        ) {
            RecoveryManager recoveryManager = new RecoveryManager(manager);
            RecoveryResult result = recoveryManager.recover(heapIndex);

            assertEquals(3, result.segmentsScanned());
            assertEquals(6, result.validRecordsReplayed());
            assertEquals(5, result.putRecords());
            assertEquals(1, result.deleteRecords());
            assertEquals(0, result.corruptedTailTruncatedBytes());
            assertEquals(6L, result.highestSequenceNumber());

            // Check active keys in index:
            // "user:1" -> updated to Alex (seq 4)
            IndexEntry e1 = heapIndex.get("user:1");
            assertNotNull(e1);
            assertEquals(4L, e1.sequenceNumber());
            Record r1 = manager.read(e1.location());
            assertEquals("Alex", r1.valueAsString());

            // "user:2" -> deleted
            assertNull(heapIndex.get("user:2"));

            // "user:3" -> Charlie (seq 3)
            IndexEntry e3 = heapIndex.get("user:3");
            assertNotNull(e3);
            assertEquals(3L, e3.sequenceNumber());
            assertEquals(
                "Charlie",
                manager.read(e3.location()).valueAsString()
            );

            // "user:4" -> David (seq 5)
            IndexEntry e4 = heapIndex.get("user:4");
            assertNotNull(e4);
            assertEquals(5L, e4.sequenceNumber());
            assertEquals("David", manager.read(e4.location()).valueAsString());

            assertEquals(3, heapIndex.size());
        }

        // Step 3: Verify identical recovery into OffHeapKeyDir
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                maxSegmentSize,
                DurabilityMode.ASYNC
            );
            KeyDir offHeapIndex = new OffHeapKeyDir(16)
        ) {
            RecoveryManager recoveryManager = new RecoveryManager(manager);
            RecoveryResult result = recoveryManager.recover(offHeapIndex);

            assertEquals(3, result.segmentsScanned());
            assertEquals(3, offHeapIndex.size());
            assertEquals(4L, offHeapIndex.get("user:1").sequenceNumber());
            assertNull(offHeapIndex.get("user:2"));
            assertEquals(3L, offHeapIndex.get("user:3").sequenceNumber());
            assertEquals(5L, offHeapIndex.get("user:4").sequenceNumber());
        }
    }

    @Test
    @DisplayName(
        "Recover and safely truncate partial write / crash tail in active segment"
    )
    void testRecoverTruncatedActiveSegmentTail(@TempDir Path tempDir)
        throws IOException {
        Path activeFile;
        long validSegmentSize;

        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                1024,
                DurabilityMode.ASYNC
            )
        ) {
            Record r1 = Record.put(1L, "k1", "v1");
            Record r2 = Record.put(2L, "k2", "v2");
            manager.append(r1);
            RecordLocation loc2 = manager.append(r2);
            activeFile = manager.getActiveSegment().path();
            validSegmentSize = loc2.offset() + loc2.recordSize();
        }

        // Simulate crash by writing a partial 10-byte incomplete record at the file tail
        try (
            FileChannel channel = FileChannel.open(
                activeFile,
                StandardOpenOption.WRITE
            )
        ) {
            channel.position(validSegmentSize);
            ByteBuffer garbage = ByteBuffer.wrap(new byte[] {
                0x43,
                0x41,
                0x53,
                0x4B,
                1,
                1,
                0,
                0,
                0,
                99,
            });
            channel.write(garbage);
        }

        // Reopen and run recovery
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                1024,
                DurabilityMode.ASYNC
            );
            KeyDir keyDir = new HeapKeyDir()
        ) {
            RecoveryManager recoveryManager = new RecoveryManager(manager);
            RecoveryResult result = recoveryManager.recover(keyDir);

            assertEquals(2, result.validRecordsReplayed());
            assertEquals(
                10,
                result.corruptedTailTruncatedBytes(),
                "Should truncate the 10 trailing uncommitted bytes"
            );
            assertEquals(2L, result.highestSequenceNumber());
            assertEquals(2, keyDir.size());

            // Active segment size must now match validSegmentSize exactly
            assertEquals(validSegmentSize, manager.getActiveSegment().size());

            // A new write should append cleanly at the truncated tail
            Record r3 = Record.put(3L, "k3", "v3");
            RecordLocation loc3 = manager.append(r3);
            assertEquals(validSegmentSize, loc3.offset());
            assertEquals(r3, manager.read(loc3));
        }
    }

    @Test
    @DisplayName(
        "Fail recovery when corruption occurs in the middle of a segment"
    )
    void testMiddleCorruptionThrowsException(@TempDir Path tempDir)
        throws IOException {
        Path segmentPath;
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                1024,
                DurabilityMode.ASYNC
            )
        ) {
            RecordLocation loc1 = manager.append(
                Record.put(1L, "key:1", "value:1")
            );
            RecordLocation loc2 = manager.append(
                Record.put(2L, "key:2", "value:2")
            );
            RecordLocation loc3 = manager.append(
                Record.put(3L, "key:3", "value:3")
            );
            segmentPath = manager.getActiveSegment().path();

            // Corrupt the middle record (loc2) by flipping bytes in value payload
            try (
                FileChannel channel = FileChannel.open(
                    segmentPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
                )
            ) {
                ByteBuffer b = ByteBuffer.allocate(1);
                channel.read(b, loc2.offset() + 32);
                b.flip();
                byte flipped = (byte) (b.get(0) ^ 0xFF);
                b.clear();
                b.put(flipped);
                b.flip();
                channel.write(b, loc2.offset() + 32);
            }
        }

        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                1024,
                DurabilityMode.ASYNC
            );
            KeyDir keyDir = new HeapKeyDir()
        ) {
            RecoveryManager recoveryManager = new RecoveryManager(manager);
            CorruptRecordException ex = assertThrows(
                CorruptRecordException.class,
                () -> recoveryManager.recover(keyDir)
            );
            assertTrue(ex.getMessage().contains("Corrupt record"));
        }
    }

    @Test
    @DisplayName(
        "Fail recovery when corruption occurs in an immutable sealed segment"
    )
    void testImmutableSegmentCorruptionFails(@TempDir Path tempDir)
        throws IOException {
        Path sealedSegmentPath;
        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                60,
                DurabilityMode.ASYNC
            )
        ) {
            manager.append(Record.put(1L, "k1", "v1"));
            manager.append(Record.put(2L, "k2", "v2")); // Rotates
            sealedSegmentPath = tempDir.resolve("segment-000001.log");

            // Corrupt a byte in segment 1
            try (
                FileChannel channel = FileChannel.open(
                    sealedSegmentPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
                )
            ) {
                ByteBuffer b = ByteBuffer.allocate(1);
                channel.read(b, 10);
                b.flip();
                b.put(0, (byte) (b.get(0) ^ 0x01));
                channel.write(b, 10);
            }
        }

        try (
            SegmentManager manager = SegmentManager.open(
                tempDir,
                60,
                DurabilityMode.ASYNC
            );
            KeyDir keyDir = new HeapKeyDir()
        ) {
            RecoveryManager recoveryManager = new RecoveryManager(manager);
            assertThrows(CorruptRecordException.class, () ->
                recoveryManager.recover(keyDir)
            );
        }
    }
}
