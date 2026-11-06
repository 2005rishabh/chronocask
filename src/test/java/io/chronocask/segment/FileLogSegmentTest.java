package io.chronocask.segment;

import io.chronocask.exception.CorruptRecordException;
import io.chronocask.record.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class FileLogSegmentTest {

    @Test
    @DisplayName("Create active segment, append records, and verify offsets and sizes")
    void testAppendAndReadRecord(@TempDir Path tempDir) throws IOException {
        Path segmentPath = tempDir.resolve("segment-000001.log");
        try (FileLogSegment segment = FileLogSegment.createActive(segmentPath, 1)) {
            assertEquals(1, segment.segmentId());
            assertEquals(0, segment.size());
            assertFalse(segment.isSealed());

            Record r1 = Record.put(1L, "user:1", "Alice");
            Record r2 = Record.put(2L, "user:2", "Bob");
            Record r3 = Record.delete(3L, "user:1");

            RecordLocation loc1 = segment.append(r1, DurabilityMode.ASYNC);
            assertEquals(0, loc1.offset());
            assertEquals(r1.totalSize(), loc1.recordSize());
            assertEquals(1, loc1.segmentId());
            assertEquals(r1.totalSize(), segment.size());

            RecordLocation loc2 = segment.append(r2, DurabilityMode.SYNC);
            assertEquals(r1.totalSize(), loc2.offset());
            assertEquals(r2.totalSize(), loc2.recordSize());

            RecordLocation loc3 = segment.append(r3, DurabilityMode.ASYNC);
            assertEquals(r1.totalSize() + r2.totalSize(), loc3.offset());

            // Read records back using locations
            Record read1 = segment.read(loc1);
            assertEquals(r1, read1);

            Record read2 = segment.read(loc2);
            assertEquals(r2, read2);

            Record read3 = segment.read(loc3);
            assertEquals(r3, read3);
            assertTrue(read3.isTombstone());
        }
    }

    @Test
    @DisplayName("Sealing segment prevents further appends but allows reads")
    void testSealSegment(@TempDir Path tempDir) throws IOException {
        Path segmentPath = tempDir.resolve("segment-000001.log");
        try (FileLogSegment segment = FileLogSegment.createActive(segmentPath, 1)) {
            Record r1 = Record.put(1L, "key", "val");
            RecordLocation loc = segment.append(r1);

            segment.seal();
            assertTrue(segment.isSealed());

            // Appends should fail
            assertThrows(IllegalStateException.class, () -> segment.append(Record.put(2L, "key2", "val2")));

            // Reads should succeed
            Record read = segment.read(loc);
            assertEquals(r1, read);
        }
    }

    @Test
    @DisplayName("Iterate sequentially across records in segment")
    void testIterator(@TempDir Path tempDir) throws IOException {
        Path segmentPath = tempDir.resolve("segment-000001.log");
        List<Record> originalRecords = List.of(
                Record.put(1L, "key1", "val1"),
                Record.put(2L, "key2", "val2"),
                Record.delete(3L, "key1"),
                Record.put(4L, "key3", "val3")
        );

        try (FileLogSegment segment = FileLogSegment.createActive(segmentPath, 1)) {
            for (Record r : originalRecords) {
                segment.append(r);
            }

            List<Record> scannedRecords = new ArrayList<>();
            List<RecordLocation> scannedLocations = new ArrayList<>();

            segment.scan(located -> {
                scannedRecords.add(located.record());
                scannedLocations.add(located.location());
            });

            assertEquals(originalRecords.size(), scannedRecords.size());
            for (int i = 0; i < originalRecords.size(); i++) {
                assertEquals(originalRecords.get(i), scannedRecords.get(i));
                assertEquals(1, scannedLocations.get(i).segmentId());
            }

            // Verify offsets line up sequentially
            long expectedOffset = 0;
            for (int i = 0; i < originalRecords.size(); i++) {
                assertEquals(expectedOffset, scannedLocations.get(i).offset());
                expectedOffset += originalRecords.get(i).totalSize();
            }
        }
    }

    @Test
    @DisplayName("Truncate segment to discard corrupted or partial tail")
    void testTruncateSegment(@TempDir Path tempDir) throws IOException {
        Path segmentPath = tempDir.resolve("segment-000001.log");
        try (FileLogSegment segment = FileLogSegment.createActive(segmentPath, 1)) {
            Record r1 = Record.put(1L, "k1", "v1");
            Record r2 = Record.put(2L, "k2", "v2");
            RecordLocation loc1 = segment.append(r1);
            RecordLocation loc2 = segment.append(r2);

            assertEquals(loc1.recordSize() + loc2.recordSize(), segment.size());

            // Truncate back to the end of record 1
            segment.truncate(loc1.recordSize());
            assertEquals(loc1.recordSize(), segment.size());

            // Appending a new record should take offset of loc2
            Record r3 = Record.put(3L, "k3", "v3");
            RecordLocation loc3 = segment.append(r3);
            assertEquals(loc1.recordSize(), loc3.offset());

            assertEquals(r1, segment.read(loc1));
            assertEquals(r3, segment.read(loc3));
        }
    }

    @Test
    @DisplayName("Detect corrupted record during read")
    void testCorruptedRecordRead(@TempDir Path tempDir) throws IOException {
        Path segmentPath = tempDir.resolve("segment-000001.log");
        RecordLocation loc;
        try (FileLogSegment segment = FileLogSegment.createActive(segmentPath, 1)) {
            Record r = Record.put(1L, "corrupt-me", "some-data");
            loc = segment.append(r);
        }

        // Corrupt a byte in the middle of the file
        try (FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(1);
            channel.read(buf, loc.offset() + 30);
            buf.flip();
            byte b = (byte) (buf.get(0) ^ 0xFF);
            buf.clear();
            buf.put(b);
            buf.flip();
            channel.write(buf, loc.offset() + 30);
        }

        try (FileLogSegment segment = FileLogSegment.openExisting(segmentPath, 1, false)) {
            assertThrows(CorruptRecordException.class, () -> segment.read(loc));
        }
    }

    @Test
    @DisplayName("Concurrent reads from multiple threads without interference")
    void testConcurrentReads(@TempDir Path tempDir) throws Exception {
        Path segmentPath = tempDir.resolve("segment-000001.log");
        int count = 100;
        List<Record> records = new ArrayList<>();
        List<RecordLocation> locations = new ArrayList<>();

        try (FileLogSegment segment = FileLogSegment.createActive(segmentPath, 1)) {
            for (int i = 0; i < count; i++) {
                Record r = Record.put(i, "key:" + i, "value:" + i);
                records.add(r);
                locations.add(segment.append(r));
            }

            int threads = 8;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<Boolean>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    // Each thread reads random records repeatedly
                    for (int i = 0; i < 200; i++) {
                        int index = (threadId * 17 + i) % count;
                        Record read = segment.read(locations.get(index));
                        if (!read.equals(records.get(index))) {
                            return false;
                        }
                    }
                    return true;
                }));
            }

            startLatch.countDown();
            for (Future<Boolean> f : futures) {
                assertTrue(f.get(5, TimeUnit.SECONDS));
            }
            executor.shutdown();
        }
    }
}
