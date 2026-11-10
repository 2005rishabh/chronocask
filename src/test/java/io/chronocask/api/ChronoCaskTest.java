package io.chronocask.api;

import io.chronocask.recovery.RecoveryResult;
import io.chronocask.segment.DurabilityMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ChronoCaskTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Basic CRUD operations and size tracking")
    void testBasicCrud(boolean offHeap, @TempDir Path tempDir) {
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .offHeapIndex(offHeap)
                .build();

        try (ChronoCask store = ChronoCask.open(config)) {
            assertEquals(offHeap, store.isOffHeap());
            assertEquals(0, store.size());
            assertTrue(store.isEmpty());

            // PUT
            store.put("user:101", "Rishabh");
            store.put("user:102", "Alice");
            assertEquals(2, store.size());
            assertFalse(store.isEmpty());

            // GET
            assertEquals("Rishabh", store.get("user:101"));
            assertEquals("Alice", store.get("user:102"));
            assertNull(store.get("user:999"));
            assertTrue(store.find("user:101").isPresent());
            assertFalse(store.find("user:999").isPresent());

            // CONTAINS
            assertTrue(store.containsKey("user:101"));
            assertTrue(store.containsKey("user:102"));
            assertFalse(store.containsKey("user:999"));

            // UPDATE / OVERWRITE
            store.put("user:101", "Rishabh Anand");
            assertEquals(2, store.size());
            assertEquals("Rishabh Anand", store.get("user:101"));

            // DELETE
            assertTrue(store.delete("user:102"));
            assertNull(store.get("user:102"));
            assertFalse(store.containsKey("user:102"));
            assertEquals(1, store.size());

            // DELETE non-existent key returns false
            assertFalse(store.delete("user:102"));
            assertFalse(store.delete("non-existent"));
        }
    }

    @Test
    @DisplayName("Persistence across restarts and recovery metrics")
    void testPersistenceAcrossRestarts(@TempDir Path tempDir) {
        // Step 1: Populate initial store and close
        try (ChronoCask store = ChronoCask.open(tempDir)) {
            store.put("k1", "v1");
            store.put("k2", "v2");
            store.put("k3", "v3");
            store.put("k1", "v1-updated");
            store.delete("k2");
            assertEquals(2, store.size());
        }

        // Step 2: Reopen and verify data recovered from disk segments
        try (ChronoCask store = ChronoCask.open(tempDir)) {
            assertEquals(2, store.size());
            assertEquals("v1-updated", store.get("k1"));
            assertNull(store.get("k2"));
            assertEquals("v3", store.get("k3"));

            RecoveryResult recovery = store.getRecoveryResult();
            assertNotNull(recovery);
            assertEquals(5, recovery.validRecordsReplayed());
            assertEquals(4, recovery.putRecords());
            assertEquals(1, recovery.deleteRecords());
            assertEquals(0, recovery.corruptedTailTruncatedBytes());
            assertTrue(recovery.highestSequenceNumber() >= 5);

            // Append new writes in reopened store
            store.put("k4", "v4");
            assertEquals(3, store.size());
            assertEquals("v4", store.get("k4"));
        }

        // Step 3: Reopen second time
        try (ChronoCask store = ChronoCask.open(tempDir)) {
            assertEquals(3, store.size());
            assertEquals("v1-updated", store.get("k1"));
            assertEquals("v3", store.get("k3"));
            assertEquals("v4", store.get("k4"));
        }
    }

    @Test
    @DisplayName("Segment rotation and reads across multiple log segments")
    void testSegmentRotationThroughEngine(@TempDir Path tempDir) {
        // Set small segment threshold (120 bytes) to force frequent rotations
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .maxSegmentSizeBytes(120)
                .offHeapIndex(true)
                .build();

        int recordCount = 40;
        try (ChronoCask store = ChronoCask.open(config)) {
            for (int i = 0; i < recordCount; i++) {
                store.put("account:" + i, "balance:" + (i * 100));
            }
            assertEquals(recordCount, store.size());

            // Segments should have rotated multiple times
            int totalSegments = store.getEngine().getSegmentManager().getAllSegments().size();
            assertTrue(totalSegments > 3, "Expected multiple segments after rotation, got: " + totalSegments);

            // Verify all keys can still be read
            for (int i = 0; i < recordCount; i++) {
                assertEquals("balance:" + (i * 100), store.get("account:" + i));
            }
        }

        // Reopen and verify complete consistency after multi-segment recovery
        try (ChronoCask store = ChronoCask.open(config)) {
            assertEquals(recordCount, store.size());
            for (int i = 0; i < recordCount; i++) {
                assertEquals("balance:" + (i * 100), store.get("account:" + i));
            }
        }
    }

    @Test
    @DisplayName("Write options: SYNC durability forces disk flush")
    void testWriteOptionsSync(@TempDir Path tempDir) {
        try (ChronoCask store = ChronoCask.open(tempDir)) {
            store.put("sync-key", "sync-value", WriteOptions.sync());
            assertEquals("sync-value", store.get("sync-key"));

            store.delete("sync-key", WriteOptions.sync());
            assertNull(store.get("sync-key"));
        }
    }

    @Test
    @DisplayName("forEach iterates all live records")
    void testForEachIteration(@TempDir Path tempDir) {
        try (ChronoCask store = ChronoCask.open(tempDir)) {
            Map<String, String> expected = new HashMap<>();
            for (int i = 0; i < 20; i++) {
                String k = "key-" + i;
                String v = "val-" + i;
                store.put(k, v);
                expected.put(k, v);
            }

            store.delete("key-5");
            expected.remove("key-5");

            Map<String, String> actual = new HashMap<>();
            store.forEach(actual::put);

            assertEquals(expected.size(), actual.size());
            assertEquals(expected, actual);
        }
    }

    @Test
    @DisplayName("Concurrent reads and writes to ChronoCask")
    void testConcurrentWorkload(@TempDir Path tempDir) throws Exception {
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .maxSegmentSizeBytes(1024)
                .offHeapIndex(true)
                .build();

        try (ChronoCask store = ChronoCask.open(config)) {
            int threads = 8;
            int opsPerThread = 200;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(1);

            Future<?>[] futures = new Future<?>[threads];
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures[t] = executor.submit(() -> {
                    try {
                        latch.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String k = "worker:" + threadId + ":k:" + i;
                            String v = "worker:" + threadId + ":v:" + i;
                            store.put(k, v);
                            assertEquals(v, store.get(k));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            latch.countDown();
            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
            executor.shutdown();

            assertEquals((long) threads * opsPerThread, store.size());
        }
    }
}
