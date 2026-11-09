package io.chronocask.index;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OffHeapKeyDirTest {

    @Test
    @DisplayName("Basic put, get, and overwrite in off-heap slots")
    void testBasicOffHeapOperations() {
        try (OffHeapKeyDir keyDir = new OffHeapKeyDir(16)) {
            assertTrue(keyDir.isOffHeap());
            assertEquals(0, keyDir.size());
            assertTrue(keyDir.isEmpty());

            IndexEntry e1 = new IndexEntry(1, 0L, 43, 7, 100L);
            assertNull(keyDir.put("user:101", e1));
            assertEquals(1, keyDir.size());
            assertFalse(keyDir.isEmpty());

            IndexEntry fetched = keyDir.get("user:101");
            assertNotNull(fetched);
            assertEquals(e1, fetched);

            // Overwrite existing key
            IndexEntry e2 = new IndexEntry(2, 2048L, 60, 24, 102L);
            IndexEntry prev = keyDir.put("user:101", e2);
            assertEquals(e1, prev);
            assertEquals(e2, keyDir.get("user:101"));
            assertEquals(1, keyDir.size());
        }
    }

    @Test
    @DisplayName("Remove entry uses tombstone and allows re-insertion")
    void testRemoveWithTombstone() {
        try (OffHeapKeyDir keyDir = new OffHeapKeyDir(16)) {
            IndexEntry e1 = new IndexEntry(1, 100L, 30, 2, 1L);
            keyDir.put("item:1", e1);

            assertTrue(keyDir.containsKey("item:1"));
            IndexEntry removed = keyDir.remove("item:1");
            assertEquals(e1, removed);
            assertFalse(keyDir.containsKey("item:1"));
            assertNull(keyDir.get("item:1"));
            assertEquals(0, keyDir.size());

            // Re-insert into tombstone slot
            IndexEntry e2 = new IndexEntry(1, 200L, 35, 7, 2L);
            assertNull(keyDir.put("item:1", e2));
            assertEquals(1, keyDir.size());
            assertEquals(e2, keyDir.get("item:1"));
        }
    }

    @Test
    @DisplayName("Automatic resize and rehash when exceeding load factor")
    void testResizeAndRehash() {
        int initialCap = 16;
        try (OffHeapKeyDir keyDir = new OffHeapKeyDir(initialCap)) {
            assertEquals(16, keyDir.capacity());

            int count = 500;
            Map<String, IndexEntry> expected = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String k = "account:key:" + i;
                IndexEntry e = new IndexEntry(i % 5, i * 64L, 64, 20, i + 1);
                expected.put(k, e);
                keyDir.put(k, e);
            }

            assertEquals(count, keyDir.size());
            assertTrue(keyDir.capacity() >= 512, "Capacity should have resized several times: " + keyDir.capacity());

            // Verify all keys remain accessible after multiple resizes
            for (Map.Entry<String, IndexEntry> entry : expected.entrySet()) {
                IndexEntry actual = keyDir.get(entry.getKey());
                assertNotNull(actual, "Missing key: " + entry.getKey());
                assertEquals(entry.getValue(), actual);
            }
        }
    }

    @Test
    @DisplayName("Dynamic key slab expansion for high key volume")
    void testKeySlabExpansion() {
        // Start with a very small 1 KB key slab to force multiple reallocations
        long smallSlab = 1024L;
        try (OffHeapKeyDir keyDir = new OffHeapKeyDir(64, smallSlab)) {
            int count = 200;
            for (int i = 0; i < count; i++) {
                String k = "very-long-prefix-for-testing-key-slab-expansion-" + i;
                IndexEntry e = new IndexEntry(1, i * 100L, 100, 30, i);
                keyDir.put(k, e);
            }

            assertEquals(count, keyDir.size());
            for (int i = 0; i < count; i++) {
                String k = "very-long-prefix-for-testing-key-slab-expansion-" + i;
                IndexEntry e = keyDir.get(k);
                assertNotNull(e);
                assertEquals(i * 100L, e.offset());
            }
        }
    }

    @Test
    @DisplayName("forEach scans all live entries in off-heap index")
    void testForEach() {
        try (OffHeapKeyDir keyDir = new OffHeapKeyDir(32)) {
            Map<String, IndexEntry> expected = new HashMap<>();
            for (int i = 0; i < 40; i++) {
                String k = "session:" + i;
                IndexEntry e = new IndexEntry(1, i * 50L, 50, 10, i);
                keyDir.put(k, e);
                expected.put(k, e);
            }

            // Remove a few keys to test tombstone skipping
            keyDir.remove("session:5");
            expected.remove("session:5");
            keyDir.remove("session:12");
            expected.remove("session:12");

            Map<String, IndexEntry> actual = new HashMap<>();
            keyDir.forEach((k, v) -> actual.put(new String(k), v));

            assertEquals(expected.size(), actual.size());
            assertEquals(expected, actual);
        }
    }

    @Test
    @DisplayName("Concurrent reads and writes to off-heap index")
    void testConcurrentAccess() throws Exception {
        try (OffHeapKeyDir keyDir = new OffHeapKeyDir(128)) {
            int threadCount = 8;
            int opsPerThread = 500;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(1);

            Future<?>[] futures = new Future<?>[threadCount];
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                futures[t] = executor.submit(() -> {
                    try {
                        latch.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String k = "worker-" + threadId + "-key-" + i;
                            IndexEntry entry = new IndexEntry(threadId, i * 48L, 48, 8, i);
                            keyDir.put(k, entry);
                            assertEquals(entry, keyDir.get(k));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            latch.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            executor.shutdown();

            assertEquals((long) threadCount * opsPerThread, keyDir.size());
        }
    }

    @Test
    @DisplayName("Closed off-heap index rejects operations and frees native memory")
    void testClose() {
        OffHeapKeyDir keyDir = new OffHeapKeyDir(16);
        keyDir.put("k", new IndexEntry(1, 0L, 30, 2, 1L));
        assertEquals(1, keyDir.size());

        keyDir.close();

        assertThrows(IllegalStateException.class, () -> keyDir.get("k"));
        assertThrows(IllegalStateException.class, () -> keyDir.put("k", new IndexEntry(1, 0L, 30, 2, 2L)));
        assertThrows(IllegalStateException.class, () -> keyDir.remove("k"));
        assertThrows(IllegalStateException.class, keyDir::size);
    }
}
