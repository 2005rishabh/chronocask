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

class HeapKeyDirTest {

    @Test
    @DisplayName("Basic put, get, and overwrite")
    void testBasicPutGetOverwrite() {
        try (KeyDir keyDir = new HeapKeyDir()) {
            assertFalse(keyDir.isOffHeap());
            assertEquals(0, keyDir.size());
            assertTrue(keyDir.isEmpty());

            IndexEntry e1 = new IndexEntry(1, 0L, 43, 7, 100L);
            assertNull(keyDir.put("user:1", e1));
            assertEquals(1, keyDir.size());
            assertFalse(keyDir.isEmpty());

            IndexEntry fetched = keyDir.get("user:1");
            assertEquals(e1, fetched);

            // Overwrite
            IndexEntry e2 = new IndexEntry(2, 1024L, 50, 14, 105L);
            IndexEntry prev = keyDir.put("user:1", e2);
            assertEquals(e1, prev);
            assertEquals(e2, keyDir.get("user:1"));
            assertEquals(1, keyDir.size());
        }
    }

    @Test
    @DisplayName("Remove entry")
    void testRemove() {
        try (KeyDir keyDir = new HeapKeyDir()) {
            IndexEntry e1 = new IndexEntry(1, 100L, 30, 2, 1L);
            keyDir.put("item:1", e1);

            assertTrue(keyDir.containsKey("item:1"));
            IndexEntry removed = keyDir.remove("item:1");
            assertEquals(e1, removed);
            assertFalse(keyDir.containsKey("item:1"));
            assertNull(keyDir.get("item:1"));
            assertEquals(0, keyDir.size());

            // Second remove returns null
            assertNull(keyDir.remove("item:1"));
        }
    }

    @Test
    @DisplayName("forEach iteration across all entries")
    void testForEach() {
        try (KeyDir keyDir = new HeapKeyDir()) {
            Map<String, IndexEntry> expected = new HashMap<>();
            for (int i = 0; i < 50; i++) {
                String k = "key:" + i;
                IndexEntry e = new IndexEntry(1, i * 40L, 40, 10, i + 1);
                keyDir.put(k, e);
                expected.put(k, e);
            }

            assertEquals(50, keyDir.size());

            Map<String, IndexEntry> actual = new HashMap<>();
            keyDir.forEach((k, v) -> actual.put(new String(k), v));

            assertEquals(expected, actual);
        }
    }

    @Test
    @DisplayName("Concurrent writes and reads")
    void testConcurrentAccess() throws Exception {
        try (KeyDir keyDir = new HeapKeyDir()) {
            int threadCount = 8;
            int opsPerThread = 1000;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(1);

            Future<?>[] futures = new Future<?>[threadCount];
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                futures[t] = executor.submit(() -> {
                    try {
                        latch.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String k = "th-" + threadId + "-key-" + i;
                            IndexEntry entry = new IndexEntry(threadId, i * 32L, 32, 4, i);
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
}
