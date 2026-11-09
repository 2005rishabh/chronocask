package io.chronocask.index;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class KeyDirContractTest {

    static Stream<KeyDir> keyDirProvider() {
        return Stream.of(
                new HeapKeyDir(),
                new OffHeapKeyDir(32)
        );
    }

    @ParameterizedTest
    @MethodSource("keyDirProvider")
    @DisplayName("Verify identical put, get, update, and remove semantics across implementations")
    void testEquivalence(KeyDir keyDir) {
        try (keyDir) {
            assertEquals(0, keyDir.size());

            Map<String, IndexEntry> reference = new HashMap<>();

            // 1. Insert 100 entries
            for (int i = 0; i < 100; i++) {
                String k = "user:session:" + i;
                IndexEntry entry = new IndexEntry(1, i * 100L, 100, 25, i + 1);
                assertNull(keyDir.put(k, entry));
                reference.put(k, entry);
            }

            assertEquals(100, keyDir.size());

            // 2. Lookup all
            for (Map.Entry<String, IndexEntry> e : reference.entrySet()) {
                assertEquals(e.getValue(), keyDir.get(e.getKey()));
            }

            // 3. Overwrite even-numbered keys
            for (int i = 0; i < 100; i += 2) {
                String k = "user:session:" + i;
                IndexEntry oldEntry = reference.get(k);
                IndexEntry newEntry = new IndexEntry(2, i * 200L, 120, 30, i + 1000);
                IndexEntry prev = keyDir.put(k, newEntry);
                assertEquals(oldEntry, prev);
                reference.put(k, newEntry);
            }

            assertEquals(100, keyDir.size());

            // 4. Verify updated lookups
            for (Map.Entry<String, IndexEntry> e : reference.entrySet()) {
                assertEquals(e.getValue(), keyDir.get(e.getKey()));
            }

            // 5. Remove first 25 keys
            for (int i = 0; i < 25; i++) {
                String k = "user:session:" + i;
                IndexEntry oldEntry = reference.remove(k);
                IndexEntry removed = keyDir.remove(k);
                assertEquals(oldEntry, removed);
                assertNull(keyDir.get(k));
            }

            assertEquals(75, keyDir.size());

            // 6. forEach equivalence
            Map<String, IndexEntry> scanned = new HashMap<>();
            keyDir.forEach((k, v) -> scanned.put(new String(k), v));
            assertEquals(reference, scanned);
        }
    }
}
