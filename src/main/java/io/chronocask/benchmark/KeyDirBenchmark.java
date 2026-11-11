package io.chronocask.benchmark;

import io.chronocask.index.HeapKeyDir;
import io.chronocask.index.IndexEntry;
import io.chronocask.index.KeyDir;
import io.chronocask.index.OffHeapKeyDir;
import org.openjdk.jmh.annotations.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark comparing HeapKeyDir (ConcurrentHashMap) vs OffHeapKeyDir (FFM Arena).
 * Evaluates:
 * - Put throughput (inserting unique and overwritten keys)
 * - Get throughput (cache hits and misses)
 * - Memory & allocation characteristics
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class KeyDirBenchmark {

    @Param({"HEAP", "OFF_HEAP"})
    private String indexType;

    @Param({"10000"})
    private int keyCount;

    private KeyDir keyDir;
    private byte[][] pregeneratedKeys;
    private IndexEntry[] pregeneratedEntries;

    @Setup(Level.Trial)
    public void setupTrial() {
        pregeneratedKeys = new byte[keyCount][];
        pregeneratedEntries = new IndexEntry[keyCount];

        for (int i = 0; i < keyCount; i++) {
            pregeneratedKeys[i] = ("benchmark:key:namespace:" + i).getBytes(StandardCharsets.UTF_8);
            pregeneratedEntries[i] = new IndexEntry(1, i * 64L, 64, 16, i + 1);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        if ("OFF_HEAP".equals(indexType)) {
            keyDir = new OffHeapKeyDir(keyCount * 2, 4 * 1024 * 1024L);
        } else {
            keyDir = new HeapKeyDir(keyCount * 2);
        }

        // Prepopulate with keys for get benchmark
        for (int i = 0; i < keyCount; i++) {
            keyDir.put(pregeneratedKeys[i], pregeneratedEntries[i]);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        if (keyDir != null) {
            keyDir.close();
        }
    }

    @Benchmark
    public IndexEntry benchmarkGet() {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        return keyDir.get(pregeneratedKeys[idx]);
    }

    @Benchmark
    public IndexEntry benchmarkPutOverwrite() {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        return keyDir.put(pregeneratedKeys[idx], pregeneratedEntries[idx]);
    }
}
