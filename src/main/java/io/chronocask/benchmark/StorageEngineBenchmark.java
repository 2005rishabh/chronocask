package io.chronocask.benchmark;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.api.WriteOptions;
import io.chronocask.segment.DurabilityMode;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark evaluating end-to-end ChronoCask engine throughput:
 * - Sequential write throughput in ASYNC vs SYNC durability modes
 * - Read throughput with O(1) KeyDir lookups
 * - Off-Heap vs Heap indexing under storage I/O
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class StorageEngineBenchmark {

    @Param({"OFF_HEAP", "HEAP"})
    private String indexType;

    @Param({"ASYNC"})
    private String durability;

    private Path tempDir;
    private ChronoCask store;
    private final int keyRange = 10000;
    private final String[] pregeneratedKeys = new String[keyRange];
    private final String[] pregeneratedValues = new String[keyRange];

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        tempDir = Files.createTempDirectory("chronocask-jmh-");

        boolean offHeap = "OFF_HEAP".equals(indexType);
        DurabilityMode dur = "SYNC".equals(durability) ? DurabilityMode.SYNC : DurabilityMode.ASYNC;

        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .offHeapIndex(offHeap)
                .defaultDurability(dur)
                .maxSegmentSizeBytes(64 * 1024 * 1024L)
                .build();

        store = ChronoCask.open(config);

        for (int i = 0; i < keyRange; i++) {
            pregeneratedKeys[i] = "bench:user:" + i;
            pregeneratedValues[i] = "payload-data-value-content-for-benchmark-" + i;
            store.put(pregeneratedKeys[i], pregeneratedValues[i]);
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        if (store != null) {
            store.close();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    @Benchmark
    public String benchmarkGet() {
        int idx = ThreadLocalRandom.current().nextInt(keyRange);
        return store.get(pregeneratedKeys[idx]);
    }

    @Benchmark
    public void benchmarkPut() {
        int idx = ThreadLocalRandom.current().nextInt(keyRange);
        store.put(pregeneratedKeys[idx], pregeneratedValues[idx]);
    }
}
