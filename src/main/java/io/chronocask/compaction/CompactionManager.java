package io.chronocask.compaction;

import io.chronocask.index.ByteArrayKey;
import io.chronocask.index.IndexEntry;
import io.chronocask.index.KeyDir;
import io.chronocask.segment.LogSegment;
import io.chronocask.segment.SegmentManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;

/**
 * Coordinates the full compaction protocol:
 * 1. Plans eligible contiguous immutable segments.
 * 2. Rewrites live records to new compacted segments and generates hint files.
 * 3. Under write lock, reconciles KeyDir and atomically replaces obsolete segments in the manifest.
 */
public class CompactionManager {

    private final SegmentManager segmentManager;
    private final CompactionPlanner planner;
    private final CompactionWorker worker;

    public CompactionManager(Path baseDir, SegmentManager segmentManager, long maxSegmentSizeBytes) {
        this(baseDir, segmentManager, maxSegmentSizeBytes, new CompactionPlanner());
    }

    public CompactionManager(
            Path baseDir,
            SegmentManager segmentManager,
            long maxSegmentSizeBytes,
            CompactionPlanner planner
    ) {
        this.segmentManager = Objects.requireNonNull(segmentManager, "segmentManager cannot be null");
        this.planner = Objects.requireNonNull(planner, "planner cannot be null");
        this.worker = new CompactionWorker(baseDir, segmentManager, maxSegmentSizeBytes);
    }

    /**
     * Executes a synchronous compaction cycle.
     *
     * @param keyDir    the active index
     * @param writeLock the engine write lock to hold during atomic publication
     * @return CompactionResult with summary statistics
     * @throws IOException on I/O or publication error
     */
    public CompactionResult compact(KeyDir keyDir, Lock writeLock) throws IOException {
        Objects.requireNonNull(keyDir, "keyDir cannot be null");
        Objects.requireNonNull(writeLock, "writeLock cannot be null");

        long startTime = System.currentTimeMillis();

        List<LogSegment> immutableSegments = segmentManager.getImmutableSegments();
        List<LogSegment> toCompact = planner.plan(immutableSegments);

        if (toCompact.isEmpty()) {
            return CompactionResult.empty();
        }

        // 1. Worker rewrites live records into temporary/new sealed segments
        CompactionWorker.ExecutionResult exec = worker.execute(toCompact, keyDir);

        // 2. Atomic publication under write lock
        writeLock.lock();
        try {
            // Reconcile KeyDir: only update keys that still point to their old pre-compaction location
            for (Map.Entry<ByteArrayKey, IndexEntry> entry : exec.updatedIndexEntries().entrySet()) {
                byte[] key = entry.getKey().getBytesUnsafe();
                IndexEntry current = keyDir.get(key);
                if (current != null && exec.obsoleteSegmentIds().contains(current.segmentId())) {
                    keyDir.put(key, entry.getValue());
                }
            }

            // Atomically publish updated manifest and swap segment registry
            segmentManager.replaceSegments(
                    exec.obsoleteSegmentIds(),
                    exec.replacementSegments(),
                    exec.replacementEntries()
            );
        } finally {
            writeLock.unlock();
        }

        long elapsedTime = System.currentTimeMillis() - startTime;

        return new CompactionResult(
                toCompact.size(),
                exec.obsoleteSegmentIds().size(),
                exec.replacementSegments().size(),
                exec.recordsScanned(),
                exec.liveRecordsRetained(),
                exec.obsoleteRecordsReclaimed(),
                exec.bytesReclaimed(),
                elapsedTime
        );
    }
}
