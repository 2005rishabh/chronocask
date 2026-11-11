package io.chronocask.compaction;

import io.chronocask.segment.LogSegment;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Plans which immutable segments are eligible for compaction.
 *
 * Invariant: To prevent resurrecting deleted keys without tracking separate deletion watermarks,
 * compaction MUST select a contiguous prefix of immutable segments starting at the oldest
 * segment in the database (index 0).
 */
public class CompactionPlanner {

    private final int maxSegmentsPerCompaction;

    public CompactionPlanner() {
        this(Integer.MAX_VALUE);
    }

    public CompactionPlanner(int maxSegmentsPerCompaction) {
        if (maxSegmentsPerCompaction <= 0) {
            throw new IllegalArgumentException("maxSegmentsPerCompaction must be positive: " + maxSegmentsPerCompaction);
        }
        this.maxSegmentsPerCompaction = maxSegmentsPerCompaction;
    }

    /**
     * Selects a contiguous prefix of immutable segments starting at index 0.
     * Returns an empty list if there are fewer than 1 immutable segments.
     */
    public List<LogSegment> plan(List<LogSegment> immutableSegments) {
        Objects.requireNonNull(immutableSegments, "immutableSegments cannot be null");

        if (immutableSegments.isEmpty()) {
            return Collections.emptyList();
        }

        int count = Math.min(immutableSegments.size(), maxSegmentsPerCompaction);
        return Collections.unmodifiableList(immutableSegments.subList(0, count));
    }
}
