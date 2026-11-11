package io.chronocask.manifest;

import java.util.Objects;

/**
 * Metadata entry for a single immutable log segment in the StorageManifest.
 *
 * @param segmentId   unique integer identifier of the segment
 * @param type        NORMAL or COMPACTED
 * @param hasHintFile true if an authoritative hint file exists for this segment
 */
public record ManifestSegmentEntry(int segmentId, SegmentType type, boolean hasHintFile) {

    public ManifestSegmentEntry {
        if (segmentId < 0) {
            throw new IllegalArgumentException("segmentId cannot be negative: " + segmentId);
        }
        Objects.requireNonNull(type, "type cannot be null");
    }

    public static ManifestSegmentEntry normal(int segmentId, boolean hasHintFile) {
        return new ManifestSegmentEntry(segmentId, SegmentType.NORMAL, hasHintFile);
    }

    public static ManifestSegmentEntry compacted(int segmentId, boolean hasHintFile) {
        return new ManifestSegmentEntry(segmentId, SegmentType.COMPACTED, hasHintFile);
    }
}
