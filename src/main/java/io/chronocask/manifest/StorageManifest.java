package io.chronocask.manifest;

import java.util.*;

/**
 * Immutable representation of the authoritative database generation.
 *
 * Ground truth for which segments and hint files belong to the active state.
 *
 * @param generation        monotonic generation counter (increments on every publication)
 * @param activeSegmentId   the current writable active segment ID
 * @param immutableSegments ordered list of immutable segments belonging to this generation
 */
public record StorageManifest(
        long generation,
        int activeSegmentId,
        List<ManifestSegmentEntry> immutableSegments
) {
    public StorageManifest {
        if (generation < 0) {
            throw new IllegalArgumentException("generation cannot be negative: " + generation);
        }
        if (activeSegmentId < 0) {
            throw new IllegalArgumentException("activeSegmentId cannot be negative: " + activeSegmentId);
        }
        immutableSegments = Collections.unmodifiableList(new ArrayList<>(immutableSegments));
    }

    /**
     * Creates an initial manifest for a new database starting at active segment 1.
     */
    public static StorageManifest initial(int initialActiveSegmentId) {
        return new StorageManifest(1L, initialActiveSegmentId, Collections.emptyList());
    }

    /**
     * Returns a new manifest representing active segment rotation.
     * The old active segment becomes an immutable normal segment, and a new active segment is assigned.
     */
    public StorageManifest withActiveSegmentRotation(int newActiveSegmentId) {
        List<ManifestSegmentEntry> updated = new ArrayList<>(immutableSegments);
        updated.add(ManifestSegmentEntry.normal(activeSegmentId, false));
        return new StorageManifest(generation + 1, newActiveSegmentId, updated);
    }

    /**
     * Returns a new manifest representing an atomic replacement of obsolete segments with new compacted segments.
     *
     * @param obsoleteSegmentIds set of segment IDs being retired
     * @param replacements       ordered list of replacement compacted segment entries
     * @return updated StorageManifest with incremented generation
     */
    public StorageManifest withReplacements(Set<Integer> obsoleteSegmentIds, List<ManifestSegmentEntry> replacements) {
        Objects.requireNonNull(obsoleteSegmentIds, "obsoleteSegmentIds cannot be null");
        Objects.requireNonNull(replacements, "replacements cannot be null");

        List<ManifestSegmentEntry> updated = new ArrayList<>();
        boolean replacementsInserted = false;

        for (ManifestSegmentEntry entry : immutableSegments) {
            if (obsoleteSegmentIds.contains(entry.segmentId())) {
                if (!replacementsInserted) {
                    updated.addAll(replacements);
                    replacementsInserted = true;
                }
            } else {
                updated.add(entry);
            }
        }

        if (!replacementsInserted) {
            updated.addAll(replacements);
        }

        return new StorageManifest(generation + 1, activeSegmentId, updated);
    }

    /**
     * Checks if a given segment ID belongs to this manifest generation (either active or immutable).
     */
    public boolean containsSegment(int segmentId) {
        if (segmentId == activeSegmentId) {
            return true;
        }
        for (ManifestSegmentEntry entry : immutableSegments) {
            if (entry.segmentId() == segmentId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a set of all authoritative segment IDs in this generation.
     */
    public Set<Integer> allSegmentIds() {
        Set<Integer> ids = new LinkedHashSet<>();
        for (ManifestSegmentEntry entry : immutableSegments) {
            ids.add(entry.segmentId());
        }
        ids.add(activeSegmentId);
        return Collections.unmodifiableSet(ids);
    }
}
