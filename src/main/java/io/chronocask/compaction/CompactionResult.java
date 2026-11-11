package io.chronocask.compaction;

/**
 * Statistics and metrics from a segment compaction operation.
 *
 * @param segmentsExamined          number of immutable segments inspected
 * @param segmentsReplaced          number of obsolete segments retired
 * @param compactedSegmentsCreated  number of newly generated compacted segments
 * @param recordsScanned            total records evaluated across compacted segments
 * @param liveRecordsRetained       live records rewritten into new segments
 * @param obsoleteRecordsReclaimed  dead/superseded records and tombstones eliminated
 * @param bytesReclaimed            bytes of disk space reclaimed
 * @param elapsedTimeMs             compaction duration in milliseconds
 */
public record CompactionResult(
        int segmentsExamined,
        int segmentsReplaced,
        int compactedSegmentsCreated,
        long recordsScanned,
        long liveRecordsRetained,
        long obsoleteRecordsReclaimed,
        long bytesReclaimed,
        long elapsedTimeMs
) {
    public static CompactionResult empty() {
        return new CompactionResult(0, 0, 0, 0, 0, 0, 0, 0);
    }
}
