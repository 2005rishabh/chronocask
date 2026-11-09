package io.chronocask.recovery;

/**
 * Summary metrics and statistics from recovering a ChronoCask database instance.
 *
 * @param segmentsScanned       total number of log segments scanned
 * @param validRecordsReplayed  number of valid records applied to the index
 * @param putRecords            number of PUT operations recovered
 * @param deleteRecords         number of DELETE (tombstone) operations recovered
 * @param corruptedTailTruncatedBytes number of truncated bytes from uncommitted/partial tail
 * @param highestSequenceNumber highest monotonic sequence number recovered
 * @param activeSegmentId       the segment ID assigned as active
 * @param elapsedTimeMs         time taken in milliseconds for recovery
 */
public record RecoveryResult(
        int segmentsScanned,
        long validRecordsReplayed,
        long putRecords,
        long deleteRecords,
        long corruptedTailTruncatedBytes,
        long highestSequenceNumber,
        int activeSegmentId,
        long elapsedTimeMs
) {
    public RecoveryResult {
        if (segmentsScanned < 0) throw new IllegalArgumentException("segmentsScanned < 0");
        if (validRecordsReplayed < 0) throw new IllegalArgumentException("validRecordsReplayed < 0");
        if (putRecords < 0) throw new IllegalArgumentException("putRecords < 0");
        if (deleteRecords < 0) throw new IllegalArgumentException("deleteRecords < 0");
        if (corruptedTailTruncatedBytes < 0) throw new IllegalArgumentException("corruptedTailTruncatedBytes < 0");
        if (highestSequenceNumber < 0) throw new IllegalArgumentException("highestSequenceNumber < 0");
    }
}
