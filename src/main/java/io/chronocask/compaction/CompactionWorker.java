package io.chronocask.compaction;

import io.chronocask.hint.HintEntry;
import io.chronocask.hint.HintFileWriter;
import io.chronocask.index.ByteArrayKey;
import io.chronocask.index.IndexEntry;
import io.chronocask.index.KeyDir;
import io.chronocask.manifest.ManifestSegmentEntry;
import io.chronocask.record.Record;
import io.chronocask.segment.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Worker responsible for scanning obsolete segments, filtering live records,
 * and writing new compacted segments and hint files.
 */
public class CompactionWorker {

    private final Path baseDir;
    private final SegmentManager segmentManager;
    private final long maxSegmentSizeBytes;

    public CompactionWorker(Path baseDir, SegmentManager segmentManager, long maxSegmentSizeBytes) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir cannot be null");
        this.segmentManager = Objects.requireNonNull(segmentManager, "segmentManager cannot be null");
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
    }

    public record ExecutionResult(
            Set<Integer> obsoleteSegmentIds,
            List<LogSegment> replacementSegments,
            List<ManifestSegmentEntry> replacementEntries,
            Map<ByteArrayKey, IndexEntry> updatedIndexEntries,
            long recordsScanned,
            long liveRecordsRetained,
            long obsoleteRecordsReclaimed,
            long bytesReclaimed
    ) {}

    public ExecutionResult execute(List<LogSegment> segmentsToCompact, KeyDir keyDir) throws IOException {
        Set<Integer> obsoleteSegmentIds = new LinkedHashSet<>();
        long totalOldBytes = 0;
        for (LogSegment seg : segmentsToCompact) {
            obsoleteSegmentIds.add(seg.segmentId());
            totalOldBytes += seg.size();
        }

        List<LogSegment> replacementSegments = new ArrayList<>();
        List<ManifestSegmentEntry> replacementEntries = new ArrayList<>();
        Map<ByteArrayKey, IndexEntry> updatedIndexEntries = new HashMap<>();

        long recordsScanned = 0;
        long liveRecordsRetained = 0;

        FileLogSegment currentCompactedSegment = null;
        List<HintEntry> currentHintEntries = new ArrayList<>();

        try {
            for (LogSegment seg : segmentsToCompact) {
                Iterator<LocatedRecord> it = seg.iterator();
                while (it.hasNext()) {
                    LocatedRecord located = it.next();
                    recordsScanned++;
                    Record record = located.record();
                    RecordLocation oldLoc = located.location();

                    // Check if this record is currently live in KeyDir
                    IndexEntry currentEntry = keyDir.get(record.key());
                    boolean isLive = (currentEntry != null &&
                            currentEntry.segmentId() == oldLoc.segmentId() &&
                            currentEntry.offset() == oldLoc.offset());

                    if (isLive) {
                        // Allocate or rotate compacted segment if needed
                        if (currentCompactedSegment == null ||
                                (currentCompactedSegment.size() > 0 &&
                                        currentCompactedSegment.size() + record.totalSize() > maxSegmentSizeBytes)) {

                            if (currentCompactedSegment != null) {
                                sealAndWriteHints(currentCompactedSegment, currentHintEntries, replacementSegments, replacementEntries);
                                currentHintEntries.clear();
                            }

                            int newSegmentId = segmentManager.allocateSegmentId();
                            Path segPath = baseDir.resolve(SegmentManager.formatSegmentFileName(newSegmentId));
                            currentCompactedSegment = FileLogSegment.createActive(segPath, newSegmentId);
                        }

                        // Append live record preserving original sequence number
                        RecordLocation newLoc = currentCompactedSegment.append(record, DurabilityMode.SYNC);
                        IndexEntry newEntry = new IndexEntry(
                                newLoc.segmentId(),
                                newLoc.offset(),
                                newLoc.recordSize(),
                                record.header().valueLength(),
                                record.sequenceNumber()
                        );

                        ByteArrayKey keyWrapper = new ByteArrayKey(record.key());
                        updatedIndexEntries.put(keyWrapper, newEntry);
                        currentHintEntries.add(HintEntry.of(newLoc.segmentId(), record.key(), newEntry));
                        liveRecordsRetained++;
                    }
                }
            }

            // Seal the final compacted segment if any records were written
            if (currentCompactedSegment != null && currentCompactedSegment.size() > 0) {
                sealAndWriteHints(currentCompactedSegment, currentHintEntries, replacementSegments, replacementEntries);
            } else if (currentCompactedSegment != null) {
                currentCompactedSegment.close();
                currentCompactedSegment.delete();
            }

            long totalNewBytes = 0;
            for (LogSegment rep : replacementSegments) {
                totalNewBytes += rep.size();
            }
            long bytesReclaimed = Math.max(0, totalOldBytes - totalNewBytes);
            long obsoleteRecordsReclaimed = recordsScanned - liveRecordsRetained;

            return new ExecutionResult(
                    obsoleteSegmentIds,
                    replacementSegments,
                    replacementEntries,
                    updatedIndexEntries,
                    recordsScanned,
                    liveRecordsRetained,
                    obsoleteRecordsReclaimed,
                    bytesReclaimed
            );
        } catch (Exception e) {
            // Cleanup incomplete replacement segments on error
            if (currentCompactedSegment != null) {
                try {
                    currentCompactedSegment.close();
                    currentCompactedSegment.delete();
                } catch (Exception ignored) {
                }
            }
            for (LogSegment rep : replacementSegments) {
                try {
                    rep.close();
                    rep.delete();
                    Path hintPath = HintFileWriter.getHintFilePath(baseDir, rep.segmentId());
                    java.nio.file.Files.deleteIfExists(hintPath);
                } catch (Exception ignored) {
                }
            }
            throw new IOException("Compaction worker failed: " + e.getMessage(), e);
        }
    }

    private void sealAndWriteHints(
            FileLogSegment segment,
            List<HintEntry> hints,
            List<LogSegment> replacementSegments,
            List<ManifestSegmentEntry> replacementEntries
    ) throws IOException {
        segment.seal();
        segment.flush(true);

        Path hintPath = HintFileWriter.write(baseDir, segment.segmentId(), hints);
        boolean hasHint = java.nio.file.Files.exists(hintPath);

        replacementSegments.add(segment);
        replacementEntries.add(ManifestSegmentEntry.compacted(segment.segmentId(), hasHint));
    }
}
