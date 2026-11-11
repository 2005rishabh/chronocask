package io.chronocask.recovery;

import io.chronocask.exception.CorruptRecordException;
import io.chronocask.exception.StorageException;
import io.chronocask.hint.HintEntry;
import io.chronocask.hint.HintFileReader;
import io.chronocask.hint.HintFileWriter;
import io.chronocask.index.IndexEntry;
import io.chronocask.index.KeyDir;
import io.chronocask.record.Record;
import io.chronocask.record.RecordCodec;
import io.chronocask.record.RecordHeader;
import io.chronocask.record.RecordType;
import io.chronocask.segment.LogSegment;
import io.chronocask.segment.RecordLocation;
import io.chronocask.segment.SegmentManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;

/**
 * Manages crash recovery and startup index rebuilding for ChronoCask.
 *
 * Responsibilities:
 * - Scans existing segments in logical order (using StorageManifest).
 * - Leverages fast hint files for immutable segments when present.
 * - For each segment, verifies framing, magic bytes, versions, and CRC32C checksums.
 * - Validates global monotonic sequence numbering across records.
 * - Handles incomplete crash tails in the active segment via safe truncation.
 * - Replays valid records into KeyDir.
 */
public class RecoveryManager {

    private final SegmentManager segmentManager;

    public RecoveryManager(SegmentManager segmentManager) {
        this.segmentManager = Objects.requireNonNull(
            segmentManager,
            "segmentManager cannot be null"
        );
    }

    public RecoveryResult recover(KeyDir keyDir) throws StorageException {
        Objects.requireNonNull(keyDir, "keyDir cannot be null");
        long startTime = System.currentTimeMillis();

        List<LogSegment> segments = segmentManager.getAllSegments();
        LogSegment activeSegment = segmentManager.getActiveSegment();

        int segmentsScanned = 0;
        long validRecords = 0;
        long putRecords = 0;
        long deleteRecords = 0;
        long truncatedBytes = 0;
        long highestSeqNum = 0;

        for (LogSegment segment : segments) {
            segmentsScanned++;
            boolean isActive =
                activeSegment != null &&
                segment.segmentId() == activeSegment.segmentId();

            SegmentRecoveryResult segResult = null;

            // Try fast recovery using hint file for immutable segments
            if (!isActive) {
                Path hintPath = HintFileWriter.getHintFilePath(
                    segmentManager.getBaseDir(),
                    segment.segmentId()
                );
                if (Files.exists(hintPath)) {
                    try {
                        segResult = recoverFromHintFile(
                            hintPath,
                            segment.segmentId(),
                            keyDir,
                            highestSeqNum
                        );
                    } catch (Exception ignored) {
                        // On hint file read error, fall back to full segment scan
                        segResult = null;
                    }
                }
            }

            // Fallback or active segment scan
            if (segResult == null) {
                segResult = recoverSegment(
                    segment,
                    keyDir,
                    isActive,
                    highestSeqNum
                );
            }

            validRecords += segResult.validRecords();
            putRecords += segResult.putRecords();
            deleteRecords += segResult.deleteRecords();
            truncatedBytes += segResult.truncatedBytes();
            if (segResult.highestSeqNum() > highestSeqNum) {
                highestSeqNum = segResult.highestSeqNum();
            }
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        int activeId = activeSegment != null ? activeSegment.segmentId() : 1;

        return new RecoveryResult(
            segmentsScanned,
            validRecords,
            putRecords,
            deleteRecords,
            truncatedBytes,
            highestSeqNum,
            activeId,
            elapsedTime
        );
    }

    private SegmentRecoveryResult recoverFromHintFile(
        Path hintPath,
        int segmentId,
        KeyDir keyDir,
        long previousHighestSeq
    ) throws IOException {
        long[] counts = new long[3]; // [valid, highestSeq]
        counts[1] = previousHighestSeq;

        HintFileReader.read(hintPath, hint -> {
            if (hint.sequenceNumber() <= counts[1] && counts[1] > 0) {
                throw new CorruptRecordException(
                    "Out-of-order sequence number in hint file " +
                        hintPath +
                        ": " +
                        hint.sequenceNumber() +
                        " <= " +
                        counts[1]
                );
            }
            counts[0]++; // valid
            counts[1] = Math.max(counts[1], hint.sequenceNumber());
            keyDir.put(hint.key(), hint.toIndexEntry());
        });

        return new SegmentRecoveryResult(counts[0], counts[0], 0, 0, counts[1]);
    }

    private SegmentRecoveryResult recoverSegment(
        LogSegment segment,
        KeyDir keyDir,
        boolean isActive,
        long previousHighestSeq
    ) {
        long validRecords = 0;
        long putRecords = 0;
        long deleteRecords = 0;
        long truncatedBytes = 0;
        long highestSeqNum = previousHighestSeq;

        Path path = segment.path();
        long currentOffset = 0;
        long segmentSize = segment.size();

        if (segmentSize == 0) {
            return new SegmentRecoveryResult(0, 0, 0, 0, previousHighestSeq);
        }

        try (
            FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.READ
            )
        ) {
            ByteBuffer headerBuf = ByteBuffer.allocate(
                RecordHeader.HEADER_SIZE
            ).order(ByteOrder.BIG_ENDIAN);

            while (currentOffset < segmentSize) {
                long remainingBytes = segmentSize - currentOffset;

                // 1. Check if there are at least 28 bytes for a full header
                if (remainingBytes < RecordHeader.HEADER_SIZE) {
                    if (isActive) {
                        truncatedBytes = remainingBytes;
                        break;
                    } else {
                        throw new CorruptRecordException(
                            "Incomplete header of " +
                                remainingBytes +
                                " bytes in immutable segment " +
                                segment.segmentId(),
                            currentOffset
                        );
                    }
                }

                headerBuf.clear();
                long readPos = currentOffset;
                while (headerBuf.hasRemaining()) {
                    int r = channel.read(headerBuf, readPos);
                    if (r == -1) break;
                    readPos += r;
                }
                headerBuf.flip();

                RecordHeader header;
                try {
                    header = RecordCodec.decodeHeader(headerBuf);
                } catch (CorruptRecordException e) {
                    if (
                        isActive &&
                        !hasValidRecordAhead(
                            channel,
                            currentOffset + 1,
                            segmentSize
                        )
                    ) {
                        truncatedBytes = remainingBytes;
                        break;
                    } else {
                        throw new CorruptRecordException(
                            "Corrupt record header in segment " +
                                segment.segmentId() +
                                ": " +
                                e.getMessage(),
                            currentOffset,
                            e
                        );
                    }
                }

                int totalRecordSize = header.totalRecordSize();

                // 2. Check if the full payload exists
                if (remainingBytes < totalRecordSize) {
                    if (isActive) {
                        truncatedBytes = remainingBytes;
                        break;
                    } else {
                        throw new CorruptRecordException(
                            "Truncated payload in immutable segment " +
                                segment.segmentId() +
                                ": expected " +
                                totalRecordSize +
                                " bytes, available " +
                                remainingBytes,
                            currentOffset
                        );
                    }
                }

                // 3. Read full record and verify CRC32C checksum
                ByteBuffer fullRecordBuf = ByteBuffer.allocate(
                    totalRecordSize
                ).order(ByteOrder.BIG_ENDIAN);
                readPos = currentOffset;
                while (fullRecordBuf.hasRemaining()) {
                    int r = channel.read(fullRecordBuf, readPos);
                    if (r == -1) break;
                    readPos += r;
                }
                fullRecordBuf.flip();

                Record record;
                try {
                    record = RecordCodec.decode(fullRecordBuf);
                } catch (CorruptRecordException e) {
                    if (
                        isActive &&
                        !hasValidRecordAhead(
                            channel,
                            currentOffset + totalRecordSize,
                            segmentSize
                        )
                    ) {
                        truncatedBytes = remainingBytes;
                        break;
                    } else {
                        throw new CorruptRecordException(
                            "Corrupt record in segment " +
                                segment.segmentId() +
                                ": " +
                                e.getMessage(),
                            currentOffset,
                            e
                        );
                    }
                }

                // Validate monotonic sequence number ordering
                if (
                    record.sequenceNumber() <= highestSeqNum &&
                    highestSeqNum > 0
                ) {
                    throw new CorruptRecordException(
                        "Out-of-order sequence number in segment " +
                            segment.segmentId() +
                            ": " +
                            record.sequenceNumber() +
                            " <= " +
                            highestSeqNum,
                        currentOffset
                    );
                }

                validRecords++;
                highestSeqNum = record.sequenceNumber();

                RecordLocation location = new RecordLocation(
                    segment.segmentId(),
                    currentOffset,
                    totalRecordSize
                );

                if (record.type() == RecordType.PUT) {
                    putRecords++;
                    IndexEntry entry = new IndexEntry(
                        location.segmentId(),
                        location.offset(),
                        location.recordSize(),
                        record.header().valueLength(),
                        record.sequenceNumber()
                    );
                    keyDir.put(record.key(), entry);
                } else if (record.type() == RecordType.DELETE) {
                    deleteRecords++;
                    keyDir.remove(record.key());
                }

                currentOffset += totalRecordSize;
            }
        } catch (IOException e) {
            throw new StorageException(
                "I/O error while recovering segment " + segment.segmentId(),
                e
            );
        }

        if (truncatedBytes > 0) {
            try {
                segment.truncate(currentOffset);
            } catch (IOException e) {
                throw new StorageException(
                    "Failed to truncate active segment " +
                        segment.segmentId() +
                        " to " +
                        currentOffset,
                    e
                );
            }
        }

        return new SegmentRecoveryResult(
            validRecords,
            putRecords,
            deleteRecords,
            truncatedBytes,
            highestSeqNum
        );
    }

    private boolean hasValidRecordAhead(
        FileChannel channel,
        long fromOffset,
        long segmentSize
    ) {
        if (fromOffset >= segmentSize) {
            return false;
        }

        ByteBuffer scanBuf = ByteBuffer.allocate(
            Math.min((int) (segmentSize - fromOffset), 4096)
        ).order(ByteOrder.BIG_ENDIAN);
        long pos = fromOffset;

        try {
            while (pos < segmentSize) {
                scanBuf.clear();
                int read = channel.read(scanBuf, pos);
                if (read <= 0) break;
                scanBuf.flip();

                for (
                    int i = 0;
                    i <= scanBuf.remaining() - RecordHeader.HEADER_SIZE;
                    i++
                ) {
                    int magic = scanBuf.getInt(i);
                    if (magic == RecordHeader.MAGIC) {
                        long candidateOffset = pos + i;
                        if (
                            candidateOffset + RecordHeader.HEADER_SIZE <=
                            segmentSize
                        ) {
                            ByteBuffer testBuf = ByteBuffer.allocate(
                                RecordHeader.HEADER_SIZE
                            ).order(ByteOrder.BIG_ENDIAN);
                            channel.read(testBuf, candidateOffset);
                            testBuf.flip();
                            try {
                                RecordHeader h = RecordCodec.decodeHeader(
                                    testBuf
                                );
                                if (
                                    candidateOffset + h.totalRecordSize() <=
                                    segmentSize
                                ) {
                                    ByteBuffer recBuf = ByteBuffer.allocate(
                                        h.totalRecordSize()
                                    ).order(ByteOrder.BIG_ENDIAN);
                                    channel.read(recBuf, candidateOffset);
                                    recBuf.flip();
                                    RecordCodec.decode(recBuf);
                                    return true;
                                }
                            } catch (Exception ignored) {}
                        }
                    }
                }
                pos += read;
            }
        } catch (IOException ignored) {}
        return false;
    }

    private record SegmentRecoveryResult(
        long validRecords,
        long putRecords,
        long deleteRecords,
        long truncatedBytes,
        long highestSeqNum
    ) {}
}
