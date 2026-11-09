package io.chronocask.recovery;

import io.chronocask.exception.CorruptRecordException;
import io.chronocask.exception.StorageException;
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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;

/**
 * Manages crash recovery and startup index rebuilding for ChronoCask.
 *
 * Responsibilities:
 * - Scans existing segments in logical order (ascending segment IDs).
 * - For each segment, verifies framing, magic bytes, versions, and CRC32C checksums.
 * - Handles incomplete crash tails:
 *     - If the very last (active) segment has an incomplete write or corruption at the file tail,
 *       the recovery protocol inspects whether subsequent bytes contain valid records. If corruption
 *       occurs strictly at the tail, the uncommitted tail is safely truncated.
 *     - If corruption occurs in the middle of a segment (with subsequent valid records) or in
 *       an immutable sealed segment, a {@link CorruptRecordException} is thrown to prevent silent data loss.
 * - Replays valid records into the provided {@link KeyDir}:
 *     - PUT records insert or update the {@link IndexEntry}.
 *     - DELETE tombstones remove the key from the index.
 * - Restores the highest sequence number so new writes continue monotonically.
 */
public class RecoveryManager {

    private final SegmentManager segmentManager;

    public RecoveryManager(SegmentManager segmentManager) {
        this.segmentManager = Objects.requireNonNull(
            segmentManager,
            "segmentManager cannot be null"
        );
    }

    /**
     * Executes the recovery protocol and rebuilds the KeyDir index.
     *
     * @param keyDir the index (HeapKeyDir or OffHeapKeyDir) to rebuild
     * @return summary statistics of the recovery
     * @throws StorageException on unrecoverable corruption or I/O failure
     */
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

            SegmentRecoveryResult segResult = recoverSegment(
                segment,
                keyDir,
                isActive
            );
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

    private SegmentRecoveryResult recoverSegment(
        LogSegment segment,
        KeyDir keyDir,
        boolean isActive
    ) {
        long validRecords = 0;
        long putRecords = 0;
        long deleteRecords = 0;
        long truncatedBytes = 0;
        long highestSeqNum = 0;

        Path path = segment.path();
        long currentOffset = 0;
        long segmentSize = segment.size();

        // If the segment is empty, nothing to scan
        if (segmentSize == 0) {
            return new SegmentRecoveryResult(0, 0, 0, 0, 0);
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
                        // Crash truncation at active segment tail
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
                        // Truncate only if this corruption is strictly at the tail with no subsequent valid records
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
                        // Crash truncation in payload at active segment tail
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
                        // Corrupt record strictly at the active tail
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

                // 4. Update recovery state and KeyDir
                validRecords++;
                if (record.sequenceNumber() > highestSeqNum) {
                    highestSeqNum = record.sequenceNumber();
                }

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

        // If active segment had an incomplete crash tail, truncate to valid offset
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

    /**
     * Checks whether valid records exist further ahead in the channel,
     * which indicates middle-of-segment corruption rather than a crash truncation tail.
     */
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
                        // Found potential valid record header magic ahead
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
                                    return true; // Valid record found ahead!
                                }
                            } catch (Exception ignored) {
                                // Keep scanning
                            }
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
