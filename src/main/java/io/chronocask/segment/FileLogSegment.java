package io.chronocask.segment;

import io.chronocask.exception.CorruptRecordException;
import io.chronocask.exception.StorageException;
import io.chronocask.record.Record;
import io.chronocask.record.RecordCodec;
import io.chronocask.record.RecordHeader;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FileChannel-backed implementation of LogSegment.
 *
 * Concurrency characteristics:
 * - Appends are serialized using an internal write lock.
 * - Random reads use positional pread (`FileChannel.read(dst, offset)`), allowing
 *   fully concurrent, lock-free reader threads on POSIX filesystems.
 */
public class FileLogSegment implements LogSegment {

    private final int segmentId;
    private final Path path;
    private final FileChannel channel;
    private final AtomicLong currentSize;
    private final Object writeLock = new Object();

    private volatile boolean sealed;
    private volatile boolean closed;

    public FileLogSegment(int segmentId, Path path, FileChannel channel, boolean sealed) throws IOException {
        this.segmentId = segmentId;
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.channel = Objects.requireNonNull(channel, "channel cannot be null");
        this.currentSize = new AtomicLong(channel.size());
        this.sealed = sealed;
        this.closed = false;
    }

    /**
     * Creates or opens an active (writable) log segment.
     */
    public static FileLogSegment createActive(Path path, int segmentId) throws IOException {
        FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        return new FileLogSegment(segmentId, path, channel, false);
    }

    /**
     * Opens an existing segment file, optionally sealing it as read-only.
     */
    public static FileLogSegment openExisting(Path path, int segmentId, boolean readOnly) throws IOException {
        if (!Files.exists(path)) {
            throw new StorageException("Segment file does not exist: " + path);
        }
        FileChannel channel = readOnly
                ? FileChannel.open(path, StandardOpenOption.READ)
                : FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new FileLogSegment(segmentId, path, channel, readOnly);
    }

    @Override
    public int segmentId() {
        return segmentId;
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public long size() {
        return currentSize.get();
    }

    @Override
    public boolean isSealed() {
        return sealed;
    }

    @Override
    public void seal() throws IOException {
        synchronized (writeLock) {
            if (sealed) {
                return;
            }
            flush(false);
            sealed = true;
        }
    }

    @Override
    public RecordLocation append(Record record, DurabilityMode durability) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(durability, "durability cannot be null");

        ByteBuffer encoded = RecordCodec.encode(record);
        int recordSize = record.totalSize();

        synchronized (writeLock) {
            ensureOpen();
            if (sealed) {
                throw new IllegalStateException("Cannot append to sealed segment: " + segmentId);
            }

            long offset = currentSize.get();
            long writePos = offset;

            while (encoded.hasRemaining()) {
                int written = channel.write(encoded, writePos);
                writePos += written;
            }

            currentSize.set(writePos);

            if (durability == DurabilityMode.SYNC) {
                channel.force(false);
            }

            return new RecordLocation(segmentId, offset, recordSize);
        }
    }

    @Override
    public Record read(long offset, int size) throws IOException {
        ensureOpen();
        if (offset < 0) {
            throw new IllegalArgumentException("Negative offset: " + offset);
        }
        if (size < RecordHeader.HEADER_SIZE) {
            throw new IllegalArgumentException("Record size smaller than header size: " + size);
        }
        if (offset + size > currentSize.get()) {
            throw new EOFException(String.format(
                    "Requested read [%d, %d] exceeds segment size %d", offset, offset + size, currentSize.get()));
        }

        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        long readPos = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, readPos);
            if (read == -1) {
                throw new EOFException("Premature EOF while reading record at offset " + offset);
            }
            readPos += read;
        }
        buffer.flip();

        try {
            return RecordCodec.decode(buffer);
        } catch (CorruptRecordException e) {
            throw new CorruptRecordException(
                    "Corrupted record in segment " + segmentId + ": " + e.getMessage(), offset, e);
        }
    }

    @Override
    public void flush(boolean forceMetadata) throws IOException {
        synchronized (writeLock) {
            if (!closed && channel.isOpen()) {
                channel.force(forceMetadata);
            }
        }
    }

    @Override
    public void truncate(long targetLength) throws IOException {
        synchronized (writeLock) {
            ensureOpen();
            if (targetLength < 0 || targetLength > currentSize.get()) {
                throw new IllegalArgumentException(
                        "Invalid target truncation length: " + targetLength + " (current size: " + currentSize.get() + ")");
            }
            channel.truncate(targetLength);
            currentSize.set(targetLength);
            channel.force(true);
        }
    }

    @Override
    public void delete() throws IOException {
        close();
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws IOException {
        synchronized (writeLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (channel.isOpen()) {
                channel.force(false);
                channel.close();
            }
        }
    }

    @Override
    public Iterator<LocatedRecord> iterator() throws IOException {
        ensureOpen();
        return new RecordIterator();
    }

    private void ensureOpen() {
        if (closed || !channel.isOpen()) {
            throw new IllegalStateException("Segment " + segmentId + " is closed");
        }
    }

    /**
     * Sequential iterator across all records in this segment.
     */
    private class RecordIterator implements Iterator<LocatedRecord> {
        private long currentOffset = 0;
        private LocatedRecord nextRecord = null;
        private boolean finished = false;

        @Override
        public boolean hasNext() {
            if (finished) {
                return false;
            }
            if (nextRecord != null) {
                return true;
            }
            advance();
            return nextRecord != null;
        }

        @Override
        public LocatedRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records in segment " + segmentId);
            }
            LocatedRecord record = nextRecord;
            nextRecord = null;
            return record;
        }

        private void advance() {
            long totalSegmentSize = currentSize.get();
            if (currentOffset >= totalSegmentSize) {
                finished = true;
                return;
            }

            // Check if there are enough bytes for at least a header
            long remainingBytes = totalSegmentSize - currentOffset;
            if (remainingBytes < RecordHeader.HEADER_SIZE) {
                finished = true;
                throw new CorruptRecordException(
                        "Incomplete header at segment tail: " + remainingBytes + " bytes remaining", currentOffset);
            }

            try {
                // Read 28-byte header to know exact payload size
                ByteBuffer headerBuf = ByteBuffer.allocate(RecordHeader.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
                long pos = currentOffset;
                while (headerBuf.hasRemaining()) {
                    int r = channel.read(headerBuf, pos);
                    if (r == -1) break;
                    pos += r;
                }
                headerBuf.flip();

                RecordHeader header = RecordCodec.decodeHeader(headerBuf);
                int totalSize = header.totalRecordSize();

                if (currentOffset + totalSize > totalSegmentSize) {
                    finished = true;
                    throw new CorruptRecordException(
                            "Incomplete record payload at segment tail: expected " + totalSize +
                                    " bytes but only " + remainingBytes + " available", currentOffset);
                }

                Record record = read(currentOffset, totalSize);
                RecordLocation location = new RecordLocation(segmentId, currentOffset, totalSize);
                nextRecord = new LocatedRecord(location, record);
                currentOffset += totalSize;
            } catch (IOException e) {
                finished = true;
                throw new StorageException("Failed reading record at offset " + currentOffset, e);
            }
        }
    }
}
