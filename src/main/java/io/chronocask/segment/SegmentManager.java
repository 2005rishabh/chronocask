package io.chronocask.segment;

import io.chronocask.exception.StorageException;
import io.chronocask.record.Record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manages the lifecycle of append-only log segments within a database directory.
 *
 * Responsibilities:
 * - Scans existing segments on startup in logical order
 * - Manages a single active writable segment and multiple immutable sealed segments
 * - Automatically rotates the active segment when it reaches the configured threshold
 * - Routes point reads to the appropriate segment
 * - Supports safe segment deletion for compaction
 */
public class SegmentManager implements Closeable {

    public static final long DEFAULT_MAX_SEGMENT_SIZE_BYTES = 64 * 1024 * 1024L; // 64 MB
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("^segment-(\\d+)\\.log$");
    private static final String SEGMENT_NAME_FORMAT = "segment-%06d.log";

    private final Path baseDir;
    private final long maxSegmentSizeBytes;
    private final DurabilityMode defaultDurability;

    private final ConcurrentNavigableMap<Integer, LogSegment> immutableSegments = new ConcurrentSkipListMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    private volatile LogSegment activeSegment;
    private volatile boolean closed = false;

    private SegmentManager(Path baseDir, long maxSegmentSizeBytes, DurabilityMode defaultDurability) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir cannot be null");
        this.maxSegmentSizeBytes = maxSegmentSizeBytes > 0 ? maxSegmentSizeBytes : DEFAULT_MAX_SEGMENT_SIZE_BYTES;
        this.defaultDurability = Objects.requireNonNull(defaultDurability, "defaultDurability cannot be null");
    }

    /**
     * Initializes a SegmentManager in the given directory with default configuration.
     */
    public static SegmentManager open(Path baseDir) throws IOException {
        return open(baseDir, DEFAULT_MAX_SEGMENT_SIZE_BYTES, DurabilityMode.ASYNC);
    }

    /**
     * Initializes a SegmentManager with custom segment size threshold and durability mode.
     */
    public static SegmentManager open(Path baseDir, long maxSegmentSizeBytes, DurabilityMode defaultDurability) throws IOException {
        SegmentManager manager = new SegmentManager(baseDir, maxSegmentSizeBytes, defaultDurability);
        manager.initialize();
        return manager;
    }

    /**
     * Discovers existing segments or creates the initial active segment.
     */
    private void initialize() throws IOException {
        Files.createDirectories(baseDir);

        List<Integer> existingIds = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, "segment-*.log")) {
            for (Path entry : stream) {
                Integer segmentId = parseSegmentId(entry.getFileName().toString());
                if (segmentId != null) {
                    existingIds.add(segmentId);
                }
            }
        }

        Collections.sort(existingIds);

        if (existingIds.isEmpty()) {
            // First time initialization: create segment-000001.log
            int initialId = 1;
            Path initialPath = baseDir.resolve(formatSegmentFileName(initialId));
            this.activeSegment = FileLogSegment.createActive(initialPath, initialId);
        } else {
            // Open all earlier segments as sealed/immutable
            for (int i = 0; i < existingIds.size() - 1; i++) {
                int id = existingIds.get(i);
                Path path = baseDir.resolve(formatSegmentFileName(id));
                LogSegment segment = FileLogSegment.openExisting(path, id, true);
                immutableSegments.put(id, segment);
            }

            // Open the highest segment as the active segment
            int activeId = existingIds.get(existingIds.size() - 1);
            Path activePath = baseDir.resolve(formatSegmentFileName(activeId));
            this.activeSegment = FileLogSegment.openExisting(activePath, activeId, false);

            // If existing active segment has already reached capacity, rotate immediately
            if (activeSegment.size() >= maxSegmentSizeBytes) {
                rotateActiveSegment();
            }
        }
    }

    /**
     * Appends a record using the default durability mode, performing segment rotation if needed.
     */
    public RecordLocation append(Record record) throws IOException {
        return append(record, defaultDurability);
    }

    /**
     * Appends a record using the specified durability mode, performing segment rotation if needed.
     */
    public RecordLocation append(Record record, DurabilityMode durability) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(durability, "durability cannot be null");

        writeLock.lock();
        try {
            ensureOpen();

            // Rotate if adding this record would exceed max segment size,
            // unless activeSegment is currently empty (to accommodate records larger than max size)
            if (activeSegment.size() > 0 && (activeSegment.size() + record.totalSize() > maxSegmentSizeBytes)) {
                rotateActiveSegment();
            }

            return activeSegment.append(record, durability);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Rotates the active segment by sealing it, adding it to immutableSegments,
     * and creating a new active segment with the next segment ID.
     * Must be called while holding writeLock.
     */
    private void rotateActiveSegment() throws IOException {
        activeSegment.seal();
        immutableSegments.put(activeSegment.segmentId(), activeSegment);

        int nextSegmentId = activeSegment.segmentId() + 1;
        Path nextSegmentPath = baseDir.resolve(formatSegmentFileName(nextSegmentId));
        this.activeSegment = FileLogSegment.createActive(nextSegmentPath, nextSegmentId);
    }

    /**
     * Reads a record from the location's designated segment.
     */
    public Record read(RecordLocation location) throws IOException {
        Objects.requireNonNull(location, "location cannot be null");
        ensureOpen();

        LogSegment segment = getSegment(location.segmentId());
        if (segment == null) {
            throw new StorageException("Segment " + location.segmentId() + " not found for " + location);
        }
        return segment.read(location);
    }

    /**
     * Retrieves a segment by its ID (active or immutable).
     */
    public LogSegment getSegment(int segmentId) {
        LogSegment currentActive = this.activeSegment;
        if (currentActive != null && currentActive.segmentId() == segmentId) {
            return currentActive;
        }
        return immutableSegments.get(segmentId);
    }

    /**
     * Returns an unmodifiable list of all immutable segments in ascending segmentId order.
     */
    public List<LogSegment> getImmutableSegments() {
        return List.copyOf(immutableSegments.values());
    }

    /**
     * Returns the current active segment.
     */
    public LogSegment getActiveSegment() {
        return activeSegment;
    }

    /**
     * Returns all open segments in logical order (immutable segments sorted, followed by active).
     */
    public List<LogSegment> getAllSegments() {
        List<LogSegment> all = new ArrayList<>(immutableSegments.values());
        LogSegment currentActive = this.activeSegment;
        if (currentActive != null && !immutableSegments.containsKey(currentActive.segmentId())) {
            all.add(currentActive);
        }
        return Collections.unmodifiableList(all);
    }

    /**
     * Deletes an immutable segment file and removes it from the manager.
     * Active segments cannot be deleted.
     */
    public void deleteSegment(int segmentId) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            if (activeSegment != null && activeSegment.segmentId() == segmentId) {
                throw new IllegalStateException("Cannot delete active segment: " + segmentId);
            }
            LogSegment segment = immutableSegments.remove(segmentId);
            if (segment != null) {
                segment.delete();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Flushes the active segment.
     */
    public void flush() throws IOException {
        writeLock.lock();
        try {
            if (!closed && activeSegment != null) {
                activeSegment.flush(false);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            if (activeSegment != null) {
                activeSegment.close();
            }
            for (LogSegment immutable : immutableSegments.values()) {
                immutable.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Path getBaseDir() {
        return baseDir;
    }

    public long getMaxSegmentSizeBytes() {
        return maxSegmentSizeBytes;
    }

    public DurabilityMode getDefaultDurability() {
        return defaultDurability;
    }

    public static String formatSegmentFileName(int segmentId) {
        return String.format(SEGMENT_NAME_FORMAT, segmentId);
    }

    public static Integer parseSegmentId(String fileName) {
        Matcher matcher = SEGMENT_FILE_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        }
        return null;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SegmentManager is closed");
        }
    }
}
