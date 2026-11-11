package io.chronocask.segment;

import io.chronocask.exception.StorageException;
import io.chronocask.hint.HintFileWriter;
import io.chronocask.manifest.ManifestManager;
import io.chronocask.manifest.ManifestSegmentEntry;
import io.chronocask.manifest.StorageManifest;
import io.chronocask.record.Record;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manages the lifecycle of append-only log segments within a database directory.
 *
 * Responsibilities:
 * - Scans existing segments using StorageManifest as the authoritative source of truth.
 * - Manages a single active writable segment and multiple immutable sealed segments.
 * - Automatically rotates the active segment when it reaches the configured threshold.
 * - Routes point reads to the appropriate segment.
 * - Supports atomic segment replacement (replaceSegments) for crash-safe compaction publication.
 */
public class SegmentManager implements Closeable {

    public static final long DEFAULT_MAX_SEGMENT_SIZE_BYTES = 64 * 1024 * 1024L; // 64 MB
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile(
        "^segment-(\\d+)\\.log$"
    );
    private static final String SEGMENT_NAME_FORMAT = "segment-%06d.log";

    private final Path baseDir;
    private final long maxSegmentSizeBytes;
    private final DurabilityMode defaultDurability;
    private final ManifestManager manifestManager;

    private final ConcurrentNavigableMap<
        Integer,
        LogSegment
    > immutableSegments = new ConcurrentSkipListMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    private volatile LogSegment activeSegment;
    private volatile int nextSegmentId = 1;
    private volatile boolean closed = false;

    private SegmentManager(
        Path baseDir,
        long maxSegmentSizeBytes,
        DurabilityMode defaultDurability
    ) {
        this.baseDir = Objects.requireNonNull(
            baseDir,
            "baseDir cannot be null"
        );
        this.maxSegmentSizeBytes =
            maxSegmentSizeBytes > 0
                ? maxSegmentSizeBytes
                : DEFAULT_MAX_SEGMENT_SIZE_BYTES;
        this.defaultDurability = Objects.requireNonNull(
            defaultDurability,
            "defaultDurability cannot be null"
        );
        this.manifestManager = new ManifestManager(baseDir);
    }

    public static SegmentManager open(Path baseDir) throws IOException {
        return open(
            baseDir,
            DEFAULT_MAX_SEGMENT_SIZE_BYTES,
            DurabilityMode.ASYNC
        );
    }

    public static SegmentManager open(
        Path baseDir,
        long maxSegmentSizeBytes,
        DurabilityMode defaultDurability
    ) throws IOException {
        SegmentManager manager = new SegmentManager(
            baseDir,
            maxSegmentSizeBytes,
            defaultDurability
        );
        manager.initialize();
        return manager;
    }

    private void initialize() throws IOException {
        Files.createDirectories(baseDir);

        StorageManifest manifest = manifestManager.load();

        if (manifest != null) {
            // Manifest is authoritative: open segments recorded in manifest
            int maxId = manifest.activeSegmentId();

            for (ManifestSegmentEntry entry : manifest.immutableSegments()) {
                int id = entry.segmentId();
                if (id > maxId) maxId = id;
                Path path = baseDir.resolve(formatSegmentFileName(id));
                LogSegment segment = FileLogSegment.openExisting(
                    path,
                    id,
                    true
                );
                immutableSegments.put(id, segment);
            }

            int activeId = manifest.activeSegmentId();
            Path activePath = baseDir.resolve(formatSegmentFileName(activeId));
            this.activeSegment = FileLogSegment.openExisting(
                activePath,
                activeId,
                false
            );

            this.nextSegmentId = maxId + 1;

            // Clean up unreferenced/stray segment files (e.g. from interrupted compaction)
            cleanStrayFiles(manifest);
        } else {
            // First time initialization or migrating un-manifested directory
            List<Integer> existingIds = new ArrayList<>();
            try (
                DirectoryStream<Path> stream = Files.newDirectoryStream(
                    baseDir,
                    "segment-*.log"
                )
            ) {
                for (Path entry : stream) {
                    Integer segmentId = parseSegmentId(
                        entry.getFileName().toString()
                    );
                    if (segmentId != null) {
                        existingIds.add(segmentId);
                    }
                }
            }

            Collections.sort(existingIds);

            if (existingIds.isEmpty()) {
                int initialId = 1;
                Path initialPath = baseDir.resolve(
                    formatSegmentFileName(initialId)
                );
                this.activeSegment = FileLogSegment.createActive(
                    initialPath,
                    initialId
                );
                this.nextSegmentId = 2;

                StorageManifest initialManifest = StorageManifest.initial(
                    initialId
                );
                manifestManager.commit(initialManifest);
            } else {
                List<ManifestSegmentEntry> entries = new ArrayList<>();
                for (int i = 0; i < existingIds.size() - 1; i++) {
                    int id = existingIds.get(i);
                    Path path = baseDir.resolve(formatSegmentFileName(id));
                    LogSegment segment = FileLogSegment.openExisting(
                        path,
                        id,
                        true
                    );
                    immutableSegments.put(id, segment);

                    boolean hasHint = Files.exists(
                        HintFileWriter.getHintFilePath(baseDir, id)
                    );
                    entries.add(ManifestSegmentEntry.normal(id, hasHint));
                }

                int activeId = existingIds.get(existingIds.size() - 1);
                Path activePath = baseDir.resolve(
                    formatSegmentFileName(activeId)
                );
                this.activeSegment = FileLogSegment.openExisting(
                    activePath,
                    activeId,
                    false
                );
                this.nextSegmentId = activeId + 1;

                StorageManifest migratedManifest = new StorageManifest(
                    1L,
                    activeId,
                    entries
                );
                manifestManager.commit(migratedManifest);
            }
        }
    }

    private void cleanStrayFiles(StorageManifest manifest) {
        Set<Integer> validIds = manifest.allSegmentIds();
        try (
            DirectoryStream<Path> stream = Files.newDirectoryStream(
                baseDir,
                "segment-*.log"
            )
        ) {
            for (Path entry : stream) {
                Integer segId = parseSegmentId(entry.getFileName().toString());
                if (segId != null && !validIds.contains(segId)) {
                    Files.deleteIfExists(entry);
                }
            }
        } catch (IOException ignored) {}
    }

    public int allocateSegmentId() {
        writeLock.lock();
        try {
            return nextSegmentId++;
        } finally {
            writeLock.unlock();
        }
    }

    public RecordLocation append(Record record) throws IOException {
        return append(record, defaultDurability);
    }

    public RecordLocation append(Record record, DurabilityMode durability)
        throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(durability, "durability cannot be null");

        writeLock.lock();
        try {
            ensureOpen();

            if (
                activeSegment.size() > 0 &&
                activeSegment.size() + record.totalSize() > maxSegmentSizeBytes
            ) {
                rotateActiveSegment();
            }

            return activeSegment.append(record, durability);
        } finally {
            writeLock.unlock();
        }
    }

    private void rotateActiveSegment() throws IOException {
        activeSegment.seal();
        immutableSegments.put(activeSegment.segmentId(), activeSegment);

        int nextId = nextSegmentId++;
        Path nextSegmentPath = baseDir.resolve(formatSegmentFileName(nextId));
        this.activeSegment = FileLogSegment.createActive(
            nextSegmentPath,
            nextId
        );

        // Durably publish to manifest
        StorageManifest current = manifestManager.getCurrentManifest();
        if (current != null) {
            StorageManifest updated = current.withActiveSegmentRotation(nextId);
            manifestManager.commit(updated);
        }
    }

    /**
     * Atomically replaces obsolete segments with newly compacted segments.
     */
    public void replaceSegments(
        Set<Integer> obsoleteSegmentIds,
        List<LogSegment> replacementSegments,
        List<ManifestSegmentEntry> replacementEntries
    ) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            Objects.requireNonNull(
                obsoleteSegmentIds,
                "obsoleteSegmentIds cannot be null"
            );
            Objects.requireNonNull(
                replacementSegments,
                "replacementSegments cannot be null"
            );
            Objects.requireNonNull(
                replacementEntries,
                "replacementEntries cannot be null"
            );

            // 1. Validate
            for (int obsoleteId : obsoleteSegmentIds) {
                if (
                    activeSegment != null &&
                    activeSegment.segmentId() == obsoleteId
                ) {
                    throw new IllegalStateException(
                        "Cannot replace active segment: " + obsoleteId
                    );
                }
                if (!immutableSegments.containsKey(obsoleteId)) {
                    throw new IllegalArgumentException(
                        "Obsolete segment not found in immutable segments: " +
                            obsoleteId
                    );
                }
            }
            for (LogSegment rep : replacementSegments) {
                if (!rep.isSealed()) {
                    throw new IllegalArgumentException(
                        "Replacement segment must be sealed: " + rep.segmentId()
                    );
                }
            }

            // 2. Ensure replacement segments are durably forced to disk
            for (LogSegment rep : replacementSegments) {
                rep.flush(true);
            }

            // 3. Atomically publish updated manifest
            StorageManifest current = manifestManager.getCurrentManifest();
            StorageManifest updated = current.withReplacements(
                obsoleteSegmentIds,
                replacementEntries
            );
            manifestManager.commit(updated);

            // 4. Update in-memory segment registry
            List<LogSegment> toDelete = new ArrayList<>();
            for (int obsoleteId : obsoleteSegmentIds) {
                LogSegment removed = immutableSegments.remove(obsoleteId);
                if (removed != null) {
                    toDelete.add(removed);
                }
            }
            for (LogSegment rep : replacementSegments) {
                immutableSegments.put(rep.segmentId(), rep);
            }

            // 5. Delete obsolete files after publication is durable
            for (LogSegment obsolete : toDelete) {
                try {
                    obsolete.delete();
                    Path hintPath = HintFileWriter.getHintFilePath(
                        baseDir,
                        obsolete.segmentId()
                    );
                    Files.deleteIfExists(hintPath);
                } catch (IOException ignored) {}
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Record read(RecordLocation location) throws IOException {
        Objects.requireNonNull(location, "location cannot be null");
        ensureOpen();

        LogSegment segment = getSegment(location.segmentId());
        if (segment == null) {
            throw new StorageException(
                "Segment " + location.segmentId() + " not found for " + location
            );
        }
        return segment.read(location);
    }

    public LogSegment getSegment(int segmentId) {
        LogSegment currentActive = this.activeSegment;
        if (currentActive != null && currentActive.segmentId() == segmentId) {
            return currentActive;
        }
        return immutableSegments.get(segmentId);
    }

    public List<LogSegment> getImmutableSegments() {
        StorageManifest manifest = manifestManager.getCurrentManifest();
        if (manifest != null) {
            List<LogSegment> ordered = new ArrayList<>();
            for (ManifestSegmentEntry entry : manifest.immutableSegments()) {
                LogSegment seg = immutableSegments.get(entry.segmentId());
                if (seg != null) {
                    ordered.add(seg);
                }
            }
            return Collections.unmodifiableList(ordered);
        }
        return List.copyOf(immutableSegments.values());
    }

    public LogSegment getActiveSegment() {
        return activeSegment;
    }

    public List<LogSegment> getAllSegments() {
        StorageManifest manifest = manifestManager.getCurrentManifest();
        if (manifest != null) {
            List<LogSegment> ordered = new ArrayList<>();
            for (ManifestSegmentEntry entry : manifest.immutableSegments()) {
                LogSegment seg = immutableSegments.get(entry.segmentId());
                if (seg != null) {
                    ordered.add(seg);
                }
            }
            if (activeSegment != null) {
                ordered.add(activeSegment);
            }
            return Collections.unmodifiableList(ordered);
        }

        List<LogSegment> all = new ArrayList<>(immutableSegments.values());
        LogSegment currentActive = this.activeSegment;
        if (
            currentActive != null &&
            !immutableSegments.containsKey(currentActive.segmentId())
        ) {
            all.add(currentActive);
        }
        return Collections.unmodifiableList(all);
    }

    public void deleteSegment(int segmentId) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            if (
                activeSegment != null && activeSegment.segmentId() == segmentId
            ) {
                throw new IllegalStateException(
                    "Cannot delete active segment: " + segmentId
                );
            }
            LogSegment segment = immutableSegments.remove(segmentId);
            if (segment != null) {
                segment.delete();
                Path hintPath = HintFileWriter.getHintFilePath(
                    baseDir,
                    segmentId
                );
                Files.deleteIfExists(hintPath);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void flush() throws IOException {
        writeLock.lock();
        try {
            if (!closed && activeSegment != null) {
                activeSegment.flush(true);
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

    public StorageManifest getManifest() {
        return manifestManager.getCurrentManifest();
    }

    public ManifestManager getManifestManager() {
        return manifestManager;
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
