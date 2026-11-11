package io.chronocask.manifest;

import io.chronocask.exception.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Manages atomic persistence and publication of the StorageManifest on disk.
 *
 * Durability protocol:
 * 1. Write serialized manifest bytes to MANIFEST.tmp.
 * 2. Flush file contents and metadata to disk with channel.force(true).
 * 3. Atomically rename MANIFEST.tmp -> MANIFEST using ATOMIC_MOVE.
 * 4. Force directory changes to disk where supported.
 */
public class ManifestManager {

    public static final String MANIFEST_FILE_NAME = "MANIFEST";
    public static final String TEMP_MANIFEST_FILE_NAME = "MANIFEST.tmp";

    private final Path baseDir;
    private final Path manifestPath;
    private final Path tempManifestPath;

    private volatile StorageManifest currentManifest;

    public ManifestManager(Path baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir cannot be null");
        this.manifestPath = baseDir.resolve(MANIFEST_FILE_NAME);
        this.tempManifestPath = baseDir.resolve(TEMP_MANIFEST_FILE_NAME);
    }

    /**
     * Loads the existing authoritative manifest from disk, or returns null if no manifest exists.
     */
    public StorageManifest load() throws IOException {
        if (!Files.exists(manifestPath)) {
            return null;
        }

        byte[] allBytes = Files.readAllBytes(manifestPath);
        ByteBuffer buffer = ByteBuffer.wrap(allBytes);
        this.currentManifest = ManifestCodec.decode(buffer);
        return this.currentManifest;
    }

    /**
     * Atomically publishes and durably writes the given manifest to disk.
     */
    public synchronized void commit(StorageManifest manifest) throws IOException {
        Objects.requireNonNull(manifest, "manifest cannot be null");

        Files.createDirectories(baseDir);
        ByteBuffer encoded = ManifestCodec.encode(manifest);

        // 1. Write to temporary file with CREATE + WRITE + TRUNCATE_EXISTING
        try (FileChannel channel = FileChannel.open(
                tempManifestPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            while (encoded.hasRemaining()) {
                channel.write(encoded);
            }
            channel.force(true); // Persist data and metadata
        }

        // 2. Atomic rename
        try {
            Files.move(tempManifestPath, manifestPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Fallback to standard replace if atomic move is not supported across filesystem boundaries
            Files.move(tempManifestPath, manifestPath, StandardCopyOption.REPLACE_EXISTING);
        }

        // 3. Sync directory metadata if supported on the platform
        syncDirectory(baseDir);

        this.currentManifest = manifest;
    }

    public StorageManifest getCurrentManifest() {
        return currentManifest;
    }

    public Path getManifestPath() {
        return manifestPath;
    }

    private static void syncDirectory(Path dir) {
        try (FileChannel dirChannel = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (Exception ignored) {
            // Some filesystems/OSes (e.g. Windows or certain virtual FS) do not allow opening directories with FileChannel
        }
    }
}
