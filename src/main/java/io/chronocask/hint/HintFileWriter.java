package io.chronocask.hint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Objects;

/**
 * Generates durable hint files for immutable segments.
 */
public final class HintFileWriter {

    public static final String HINTS_DIR_NAME = "hints";
    public static final String HINT_FILE_FORMAT = "segment-%06d.hint";

    private HintFileWriter() {
    }

    public static Path write(Path baseDir, int segmentId, Collection<HintEntry> entries) throws IOException {
        Objects.requireNonNull(baseDir, "baseDir cannot be null");
        Objects.requireNonNull(entries, "entries cannot be null");

        Path hintsDir = baseDir.resolve(HINTS_DIR_NAME);
        Files.createDirectories(hintsDir);

        String fileName = String.format(HINT_FILE_FORMAT, segmentId);
        Path finalPath = hintsDir.resolve(fileName);
        Path tmpPath = hintsDir.resolve(fileName + ".tmp");

        try (FileChannel channel = FileChannel.open(
                tmpPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer headerBuf = HintFileCodec.encodeFileHeader(segmentId, entries.size());
            while (headerBuf.hasRemaining()) {
                channel.write(headerBuf);
            }

            for (HintEntry entry : entries) {
                ByteBuffer entryBuf = HintFileCodec.encodeEntry(entry);
                while (entryBuf.hasRemaining()) {
                    channel.write(entryBuf);
                }
            }

            channel.force(true); // Persist data and metadata
        }

        Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDirectory(hintsDir);

        return finalPath;
    }

    public static Path getHintFilePath(Path baseDir, int segmentId) {
        return baseDir.resolve(HINTS_DIR_NAME).resolve(String.format(HINT_FILE_FORMAT, segmentId));
    }

    private static void syncDirectory(Path dir) {
        try (FileChannel dirChannel = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (Exception ignored) {
        }
    }
}
