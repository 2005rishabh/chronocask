package io.chronocask.hint;

import io.chronocask.exception.CorruptRecordException;
import io.chronocask.exception.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Reads and verifies hint files to rebuild index metadata without reading payload values from log segments.
 */
public final class HintFileReader {

    private HintFileReader() {}

    public static List<HintEntry> readAll(Path hintFilePath)
        throws IOException {
        List<HintEntry> list = new ArrayList<>();
        read(hintFilePath, list::add);
        return list;
    }

    public static void read(Path hintFilePath, Consumer<HintEntry> consumer)
        throws IOException {
        Objects.requireNonNull(hintFilePath, "hintFilePath cannot be null");
        Objects.requireNonNull(consumer, "consumer cannot be null");

        if (!Files.exists(hintFilePath)) {
            throw new StorageException(
                "Hint file does not exist: " + hintFilePath
            );
        }

        try (
            FileChannel channel = FileChannel.open(
                hintFilePath,
                StandardOpenOption.READ
            )
        ) {
            long fileSize = channel.size();
            if (fileSize < HintFileCodec.FILE_HEADER_SIZE) {
                throw new CorruptRecordException(
                    "Hint file smaller than header: " + fileSize + " bytes"
                );
            }

            // Read file header
            ByteBuffer headerBuf = ByteBuffer.allocate(
                HintFileCodec.FILE_HEADER_SIZE
            ).order(ByteOrder.BIG_ENDIAN);
            channel.read(headerBuf, 0);
            headerBuf.flip();
            HintFileCodec.HintFileHeader header =
                HintFileCodec.decodeFileHeader(headerBuf);

            int segmentId = header.segmentId();
            long currentOffset = HintFileCodec.FILE_HEADER_SIZE;

            // Stream entries
            ByteBuffer entryHeaderBuf = ByteBuffer.allocate(
                HintFileCodec.RECORD_HEADER_SIZE
            ).order(ByteOrder.BIG_ENDIAN);

            while (currentOffset < fileSize) {
                long remaining = fileSize - currentOffset;
                if (remaining < HintFileCodec.RECORD_HEADER_SIZE) {
                    throw new CorruptRecordException(
                        "Truncated hint record header at offset " +
                            currentOffset
                    );
                }

                entryHeaderBuf.clear();
                channel.read(entryHeaderBuf, currentOffset);
                entryHeaderBuf.flip();

                int keyLength = entryHeaderBuf.getInt(24); // Offset 24 is keyLength (0..8 seq, 8..16 off, 16..20 recSize, 20..24 valLen, 24..28 keyLen)
                int totalRecordSize =
                    HintFileCodec.RECORD_HEADER_SIZE + keyLength;

                if (remaining < totalRecordSize) {
                    throw new CorruptRecordException(
                        "Truncated hint record key at offset " + currentOffset
                    );
                }

                ByteBuffer fullRecordBuf = ByteBuffer.allocate(
                    totalRecordSize
                ).order(ByteOrder.BIG_ENDIAN);
                channel.read(fullRecordBuf, currentOffset);
                fullRecordBuf.flip();

                HintEntry entry = HintFileCodec.decodeEntry(
                    fullRecordBuf,
                    segmentId
                );
                consumer.accept(entry);

                currentOffset += totalRecordSize;
            }
        }
    }
}
