package io.chronocask.server.resp;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming parser for REdis Serialization Protocol (RESP2) and inline commands.
 */
public final class RespReader {

    private final InputStream in;

    public RespReader(InputStream in) {
        this.in = in;
    }

    /**
     * Returns true if there are unread bytes already buffered in the underlying stream.
     * Used for smart pipelined batch coalescing to avoid redundant write syscalls.
     */
    public boolean hasBufferedBytes() throws IOException {
        return in.available() > 0;
    }

    /**
     * Reads the next complete RespValue from the stream.
     * Returns null if end of stream is reached before reading any byte.
     *
     * @throws IOException on I/O error or protocol violation
     */
    public RespValue readValue() throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // Clean EOF
        }

        return switch (firstByte) {
            case '+' -> RespValue.simpleString(readLine());
            case '-' -> RespValue.error(readLine());
            case ':' -> RespValue.integer(Long.parseLong(readLine()));
            case '$' -> readBulkString();
            case '*' -> readArray();
            case '\r', '\n' -> readValue(); // Skip empty blank lines
            default -> readInlineCommand(firstByte);
        };
    }

    private RespValue readBulkString() throws IOException {
        long length = Long.parseLong(readLine());
        if (length < 0) {
            return RespValue.NULL_BULK_STRING;
        }
        if (length > 512 * 1024 * 1024L) {
            // 512MB max bulk string protection
            throw new IOException("Bulk string exceeds 512MB limit: " + length);
        }

        int len = (int) length;
        byte[] buffer = in.readNBytes(len);
        if (buffer.length < len) {
            throw new EOFException(
                "Unexpected EOF while reading bulk string data (expected " +
                    len +
                    ", got " +
                    buffer.length +
                    ")"
            );
        }

        // Read trailing CRLF
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException(
                "Protocol error: expected CRLF after bulk string, found [" +
                    cr +
                    ", " +
                    lf +
                    "]"
            );
        }

        return RespValue.bulkString(buffer);
    }

    private RespValue readArray() throws IOException {
        long count = Long.parseLong(readLine());
        if (count < 0) {
            return RespValue.NULL_BULK_STRING;
        }

        List<RespValue> elements = new ArrayList<>((int) count);
        for (int i = 0; i < count; i++) {
            RespValue val = readValue();
            if (val == null) {
                throw new EOFException(
                    "Unexpected EOF while parsing RESP array elements"
                );
            }
            elements.add(val);
        }
        return RespValue.array(elements);
    }

    private RespValue readInlineCommand(int firstByte) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32);
        buffer.write(firstByte);

        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next != '\n' && next != -1) {
                    buffer.write(b);
                    buffer.write(next);
                } else {
                    break;
                }
            } else if (b == '\n') {
                break;
            } else {
                buffer.write(b);
            }
        }

        String line = buffer.toString(StandardCharsets.UTF_8).trim();
        if (line.isEmpty()) {
            return readValue();
        }

        // Tokenize command tokens respecting space boundaries
        String[] tokens = line.split("\\s+");
        List<RespValue> arrayElements = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            arrayElements.add(RespValue.bulkString(token));
        }
        return RespValue.array(arrayElements);
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n' || next == -1) {
                    break;
                }
                buffer.write(b);
                buffer.write(next);
            } else if (b == '\n') {
                break;
            } else {
                buffer.write(b);
            }
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }
}
