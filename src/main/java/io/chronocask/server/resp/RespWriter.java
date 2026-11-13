package io.chronocask.server.resp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for writing RESP2 formatted values to an OutputStream.
 */
public final class RespWriter {

    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private static final byte[] NULL_BULK = new byte[]{'$', '-', '1', '\r', '\n'};
    private static final byte[] NULL_ARRAY = new byte[]{'*', '-', '1', '\r', '\n'};

    private final OutputStream out;

    public RespWriter(OutputStream out) {
        this.out = out;
    }

    /**
     * Serializes a RespValue to the output stream.
     */
    public void write(RespValue value) throws IOException {
        if (value == null || value.isNull()) {
            out.write(NULL_BULK);
            return;
        }

        switch (value.type()) {
            case SIMPLE_STRING -> {
                out.write('+');
                out.write(value.text().getBytes(StandardCharsets.UTF_8));
                out.write(CRLF);
            }
            case ERROR -> {
                out.write('-');
                out.write(value.text().getBytes(StandardCharsets.UTF_8));
                out.write(CRLF);
            }
            case INTEGER -> {
                out.write(':');
                out.write(Long.toString(value.integer()).getBytes(StandardCharsets.US_ASCII));
                out.write(CRLF);
            }
            case BULK_STRING -> {
                byte[] bytes = value.bytes();
                if (bytes == null) {
                    out.write(NULL_BULK);
                } else {
                    out.write('$');
                    out.write(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
                    out.write(CRLF);
                    out.write(bytes);
                    out.write(CRLF);
                }
            }
            case ARRAY -> {
                var list = value.array();
                out.write('*');
                out.write(Integer.toString(list.size()).getBytes(StandardCharsets.US_ASCII));
                out.write(CRLF);
                for (RespValue elem : list) {
                    write(elem);
                }
            }
            case NULL -> out.write(NULL_BULK);
        }
    }

    /**
     * Convenience method to write and immediately flush.
     */
    public void writeAndFlush(RespValue value) throws IOException {
        write(value);
        flush();
    }

    public void flush() throws IOException {
        out.flush();
    }
}
