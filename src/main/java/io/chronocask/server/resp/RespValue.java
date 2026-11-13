package io.chronocask.server.resp;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable representation of a RESP protocol value.
 */
public final class RespValue {

    public static final RespValue OK = new RespValue(RespType.SIMPLE_STRING, "OK", null, 0, null);
    public static final RespValue PONG = new RespValue(RespType.SIMPLE_STRING, "PONG", null, 0, null);
    public static final RespValue NULL_BULK_STRING = new RespValue(RespType.NULL, null, null, -1, null);
    public static final RespValue EMPTY_ARRAY = new RespValue(RespType.ARRAY, null, null, 0, Collections.emptyList());

    private final RespType type;
    private final String text;
    private final byte[] bytes;
    private final long integer;
    private final List<RespValue> array;

    private RespValue(RespType type, String text, byte[] bytes, long integer, List<RespValue> array) {
        this.type = type;
        this.text = text;
        this.bytes = bytes;
        this.integer = integer;
        this.array = array;
    }

    public static RespValue simpleString(String text) {
        Objects.requireNonNull(text, "text cannot be null");
        return new RespValue(RespType.SIMPLE_STRING, text, null, 0, null);
    }

    public static RespValue error(String message) {
        Objects.requireNonNull(message, "message cannot be null");
        return new RespValue(RespType.ERROR, message, null, 0, null);
    }

    public static RespValue integer(long value) {
        return new RespValue(RespType.INTEGER, null, null, value, null);
    }

    public static RespValue bulkString(byte[] bytes) {
        if (bytes == null) {
            return NULL_BULK_STRING;
        }
        return new RespValue(RespType.BULK_STRING, null, bytes, bytes.length, null);
    }

    public static RespValue bulkString(String string) {
        if (string == null) {
            return NULL_BULK_STRING;
        }
        return bulkString(string.getBytes(StandardCharsets.UTF_8));
    }

    public static RespValue array(List<RespValue> elements) {
        if (elements == null) {
            return NULL_BULK_STRING;
        }
        return new RespValue(RespType.ARRAY, null, null, elements.size(), List.copyOf(elements));
    }

    public RespType type() {
        return type;
    }

    public String text() {
        return text;
    }

    public byte[] bytes() {
        return bytes;
    }

    public String asString() {
        if (text != null) {
            return text;
        }
        if (bytes != null) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (type == RespType.INTEGER) {
            return Long.toString(integer);
        }
        return null;
    }

    public long integer() {
        return integer;
    }

    public List<RespValue> array() {
        return array != null ? array : Collections.emptyList();
    }

    public boolean isNull() {
        return type == RespType.NULL;
    }

    @Override
    public String toString() {
        return switch (type) {
            case SIMPLE_STRING -> "+" + text;
            case ERROR -> "-" + text;
            case INTEGER -> ":" + integer;
            case BULK_STRING -> "$" + (bytes != null ? bytes.length : -1);
            case ARRAY -> "*" + (array != null ? array.size() : -1);
            case NULL -> "$-1";
        };
    }
}
