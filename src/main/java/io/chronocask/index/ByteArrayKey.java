package io.chronocask.index;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable wrapper around a byte array key for use in hash maps and comparisons.
 */
public final class ByteArrayKey implements Comparable<ByteArrayKey> {

    private final byte[] bytes;
    private final int hashCode;

    public ByteArrayKey(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes cannot be null");
        this.bytes = bytes.clone();
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    public static ByteArrayKey of(byte[] bytes) {
        return new ByteArrayKey(bytes);
    }

    public static ByteArrayKey of(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new ByteArrayKey(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public byte[] getBytesUnsafe() {
        return bytes;
    }

    public int length() {
        return bytes.length;
    }

    public String asString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteArrayKey that = (ByteArrayKey) o;
        return hashCode == that.hashCode && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public int compareTo(ByteArrayKey other) {
        return Arrays.compare(this.bytes, other.bytes);
    }

    @Override
    public String toString() {
        return asString();
    }
}
