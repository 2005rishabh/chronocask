package io.chronocask.raft.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable command replicated across the Raft cluster and applied to the state machine.
 */
public record RaftCommand(CommandType type, byte[] key, byte[] value) {

    public static final RaftCommand NO_OP = new RaftCommand(CommandType.NOOP, new byte[0], new byte[0]);

    public RaftCommand {
        Objects.requireNonNull(type, "type cannot be null");
        key = key != null ? key : new byte[0];
        value = value != null ? value : new byte[0];
    }

    public static RaftCommand put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return new RaftCommand(CommandType.PUT, key, value);
    }

    public static RaftCommand put(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public static RaftCommand delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new RaftCommand(CommandType.DELETE, key, new byte[0]);
    }

    public static RaftCommand delete(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return delete(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Serializes this command into a binary payload.
     * Format:
     * [type (1 byte)] + [keyLen (4 bytes)] + [key] + [valLen (4 bytes)] + [val]
     */
    public byte[] serialize() {
        int capacity = 1 + 4 + key.length + 4 + value.length;
        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.put(type.code());
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(value.length);
        buf.put(value);
        return buf.array();
    }

    /**
     * Deserializes a binary payload into a RaftCommand.
     */
    public static RaftCommand deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return NO_OP;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        byte code = buf.get();
        CommandType type = CommandType.fromCode(code);

        int keyLen = buf.getInt();
        byte[] key = new byte[keyLen];
        buf.get(key);

        int valLen = buf.getInt();
        byte[] val = new byte[valLen];
        buf.get(val);

        return new RaftCommand(type, key, val);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaftCommand that)) return false;
        return type == that.type &&
                Arrays.equals(key, that.key) &&
                Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return switch (type) {
            case PUT -> "PUT(" + new String(key, StandardCharsets.UTF_8) + ")";
            case DELETE -> "DELETE(" + new String(key, StandardCharsets.UTF_8) + ")";
            case NOOP -> "NOOP";
        };
    }
}
