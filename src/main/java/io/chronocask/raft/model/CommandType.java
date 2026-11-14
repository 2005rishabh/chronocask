package io.chronocask.raft.model;

/**
 * Types of state-machine mutation commands replicated via Raft.
 */
public enum CommandType {
    PUT((byte) 1),
    DELETE((byte) 2),
    NOOP((byte) 3);

    private final byte code;

    CommandType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static CommandType fromCode(byte code) {
        return switch (code) {
            case 1 -> PUT;
            case 2 -> DELETE;
            case 3 -> NOOP;
            default -> throw new IllegalArgumentException("Unknown CommandType code: " + code);
        };
    }
}
