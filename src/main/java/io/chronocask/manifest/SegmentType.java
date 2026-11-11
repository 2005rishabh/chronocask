package io.chronocask.manifest;

/**
 * Type and role of a segment recorded in the manifest.
 */
public enum SegmentType {
    NORMAL((byte) 1),
    COMPACTED((byte) 2);

    private final byte code;

    SegmentType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static SegmentType fromCode(byte code) {
        for (SegmentType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown SegmentType code: " + code);
    }
}
