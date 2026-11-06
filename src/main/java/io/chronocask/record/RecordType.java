package io.chronocask.record;

import io.chronocask.exception.CorruptRecordException;

/**
 * Type of operation stored in a ChronoCask log record.
 */
public enum RecordType {
    PUT((byte) 1),
    DELETE((byte) 2);

    private final byte code;

    RecordType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    /**
     * Resolves a RecordType from its serialized byte representation.
     *
     * @param code byte code from record header
     * @return matching RecordType
     * @throws CorruptRecordException if code is unrecognized
     */
    public static RecordType fromCode(byte code) {
        for (RecordType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new CorruptRecordException("Unknown record type byte: " + code);
    }
}
