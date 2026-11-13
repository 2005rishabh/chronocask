package io.chronocask.server.resp;

/**
 * Supported data types in the REdis Serialization Protocol (RESP2).
 */
public enum RespType {
    /** Simple string preceded by '+' */
    SIMPLE_STRING,
    /** Error message preceded by '-' */
    ERROR,
    /** Signed 64-bit integer preceded by ':' */
    INTEGER,
    /** Binary-safe bulk string preceded by '$' */
    BULK_STRING,
    /** Array of RESP elements preceded by '*' */
    ARRAY,
    /** Null bulk string or null array */
    NULL
}
