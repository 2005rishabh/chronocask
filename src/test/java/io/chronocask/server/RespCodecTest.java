package io.chronocask.server;

import io.chronocask.server.resp.RespReader;
import io.chronocask.server.resp.RespType;
import io.chronocask.server.resp.RespValue;
import io.chronocask.server.resp.RespWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RESP2 Protocol Codec Unit Tests")
class RespCodecTest {

    @Test
    @DisplayName("Should encode and decode Simple String")
    void testSimpleString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);
        writer.writeAndFlush(RespValue.simpleString("OK"));

        byte[] raw = out.toByteArray();
        assertEquals("+OK\r\n", new String(raw, StandardCharsets.UTF_8));

        RespReader reader = new RespReader(new ByteArrayInputStream(raw));
        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertEquals(RespType.SIMPLE_STRING, decoded.type());
        assertEquals("OK", decoded.text());
    }

    @Test
    @DisplayName("Should encode and decode Error")
    void testError() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);
        writer.writeAndFlush(RespValue.error("ERR invalid syntax"));

        byte[] raw = out.toByteArray();
        assertEquals("-ERR invalid syntax\r\n", new String(raw, StandardCharsets.UTF_8));

        RespReader reader = new RespReader(new ByteArrayInputStream(raw));
        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertEquals(RespType.ERROR, decoded.type());
        assertEquals("ERR invalid syntax", decoded.text());
    }

    @Test
    @DisplayName("Should encode and decode Integer")
    void testInteger() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);
        writer.writeAndFlush(RespValue.integer(123456789L));

        byte[] raw = out.toByteArray();
        assertEquals(":123456789\r\n", new String(raw, StandardCharsets.UTF_8));

        RespReader reader = new RespReader(new ByteArrayInputStream(raw));
        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertEquals(RespType.INTEGER, decoded.type());
        assertEquals(123456789L, decoded.integer());
    }

    @Test
    @DisplayName("Should encode and decode Bulk String with binary payload")
    void testBulkStringBinary() throws IOException {
        byte[] payload = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF, '\r', '\n', 'A'};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);
        writer.writeAndFlush(RespValue.bulkString(payload));

        byte[] raw = out.toByteArray();
        RespReader reader = new RespReader(new ByteArrayInputStream(raw));
        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertEquals(RespType.BULK_STRING, decoded.type());
        assertArrayEquals(payload, decoded.bytes());
    }

    @Test
    @DisplayName("Should encode and decode Null Bulk String")
    void testNullBulkString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);
        writer.writeAndFlush(RespValue.NULL_BULK_STRING);

        byte[] raw = out.toByteArray();
        assertEquals("$-1\r\n", new String(raw, StandardCharsets.UTF_8));

        RespReader reader = new RespReader(new ByteArrayInputStream(raw));
        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertTrue(decoded.isNull());
    }

    @Test
    @DisplayName("Should encode and decode Array of commands")
    void testArray() throws IOException {
        RespValue command = RespValue.array(List.of(
                RespValue.bulkString("SET"),
                RespValue.bulkString("user:101"),
                RespValue.bulkString("Rishabh")
        ));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);
        writer.writeAndFlush(command);

        byte[] raw = out.toByteArray();
        String expected = "*3\r\n$3\r\nSET\r\n$8\r\nuser:101\r\n$7\r\nRishabh\r\n";
        assertEquals(expected, new String(raw, StandardCharsets.UTF_8));

        RespReader reader = new RespReader(new ByteArrayInputStream(raw));
        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertEquals(RespType.ARRAY, decoded.type());
        assertEquals(3, decoded.array().size());
        assertEquals("SET", decoded.array().get(0).asString());
        assertEquals("user:101", decoded.array().get(1).asString());
        assertEquals("Rishabh", decoded.array().get(2).asString());
    }

    @Test
    @DisplayName("Should parse inline commands from raw string")
    void testInlineCommands() throws IOException {
        String inline = "SET key123 value456\r\n";
        RespReader reader = new RespReader(new ByteArrayInputStream(inline.getBytes(StandardCharsets.UTF_8)));

        RespValue decoded = reader.readValue();
        assertNotNull(decoded);
        assertEquals(RespType.ARRAY, decoded.type());
        assertEquals(3, decoded.array().size());
        assertEquals("SET", decoded.array().get(0).asString());
        assertEquals("key123", decoded.array().get(1).asString());
        assertEquals("value456", decoded.array().get(2).asString());
    }
}
