package io.chronocask.server;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.server.resp.RespReader;
import io.chronocask.server.resp.RespType;
import io.chronocask.server.resp.RespValue;
import io.chronocask.server.resp.RespWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ChronoCask Standalone TCP Server Integration Tests")
class ChronoCaskServerTest {

    @TempDir
    Path tempDir;

    private ChronoCask store;
    private ChronoCaskServer server;

    @BeforeEach
    void setUp() throws IOException {
        ChronoCaskConfig config = ChronoCaskConfig.builder(tempDir)
                .offHeapIndex(true)
                .build();
        store = ChronoCask.open(config);
        server = ChronoCaskServer.create(store, "127.0.0.1", 0);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (store != null) {
            store.close();
        }
    }

    private RespValue executeCommand(RespValue request) throws IOException {
        try (Socket socket = new Socket(server.getHost(), server.getPort());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream())) {

            RespWriter writer = new RespWriter(out);
            RespReader reader = new RespReader(in);

            writer.writeAndFlush(request);
            return reader.readValue();
        }
    }

    @Test
    @DisplayName("Should handle PING with PONG and with echo message")
    void testPing() throws IOException {
        // PING
        RespValue ping = RespValue.array(List.of(RespValue.bulkString("PING")));
        RespValue res1 = executeCommand(ping);
        assertNotNull(res1);
        assertEquals(RespType.SIMPLE_STRING, res1.type());
        assertEquals("PONG", res1.text());

        // PING hello
        RespValue pingEcho = RespValue.array(List.of(RespValue.bulkString("PING"), RespValue.bulkString("hello-world")));
        RespValue res2 = executeCommand(pingEcho);
        assertNotNull(res2);
        assertEquals(RespType.BULK_STRING, res2.type());
        assertEquals("hello-world", res2.asString());
    }

    @Test
    @DisplayName("Should execute SET and GET with binary safety")
    void testSetAndGet() throws IOException {
        RespValue setCmd = RespValue.array(List.of(
                RespValue.bulkString("SET"),
                RespValue.bulkString("user:101"),
                RespValue.bulkString("Alice")
        ));
        RespValue setRes = executeCommand(setCmd);
        assertNotNull(setRes);
        assertEquals(RespType.SIMPLE_STRING, setRes.type());
        assertEquals("OK", setRes.text());

        RespValue getCmd = RespValue.array(List.of(
                RespValue.bulkString("GET"),
                RespValue.bulkString("user:101")
        ));
        RespValue getRes = executeCommand(getCmd);
        assertNotNull(getRes);
        assertEquals(RespType.BULK_STRING, getRes.type());
        assertEquals("Alice", getRes.asString());
    }

    @Test
    @DisplayName("Should return null bulk string when GET misses")
    void testGetMissingKey() throws IOException {
        RespValue getCmd = RespValue.array(List.of(
                RespValue.bulkString("GET"),
                RespValue.bulkString("non_existing_key")
        ));
        RespValue getRes = executeCommand(getCmd);
        assertNotNull(getRes);
        assertTrue(getRes.isNull());
    }

    @Test
    @DisplayName("Should execute DEL and EXISTS commands correctly")
    void testDelAndExists() throws IOException {
        executeCommand(RespValue.array(List.of(RespValue.bulkString("SET"), RespValue.bulkString("k1"), RespValue.bulkString("v1"))));
        executeCommand(RespValue.array(List.of(RespValue.bulkString("SET"), RespValue.bulkString("k2"), RespValue.bulkString("v2"))));

        // EXISTS k1 k2 k3 -> 2
        RespValue existsRes = executeCommand(RespValue.array(List.of(
                RespValue.bulkString("EXISTS"),
                RespValue.bulkString("k1"),
                RespValue.bulkString("k2"),
                RespValue.bulkString("k3")
        )));
        assertNotNull(existsRes);
        assertEquals(RespType.INTEGER, existsRes.type());
        assertEquals(2, existsRes.integer());

        // DEL k1 k3 -> 1 deleted
        RespValue delRes = executeCommand(RespValue.array(List.of(
                RespValue.bulkString("DEL"),
                RespValue.bulkString("k1"),
                RespValue.bulkString("k3")
        )));
        assertNotNull(delRes);
        assertEquals(RespType.INTEGER, delRes.type());
        assertEquals(1, delRes.integer());

        // GET k1 -> null
        RespValue getRes = executeCommand(RespValue.array(List.of(RespValue.bulkString("GET"), RespValue.bulkString("k1"))));
        assertTrue(getRes.isNull());
    }

    @Test
    @DisplayName("Should report accurate DBSIZE and KEYS")
    void testDbSizeAndKeys() throws IOException {
        executeCommand(RespValue.array(List.of(RespValue.bulkString("SET"), RespValue.bulkString("alpha"), RespValue.bulkString("1"))));
        executeCommand(RespValue.array(List.of(RespValue.bulkString("SET"), RespValue.bulkString("beta"), RespValue.bulkString("2"))));

        RespValue sizeRes = executeCommand(RespValue.array(List.of(RespValue.bulkString("DBSIZE"))));
        assertEquals(RespType.INTEGER, sizeRes.type());
        assertEquals(2, sizeRes.integer());

        RespValue keysRes = executeCommand(RespValue.array(List.of(RespValue.bulkString("KEYS"), RespValue.bulkString("*"))));
        assertEquals(RespType.ARRAY, keysRes.type());
        assertEquals(2, keysRes.array().size());
    }

    @Test
    @DisplayName("Should return engine stats for INFO")
    void testInfo() throws IOException {
        RespValue infoRes = executeCommand(RespValue.array(List.of(RespValue.bulkString("INFO"))));
        assertNotNull(infoRes);
        assertEquals(RespType.BULK_STRING, infoRes.type());
        String infoStr = infoRes.asString();
        assertTrue(infoStr.contains("chronocask_version:0.1.0"));
        assertTrue(infoStr.contains("index_type:off-heap"));
    }

    @Test
    @DisplayName("Should execute administrative COMPACT command")
    void testCompact() throws IOException {
        executeCommand(RespValue.array(List.of(RespValue.bulkString("SET"), RespValue.bulkString("foo"), RespValue.bulkString("bar"))));

        RespValue compactRes = executeCommand(RespValue.array(List.of(RespValue.bulkString("COMPACT"))));
        assertNotNull(compactRes);
        assertEquals(RespType.SIMPLE_STRING, compactRes.type());
        assertTrue(compactRes.text().startsWith("OK compacted"));
    }

    @Test
    @DisplayName("Should handle QUIT and close connection cleanly")
    void testQuit() throws IOException {
        try (Socket socket = new Socket(server.getHost(), server.getPort());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream())) {

            RespWriter writer = new RespWriter(out);
            RespReader reader = new RespReader(in);

            writer.writeAndFlush(RespValue.array(List.of(RespValue.bulkString("QUIT"))));
            RespValue res = reader.readValue();
            assertNotNull(res);
            assertEquals("OK", res.text());

            // Next read should be EOF because server closed socket
            assertNull(reader.readValue());
        }
    }

    @Test
    @DisplayName("Should support raw inline commands from client")
    void testInlineCommand() throws IOException {
        try (Socket socket = new Socket(server.getHost(), server.getPort());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream())) {

            // Send raw inline PING\r\n
            out.write("PING\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();

            RespReader reader = new RespReader(in);
            RespValue res = reader.readValue();
            assertNotNull(res);
            assertEquals("PONG", res.text());
        }
    }
}
