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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ChronoCask Server Concurrent Client Load Test")
class ConcurrentServerLoadTest {

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

    @Test
    @DisplayName("Should sustain high concurrency across parallel virtual thread clients")
    void testConcurrentClients() throws Exception {
        int numClients = 16;
        int opsPerClient = 100;
        ExecutorService clientPool = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numClients);
        AtomicInteger successfulOps = new AtomicInteger(0);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int c = 0; c < numClients; c++) {
            final int clientId = c;
            clientPool.submit(() -> {
                try {
                    startLatch.await();
                    try (Socket socket = new Socket(server.getHost(), server.getPort());
                         BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 8192);
                         BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 8192)) {

                        RespWriter writer = new RespWriter(out);
                        RespReader reader = new RespReader(in);

                        for (int i = 0; i < opsPerClient; i++) {
                            String key = "client:" + clientId + ":key:" + i;
                            String val = "val:" + clientId + ":" + i;

                            // SET
                            writer.write(RespValue.array(List.of(
                                    RespValue.bulkString("SET"),
                                    RespValue.bulkString(key),
                                    RespValue.bulkString(val)
                            )));
                            writer.flush();
                            RespValue setRes = reader.readValue();
                            assertEquals(RespType.SIMPLE_STRING, setRes.type());
                            assertEquals("OK", setRes.text());
                            successfulOps.incrementAndGet();

                            // GET
                            writer.write(RespValue.array(List.of(
                                    RespValue.bulkString("GET"),
                                    RespValue.bulkString(key)
                            )));
                            writer.flush();
                            RespValue getRes = reader.readValue();
                            assertEquals(RespType.BULK_STRING, getRes.type());
                            assertEquals(val, getRes.asString());
                            successfulOps.incrementAndGet();
                        }
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "Concurrent clients timed out");
        assertTrue(errors.isEmpty(), "Encountered client errors: " + errors);
        assertEquals(numClients * opsPerClient * 2, successfulOps.get());
        clientPool.shutdown();
    }
}
