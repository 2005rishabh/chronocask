package io.chronocask.server;

import io.chronocask.api.ChronoCask;
import io.chronocask.raft.DistributedChronoCask;
import io.chronocask.server.command.CommandHandler;
import io.chronocask.server.command.CommandResult;
import io.chronocask.server.resp.RespReader;
import io.chronocask.server.resp.RespValue;
import io.chronocask.server.resp.RespWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-performance standalone or distributed TCP server for ChronoCask supporting the
 * Redis Serialization Protocol (RESP2) using Java 25 Virtual Threads.
 */
public final class ChronoCaskServer implements Closeable {

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 7379;
    private static final int BUFFER_SIZE = 64 * 1024; // 64 KB socket/stream buffers

    private final ChronoCask store;
    private final String host;
    private final int requestedPort;
    private final CommandHandler commandHandler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket serverSocket;
    private ExecutorService virtualThreadExecutor;
    private Thread acceptThread;
    private volatile int boundPort = -1;

    private ChronoCaskServer(ChronoCask store, String host, int port) {
        this.store = Objects.requireNonNull(store, "store cannot be null");
        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.requestedPort = port;
        this.commandHandler = new CommandHandler(store);
    }

    private ChronoCaskServer(
        DistributedChronoCask distributedStore,
        String host,
        int port
    ) {
        Objects.requireNonNull(
            distributedStore,
            "distributedStore cannot be null"
        );
        this.store = distributedStore.getStore();
        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.requestedPort = port;
        this.commandHandler = new CommandHandler(distributedStore);
    }

    public static ChronoCaskServer create(ChronoCask store) {
        return create(store, DEFAULT_HOST, DEFAULT_PORT);
    }

    public static ChronoCaskServer create(ChronoCask store, int port) {
        return create(store, DEFAULT_HOST, port);
    }

    public static ChronoCaskServer create(
        ChronoCask store,
        String host,
        int port
    ) {
        return new ChronoCaskServer(store, host, port);
    }

    public static ChronoCaskServer create(
        DistributedChronoCask distributedStore
    ) {
        return create(distributedStore, DEFAULT_HOST, DEFAULT_PORT);
    }

    public static ChronoCaskServer create(
        DistributedChronoCask distributedStore,
        int port
    ) {
        return create(distributedStore, DEFAULT_HOST, port);
    }

    public static ChronoCaskServer create(
        DistributedChronoCask distributedStore,
        String host,
        int port
    ) {
        return new ChronoCaskServer(distributedStore, host, port);
    }

    /**
     * Starts listening for client connections.
     */
    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(host, requestedPort));
        boundPort = serverSocket.getLocalPort();

        virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        running.set(true);

        acceptThread = Thread.ofVirtual()
            .name("chronocask-accept-", boundPort)
            .start(this::acceptLoop);
    }

    private void acceptLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                clientSocket.setReceiveBufferSize(BUFFER_SIZE);
                clientSocket.setSendBufferSize(BUFFER_SIZE);
                virtualThreadExecutor.submit(() -> handleClient(clientSocket));
            } catch (SocketException se) {
                // Server socket closed during shutdown
                break;
            } catch (IOException e) {
                if (!running.get()) {
                    break;
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try {
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(BUFFER_SIZE);
            socket.setSendBufferSize(BUFFER_SIZE);
        } catch (SocketException ignored) {}

        try (
            socket;
            BufferedInputStream in = new BufferedInputStream(
                socket.getInputStream(),
                BUFFER_SIZE
            );
            BufferedOutputStream out = new BufferedOutputStream(
                socket.getOutputStream(),
                BUFFER_SIZE
            )
        ) {
            RespReader reader = new RespReader(in);
            RespWriter writer = new RespWriter(out);

            while (running.get() && !socket.isClosed()) {
                RespValue request = reader.readValue();
                if (request == null) {
                    break; // Client closed connection
                }

                CommandResult result = commandHandler.execute(request);
                writer.write(result.response());

                // Smart batch flush: Only flush to the OS kernel when the pipelined batch in the input buffer is drained
                if (!reader.hasBufferedBytes() || result.closeConnection()) {
                    writer.flush();
                }

                if (result.closeConnection()) {
                    break;
                }
            }
        } catch (IOException ignored) {
            // Socket reset or client disconnected
        }
    }

    /**
     * Returns the port the server is actively listening on.
     */
    public int getPort() {
        return boundPort;
    }

    public String getHost() {
        return host;
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Stops the server and closes all resources.
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}

        if (acceptThread != null) {
            acceptThread.interrupt();
        }

        if (virtualThreadExecutor != null) {
            virtualThreadExecutor.shutdownNow();
            try {
                virtualThreadExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        stop();
    }
}
