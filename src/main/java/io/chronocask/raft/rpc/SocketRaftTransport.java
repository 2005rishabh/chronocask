package io.chronocask.raft.rpc;

import io.chronocask.raft.model.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Production RaftTransport transmitting Raft RPCs over real TCP sockets
 * using Java 25 Virtual Threads and binary length-prefixed framing.
 */
public class SocketRaftTransport implements RaftTransport {

    private static final int CONNECT_TIMEOUT_MS = 300;
    private static final int READ_TIMEOUT_MS = 1000;

    private final NodeId localNodeId;
    private final String bindHost;
    private final int requestedPort;
    private final Map<NodeId, InetSocketAddress> peerAddresses = new ConcurrentHashMap<>();
    private final ExecutorService virtualExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile RaftRpcEndpoint localEndpoint;
    private volatile int boundPort = -1;

    public SocketRaftTransport(NodeId localNodeId, String bindHost, int port, Map<NodeId, InetSocketAddress> peers) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost cannot be null");
        this.requestedPort = port;
        if (peers != null) {
            this.peerAddresses.putAll(peers);
        }
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(bindHost, requestedPort));
        boundPort = serverSocket.getLocalPort();
        running.set(true);

        acceptThread = Thread.ofVirtual()
                .name("raft-tcp-accept-" + localNodeId.id())
                .start(this::acceptLoop);
    }

    public int getPort() {
        return boundPort;
    }

    public void registerPeer(NodeId nodeId, InetSocketAddress address) {
        peerAddresses.put(nodeId, address);
    }

    private void acceptLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(READ_TIMEOUT_MS);
                virtualExecutor.submit(() -> handleInboundConnection(socket));
            } catch (SocketException se) {
                break; // Server socket closed
            } catch (IOException e) {
                if (!running.get()) break;
            }
        }
    }

    private void handleInboundConnection(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 8192));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 8192))) {

            RaftRpcEndpoint endpoint = this.localEndpoint;
            if (endpoint == null) {
                return;
            }

            Object message = RaftRpcCodec.decodeFrame(in);
            if (message instanceof RequestVoteArgs voteArgs) {
                RequestVoteReply reply = endpoint.handleRequestVote(voteArgs);
                RaftRpcCodec.encodeRequestVoteReply(reply, out);
            } else if (message instanceof AppendEntriesArgs appendArgs) {
                AppendEntriesReply reply = endpoint.handleAppendEntries(appendArgs);
                RaftRpcCodec.encodeAppendEntriesReply(reply, out);
            }
        } catch (IOException ignored) {
            // Client closed connection or timeout
        }
    }

    @Override
    public CompletableFuture<RequestVoteReply> sendRequestVote(NodeId target, RequestVoteArgs args) {
        InetSocketAddress targetAddr = peerAddresses.get(target);
        if (targetAddr == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("No known socket address for peer: " + target));
        }

        return CompletableFuture.supplyAsync(() -> {
            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(targetAddr, CONNECT_TIMEOUT_MS);
                socket.setSoTimeout(READ_TIMEOUT_MS);

                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 1024));
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1024));

                RaftRpcCodec.encodeRequestVoteArgs(args, out);
                Object reply = RaftRpcCodec.decodeFrame(in);
                if (reply instanceof RequestVoteReply rvr) {
                    return rvr;
                }
                throw new IOException("Unexpected RPC reply type: " + (reply != null ? reply.getClass() : "null"));
            } catch (IOException e) {
                throw new CompletionException("Failed to send RequestVote to " + target, e);
            }
        }, virtualExecutor);
    }

    @Override
    public CompletableFuture<AppendEntriesReply> sendAppendEntries(NodeId target, AppendEntriesArgs args) {
        InetSocketAddress targetAddr = peerAddresses.get(target);
        if (targetAddr == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("No known socket address for peer: " + target));
        }

        return CompletableFuture.supplyAsync(() -> {
            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(targetAddr, CONNECT_TIMEOUT_MS);
                socket.setSoTimeout(READ_TIMEOUT_MS);

                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 4096));
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1024));

                RaftRpcCodec.encodeAppendEntriesArgs(args, out);
                Object reply = RaftRpcCodec.decodeFrame(in);
                if (reply instanceof AppendEntriesReply aer) {
                    return aer;
                }
                throw new IOException("Unexpected RPC reply type: " + (reply != null ? reply.getClass() : "null"));
            } catch (IOException e) {
                throw new CompletionException("Failed to send AppendEntries to " + target, e);
            }
        }, virtualExecutor);
    }

    @Override
    public void registerEndpoint(NodeId node, RaftRpcEndpoint endpoint) {
        if (node.equals(localNodeId)) {
            this.localEndpoint = endpoint;
        }
    }

    @Override
    public void unregisterEndpoint(NodeId node) {
        if (node.equals(localNodeId)) {
            this.localEndpoint = null;
        }
    }

    @Override
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }

        if (acceptThread != null) {
            acceptThread.interrupt();
        }

        virtualExecutor.shutdownNow();
    }
}
