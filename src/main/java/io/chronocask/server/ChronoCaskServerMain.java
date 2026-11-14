package io.chronocask.server;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.raft.DistributedChronoCask;
import io.chronocask.raft.RaftConfig;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.rpc.SocketRaftTransport;
import io.chronocask.segment.DurabilityMode;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Main command-line application launcher for ChronoCask Server.
 * Supports both single-node standalone mode and distributed multi-node Raft clusters.
 */
public final class ChronoCaskServerMain {

    private static final String BANNER = """
       ______ __                                  ______           __
      / ____// /_   _____ ____   ____   ____     / ____/____ _ _____/ /__
     / /    / __ \\ / ___// __ \\ / __ \\ / __ \\   / /    / __ `/ ___// //_/
    / /___ / / / // /   / /_/ // / / // /_/ /  / /___ / /_/ /(__  )/ ,<
    \\____//_/ /_//_/    \\____//_/ /_/ \\____/   \\____/ \\__,_//____//_/|_|
                                    Distributed Storage Engine 0.1.0
    """;

    public static void main(String[] args) {
        System.out.println(BANNER);

        int port = 7379;
        String host = "0.0.0.0";
        Path dataDir = Path.of("./data");
        boolean offHeap = true;
        DurabilityMode durability = DurabilityMode.ASYNC;
        long maxSegmentSizeBytes = 64 * 1024 * 1024L;

        String nodeIdStr = null;
        int raftPort = 9379;
        String peersStr = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port", "-p" -> {
                    if (i + 1 < args.length) port = Integer.parseInt(args[++i]);
                }
                case "--host", "-h" -> {
                    if (i + 1 < args.length) host = args[++i];
                }
                case "--dir", "-d" -> {
                    if (i + 1 < args.length) dataDir = Path.of(args[++i]);
                }
                case "--off-heap" -> {
                    if (i + 1 < args.length) offHeap = Boolean.parseBoolean(
                        args[++i]
                    );
                }
                case "--sync" -> durability = DurabilityMode.SYNC;
                case "--segment-size-mb" -> {
                    if (i + 1 < args.length) {
                        maxSegmentSizeBytes =
                            Long.parseLong(args[++i]) * 1024 * 1024L;
                    }
                }
                case "--node-id" -> {
                    if (i + 1 < args.length) nodeIdStr = args[++i];
                }
                case "--raft-port" -> {
                    if (i + 1 < args.length) raftPort = Integer.parseInt(
                        args[++i]
                    );
                }
                case "--peers" -> {
                    if (i + 1 < args.length) peersStr = args[++i];
                }
            }
        }

        try {
            ChronoCaskConfig config = ChronoCaskConfig.builder(dataDir)
                .offHeapIndex(offHeap)
                .defaultDurability(durability)
                .maxSegmentSizeBytes(maxSegmentSizeBytes)
                .build();

            boolean clusterMode = nodeIdStr != null || peersStr != null;

            if (!clusterMode) {
                // Standalone Single-Node Mode
                System.out.printf(
                    "[ChronoCask] Opening standalone storage engine at '%s' (off-heap=%b, durability=%s)%n",
                    dataDir.toAbsolutePath(),
                    offHeap,
                    durability
                );
                ChronoCask store = ChronoCask.open(config);

                ChronoCaskServer server = ChronoCaskServer.create(
                    store,
                    host,
                    port
                );
                server.start();

                System.out.printf(
                    "[ChronoCask] Ready to accept Redis/RESP connections on %s:%d%n",
                    host,
                    server.getPort()
                );
                System.out.println("[ChronoCask] Press Ctrl+C to terminate.");

                Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        System.out.println(
                            "[ChronoCask] Gracefully shutting down server..."
                        );
                        server.stop();
                        store.close();
                        System.out.println(
                            "[ChronoCask] Storage cleanly flushed and closed."
                        );
                    })
                );

                Thread.currentThread().join();
            } else {
                // Distributed Multi-Node Raft Cluster Mode
                NodeId localNodeId = NodeId.of(
                    nodeIdStr != null ? nodeIdStr : "node-1"
                );
                Map<NodeId, InetSocketAddress> peerMap = parsePeers(peersStr);

                System.out.printf(
                    "[ChronoCask Raft] Bootstrapping cluster node '%s' (raftPort=%d, peers=%s)...%n",
                    localNodeId.id(),
                    raftPort,
                    peerMap.keySet()
                );

                SocketRaftTransport transport = new SocketRaftTransport(
                    localNodeId,
                    host,
                    raftPort,
                    peerMap
                );
                transport.start();

                RaftConfig raftConfig = RaftConfig.builder(localNodeId)
                    .peers(peerMap.keySet())
                    .build();

                DistributedChronoCask distStore = new DistributedChronoCask(
                    config,
                    raftConfig,
                    transport
                );
                distStore.start();

                ChronoCaskServer server = ChronoCaskServer.create(
                    distStore,
                    host,
                    port
                );
                server.start();

                System.out.printf(
                    "[ChronoCask Cluster] Node '%s' accepting Redis/RESP on %s:%d (Raft RPC on port %d)%n",
                    localNodeId.id(),
                    host,
                    server.getPort(),
                    transport.getPort()
                );
                System.out.println(
                    "[ChronoCask Cluster] Press Ctrl+C to terminate."
                );

                Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        System.out.println(
                            "[ChronoCask Cluster] Gracefully shutting down cluster node..."
                        );
                        server.stop();
                        try {
                            distStore.close();
                        } catch (Exception ignored) {}
                        transport.close();
                        System.out.println(
                            "[ChronoCask Cluster] Node cleanly stopped."
                        );
                    })
                );

                Thread.currentThread().join();
            }
        } catch (Exception e) {
            System.err.println(
                "[ChronoCask] Fatal error starting server: " + e.getMessage()
            );
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Map<NodeId, InetSocketAddress> parsePeers(String peersStr) {
        Map<NodeId, InetSocketAddress> map = new HashMap<>();
        if (peersStr == null || peersStr.isBlank()) {
            return map;
        }

        String[] tokens = peersStr.split(",");
        for (String token : tokens) {
            String[] parts = token.trim().split("=");
            if (parts.length == 2) {
                NodeId pId = NodeId.of(parts[0].trim());
                String[] hostPort = parts[1].trim().split(":");
                String pHost = hostPort[0];
                int pPort = Integer.parseInt(hostPort[1]);
                map.put(pId, new InetSocketAddress(pHost, pPort));
            }
        }
        return map;
    }
}
