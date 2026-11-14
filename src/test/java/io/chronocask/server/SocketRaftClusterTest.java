package io.chronocask.server;

import static org.junit.jupiter.api.Assertions.*;

import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.raft.DistributedChronoCask;
import io.chronocask.raft.RaftConfig;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.rpc.SocketRaftTransport;
import io.chronocask.server.resp.RespReader;
import io.chronocask.server.resp.RespType;
import io.chronocask.server.resp.RespValue;
import io.chronocask.server.resp.RespWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Multi-Node TCP Raft Cluster & Redis Server Integration Tests")
class SocketRaftClusterTest {

    @TempDir
    Path tempBaseDir;

    private static class ClusterMember {

        final NodeId id;
        final int clientPort;
        final int raftPort;
        final Path dataDir;
        SocketRaftTransport transport;
        DistributedChronoCask distStore;
        ChronoCaskServer server;

        ClusterMember(NodeId id, int clientPort, int raftPort, Path dataDir) {
            this.id = id;
            this.clientPort = clientPort;
            this.raftPort = raftPort;
            this.dataDir = dataDir;
        }

        void stop() {
            if (server != null) server.stop();
            if (distStore != null) {
                try {
                    distStore.close();
                } catch (Exception ignored) {}
            }
            if (transport != null) transport.close();
        }
    }

    private final Map<NodeId, ClusterMember> members = new HashMap<>();

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        NodeId n1 = NodeId.of("node-1");
        NodeId n2 = NodeId.of("node-2");
        NodeId n3 = NodeId.of("node-3");

        int cPort1 = findFreePort();
        int cPort2 = findFreePort();
        int cPort3 = findFreePort();

        int rPort1 = findFreePort();
        int rPort2 = findFreePort();
        int rPort3 = findFreePort();

        members.put(
            n1,
            new ClusterMember(n1, cPort1, rPort1, tempBaseDir.resolve("node-1"))
        );
        members.put(
            n2,
            new ClusterMember(n2, cPort2, rPort2, tempBaseDir.resolve("node-2"))
        );
        members.put(
            n3,
            new ClusterMember(n3, cPort3, rPort3, tempBaseDir.resolve("node-3"))
        );

        Map<NodeId, InetSocketAddress> allRaftPeers = new HashMap<>();
        allRaftPeers.put(n1, new InetSocketAddress("127.0.0.1", rPort1));
        allRaftPeers.put(n2, new InetSocketAddress("127.0.0.1", rPort2));
        allRaftPeers.put(n3, new InetSocketAddress("127.0.0.1", rPort3));

        for (ClusterMember member : members.values()) {
            Map<NodeId, InetSocketAddress> peerMap = new HashMap<>(
                allRaftPeers
            );
            peerMap.remove(member.id);

            member.transport = new SocketRaftTransport(
                member.id,
                "127.0.0.1",
                member.raftPort,
                peerMap
            );
            member.transport.start();

            ChronoCaskConfig storeConfig = ChronoCaskConfig.builder(
                member.dataDir
            )
                .offHeapIndex(true)
                .build();
            RaftConfig raftConfig = RaftConfig.builder(member.id)
                .peers(peerMap.keySet())
                .electionTimeout(150, 300)
                .heartbeatInterval(40)
                .build();

            member.distStore = new DistributedChronoCask(
                storeConfig,
                raftConfig,
                member.transport
            );
            member.distStore.start();

            member.server = ChronoCaskServer.create(
                member.distStore,
                "127.0.0.1",
                member.clientPort
            );
            member.server.start();
        }
    }

    @AfterEach
    void tearDown() {
        for (ClusterMember m : members.values()) {
            m.stop();
        }
    }

    private RespValue sendRedisCommand(int port, RespValue request)
        throws IOException {
        try (
            Socket socket = new Socket("127.0.0.1", port);
            BufferedOutputStream out = new BufferedOutputStream(
                socket.getOutputStream(),
                8192
            );
            BufferedInputStream in = new BufferedInputStream(
                socket.getInputStream(),
                8192
            )
        ) {
            RespWriter writer = new RespWriter(out);
            RespReader reader = new RespReader(in);

            writer.writeAndFlush(request);
            return reader.readValue();
        }
    }

    private ClusterMember awaitLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 6000;
        while (System.currentTimeMillis() < deadline) {
            for (ClusterMember m : members.values()) {
                if (m.distStore.isLeader()) {
                    return m;
                }
            }
            Thread.sleep(30);
        }
        throw new IllegalStateException(
            "Failed to elect leader over TCP Raft transport"
        );
    }

    @Test
    @DisplayName(
        "Should elect leader, replicate SET/GET over TCP, reject follower writes, and failover"
    )
    void testClusterLifecycleOverTcp() throws Exception {
        // 1. Await TCP Raft leader election
        ClusterMember leader = awaitLeader();
        assertNotNull(
            leader,
            "Cluster must elect a leader over real TCP sockets"
        );

        // Allow leadership heartbeats to stabilize
        Thread.sleep(120);

        // Find a follower
        ClusterMember follower = members
            .values()
            .stream()
            .filter(m -> !m.id.equals(leader.id))
            .findFirst()
            .orElseThrow();

        // 2. Query INFO from leader over Redis protocol
        RespValue infoRes = sendRedisCommand(
            leader.clientPort,
            RespValue.array(List.of(RespValue.bulkString("INFO")))
        );
        assertEquals(RespType.BULK_STRING, infoRes.type());
        String infoStr = infoRes.asString();
        assertTrue(infoStr.contains("raft_enabled:true"));
        assertTrue(infoStr.contains("role:LEADER"));

        // 3. Propose SET on leader via Redis protocol
        RespValue setCmd = RespValue.array(
            List.of(
                RespValue.bulkString("SET"),
                RespValue.bulkString("cluster:key:alpha"),
                RespValue.bulkString("omega-value")
            )
        );
        RespValue setReply = sendRedisCommand(leader.clientPort, setCmd);
        assertEquals(
            RespType.SIMPLE_STRING,
            setReply.type(),
            "Expected simple string OK but got: " + setReply.text()
        );
        assertEquals("OK", setReply.text());

        // 4. Follower should reject direct write with NOT_LEADER error pointing to leader
        RespValue followerSet = sendRedisCommand(follower.clientPort, setCmd);
        assertEquals(RespType.ERROR, followerSet.type());
        assertTrue(
            followerSet.text().startsWith("NOT_LEADER"),
            "Follower must reject write: " + followerSet.text()
        );

        // Wait a brief heartbeat for follower to apply state
        Thread.sleep(150);

        // 5. Query GET on ALL 3 nodes via standard Redis client: must return replicated value
        RespValue getCmd = RespValue.array(
            List.of(
                RespValue.bulkString("GET"),
                RespValue.bulkString("cluster:key:alpha")
            )
        );
        for (ClusterMember m : members.values()) {
            RespValue getReply = sendRedisCommand(m.clientPort, getCmd);
            assertEquals(RespType.BULK_STRING, getReply.type());
            assertEquals(
                "omega-value",
                getReply.asString(),
                "Node " + m.id + " must hold replicated value"
            );
        }

        // 6. Leader Failover Test: Crash/Stop current leader
        NodeId crashedLeaderId = leader.id;
        leader.stop();

        // 7. Surviving 2 nodes must elect a new leader
        long failoverDeadline = System.currentTimeMillis() + 6000;
        ClusterMember newLeader = null;
        while (System.currentTimeMillis() < failoverDeadline) {
            for (ClusterMember m : members.values()) {
                if (!m.id.equals(crashedLeaderId) && m.distStore.isLeader()) {
                    newLeader = m;
                    break;
                }
            }
            if (newLeader != null) break;
            Thread.sleep(30);
        }

        assertNotNull(
            newLeader,
            "Surviving nodes must elect a new leader upon failover"
        );
        assertNotEquals(crashedLeaderId, newLeader.id);

        Thread.sleep(100);

        // 8. New leader must accept new writes and achieve quorum with surviving follower
        RespValue setFailoverCmd = RespValue.array(
            List.of(
                RespValue.bulkString("SET"),
                RespValue.bulkString("cluster:status"),
                RespValue.bulkString("failover-recovered")
            )
        );
        RespValue failoverSetReply = sendRedisCommand(
            newLeader.clientPort,
            setFailoverCmd
        );
        assertEquals(
            RespType.SIMPLE_STRING,
            failoverSetReply.type(),
            "Expected OK but got: " + failoverSetReply.text()
        );
        assertEquals("OK", failoverSetReply.text());
    }
}
