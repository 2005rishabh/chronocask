package io.chronocask.raft;

import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.rpc.LocalRaftTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DistributedChronoCask High-Level Client API Integration Tests")
class DistributedChronoCaskTest {

    @TempDir
    Path tempBaseDir;

    private LocalRaftTransport transport;
    private final Map<NodeId, DistributedChronoCask> cluster = new HashMap<>();

    @BeforeEach
    void setUp() {
        transport = new LocalRaftTransport();

        NodeId n1 = NodeId.of("cluster-node-1");
        NodeId n2 = NodeId.of("cluster-node-2");
        NodeId n3 = NodeId.of("cluster-node-3");
        Set<NodeId> all = Set.of(n1, n2, n3);

        for (NodeId id : all) {
            Set<NodeId> peers = new HashSet<>(all);
            peers.remove(id);

            DistributedChronoCask node = DistributedChronoCask.open(
                    tempBaseDir.resolve(id.id()),
                    id,
                    peers,
                    transport
            );
            cluster.put(id, node);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        for (DistributedChronoCask node : cluster.values()) {
            node.close();
        }
        transport.close();
    }

    private DistributedChronoCask awaitLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 4000;
        while (System.currentTimeMillis() < deadline) {
            for (DistributedChronoCask node : cluster.values()) {
                if (node.isLeader()) {
                    return node;
                }
            }
            Thread.sleep(30);
        }
        throw new IllegalStateException("Failed to elect leader");
    }

    @Test
    @DisplayName("Should execute distributed PUT, GET, and DELETE through leader node")
    void testDistributedOperations() throws Exception {
        DistributedChronoCask leader = awaitLeader();
        assertNotNull(leader);

        // 1. PUT
        leader.put("config:cache_size", "1024MB");

        // 2. Local read on leader
        assertEquals("1024MB", leader.get("config:cache_size"));

        // Wait a brief moment for follower commit application
        Thread.sleep(150);

        // 3. Follower reads
        for (DistributedChronoCask node : cluster.values()) {
            assertEquals("1024MB", node.get("config:cache_size"),
                    "Node " + node.getNodeId() + " must have the replicated entry");
        }

        // 4. DELETE
        leader.delete("config:cache_size");
        Thread.sleep(150);

        for (DistributedChronoCask node : cluster.values()) {
            assertNull(node.get("config:cache_size"),
                    "Node " + node.getNodeId() + " must have deleted the entry");
        }
    }
}
