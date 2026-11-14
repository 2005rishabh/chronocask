package io.chronocask.raft;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.raft.log.RaftLog;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.model.RaftCommand;
import io.chronocask.raft.model.RaftRole;
import io.chronocask.raft.rpc.LocalRaftTransport;
import io.chronocask.raft.state.ChronoCaskStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Raft Network Partition, Leader Failover, and Cluster Healing Tests")
class RaftPartitionAndFailoverTest {

    @TempDir
    Path tempBaseDir;

    private LocalRaftTransport transport;
    private final Map<NodeId, RaftNode> nodes = new HashMap<>();
    private final Map<NodeId, ChronoCask> stores = new HashMap<>();

    @BeforeEach
    void setUp() throws IOException {
        transport = new LocalRaftTransport();

        NodeId n1 = NodeId.of("node-1");
        NodeId n2 = NodeId.of("node-2");
        NodeId n3 = NodeId.of("node-3");
        Set<NodeId> allNodes = Set.of(n1, n2, n3);

        for (NodeId id : allNodes) {
            Set<NodeId> peers = new HashSet<>(allNodes);
            peers.remove(id);

            Path nodeDir = tempBaseDir.resolve(id.id());
            ChronoCaskConfig storeCfg = ChronoCaskConfig.builder(nodeDir).offHeapIndex(true).build();
            ChronoCask store = ChronoCask.open(storeCfg);
            stores.put(id, store);

            ChronoCaskStateMachine sm = new ChronoCaskStateMachine(store);
            RaftConfig raftCfg = RaftConfig.builder(id)
                    .peers(peers)
                    .electionTimeout(70, 140)
                    .heartbeatInterval(25)
                    .build();

            RaftNode node = new RaftNode(raftCfg, new RaftLog(), sm, transport);
            nodes.put(id, node);
            node.start();
        }
    }

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes.values()) {
            node.close();
        }
        for (ChronoCask store : stores.values()) {
            store.close();
        }
        transport.close();
    }

    private RaftNode awaitLeader(Collection<RaftNode> candidates) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 4000;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : candidates) {
                if (node.getRole() == RaftRole.LEADER) {
                    return node;
                }
            }
            Thread.sleep(30);
        }
        throw new IllegalStateException("Failed to elect leader among candidates");
    }

    @Test
    @DisplayName("Cluster should survive leader isolation, elect new leader, and heal cleanly")
    void testPartitionAndFailover() throws Exception {
        // 1. Initial election
        RaftNode initialLeader = awaitLeader(nodes.values());
        assertNotNull(initialLeader);
        NodeId oldLeaderId = initialLeader.getLocalNodeId();

        // 2. Commit initial write
        CompletableFuture<Boolean> w1 = initialLeader.propose(RaftCommand.put("cluster:status", "healthy"));
        assertTrue(w1.get(3, TimeUnit.SECONDS));

        // 3. Partition: Isolate the old leader
        transport.isolate(oldLeaderId);

        // 4. Remaining nodes (2 of 3) form a majority quorum and elect a new leader
        List<RaftNode> remainingNodes = nodes.values().stream()
                .filter(n -> !n.getLocalNodeId().equals(oldLeaderId))
                .toList();

        RaftNode newLeader = awaitLeader(remainingNodes);
        assertNotNull(newLeader);
        assertNotEquals(oldLeaderId, newLeader.getLocalNodeId(), "New leader must be one of the non-isolated nodes");

        // 5. Propose write to new leader: must succeed with 2/3 majority
        CompletableFuture<Boolean> w2 = newLeader.propose(RaftCommand.put("cluster:status", "partitioned-survived"));
        assertTrue(w2.get(3, TimeUnit.SECONDS));

        // 6. Attempt write on old isolated leader: cannot achieve quorum
        CompletableFuture<Boolean> isolatedWrite = initialLeader.propose(RaftCommand.put("ghost", "lost"));
        assertThrows(TimeoutException.class, () -> isolatedWrite.get(400, TimeUnit.MILLISECONDS));

        // 7. Heal the partition
        transport.healAll();

        // Wait a few heartbeats for leader to re-assert authority over reconnected node
        Thread.sleep(200);

        // 8. Reconnected node must recognize new leader and apply replicated changes
        assertEquals(RaftRole.FOLLOWER, initialLeader.getRole(), "Old leader must step down to follower");
        assertEquals(newLeader.getLocalNodeId(), initialLeader.getCurrentLeader(), "Old leader must acknowledge new leader");

        // Verify state machine across all 3 nodes is consistent!
        ChronoCask healedStore = stores.get(oldLeaderId);
        assertEquals("partitioned-survived", healedStore.get("cluster:status"),
                "Healed node must catch up and have latest committed state");
        assertNull(healedStore.get("ghost"), "Uncommitted ghost write on isolated node must not be committed");
    }
}
