package io.chronocask.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.raft.log.RaftLog;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.model.RaftCommand;
import io.chronocask.raft.model.RaftRole;
import io.chronocask.raft.rpc.LocalRaftTransport;
import io.chronocask.raft.state.ChronoCaskStateMachine;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Raft Log Replication and State Machine Commit Integration Tests")
class RaftReplicationTest {

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
            ChronoCaskConfig storeCfg = ChronoCaskConfig.builder(nodeDir)
                .offHeapIndex(true)
                .build();
            ChronoCask store = ChronoCask.open(storeCfg);
            stores.put(id, store);

            ChronoCaskStateMachine sm = new ChronoCaskStateMachine(store);
            RaftConfig raftCfg = RaftConfig.builder(id)
                .peers(peers)
                .electionTimeout(80, 160)
                .heartbeatInterval(30)
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

    private RaftNode awaitLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 4000;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : nodes.values()) {
                if (node.getRole() == RaftRole.LEADER) {
                    return node;
                }
            }
            Thread.sleep(30);
        }
        throw new IllegalStateException("Failed to elect leader in time");
    }

    @Test
    @DisplayName(
        "Leader write proposal should replicate to all nodes and apply to state machine"
    )
    void testLogReplicationAndStateMachineCommit() throws Exception {
        RaftNode leader = awaitLeader();
        assertNotNull(leader);

        // Propose PUT command
        RaftCommand cmd = RaftCommand.put("user:101", "Rishabh");
        CompletableFuture<Boolean> commitFuture = leader.propose(cmd);

        // Await consensus quorum
        Boolean success = commitFuture.get(3, TimeUnit.SECONDS);
        assertTrue(success, "Replication must succeed on quorum");

        // Verify leader's local store
        ChronoCask leaderStore = stores.get(leader.getLocalNodeId());
        assertEquals("Rishabh", leaderStore.get("user:101"));

        // Wait a short heartbeat duration for followers to advance their commit index
        Thread.sleep(150);

        // Verify all 3 stores have identical data
        for (Map.Entry<NodeId, ChronoCask> entry : stores.entrySet()) {
            assertEquals(
                "Rishabh",
                entry.getValue().get("user:101"),
                "Node " +
                    entry.getKey() +
                    " must have replicated and applied the key"
            );
        }

        // Propose DELETE command
        RaftCommand delCmd = RaftCommand.delete("user:101");
        CompletableFuture<Boolean> delFuture = leader.propose(delCmd);
        assertTrue(delFuture.get(3, TimeUnit.SECONDS));

        Thread.sleep(150);
        for (Map.Entry<NodeId, ChronoCask> entry : stores.entrySet()) {
            assertNull(
                entry.getValue().get("user:101"),
                "Node " + entry.getKey() + " must have deleted the key"
            );
        }
    }

    @Test
    @DisplayName("Follower should reject proposal and point to leader")
    void testFollowerRejection() throws Exception {
        RaftNode leader = awaitLeader();
        RaftNode follower = nodes
            .values()
            .stream()
            .filter(n -> n.getRole() == RaftRole.FOLLOWER)
            .findFirst()
            .orElseThrow();

        CompletableFuture<Boolean> future = follower.propose(
            RaftCommand.put("k", "v")
        );
        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            future.get(1, TimeUnit.SECONDS)
        );
        assertTrue(ex.getCause() instanceof NotLeaderException);

        NotLeaderException nle = (NotLeaderException) ex.getCause();
        assertEquals(leader.getLocalNodeId(), nle.getLeaderId());
    }
}
