package io.chronocask.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.chronocask.raft.log.RaftLog;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.model.RaftRole;
import io.chronocask.raft.model.RequestVoteArgs;
import io.chronocask.raft.model.RequestVoteReply;
import io.chronocask.raft.rpc.LocalRaftTransport;
import io.chronocask.raft.state.RaftStateMachine;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Raft Leader Election and Vote Safety Tests")
class RaftElectionTest {

    private LocalRaftTransport transport;
    private final List<RaftNode> nodes = new ArrayList<>();

    @BeforeEach
    void setUp() {
        transport = new LocalRaftTransport();
    }

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes) {
            node.close();
        }
        transport.close();
    }

    private RaftNode createNode(
        NodeId id,
        Set<NodeId> peers,
        long minTimeout,
        long maxTimeout
    ) {
        RaftConfig cfg = RaftConfig.builder(id)
            .peers(peers)
            .electionTimeout(minTimeout, maxTimeout)
            .heartbeatInterval(30)
            .build();
        RaftStateMachine dummySm = new DummyStateMachine();
        RaftNode node = new RaftNode(cfg, new RaftLog(), dummySm, transport);
        nodes.add(node);
        return node;
    }

    @Test
    @DisplayName("Single-node cluster should automatically elect itself leader")
    void testSingleNodeElection() throws Exception {
        NodeId n1 = NodeId.of("node-1");
        RaftNode node = createNode(n1, Set.of(), 50, 100);
        node.start();

        // Wait up to 1 second for election to trigger
        long deadline = System.currentTimeMillis() + 1000;
        while (
            System.currentTimeMillis() < deadline &&
            node.getRole() != RaftRole.LEADER
        ) {
            Thread.sleep(20);
        }

        assertEquals(RaftRole.LEADER, node.getRole());
        assertEquals(n1, node.getCurrentLeader());
        assertTrue(node.getCurrentTerm() >= 1);
    }

    @Test
    @DisplayName("3-node cluster should elect exactly one leader with quorum")
    void testThreeNodeElection() throws Exception {
        NodeId n1 = NodeId.of("node-1");
        NodeId n2 = NodeId.of("node-2");
        NodeId n3 = NodeId.of("node-3");

        RaftNode node1 = createNode(n1, Set.of(n2, n3), 80, 150);
        RaftNode node2 = createNode(n2, Set.of(n1, n3), 80, 150);
        RaftNode node3 = createNode(n3, Set.of(n1, n2), 80, 150);

        node1.start();
        node2.start();
        node3.start();

        // Wait for leader election
        long deadline = System.currentTimeMillis() + 3000;
        RaftNode leader = null;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : List.of(node1, node2, node3)) {
                if (n.getRole() == RaftRole.LEADER) {
                    leader = n;
                    break;
                }
            }
            if (leader != null) break;
            Thread.sleep(30);
        }

        assertNotNull(leader, "A cluster leader should have been elected");
        assertEquals(RaftRole.LEADER, leader.getRole());

        // Allow heartbeats to stabilize cluster states
        Thread.sleep(150);

        // Count leaders in the cluster: must be exactly 1
        int leaderCount = 0;
        for (RaftNode n : List.of(node1, node2, node3)) {
            if (n.getRole() == RaftRole.LEADER) {
                leaderCount++;
            }
        }
        assertEquals(
            1,
            leaderCount,
            "Exactly one leader should exist in a 3-node cluster"
        );
    }

    @Test
    @DisplayName("Should reject vote if candidate log is less up-to-date")
    void testVoteLogCompletenessCheck() {
        NodeId n1 = NodeId.of("node-1");
        RaftNode node = createNode(n1, Set.of(NodeId.of("node-2")), 200, 400);
        node.start();

        // Simulate local log having term 2, index 5
        node.getLog().append(1, new byte[] { 1 });
        node.getLog().append(2, new byte[] { 2 });

        // Candidate requests vote with term 3, but lastLogTerm 1 (stale log!)
        RequestVoteArgs staleCandidate = new RequestVoteArgs(
            3,
            NodeId.of("node-2"),
            10,
            1
        );
        RequestVoteReply reply = node.handleRequestVote(staleCandidate);

        assertFalse(
            reply.voteGranted(),
            "Should reject vote because candidate lastLogTerm (1) < local lastLogTerm (2)"
        );

        // Candidate with term 3 and lastLogTerm 2, lastLogIndex 2 (up to date!)
        RequestVoteArgs freshCandidate = new RequestVoteArgs(
            3,
            NodeId.of("node-2"),
            2,
            2
        );
        RequestVoteReply reply2 = node.handleRequestVote(freshCandidate);

        assertTrue(
            reply2.voteGranted(),
            "Should grant vote because candidate log is up to date"
        );
    }

    private static class DummyStateMachine implements RaftStateMachine {

        private long lastApplied = 0;

        @Override
        public void apply(io.chronocask.raft.model.LogEntry entry) {
            lastApplied = entry.index();
        }

        @Override
        public byte[] query(byte[] key) {
            return null;
        }

        @Override
        public long lastAppliedIndex() {
            return lastApplied;
        }
    }
}
