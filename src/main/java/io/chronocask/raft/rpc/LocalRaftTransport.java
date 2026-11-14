package io.chronocask.raft.rpc;

import io.chronocask.raft.model.AppendEntriesArgs;
import io.chronocask.raft.model.AppendEntriesReply;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.model.RequestVoteArgs;
import io.chronocask.raft.model.RequestVoteReply;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Thread-safe in-memory RaftTransport enabling fast intra-process clusters
 * and programmable network failure injection (partitions, isolation, packet drops).
 */
public class LocalRaftTransport implements RaftTransport {

    private final Map<NodeId, RaftRpcEndpoint> endpoints = new ConcurrentHashMap<>();
    private final Set<NodeId> isolatedNodes = ConcurrentHashMap.newKeySet();
    private final Map<NodeId, Set<NodeId>> blockedConnections = new ConcurrentHashMap<>();
    private final ExecutorService dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public void registerEndpoint(NodeId node, RaftRpcEndpoint endpoint) {
        endpoints.put(node, endpoint);
    }

    @Override
    public void unregisterEndpoint(NodeId node) {
        endpoints.remove(node);
        isolatedNodes.remove(node);
        blockedConnections.remove(node);
    }

    @Override
    public CompletableFuture<RequestVoteReply> sendRequestVote(NodeId target, RequestVoteArgs args) {
        if (!canCommunicate(args.candidateId(), target)) {
            return CompletableFuture.failedFuture(new RuntimeException("Network link broken between " + args.candidateId() + " and " + target));
        }

        RaftRpcEndpoint endpoint = endpoints.get(target);
        if (endpoint == null) {
            return CompletableFuture.failedFuture(new RuntimeException("Endpoint not registered for node: " + target));
        }

        return CompletableFuture.supplyAsync(() -> endpoint.handleRequestVote(args), dispatchExecutor);
    }

    @Override
    public CompletableFuture<AppendEntriesReply> sendAppendEntries(NodeId target, AppendEntriesArgs args) {
        if (!canCommunicate(args.leaderId(), target)) {
            return CompletableFuture.failedFuture(new RuntimeException("Network link broken between " + args.leaderId() + " and " + target));
        }

        RaftRpcEndpoint endpoint = endpoints.get(target);
        if (endpoint == null) {
            return CompletableFuture.failedFuture(new RuntimeException("Endpoint not registered for node: " + target));
        }

        return CompletableFuture.supplyAsync(() -> endpoint.handleAppendEntries(args), dispatchExecutor);
    }

    private boolean canCommunicate(NodeId from, NodeId to) {
        if (from == null || to == null) return false;
        if (isolatedNodes.contains(from) || isolatedNodes.contains(to)) {
            return false;
        }
        Set<NodeId> blocked = blockedConnections.get(from);
        return blocked == null || !blocked.contains(to);
    }

    // --- Network Failure Simulation API ---

    /**
     * Isolates a node from all communication (incoming and outgoing).
     */
    public void isolate(NodeId node) {
        isolatedNodes.add(node);
    }

    /**
     * Reconnects an isolated node back to the network.
     */
    public void heal(NodeId node) {
        isolatedNodes.remove(node);
        blockedConnections.remove(node);
    }

    /**
     * Creates a two-way network partition between partition A and partition B.
     */
    public void partition(Set<NodeId> groupA, Set<NodeId> groupB) {
        for (NodeId a : groupA) {
            blockedConnections.computeIfAbsent(a, k -> ConcurrentHashMap.newKeySet()).addAll(groupB);
        }
        for (NodeId b : groupB) {
            blockedConnections.computeIfAbsent(b, k -> ConcurrentHashMap.newKeySet()).addAll(groupA);
        }
    }

    /**
     * Heals all network partitions and isolates across all nodes.
     */
    public void healAll() {
        isolatedNodes.clear();
        blockedConnections.clear();
    }

    @Override
    public void close() {
        dispatchExecutor.shutdownNow();
    }
}
