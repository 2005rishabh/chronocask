package io.chronocask.raft.rpc;

import io.chronocask.raft.model.*;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Transport contract for transmitting Raft RPCs asynchronously between cluster nodes.
 */
public interface RaftTransport extends Closeable {

    /**
     * Sends a RequestVote RPC asynchronously to the target node.
     */
    CompletableFuture<RequestVoteReply> sendRequestVote(NodeId target, RequestVoteArgs args);

    /**
     * Sends an AppendEntries RPC asynchronously to the target node.
     */
    CompletableFuture<AppendEntriesReply> sendAppendEntries(NodeId target, AppendEntriesArgs args);

    /**
     * Registers a local node endpoint with this transport to receive incoming calls.
     */
    void registerEndpoint(NodeId node, RaftRpcEndpoint endpoint);

    /**
     * Unregisters a node endpoint from this transport.
     */
    void unregisterEndpoint(NodeId node);

    @Override
    default void close() {
    }
}
