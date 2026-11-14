package io.chronocask.raft;

import io.chronocask.raft.model.NodeId;

/**
 * Thrown when a write mutation is proposed to a non-leader Raft node.
 */
public class NotLeaderException extends RuntimeException {

    private final NodeId leaderId;

    public NotLeaderException(NodeId leaderId) {
        super(leaderId != null
                ? "Node is not the leader. Current cluster leader is: " + leaderId
                : "Node is not the leader and current cluster leader is unknown");
        this.leaderId = leaderId;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }
}
