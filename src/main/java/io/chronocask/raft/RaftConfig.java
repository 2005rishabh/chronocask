package io.chronocask.raft;

import io.chronocask.raft.model.NodeId;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration options for a Raft consensus node.
 */
public record RaftConfig(
        NodeId localNodeId,
        Set<NodeId> peers,
        long minElectionTimeoutMs,
        long maxElectionTimeoutMs,
        long heartbeatIntervalMs
) {

    public static final long DEFAULT_MIN_ELECTION_TIMEOUT_MS = 150;
    public static final long DEFAULT_MAX_ELECTION_TIMEOUT_MS = 300;
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 50;

    public RaftConfig {
        Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
        peers = peers != null ? Set.copyOf(peers) : Collections.emptySet();
        if (minElectionTimeoutMs <= 0 || maxElectionTimeoutMs <= minElectionTimeoutMs) {
            throw new IllegalArgumentException("Invalid election timeout window: ["
                    + minElectionTimeoutMs + ", " + maxElectionTimeoutMs + "]");
        }
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs >= minElectionTimeoutMs) {
            throw new IllegalArgumentException("Heartbeat interval must be > 0 and < minElectionTimeout");
        }
    }

    public static Builder builder(NodeId localNodeId) {
        return new Builder(localNodeId);
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    public int quorumSize() {
        return (clusterSize() / 2) + 1;
    }

    public static class Builder {
        private final NodeId localNodeId;
        private Set<NodeId> peers = Collections.emptySet();
        private long minElectionTimeoutMs = DEFAULT_MIN_ELECTION_TIMEOUT_MS;
        private long maxElectionTimeoutMs = DEFAULT_MAX_ELECTION_TIMEOUT_MS;
        private long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

        public Builder(NodeId localNodeId) {
            this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
        }

        public Builder peers(Set<NodeId> peers) {
            this.peers = peers;
            return this;
        }

        public Builder electionTimeout(long minMs, long maxMs) {
            this.minElectionTimeoutMs = minMs;
            this.maxElectionTimeoutMs = maxMs;
            return this;
        }

        public Builder heartbeatInterval(long ms) {
            this.heartbeatIntervalMs = ms;
            return this;
        }

        public RaftConfig build() {
            return new RaftConfig(localNodeId, peers, minElectionTimeoutMs, maxElectionTimeoutMs, heartbeatIntervalMs);
        }
    }
}
