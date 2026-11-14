package io.chronocask.raft.model;

/**
 * Standard roles for a node in the Raft consensus protocol.
 */
public enum RaftRole {
    /** Passive follower responding to heartbeats and log replication from the leader. */
    FOLLOWER,
    /** Candidate campaigning for leadership after an election timeout. */
    CANDIDATE,
    /** Elected cluster leader servicing client requests and replicating log entries. */
    LEADER
}
