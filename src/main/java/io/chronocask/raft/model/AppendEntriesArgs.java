package io.chronocask.raft.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Invoked by leader to replicate log entries and as periodic heartbeat (Section 5.3 of Raft paper).
 *
 * @param term         leader's term
 * @param leaderId     so follower can redirect clients
 * @param prevLogIndex index of log entry immediately preceding new ones
 * @param prevLogTerm  term of prevLogIndex entry
 * @param entries      log entries to store (empty for heartbeat)
 * @param leaderCommit leader's commitIndex
 */
public record AppendEntriesArgs(
        long term,
        NodeId leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) implements Serializable {

    public AppendEntriesArgs {
        Objects.requireNonNull(leaderId, "leaderId cannot be null");
        entries = entries != null ? List.copyOf(entries) : Collections.emptyList();
    }

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }
}
