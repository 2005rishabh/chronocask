package io.chronocask.raft.model;

import java.io.Serializable;

/**
 * Result returned by a node in response to {@link AppendEntriesArgs}.
 *
 * @param term       currentTerm, for leader to update itself
 * @param success    true if follower contained entry matching prevLogIndex and prevLogTerm
 * @param matchIndex the highest index known to be replicated on the follower
 */
public record AppendEntriesReply(
        long term,
        boolean success,
        long matchIndex
) implements Serializable {
}
