package io.chronocask.raft.model;

import java.io.Serializable;

/**
 * Result returned by a node in response to a {@link RequestVoteArgs}.
 *
 * @param term        currentTerm, for candidate to update itself
 * @param voteGranted true means candidate received vote
 */
public record RequestVoteReply(
        long term,
        boolean voteGranted
) implements Serializable {
}
