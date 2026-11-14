package io.chronocask.raft.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Invoked by candidates to gather votes from cluster peers (Section 5.2 of Raft paper).
 *
 * @param term         candidate's term
 * @param candidateId  candidate requesting vote
 * @param lastLogIndex index of candidate's last log entry
 * @param lastLogTerm  term of candidate's last log entry
 */
public record RequestVoteArgs(
        long term,
        NodeId candidateId,
        long lastLogIndex,
        long lastLogTerm
) implements Serializable {

    public RequestVoteArgs {
        Objects.requireNonNull(candidateId, "candidateId cannot be null");
    }
}
