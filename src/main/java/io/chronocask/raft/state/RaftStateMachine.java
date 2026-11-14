package io.chronocask.raft.state;

import io.chronocask.raft.model.LogEntry;

/**
 * Replicated state machine contract.
 * Raft guarantees that all non-faulty nodes apply identical sequences of log entries
 * to their local state machines.
 */
public interface RaftStateMachine {

    /**
     * Applies a committed log entry to the state machine.
     *
     * @param entry the committed entry to apply
     */
    void apply(LogEntry entry);

    /**
     * Reads state for a given key directly from the state machine.
     */
    byte[] query(byte[] key);

    /**
     * Returns the highest log index applied so far.
     */
    long lastAppliedIndex();
}
