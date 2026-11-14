package io.chronocask.raft.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * An individual log entry in the replicated Raft log.
 *
 * @param term    the election term in which the entry was created by the leader
 * @param index   monotonic 1-based log position
 * @param command serialized command payload
 */
public record LogEntry(long term, long index, byte[] command) implements Serializable {

    public LogEntry {
        if (term < 0) {
            throw new IllegalArgumentException("term cannot be negative: " + term);
        }
        if (index <= 0) {
            throw new IllegalArgumentException("log index must be >= 1: " + index);
        }
        command = command != null ? command : new byte[0];
    }

    public static LogEntry of(long term, long index, byte[] command) {
        return new LogEntry(term, index, command);
    }

    public static LogEntry of(long term, long index, RaftCommand command) {
        Objects.requireNonNull(command, "command cannot be null");
        return new LogEntry(term, index, command.serialize());
    }

    public RaftCommand toCommand() {
        return RaftCommand.deserialize(command);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry that)) return false;
        return term == that.term && index == that.index && Arrays.equals(command, that.command);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, index);
        result = 31 * result + Arrays.hashCode(command);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry[term=" + term + ", index=" + index + ", bytes=" + command.length + "]";
    }
}
