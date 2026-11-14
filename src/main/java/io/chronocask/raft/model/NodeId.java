package io.chronocask.raft.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Strongly-typed unique identifier for a node in a Raft cluster.
 */
public record NodeId(String id) implements Comparable<NodeId>, Serializable {

    public NodeId {
        Objects.requireNonNull(id, "id cannot be null");
        if (id.isBlank()) {
            throw new IllegalArgumentException("id cannot be blank");
        }
    }

    public static NodeId of(String id) {
        return new NodeId(id);
    }

    @Override
    public int compareTo(NodeId other) {
        return this.id.compareTo(other.id);
    }

    @Override
    public String toString() {
        return id;
    }
}
