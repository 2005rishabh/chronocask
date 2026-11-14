package io.chronocask.raft.state;

import io.chronocask.api.ChronoCask;
import io.chronocask.raft.model.LogEntry;
import io.chronocask.raft.model.RaftCommand;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * State machine adapter that applies committed Raft commands directly to a ChronoCask instance.
 */
public class ChronoCaskStateMachine implements RaftStateMachine {

    private final ChronoCask store;
    private final AtomicLong lastAppliedIndex = new AtomicLong(0);

    public ChronoCaskStateMachine(ChronoCask store) {
        this.store = Objects.requireNonNull(store, "store cannot be null");
    }

    @Override
    public synchronized void apply(LogEntry entry) {
        Objects.requireNonNull(entry, "entry cannot be null");
        if (entry.index() <= lastAppliedIndex.get()) {
            return; // Already applied
        }

        RaftCommand cmd = entry.toCommand();
        switch (cmd.type()) {
            case PUT -> store.put(cmd.key(), cmd.value());
            case DELETE -> store.delete(cmd.key());
            case NOOP -> {
                // No state change
            }
        }
        lastAppliedIndex.set(entry.index());
    }

    @Override
    public byte[] query(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        return store.get(key);
    }

    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex.get();
    }

    public ChronoCask getStore() {
        return store;
    }
}
