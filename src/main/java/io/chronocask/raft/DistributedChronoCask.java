package io.chronocask.raft;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.compaction.CompactionResult;
import io.chronocask.raft.log.RaftLog;
import io.chronocask.raft.model.NodeId;
import io.chronocask.raft.model.RaftCommand;
import io.chronocask.raft.model.RaftRole;
import io.chronocask.raft.rpc.RaftTransport;
import io.chronocask.raft.state.ChronoCaskStateMachine;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * Distributed ChronoCask Node orchestrating an underlying ChronoCask storage instance,
 * a ChronoCaskStateMachine, and a RaftNode consensus engine.
 */
public class DistributedChronoCask implements Closeable {

    private final ChronoCask store;
    private final ChronoCaskStateMachine stateMachine;
    private final RaftLog log;
    private final RaftNode raftNode;

    public DistributedChronoCask(
        ChronoCaskConfig storeConfig,
        RaftConfig raftConfig,
        RaftTransport transport
    ) {
        this.store = ChronoCask.open(storeConfig);
        this.stateMachine = new ChronoCaskStateMachine(store);
        this.log = new RaftLog();
        this.raftNode = new RaftNode(raftConfig, log, stateMachine, transport);
    }

    public static DistributedChronoCask open(
        Path dataDir,
        NodeId nodeId,
        Set<NodeId> peers,
        RaftTransport transport
    ) {
        ChronoCaskConfig storeConfig = ChronoCaskConfig.builder(dataDir)
            .offHeapIndex(true)
            .build();
        RaftConfig raftConfig = RaftConfig.builder(nodeId).peers(peers).build();
        DistributedChronoCask node = new DistributedChronoCask(
            storeConfig,
            raftConfig,
            transport
        );
        node.start();
        return node;
    }

    public void start() {
        raftNode.start();
    }

    /**
     * Replicates and commits a key-value write across the Raft cluster.
     */
    public void put(String key, String value) throws TimeoutException {
        put(
            key.getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8)
        );
    }

    public void put(byte[] key, byte[] value) throws TimeoutException {
        RaftCommand cmd = RaftCommand.put(key, value);
        awaitCommit(raftNode.propose(cmd));
    }

    /**
     * Reads the current value for the key from the local state machine.
     */
    public String get(String key) {
        byte[] val = get(key.getBytes(StandardCharsets.UTF_8));
        return val != null ? new String(val, StandardCharsets.UTF_8) : null;
    }

    public byte[] get(byte[] key) {
        return stateMachine.query(key);
    }

    public Optional<String> find(String key) {
        return Optional.ofNullable(get(key));
    }

    /**
     * Replicates and commits a key deletion across the Raft cluster.
     */
    public boolean delete(String key) throws TimeoutException {
        return delete(key.getBytes(StandardCharsets.UTF_8));
    }

    public boolean delete(byte[] key) throws TimeoutException {
        boolean wasPresent = containsKey(key);
        RaftCommand cmd = RaftCommand.delete(key);
        awaitCommit(raftNode.propose(cmd));
        return wasPresent;
    }

    public boolean containsKey(byte[] key) {
        return store.containsKey(key);
    }

    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    public long size() {
        return store.size();
    }

    public void forEachBytes(BiConsumer<byte[], byte[]> action) {
        store.forEachBytes(action);
    }

    public CompactionResult compact() {
        return store.compact();
    }

    public void sync() {
        store.sync();
    }

    public boolean isOffHeap() {
        return store.isOffHeap();
    }

    public long currentSequenceNumber() {
        return store.currentSequenceNumber();
    }

    private void awaitCommit(CompletableFuture<Boolean> future)
        throws TimeoutException {
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Raft proposal interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Raft replication failed", e.getCause());
        }
    }

    public boolean isLeader() {
        return raftNode.getRole() == RaftRole.LEADER;
    }

    public NodeId getLeaderId() {
        return raftNode.getCurrentLeader();
    }

    public NodeId getNodeId() {
        return raftNode.getLocalNodeId();
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public ChronoCask getStore() {
        return store;
    }

    @Override
    public void close() throws IOException {
        raftNode.close();
        store.close();
    }
}
