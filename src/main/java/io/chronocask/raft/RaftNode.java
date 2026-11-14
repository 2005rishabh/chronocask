package io.chronocask.raft;

import io.chronocask.raft.log.RaftLog;
import io.chronocask.raft.model.*;
import io.chronocask.raft.rpc.RaftRpcEndpoint;
import io.chronocask.raft.rpc.RaftTransport;
import io.chronocask.raft.state.RaftStateMachine;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core Raft Consensus Engine implementing leader election, log replication,
 * commit coordination, and state-machine application (Ongaro & Ousterhout).
 */
public class RaftNode implements RaftRpcEndpoint, Closeable {

    private final RaftConfig config;
    private final RaftLog log;
    private final RaftStateMachine stateMachine;
    private final RaftTransport transport;
    private final NodeId localNodeId;

    // Persistent & volatile consensus state
    private long currentTerm = 0;
    private NodeId votedFor = null;
    private RaftRole role = RaftRole.FOLLOWER;
    private NodeId currentLeader = null;
    private long commitIndex = 0;
    private long lastApplied = 0;

    // Leader-specific tracking
    private final Map<NodeId, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> matchIndex = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Boolean>> pendingProposals =
        new ConcurrentHashMap<>();

    // Timers & lifecycle
    private final ScheduledExecutorService timerScheduler;
    private ScheduledFuture<?> electionTickTask;
    private ScheduledFuture<?> heartbeatTask;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile long lastHeartbeatTimestamp = 0;
    private volatile long currentElectionTimeoutMs = 0;

    public RaftNode(
        RaftConfig config,
        RaftLog log,
        RaftStateMachine stateMachine,
        RaftTransport transport
    ) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.log = Objects.requireNonNull(log, "log cannot be null");
        this.stateMachine = Objects.requireNonNull(
            stateMachine,
            "stateMachine cannot be null"
        );
        this.transport = Objects.requireNonNull(
            transport,
            "transport cannot be null"
        );
        this.localNodeId = config.localNodeId();
        this.timerScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual()
                .name("raft-timer-" + localNodeId.id())
                .factory()
        );
    }

    /**
     * Starts this Raft node: registers RPC endpoint and launches election/heartbeat timers.
     */
    public synchronized void start() {
        if (running.get()) {
            return;
        }
        running.set(true);
        transport.registerEndpoint(localNodeId, this);

        resetElectionTimeout();

        // Check election timeout every 20 ms
        electionTickTask = timerScheduler.scheduleWithFixedDelay(
            this::checkElectionTimeout,
            20,
            20,
            TimeUnit.MILLISECONDS
        );

        // Send heartbeats every heartbeatIntervalMs
        heartbeatTask = timerScheduler.scheduleWithFixedDelay(
            this::sendHeartbeatsIfLeader,
            config.heartbeatIntervalMs(),
            config.heartbeatIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }

    // --- State Inquiries ---

    public synchronized RaftRole getRole() {
        return role;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized NodeId getCurrentLeader() {
        return currentLeader;
    }

    public synchronized NodeId getVotedFor() {
        return votedFor;
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized long getLastApplied() {
        return lastApplied;
    }

    public NodeId getLocalNodeId() {
        return localNodeId;
    }

    public RaftLog getLog() {
        return log;
    }

    public RaftStateMachine getStateMachine() {
        return stateMachine;
    }

    // --- Client Propose API ---

    /**
     * Proposes a new command to be replicated and committed across the Raft cluster.
     *
     * @param command serialized command payload
     * @return CompletableFuture completing when the command is committed and applied
     */
    public synchronized CompletableFuture<Boolean> propose(byte[] command) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("RaftNode is stopped")
            );
        }
        if (role != RaftRole.LEADER) {
            return CompletableFuture.failedFuture(
                new NotLeaderException(currentLeader)
            );
        }

        LogEntry entry = log.append(currentTerm, command);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pendingProposals.put(entry.index(), future);

        // Single-node cluster fast path
        if (config.peers().isEmpty()) {
            commitIndex = entry.index();
            applyCommittedEntries();
            return future;
        }

        // Trigger eager broadcast to peers
        broadcastAppendEntries();
        return future;
    }

    public CompletableFuture<Boolean> propose(RaftCommand command) {
        return propose(command.serialize());
    }

    // --- Election & Timer Logic ---

    private synchronized void checkElectionTimeout() {
        if (!running.get() || role == RaftRole.LEADER) {
            return;
        }

        long elapsed = System.currentTimeMillis() - lastHeartbeatTimestamp;
        if (elapsed >= currentElectionTimeoutMs) {
            startElection();
        }
    }

    private synchronized void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = localNodeId;
        currentLeader = null;
        resetElectionTimeout();

        long savedTerm = currentTerm;
        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();

        // Single node cluster: automatically elect self
        if (config.peers().isEmpty()) {
            becomeLeader();
            return;
        }

        final int[] votesGranted = { 1 }; // Self vote
        int quorum = config.quorumSize();

        for (NodeId peer : config.peers()) {
            RequestVoteArgs args = new RequestVoteArgs(
                savedTerm,
                localNodeId,
                lastLogIndex,
                lastLogTerm
            );
            transport.sendRequestVote(peer, args).whenComplete((reply, err) -> {
                if (err == null && reply != null) {
                    handleVoteReply(
                        peer,
                        reply,
                        savedTerm,
                        votesGranted,
                        quorum
                    );
                }
            });
        }
    }

    private synchronized void handleVoteReply(
        NodeId peer,
        RequestVoteReply reply,
        long electionTerm,
        int[] votesGranted,
        int quorum
    ) {
        if (!running.get()) {
            return;
        }

        if (reply.term() > currentTerm) {
            stepDown(reply.term());
            return;
        }

        if (
            role == RaftRole.CANDIDATE &&
            currentTerm == electionTerm &&
            reply.voteGranted()
        ) {
            votesGranted[0]++;
            if (votesGranted[0] >= quorum) {
                becomeLeader();
            }
        }
    }

    private synchronized void becomeLeader() {
        if (role != RaftRole.CANDIDATE && !config.peers().isEmpty()) {
            return;
        }
        role = RaftRole.LEADER;
        currentLeader = localNodeId;

        for (NodeId peer : config.peers()) {
            nextIndex.put(peer, log.lastIndex() + 1L);
            matchIndex.put(peer, 0L);
        }

        // Send initial heartbeat to announce leadership and suppress elections
        broadcastAppendEntries();
    }

    private synchronized void sendHeartbeatsIfLeader() {
        if (!running.get() || role != RaftRole.LEADER) {
            return;
        }
        broadcastAppendEntries();
    }

    private synchronized void broadcastAppendEntries() {
        if (role != RaftRole.LEADER) {
            return;
        }

        for (NodeId peer : config.peers()) {
            sendAppendEntriesTo(peer);
        }
    }

    private synchronized void sendAppendEntriesTo(NodeId peer) {
        if (role != RaftRole.LEADER) {
            return;
        }

        long prevIndex =
            nextIndex.getOrDefault(peer, log.lastIndex() + 1L) - 1L;
        long prevTerm = log.getTerm(prevIndex);
        List<LogEntry> entries = log.entriesFrom(prevIndex + 1L);

        AppendEntriesArgs args = new AppendEntriesArgs(
            currentTerm,
            localNodeId,
            prevIndex,
            prevTerm,
            entries,
            commitIndex
        );

        long targetTerm = currentTerm;
        transport.sendAppendEntries(peer, args).whenComplete((reply, err) -> {
            if (err == null && reply != null) {
                handleAppendEntriesReply(peer, reply, targetTerm, args);
            }
        });
    }

    private synchronized void handleAppendEntriesReply(
        NodeId peer,
        AppendEntriesReply reply,
        long leaderTerm,
        AppendEntriesArgs args
    ) {
        if (!running.get()) {
            return;
        }

        if (reply.term() > currentTerm) {
            stepDown(reply.term());
            return;
        }

        if (role != RaftRole.LEADER || currentTerm != leaderTerm) {
            return;
        }

        if (reply.success()) {
            long newMatch = reply.matchIndex();
            matchIndex.put(peer, newMatch);
            nextIndex.put(peer, newMatch + 1L);
            checkAndUpdateCommitIndex();
        } else {
            // Replication failed: decrement nextIndex and retry on next cycle
            long currentNext = nextIndex.getOrDefault(peer, 1L);
            if (currentNext > 1L) {
                nextIndex.put(peer, currentNext - 1L);
            }
        }
    }

    private synchronized void checkAndUpdateCommitIndex() {
        if (role != RaftRole.LEADER) {
            return;
        }

        long lastIdx = log.lastIndex();
        for (long n = commitIndex + 1L; n <= lastIdx; n++) {
            if (log.getTerm(n) == currentTerm) {
                int matchCount = 1; // Self matches local log
                for (NodeId peer : config.peers()) {
                    if (matchIndex.getOrDefault(peer, 0L) >= n) {
                        matchCount++;
                    }
                }
                if (matchCount >= config.quorumSize()) {
                    commitIndex = n;
                }
            }
        }

        applyCommittedEntries();
    }

    private synchronized void applyCommittedEntries() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = log.getEntry(lastApplied);
            if (entry != null) {
                stateMachine.apply(entry);
                CompletableFuture<Boolean> future = pendingProposals.remove(
                    lastApplied
                );
                if (future != null) {
                    future.complete(true);
                }
            }
        }
    }

    private synchronized void stepDown(long higherTerm) {
        currentTerm = higherTerm;
        role = RaftRole.FOLLOWER;
        votedFor = null;
        currentLeader = null;
        resetElectionTimeout();
        failPendingProposals(new NotLeaderException(null));
    }

    private synchronized void failPendingProposals(Throwable t) {
        for (CompletableFuture<Boolean> f : pendingProposals.values()) {
            f.completeExceptionally(t);
        }
        pendingProposals.clear();
    }

    private void resetElectionTimeout() {
        lastHeartbeatTimestamp = System.currentTimeMillis();
        long min = config.minElectionTimeoutMs();
        long max = config.maxElectionTimeoutMs();
        currentElectionTimeoutMs = ThreadLocalRandom.current().nextLong(
            min,
            max + 1
        );
    }

    // --- RaftRpcEndpoint Handler Implementation ---

    @Override
    public synchronized RequestVoteReply handleRequestVote(
        RequestVoteArgs args
    ) {
        if (!running.get()) {
            return new RequestVoteReply(currentTerm, false);
        }

        // Rule 1: Reply false if term < currentTerm
        if (args.term() < currentTerm) {
            return new RequestVoteReply(currentTerm, false);
        }

        if (args.term() > currentTerm) {
            stepDown(args.term());
        }

        // Rule 2: If votedFor is null or candidateId, and candidate's log is up-to-date
        boolean canVote =
            votedFor == null || votedFor.equals(args.candidateId());
        boolean logUpToDate = isLogUpToDate(
            args.lastLogTerm(),
            args.lastLogIndex()
        );

        if (canVote && logUpToDate) {
            votedFor = args.candidateId();
            resetElectionTimeout();
            return new RequestVoteReply(currentTerm, true);
        }

        return new RequestVoteReply(currentTerm, false);
    }

    @Override
    public synchronized AppendEntriesReply handleAppendEntries(
        AppendEntriesArgs args
    ) {
        if (!running.get()) {
            return new AppendEntriesReply(currentTerm, false, 0);
        }

        // Rule 1: Reply false if term < currentTerm
        if (args.term() < currentTerm) {
            return new AppendEntriesReply(currentTerm, false, log.lastIndex());
        }

        if (args.term() > currentTerm) {
            stepDown(args.term());
        }

        // Valid leader heartbeat: reset timer and recognize leader
        currentLeader = args.leaderId();
        role = RaftRole.FOLLOWER;
        resetElectionTimeout();

        // Rule 2: Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm
        if (args.prevLogIndex() > 0) {
            if (args.prevLogIndex() > log.lastIndex()) {
                return new AppendEntriesReply(
                    currentTerm,
                    false,
                    log.lastIndex()
                );
            }
            if (log.getTerm(args.prevLogIndex()) != args.prevLogTerm()) {
                return new AppendEntriesReply(
                    currentTerm,
                    false,
                    log.lastIndex()
                );
            }
        }

        // Rule 3 & 4: Append any new entries not already in the log
        long newMatchIndex = log.reconcileAndAppend(
            args.prevLogIndex(),
            args.entries()
        );

        // Rule 5: Update commitIndex
        if (args.leaderCommit() > commitIndex) {
            commitIndex = Math.min(args.leaderCommit(), log.lastIndex());
            applyCommittedEntries();
        }

        return new AppendEntriesReply(currentTerm, true, newMatchIndex);
    }

    private boolean isLogUpToDate(
        long candidateLastTerm,
        long candidateLastIndex
    ) {
        long myLastTerm = log.lastTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= log.lastIndex();
    }

    @Override
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (electionTickTask != null) {
            electionTickTask.cancel(true);
        }
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
        }
        timerScheduler.shutdownNow();
        transport.unregisterEndpoint(localNodeId);
        failPendingProposals(new IllegalStateException("RaftNode closed"));
    }
}
