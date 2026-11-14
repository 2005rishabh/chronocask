package io.chronocask.raft.rpc;

import io.chronocask.raft.model.AppendEntriesArgs;
import io.chronocask.raft.model.AppendEntriesReply;
import io.chronocask.raft.model.RequestVoteArgs;
import io.chronocask.raft.model.RequestVoteReply;

/**
 * RPC endpoint interface implemented by a Raft node to handle incoming Raft protocol messages.
 */
public interface RaftRpcEndpoint {

    /**
     * Handles an incoming RequestVote RPC from a candidate.
     */
    RequestVoteReply handleRequestVote(RequestVoteArgs args);

    /**
     * Handles an incoming AppendEntries RPC (heartbeat or log replication) from a leader.
     */
    AppendEntriesReply handleAppendEntries(AppendEntriesArgs args);
}
