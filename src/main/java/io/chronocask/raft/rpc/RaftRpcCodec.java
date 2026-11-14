package io.chronocask.raft.rpc;

import io.chronocask.raft.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance binary serializer and deserializer for Raft RPC protocol messages.
 *
 * Frame Format:
 * [MAGIC (4 bytes: 0x52414654 'RAFT')] + [MSG_TYPE (1 byte)] + [LENGTH (4 bytes)] + [PAYLOAD]
 */
public final class RaftRpcCodec {

    public static final int MAGIC = 0x52414654; // 'RAFT' in ASCII
    public static final byte TYPE_REQUEST_VOTE_ARGS = 1;
    public static final byte TYPE_REQUEST_VOTE_REPLY = 2;
    public static final byte TYPE_APPEND_ENTRIES_ARGS = 3;
    public static final byte TYPE_APPEND_ENTRIES_REPLY = 4;

    private RaftRpcCodec() {
    }

    // --- Serialization ---

    public static void encodeRequestVoteArgs(RequestVoteArgs args, DataOutputStream out) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(64);
        DataOutputStream pOut = new DataOutputStream(payload);
        pOut.writeLong(args.term());
        writeString(args.candidateId().id(), pOut);
        pOut.writeLong(args.lastLogIndex());
        pOut.writeLong(args.lastLogTerm());
        pOut.flush();

        writeFrame(TYPE_REQUEST_VOTE_ARGS, payload.toByteArray(), out);
    }

    public static void encodeRequestVoteReply(RequestVoteReply reply, DataOutputStream out) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(16);
        DataOutputStream pOut = new DataOutputStream(payload);
        pOut.writeLong(reply.term());
        pOut.writeBoolean(reply.voteGranted());
        pOut.flush();

        writeFrame(TYPE_REQUEST_VOTE_REPLY, payload.toByteArray(), out);
    }

    public static void encodeAppendEntriesArgs(AppendEntriesArgs args, DataOutputStream out) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(256);
        DataOutputStream pOut = new DataOutputStream(payload);
        pOut.writeLong(args.term());
        writeString(args.leaderId().id(), pOut);
        pOut.writeLong(args.prevLogIndex());
        pOut.writeLong(args.prevLogTerm());
        pOut.writeLong(args.leaderCommit());

        List<LogEntry> entries = args.entries();
        pOut.writeInt(entries.size());
        for (LogEntry entry : entries) {
            pOut.writeLong(entry.term());
            pOut.writeLong(entry.index());
            byte[] cmd = entry.command();
            pOut.writeInt(cmd.length);
            pOut.write(cmd);
        }
        pOut.flush();

        writeFrame(TYPE_APPEND_ENTRIES_ARGS, payload.toByteArray(), out);
    }

    public static void encodeAppendEntriesReply(AppendEntriesReply reply, DataOutputStream out) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(24);
        DataOutputStream pOut = new DataOutputStream(payload);
        pOut.writeLong(reply.term());
        pOut.writeBoolean(reply.success());
        pOut.writeLong(reply.matchIndex());
        pOut.flush();

        writeFrame(TYPE_APPEND_ENTRIES_REPLY, payload.toByteArray(), out);
    }

    private static void writeFrame(byte type, byte[] payload, DataOutputStream out) throws IOException {
        out.writeInt(MAGIC);
        out.writeByte(type);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    // --- Deserialization ---

    public static Object decodeFrame(DataInputStream in) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw new IOException(String.format("Invalid Raft RPC magic header: 0x%08X", magic));
        }

        byte type = in.readByte();
        int length = in.readInt();
        if (length < 0 || length > 64 * 1024 * 1024) { // 64MB protection limit
            throw new IOException("Raft RPC frame length out of bounds: " + length);
        }

        byte[] payloadBytes = in.readNBytes(length);
        if (payloadBytes.length < length) {
            throw new EOFException("Premature EOF reading Raft RPC payload");
        }

        DataInputStream pIn = new DataInputStream(new ByteArrayInputStream(payloadBytes));
        return switch (type) {
            case TYPE_REQUEST_VOTE_ARGS -> decodeRequestVoteArgs(pIn);
            case TYPE_REQUEST_VOTE_REPLY -> decodeRequestVoteReply(pIn);
            case TYPE_APPEND_ENTRIES_ARGS -> decodeAppendEntriesArgs(pIn);
            case TYPE_APPEND_ENTRIES_REPLY -> decodeAppendEntriesReply(pIn);
            default -> throw new IOException("Unknown Raft RPC message type: " + type);
        };
    }

    private static RequestVoteArgs decodeRequestVoteArgs(DataInputStream in) throws IOException {
        long term = in.readLong();
        String candidateId = readString(in);
        long lastLogIndex = in.readLong();
        long lastLogTerm = in.readLong();
        return new RequestVoteArgs(term, NodeId.of(candidateId), lastLogIndex, lastLogTerm);
    }

    private static RequestVoteReply decodeRequestVoteReply(DataInputStream in) throws IOException {
        long term = in.readLong();
        boolean voteGranted = in.readBoolean();
        return new RequestVoteReply(term, voteGranted);
    }

    private static AppendEntriesArgs decodeAppendEntriesArgs(DataInputStream in) throws IOException {
        long term = in.readLong();
        String leaderId = readString(in);
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();
        long leaderCommit = in.readLong();

        int numEntries = in.readInt();
        List<LogEntry> entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            long eTerm = in.readLong();
            long eIndex = in.readLong();
            int cmdLen = in.readInt();
            byte[] cmd = in.readNBytes(cmdLen);
            entries.add(LogEntry.of(eTerm, eIndex, cmd));
        }

        return new AppendEntriesArgs(term, NodeId.of(leaderId), prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    private static AppendEntriesReply decodeAppendEntriesReply(DataInputStream in) throws IOException {
        long term = in.readLong();
        boolean success = in.readBoolean();
        long matchIndex = in.readLong();
        return new AppendEntriesReply(term, success, matchIndex);
    }

    private static void writeString(String s, DataOutputStream out) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readUnsignedShort();
        byte[] bytes = in.readNBytes(len);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
