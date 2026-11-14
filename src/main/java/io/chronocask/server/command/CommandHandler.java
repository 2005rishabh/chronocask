package io.chronocask.server.command;

import io.chronocask.api.ChronoCask;
import io.chronocask.compaction.CompactionResult;
import io.chronocask.raft.DistributedChronoCask;
import io.chronocask.raft.NotLeaderException;
import io.chronocask.raft.RaftNode;
import io.chronocask.raft.model.NodeId;
import io.chronocask.server.resp.RespType;
import io.chronocask.server.resp.RespValue;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Dispatches parsed RESP commands to either a standalone ChronoCask storage engine
 * or a distributed Raft-backed ChronoCask cluster node.
 */
public final class CommandHandler {

    private final ChronoCask store;
    private final DistributedChronoCask distributedStore;
    private final Instant startTime;

    public CommandHandler(ChronoCask store) {
        this.store = Objects.requireNonNull(store, "store cannot be null");
        this.distributedStore = null;
        this.startTime = Instant.now();
    }

    public CommandHandler(DistributedChronoCask distributedStore) {
        this.distributedStore = Objects.requireNonNull(
            distributedStore,
            "distributedStore cannot be null"
        );
        this.store = distributedStore.getStore();
        this.startTime = Instant.now();
    }

    /**
     * Executes a parsed RESP command and produces a CommandResult.
     */
    public CommandResult execute(RespValue request) {
        if (request == null || request.type() != RespType.ARRAY) {
            return CommandResult.reply(
                RespValue.error("ERR invalid command format")
            );
        }

        List<RespValue> elements = request.array();
        if (elements.isEmpty()) {
            return CommandResult.reply(RespValue.error("ERR empty command"));
        }

        String cmdName = elements.getFirst().asString();
        if (cmdName == null) {
            return CommandResult.reply(RespValue.error("ERR null command"));
        }

        String command = cmdName.toUpperCase(Locale.ROOT);
        try {
            return switch (command) {
                case "PING" -> handlePing(elements);
                case "SET" -> handleSet(elements);
                case "GET" -> handleGet(elements);
                case "DEL" -> handleDel(elements);
                case "EXISTS" -> handleExists(elements);
                case "DBSIZE" -> handleDbSize();
                case "KEYS" -> handleKeys(elements);
                case "INFO" -> handleInfo();
                case "CONFIG" -> handleConfig(elements);
                case "CLIENT" -> handleClient(elements);
                case "COMPACT" -> handleCompact();
                case "SYNC" -> handleSync();
                case "RAFT", "CLUSTER" -> handleRaftStatus();
                case "COMMAND" -> handleCommandDocs();
                case "QUIT" -> CommandResult.quit();
                default -> CommandResult.reply(
                    RespValue.error("ERR unknown command '" + cmdName + "'")
                );
            };
        } catch (NotLeaderException nle) {
            NodeId lId = nle.getLeaderId();
            return CommandResult.reply(
                RespValue.error(
                    "NOT_LEADER " + (lId != null ? lId.id() : "unknown")
                )
            );
        } catch (Exception e) {
            return CommandResult.reply(
                RespValue.error(
                    "ERR " +
                        (e.getMessage() != null
                            ? e.getMessage()
                            : e.getClass().getSimpleName())
                )
            );
        }
    }

    private CommandResult handlePing(List<RespValue> args) {
        if (args.size() == 1) {
            return CommandResult.reply(RespValue.PONG);
        } else if (args.size() == 2) {
            byte[] msg = args.get(1).bytes();
            return CommandResult.reply(RespValue.bulkString(msg));
        } else {
            return CommandResult.reply(
                RespValue.error(
                    "ERR wrong number of arguments for 'ping' command"
                )
            );
        }
    }

    private CommandResult handleSet(List<RespValue> args) {
        if (args.size() < 3) {
            return CommandResult.reply(
                RespValue.error(
                    "ERR wrong number of arguments for 'set' command"
                )
            );
        }

        byte[] key = args.get(1).bytes();
        byte[] value = args.get(2).bytes();
        if (key == null || value == null) {
            return CommandResult.reply(
                RespValue.error("ERR invalid key or value")
            );
        }

        try {
            if (distributedStore != null) {
                distributedStore.put(key, value);
            } else {
                store.put(key, value);
            }
            return CommandResult.reply(RespValue.OK);
        } catch (NotLeaderException nle) {
            NodeId lId = nle.getLeaderId();
            return CommandResult.reply(
                RespValue.error(
                    "NOT_LEADER " + (lId != null ? lId.id() : "unknown")
                )
            );
        } catch (Exception e) {
            if (e.getCause() instanceof NotLeaderException nle) {
                NodeId lId = nle.getLeaderId();
                return CommandResult.reply(
                    RespValue.error(
                        "NOT_LEADER " + (lId != null ? lId.id() : "unknown")
                    )
                );
            }
            return CommandResult.reply(
                RespValue.error("ERR " + e.getMessage())
            );
        }
    }

    private CommandResult handleGet(List<RespValue> args) {
        if (args.size() != 2) {
            return CommandResult.reply(
                RespValue.error(
                    "ERR wrong number of arguments for 'get' command"
                )
            );
        }

        byte[] key = args.get(1).bytes();
        if (key == null) {
            return CommandResult.reply(RespValue.NULL_BULK_STRING);
        }

        byte[] val =
            distributedStore != null
                ? distributedStore.get(key)
                : store.get(key);
        if (val == null) {
            return CommandResult.reply(RespValue.NULL_BULK_STRING);
        }
        return CommandResult.reply(RespValue.bulkString(val));
    }

    private CommandResult handleDel(List<RespValue> args) {
        if (args.size() < 2) {
            return CommandResult.reply(
                RespValue.error(
                    "ERR wrong number of arguments for 'del' command"
                )
            );
        }

        try {
            long deletedCount = 0;
            for (int i = 1; i < args.size(); i++) {
                byte[] key = args.get(i).bytes();
                if (key != null) {
                    boolean removed =
                        distributedStore != null
                            ? distributedStore.delete(key)
                            : store.delete(key);
                    if (removed) {
                        deletedCount++;
                    }
                }
            }
            return CommandResult.reply(RespValue.integer(deletedCount));
        } catch (NotLeaderException nle) {
            NodeId lId = nle.getLeaderId();
            return CommandResult.reply(
                RespValue.error(
                    "NOT_LEADER " + (lId != null ? lId.id() : "unknown")
                )
            );
        } catch (Exception e) {
            if (e.getCause() instanceof NotLeaderException nle) {
                NodeId lId = nle.getLeaderId();
                return CommandResult.reply(
                    RespValue.error(
                        "NOT_LEADER " + (lId != null ? lId.id() : "unknown")
                    )
                );
            }
            return CommandResult.reply(
                RespValue.error("ERR " + e.getMessage())
            );
        }
    }

    private CommandResult handleExists(List<RespValue> args) {
        if (args.size() < 2) {
            return CommandResult.reply(
                RespValue.error(
                    "ERR wrong number of arguments for 'exists' command"
                )
            );
        }

        long count = 0;
        for (int i = 1; i < args.size(); i++) {
            byte[] key = args.get(i).bytes();
            if (key != null && store.containsKey(key)) {
                count++;
            }
        }
        return CommandResult.reply(RespValue.integer(count));
    }

    private CommandResult handleDbSize() {
        return CommandResult.reply(RespValue.integer(store.size()));
    }

    private CommandResult handleKeys(List<RespValue> args) {
        List<RespValue> keysList = new ArrayList<>();
        store.forEachBytes((key, val) ->
            keysList.add(RespValue.bulkString(key))
        );
        return CommandResult.reply(RespValue.array(keysList));
    }

    private CommandResult handleInfo() {
        StringBuilder sb = new StringBuilder(512);
        long uptime = Duration.between(startTime, Instant.now()).toSeconds();

        sb.append("# Server\r\n");
        sb.append("chronocask_version:0.1.0\r\n");
        sb.append("java_version:")
            .append(System.getProperty("java.version", "unknown"))
            .append("\r\n");
        sb.append("os_name:")
            .append(System.getProperty("os.name", "unknown"))
            .append("\r\n");
        sb.append("uptime_in_seconds:").append(uptime).append("\r\n");

        sb.append("# Storage\r\n");
        sb.append("index_type:")
            .append(store.isOffHeap() ? "off-heap" : "heap")
            .append("\r\n");
        sb.append("total_keys:").append(store.size()).append("\r\n");
        sb.append("current_sequence_number:")
            .append(store.currentSequenceNumber())
            .append("\r\n");

        if (
            store.getEngine() != null &&
            store.getEngine().getSegmentManager() != null
        ) {
            var segMgr = store.getEngine().getSegmentManager();
            var active = segMgr.getActiveSegment();
            sb.append("active_segment_id:")
                .append(active != null ? active.segmentId() : -1)
                .append("\r\n");
            sb.append("immutable_segments_count:")
                .append(segMgr.getImmutableSegments().size())
                .append("\r\n");
        }

        sb.append("# Raft\r\n");
        if (distributedStore != null) {
            RaftNode rn = distributedStore.getRaftNode();
            sb.append("raft_enabled:true\r\n");
            sb.append("node_id:")
                .append(distributedStore.getNodeId().id())
                .append("\r\n");
            sb.append("role:").append(rn.getRole().name()).append("\r\n");
            sb.append("current_term:")
                .append(rn.getCurrentTerm())
                .append("\r\n");
            NodeId leader = distributedStore.getLeaderId();
            sb.append("leader_id:")
                .append(leader != null ? leader.id() : "none")
                .append("\r\n");
            sb.append("commit_index:")
                .append(rn.getCommitIndex())
                .append("\r\n");
            sb.append("last_applied:")
                .append(rn.getLastApplied())
                .append("\r\n");
        } else {
            sb.append("raft_enabled:false\r\n");
        }

        return CommandResult.reply(RespValue.bulkString(sb.toString()));
    }

    private CommandResult handleRaftStatus() {
        if (distributedStore == null) {
            return CommandResult.reply(
                RespValue.bulkString("raft_enabled:false\r\n")
            );
        }
        RaftNode rn = distributedStore.getRaftNode();
        NodeId leader = distributedStore.getLeaderId();
        String summary = String.format(
            "node_id:%s,role:%s,term:%d,leader:%s,commit_index:%d,last_applied:%d",
            distributedStore.getNodeId().id(),
            rn.getRole().name(),
            rn.getCurrentTerm(),
            leader != null ? leader.id() : "none",
            rn.getCommitIndex(),
            rn.getLastApplied()
        );
        return CommandResult.reply(RespValue.bulkString(summary));
    }

    private CommandResult handleConfig(List<RespValue> args) {
        int offset =
            args.size() > 0 && "CONFIG".equalsIgnoreCase(args.get(0).asString())
                ? 1
                : 0;
        if (
            args.size() >= offset + 2 &&
            "GET".equalsIgnoreCase(args.get(offset).asString())
        ) {
            String param = args.get(offset + 1).asString();
            if (param != null) {
                String p = param.toLowerCase(Locale.ROOT);
                if (p.contains("save")) {
                    return CommandResult.reply(
                        RespValue.array(
                            List.of(
                                RespValue.bulkString("save"),
                                RespValue.bulkString("")
                            )
                        )
                    );
                }
                if (p.contains("appendonly")) {
                    return CommandResult.reply(
                        RespValue.array(
                            List.of(
                                RespValue.bulkString("appendonly"),
                                RespValue.bulkString("yes")
                            )
                        )
                    );
                }
            }
            return CommandResult.reply(RespValue.array(List.of()));
        }
        return CommandResult.reply(RespValue.simpleString("OK"));
    }

    private CommandResult handleClient(List<RespValue> args) {
        return CommandResult.reply(RespValue.OK);
    }

    private CommandResult handleCompact() {
        CompactionResult res = store.compact();
        String summary = String.format(
            "OK compacted %d segments into %d segment(s), reclaimed %d bytes",
            res.segmentsReplaced(),
            res.compactedSegmentsCreated(),
            res.bytesReclaimed()
        );
        return CommandResult.reply(RespValue.simpleString(summary));
    }

    private CommandResult handleSync() {
        store.sync();
        return CommandResult.reply(RespValue.OK);
    }

    private CommandResult handleCommandDocs() {
        return CommandResult.reply(RespValue.EMPTY_ARRAY);
    }
}
