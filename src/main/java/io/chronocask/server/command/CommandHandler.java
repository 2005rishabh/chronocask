package io.chronocask.server.command;

import io.chronocask.api.ChronoCask;
import io.chronocask.compaction.CompactionResult;
import io.chronocask.server.resp.RespType;
import io.chronocask.server.resp.RespValue;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Dispatches parsed RESP commands to the underlying ChronoCask storage engine.
 */
public final class CommandHandler {

    private final ChronoCask store;
    private final Instant startTime;

    public CommandHandler(ChronoCask store) {
        this.store = Objects.requireNonNull(store, "store cannot be null");
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
                case "COMMAND" -> handleCommandDocs();
                case "QUIT" -> CommandResult.quit();
                default -> CommandResult.reply(
                    RespValue.error("ERR unknown command '" + cmdName + "'")
                );
            };
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

        store.put(key, value);
        return CommandResult.reply(RespValue.OK);
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

        byte[] val = store.get(key);
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

        long deletedCount = 0;
        for (int i = 1; i < args.size(); i++) {
            byte[] key = args.get(i).bytes();
            if (key != null && store.delete(key)) {
                deletedCount++;
            }
        }
        return CommandResult.reply(RespValue.integer(deletedCount));
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
        // Collect all keys currently in the index
        List<RespValue> keysList = new ArrayList<>();
        store.forEachBytes((key, val) ->
            keysList.add(RespValue.bulkString(key))
        );
        return CommandResult.reply(RespValue.array(keysList));
    }

    private CommandResult handleInfo() {
        StringBuilder sb = new StringBuilder(256);
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

        return CommandResult.reply(RespValue.bulkString(sb.toString()));
    }

    private CommandResult handleConfig(List<RespValue> args) {
        // args may be [CONFIG, GET, save] or [GET, save]
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
        // Return empty array for client handshake negotiation
        return CommandResult.reply(RespValue.EMPTY_ARRAY);
    }
}
