package io.chronocask.raft;

import io.chronocask.raft.model.CommandType;
import io.chronocask.raft.model.LogEntry;
import io.chronocask.raft.model.RaftCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Raft Command and LogEntry Binary Codec Tests")
class RaftCommandCodecTest {

    @Test
    @DisplayName("Should encode and decode PUT command with binary safety")
    void testPutCommandCodec() {
        byte[] key = "user:session:101".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[]{0x00, 0x01, (byte) 0xFE, (byte) 0xFF, 'A', 'B', 'C'};

        RaftCommand original = RaftCommand.put(key, value);
        byte[] serialized = original.serialize();

        RaftCommand decoded = RaftCommand.deserialize(serialized);
        assertEquals(CommandType.PUT, decoded.type());
        assertArrayEquals(key, decoded.key());
        assertArrayEquals(value, decoded.value());
        assertEquals(original, decoded);
    }

    @Test
    @DisplayName("Should encode and decode DELETE command")
    void testDeleteCommandCodec() {
        byte[] key = "session:expired:99".getBytes(StandardCharsets.UTF_8);

        RaftCommand original = RaftCommand.delete(key);
        byte[] serialized = original.serialize();

        RaftCommand decoded = RaftCommand.deserialize(serialized);
        assertEquals(CommandType.DELETE, decoded.type());
        assertArrayEquals(key, decoded.key());
        assertEquals(0, decoded.value().length);
        assertEquals(original, decoded);
    }

    @Test
    @DisplayName("Should encode and decode NOOP command")
    void testNoopCommandCodec() {
        RaftCommand original = RaftCommand.NO_OP;
        byte[] serialized = original.serialize();

        RaftCommand decoded = RaftCommand.deserialize(serialized);
        assertEquals(CommandType.NOOP, decoded.type());
        assertEquals(original, decoded);
    }

    @Test
    @DisplayName("Should create LogEntry and roundtrip to RaftCommand")
    void testLogEntryRoundtrip() {
        RaftCommand cmd = RaftCommand.put("cluster:cfg", "v1.0.0");
        LogEntry entry = LogEntry.of(2, 5, cmd);

        assertEquals(2, entry.term());
        assertEquals(5, entry.index());
        assertEquals(cmd, entry.toCommand());
    }
}
