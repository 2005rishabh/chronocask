package io.chronocask.raft;

import io.chronocask.raft.log.RaftLog;
import io.chronocask.raft.model.LogEntry;
import io.chronocask.raft.model.RaftCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Raft Log Management and Reconciliation Unit Tests")
class RaftLogTest {

    private RaftLog log;

    @BeforeEach
    void setUp() {
        log = new RaftLog();
    }

    @Test
    @DisplayName("Should append entries with 1-based monotonic indexing")
    void testBasicAppends() {
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());

        LogEntry e1 = log.append(1, RaftCommand.put("k1", "v1").serialize());
        assertEquals(1, e1.index());
        assertEquals(1, e1.term());
        assertEquals(1, log.lastIndex());
        assertEquals(1, log.lastTerm());

        LogEntry e2 = log.append(1, RaftCommand.put("k2", "v2").serialize());
        assertEquals(2, e2.index());
        assertEquals(2, log.lastIndex());

        LogEntry e3 = log.append(2, RaftCommand.delete("k1").serialize());
        assertEquals(3, e3.index());
        assertEquals(2, e3.term());
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());
    }

    @Test
    @DisplayName("Should query entry and term by index")
    void testGetEntryAndTerm() {
        log.append(1, RaftCommand.put("k1", "v1").serialize());
        log.append(2, RaftCommand.put("k2", "v2").serialize());

        assertEquals(0, log.getTerm(0));
        assertEquals(1, log.getTerm(1));
        assertEquals(2, log.getTerm(2));
        assertEquals(-1, log.getTerm(3));

        assertNotNull(log.getEntry(1));
        assertEquals("k1", new String(log.getEntry(1).toCommand().key()));
        assertNull(log.getEntry(99));
    }

    @Test
    @DisplayName("Should truncate log suffix on conflicting entries")
    void testTruncateFrom() {
        log.append(1, RaftCommand.put("k1", "v1").serialize()); // idx 1
        log.append(1, RaftCommand.put("k2", "v2").serialize()); // idx 2
        log.append(1, RaftCommand.put("k3", "v3").serialize()); // idx 3
        assertEquals(3, log.lastIndex());

        // Truncate from index 2 onwards
        log.truncateFrom(2);
        assertEquals(1, log.lastIndex());
        assertNotNull(log.getEntry(1));
        assertNull(log.getEntry(2));
    }

    @Test
    @DisplayName("Should reconcile matching and conflicting incoming entries")
    void testReconcileAndAppend() {
        log.append(1, RaftCommand.put("k1", "v1").serialize()); // idx 1, term 1
        log.append(1, RaftCommand.put("k2", "v2").serialize()); // idx 2, term 1

        // Reconcile: append entry 3 with term 2
        LogEntry e3 = LogEntry.of(2, 3, RaftCommand.put("k3", "v3"));
        long match1 = log.reconcileAndAppend(2, List.of(e3));
        assertEquals(3, match1);
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());

        // Now simulate conflicting entry at index 2 (term 3 instead of term 1)
        LogEntry conflictingE2 = LogEntry.of(3, 2, RaftCommand.put("k2_alt", "v2_alt"));
        LogEntry newE3 = LogEntry.of(3, 3, RaftCommand.put("k3_alt", "v3_alt"));

        long match2 = log.reconcileAndAppend(1, List.of(conflictingE2, newE3));
        assertEquals(3, match2);
        assertEquals(3, log.lastIndex());
        assertEquals(3, log.lastTerm());
        assertEquals(3, log.getTerm(2));
        assertEquals(3, log.getTerm(3));
    }

    @Test
    @DisplayName("Should retrieve entries range via entriesFrom")
    void testEntriesFrom() {
        for (int i = 1; i <= 5; i++) {
            log.append(1, RaftCommand.put("k" + i, "v" + i).serialize());
        }

        List<LogEntry> from3 = log.entriesFrom(3);
        assertEquals(3, from3.size());
        assertEquals(3, from3.get(0).index());
        assertEquals(5, from3.get(2).index());

        assertTrue(log.entriesFrom(10).isEmpty());
    }
}
