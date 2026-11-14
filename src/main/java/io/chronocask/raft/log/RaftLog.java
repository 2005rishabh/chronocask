package io.chronocask.raft.log;

import io.chronocask.raft.model.LogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Replicated Raft Log managing sequence of log entries, monotonic 1-based indexing,
 * conflict truncation, and range queries.
 */
public class RaftLog {

    // entries list: index 0 corresponds to Raft log index 1
    private final List<LogEntry> entries = new ArrayList<>();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwl.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwl.writeLock();

    public RaftLog() {
    }

    /**
     * Appends a new command to the log in the given term.
     *
     * @param term    election term
     * @param command serialized command bytes
     * @return created LogEntry
     */
    public LogEntry append(long term, byte[] command) {
        writeLock.lock();
        try {
            long newIndex = entries.size() + 1L;
            LogEntry entry = LogEntry.of(term, newIndex, command);
            entries.add(entry);
            return entry;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Appends an existing LogEntry to the log.
     */
    public void append(LogEntry entry) {
        writeLock.lock();
        try {
            if (entry.index() != entries.size() + 1L) {
                throw new IllegalArgumentException("Cannot append out-of-order entry. Expected index "
                        + (entries.size() + 1L) + ", got " + entry.index());
            }
            entries.add(entry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Reconciles incoming entries starting after {@code prevLogIndex}.
     * If an existing entry conflicts with a new one (same index but different term),
     * delete the existing entry and all that follow it. Then append any new entries.
     *
     * @param prevLogIndex index immediately preceding new entries
     * @param newEntries   entries to reconcile and append
     * @return highest index in the log after reconciliation
     */
    public long reconcileAndAppend(long prevLogIndex, List<LogEntry> newEntries) {
        writeLock.lock();
        try {
            if (newEntries == null || newEntries.isEmpty()) {
                return lastIndex();
            }

            for (LogEntry newEntry : newEntries) {
                long idx = newEntry.index();
                if (idx <= entries.size()) {
                    LogEntry existing = entries.get((int) (idx - 1));
                    if (existing.term() != newEntry.term()) {
                        // Conflict detected: truncate from idx to end
                        truncateFrom(idx);
                        entries.add(newEntry);
                    }
                    // If term matches, entry is already identical, do nothing
                } else if (idx == entries.size() + 1) {
                    entries.add(newEntry);
                } else {
                    throw new IllegalStateException("Gap detected in Raft log append. Expected index "
                            + (entries.size() + 1) + ", got " + idx);
                }
            }
            return lastIndex();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Truncates all entries from the specified index (inclusive) to the end of the log.
     */
    public void truncateFrom(long fromIndex) {
        writeLock.lock();
        try {
            if (fromIndex <= 0) {
                entries.clear();
                return;
            }
            int targetSize = (int) (fromIndex - 1);
            while (entries.size() > targetSize) {
                entries.remove(entries.size() - 1);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns the entry at the given 1-based log index, or null if outside bounds.
     */
    public LogEntry getEntry(long index) {
        readLock.lock();
        try {
            if (index <= 0 || index > entries.size()) {
                return null;
            }
            return entries.get((int) (index - 1));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the term for the given index. Index 0 returns term 0 (baseline).
     */
    public long getTerm(long index) {
        readLock.lock();
        try {
            if (index == 0) {
                return 0;
            }
            if (index < 0 || index > entries.size()) {
                return -1;
            }
            return entries.get((int) (index - 1)).term();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the 1-based index of the last entry in the log, or 0 if empty.
     */
    public long lastIndex() {
        readLock.lock();
        try {
            return entries.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the term of the last entry in the log, or 0 if empty.
     */
    public long lastTerm() {
        readLock.lock();
        try {
            if (entries.isEmpty()) {
                return 0;
            }
            return entries.getLast().term();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns an unmodifiable sublist of entries starting from {@code fromIndex} (inclusive).
     */
    public List<LogEntry> entriesFrom(long fromIndex) {
        readLock.lock();
        try {
            if (fromIndex <= 0 || fromIndex > entries.size()) {
                return Collections.emptyList();
            }
            int start = (int) (fromIndex - 1);
            return List.copyOf(entries.subList(start, entries.size()));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Total number of entries in the log.
     */
    public int size() {
        readLock.lock();
        try {
            return entries.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Clears all log entries.
     */
    public void clear() {
        writeLock.lock();
        try {
            entries.clear();
        } finally {
            writeLock.unlock();
        }
    }
}
