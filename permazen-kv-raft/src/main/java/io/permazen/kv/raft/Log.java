
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import io.permazen.kv.raft.msg.AppendRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

/**
 * Raft log information.
 *
 * <p>
 * Holds unapplied as well as recently applied log entries, as well as the last applied index, term, and configuration.
 */
final class Log {

    public static final int MIN_APPLIED = 10;                               // min # applied log entries to keep around
    public static final int MAX_APPLIED = 500;                              // max # applied log entries to keep around

    private final RaftKVDatabase raft;
    @GuardedBy("raft")
    private final LogEntry[] applied = new LogEntry[MAX_APPLIED];           // already applied log entries (circular buffer)
    @GuardedBy("raft")
    private int numApplied;                                                 // the number of valid entries in "applied"
    @GuardedBy("raft")
    private final ArrayList<LogEntry> unapplied = new ArrayList<>();        // unapplied log entries (empty if unconfigured)
    @GuardedBy("raft")
    private long lastAppliedTerm;                                           // last applied term (zero if unconfigured)
    @GuardedBy("raft")
    private long lastAppliedIndex;                                          // last applied index (zero if unconfigured)
    @GuardedBy("raft")
    private HashMap<String, String> lastAppliedConfig = new HashMap<>();    // last applied config (empty if none)

// Constructor

    Log(RaftKVDatabase raft) {
        Preconditions.checkArgument(raft != null);
        this.raft = raft;
    }

// Reset

    public void reset(boolean deleteFiles) {
        this.reset(0, 0, Collections.emptyMap(), deleteFiles);
    }

    public void reset(long lastAppliedTerm, long lastAppliedIndex, Map<String, String> config, boolean deleteFiles) {
        this.reset(lastAppliedTerm, lastAppliedIndex, Collections.emptyList(), config, deleteFiles);
    }

    public void reset(long lastAppliedTerm, long lastAppliedIndex,
      List<LogEntry> entries, Map<String, String> config, boolean deleteFiles) {

        // Sanity check
        Preconditions.checkArgument(lastAppliedTerm >= 0);
        Preconditions.checkArgument(lastAppliedIndex >= 0);
        Preconditions.checkArgument(entries != null);
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(entries.isEmpty()
          || (entries.get(0).getIndex() <= lastAppliedIndex + 1
           && entries.get(entries.size() - 1).getIndex() >= lastAppliedIndex));

        // Optionally delete log entry files on disk
        if (deleteFiles) {

            // Delete applied log files
            for (int i = 0; i < this.applied.length; i++) {
                final LogEntry logEntry = this.applied[i];
                if (logEntry != null)
                    this.raft.deleteFile(logEntry.getFile(), "old log file");
            }

            // Delete unapplied log files
            for (LogEntry logEntry : this.unapplied)
                this.raft.deleteFile(logEntry.getFile(), "old log file");
        }

        // Reset state
        Arrays.fill(this.applied, null);
        this.numApplied = 0;
        this.unapplied.clear();
        this.lastAppliedTerm = lastAppliedTerm;
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedConfig.clear();
        this.lastAppliedConfig.putAll(config);
        assert this.checkState();

        // Import log entries
        long prevIndex = -1;
        long prevTerm = -1;
        for (LogEntry logEntry : entries) {
            final long index = logEntry.getIndex();
            final long term = logEntry.getTerm();
            Preconditions.checkArgument(prevIndex == -1 || index == prevIndex + 1);
            Preconditions.checkArgument(prevTerm == -1 || term >= prevTerm);
            if (index > this.lastAppliedIndex)
                this.unapplied.add(logEntry);
            else {
                final int appliedSlot = this.getAppliedSlot(index);
                if (appliedSlot != -1) {
                    assert this.applied[appliedSlot] == null;
                    this.applied[appliedSlot] = logEntry;
                    this.numApplied++;
                } else
                    this.raft.deleteFile(logEntry.getFile(), "old log file");
            }
            prevIndex = index;
            prevTerm = term;
        }
        Preconditions.checkArgument(prevIndex == -1 || prevIndex >= lastAppliedIndex);
        assert this.checkState();
    }

// Accessors

    /**
     * Get the index of the first {@link LogEntry} for which we know the term.
     */
    public long getFirstIndex() {
        assert Thread.holdsLock(this.raft);
        return this.numApplied > 0 ? this.lastAppliedIndex - this.numApplied + 1 : this.lastAppliedIndex;
    }

    /**
     * Get the index of the last {@link LogEntry} for which we know the term.
     *
     * <p>
     * This is equal to the index of the last unapplied log entry, or else {@link #getLastAppliedIndex}
     * if there are no unapplied log entries.
     */
    public long getLastIndex() {
        assert Thread.holdsLock(this.raft);
        return this.lastAppliedIndex + this.unapplied.size();
    }

    /**
     * Get the term corresponding to {@link #getLastIndex}.
     */
    public long getLastTerm() {
        return this.getTermAtIndex(this.getLastIndex());
    }

    /**
     * Get the log entries not yet applied to the state machine.
     *
     * @return an immutable list
     */
    public List<LogEntry> getUnapplied() {
        assert Thread.holdsLock(this.raft);
        return Collections.unmodifiableList(this.unapplied);
    }

    /**
     * Get the number of already applied log entries currently retained.
     */
    public int getNumApplied() {
        assert Thread.holdsLock(this.raft);
        return this.numApplied;
    }

    /**
     * Get the number of log entries not yet applied to the state machine.
     */
    public int getNumUnapplied() {
        assert Thread.holdsLock(this.raft);
        return this.unapplied.size();
    }

    public int getNumTotal() {
        return this.getNumApplied() + getNumUnapplied();
    }

    /**
     * Get the index of the log entry last applied to the state machine.
     */
    public long getLastAppliedIndex() {
        assert Thread.holdsLock(this.raft);
        return this.lastAppliedIndex;
    }

    /**
     * Get the term associated with {@link #getLastAppliedIndex}.
     */
    public long getLastAppliedTerm() {
        assert Thread.holdsLock(this.raft);
        return this.lastAppliedTerm;
    }

    /**
     * Get the config associated with {@link #getLastAppliedIndex}.
     */
    public Map<String, String> getLastAppliedConfig() {
        assert Thread.holdsLock(this.raft);
        return Collections.unmodifiableMap(this.lastAppliedConfig);
    }

    /**
     * Get the term of the unapplied log entry at the specified index.
     *
     * @param index log index; must correspond to an unapplied log entry or be equal to {@link #getLastAppliedIndex}
     * @throws IllegalArgumentException if {@code index} is bogus
     */
    public long getTermAtIndex(long index) {
        assert Thread.holdsLock(this.raft);
        Preconditions.checkArgument(index >= this.lastAppliedIndex);
        Preconditions.checkArgument(index <= this.getLastIndex());
        return index == this.lastAppliedIndex ?
          this.lastAppliedTerm : this.unapplied.get((int)(index - this.lastAppliedIndex - 1)).getTerm();
    }

    /**
     * Get the unapplied log entry at the specified index.
     *
     * @param index log index; must correspond to an unapplied log entry
     * @throws IllegalArgumentException if {@code index} is bogus
     */
    public LogEntry getEntryAtIndex(long index) {
        assert Thread.holdsLock(this.raft);
        Preconditions.checkArgument(index > this.lastAppliedIndex);
        Preconditions.checkArgument(index <= this.getLastIndex());
        return this.unapplied.get((int)(index - this.lastAppliedIndex - 1));
    }

    /**
     * Get the term of the log entry at the specified index, if known.
     *
     * @param index log index; may be anything
     * @return log entry term or zero if unknown
     */
    public long getTermAtIndexIfKnown(long index) {
        assert Thread.holdsLock(this.raft);
        if (index > this.lastAppliedIndex) {
            if (index > this.getLastIndex())
                return 0;
            return this.unapplied.get((int)(index - this.lastAppliedIndex - 1)).getTerm();
        }
        if (index < this.lastAppliedIndex) {
            final int appliedSlot = this.getAppliedSlot(index);
            if (appliedSlot == -1)
                return 0;
            final LogEntry logEntry = this.applied[appliedSlot];
            return logEntry != null ? logEntry.getTerm() : 0;
        }
        return this.lastAppliedTerm;
    }

    /**
     * Get the applied or unapplied log entry at the specified index, if known.
     *
     * @param index log index; may be anything
     * @return log entry or null if unknown
     */
    public LogEntry getEntryAtIndexIfKnown(long index) {
        assert Thread.holdsLock(this.raft);
        if (index > this.lastAppliedIndex) {
            index -= this.lastAppliedIndex + 1;
            return index < this.unapplied.size() ? this.unapplied.get((int)index) : null;
        }
        final int appliedSlot = this.getAppliedSlot(index);
        if (appliedSlot == -1)
            return null;
        return this.applied[appliedSlot];
    }

    /**
     * Reconstruct the current config by starting with the last applied config and applying
     * configuration deltas from unapplied log entries.
     */
    public Map<String, String> buildCurrentConfig() {

        // Start with last applied config
        @SuppressWarnings("unchecked")
        final HashMap<String, String> config = (HashMap<String, String>)this.lastAppliedConfig.clone();

        // Apply any changes found in uncommitted log entries
        for (LogEntry logEntry : this.unapplied)
            logEntry.applyConfigChange(config);

        // Done
        return config;
    }

    public long getUnappliedLogMemoryUsage() {
        assert Thread.holdsLock(this.raft);
        long total = 0;
        for (LogEntry logEntry : this.unapplied)
            total += logEntry.getFileSize();
        return total;
    }

// Mutators

    /**
     * Move the next unapplied {@link LogEntry} to the applied log and update "last applied" state.
     */
    public void applyNextLogEntry() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Remove entry from "unapplied list
        final LogEntry logEntry = this.unapplied.remove(0);

        // Update "last applied" info
        final long index = ++this.lastAppliedIndex;
        assert logEntry.getIndex() == index;
        this.lastAppliedTerm = logEntry.getTerm();
        logEntry.applyConfigChange(this.lastAppliedConfig);

        // Add entry to "applied" list
        final int appliedSlot = this.getAppliedSlot(index);
        assert appliedSlot != -1;
        final LogEntry oldEntry = this.applied[appliedSlot];
        assert (oldEntry != null) == (this.numApplied == MAX_APPLIED);
        if (oldEntry != null)
            this.raft.deleteFile(oldEntry.getFile(), "old log file");
        this.applied[appliedSlot] = logEntry;
        if (this.numApplied < MAX_APPLIED)
            this.numApplied++;

        // Discard associated Writes object to save memory
        logEntry.discardWrites();
        assert this.checkState();
    }

    /**
     * Add a new {@link LogEntry} to the unapplied log.
     */
    public void addLogEntry(LogEntry logEntry) {
        assert Thread.holdsLock(this.raft);
        Preconditions.checkArgument(logEntry.getIndex() == this.getLastIndex() + 1);
        this.unapplied.add(logEntry);
        assert this.checkState();
    }

    /**
     * Discard unapplied log entries at or above the specified index because they have been overwritten.
     *
     * @param startingIndex starting index for discard
     */
    public void discardLogEntries(final long startingIndex, AppendRequest msg) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        Preconditions.checkArgument(startingIndex > this.lastAppliedIndex);
        final int minListIndex = (int)(startingIndex - this.lastAppliedIndex - 1);
        final int maxListIndex = this.unapplied.size();
        if (minListIndex >= maxListIndex)
            return;

        // Delete log entries and associated files
        final List<LogEntry> conflictList = this.unapplied.subList(minListIndex, maxListIndex);
        for (LogEntry logEntry : conflictList) {
            if (this.raft.logger.isDebugEnabled())
                this.raft.debug("deleting log entry " + logEntry + " overwritten by " + msg);
            this.raft.deleteFile(logEntry.getFile(), "overwritten log file");
        }
        conflictList.clear();
        assert this.checkState();
    }

    /**
     * Discard applied log entries up to the specified index because they are no longer needed.
     *
     * @param maxDiscardIndex maximum index of applied log entries to discard
     * @throws IllegalArgumentException if {@code maxDiscardIndex} is greater than the last applied index
     */
    public void discardAppliedLogEntries(long maxDiscardIndex) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        Preconditions.checkArgument(maxDiscardIndex <= this.lastAppliedIndex);
        final long minDiscardIndex = this.lastAppliedIndex - this.numApplied + 1;

        // Keep a minimum number of applied log entries just for good measure
        maxDiscardIndex = Math.min(maxDiscardIndex, this.lastAppliedIndex - MIN_APPLIED);

        // Delete applied log entries and associated files
        for (long index = minDiscardIndex; index <= maxDiscardIndex; index++) {
            final int appliedSlot = this.getAppliedSlot(index);
            assert appliedSlot != -1;
            final LogEntry logEntry = this.applied[appliedSlot];
            assert logEntry != null;
            if (this.raft.logger.isDebugEnabled())
                this.raft.debug("deleting log entry " + logEntry + " no longer needed");
            this.raft.deleteFile(logEntry.getFile(), "no longer needed");
            this.applied[appliedSlot] = null;
            this.numApplied--;
        }
        assert this.checkState();
    }

// Other

    /**
     * Get the array index in {@code this.applied} for the already-applied log entry with the given index.
     *
     * <p>
     * If this returns a value &gt;= 0, there may or may not actually be a {@link LogEntry} there.
     *
     * @param index log index
     * @return array index in {@code this.applied} corresponding to {@code index}, or -1 if {@code index} is too old
     * @throws IllegalArgumentException if {@code index} is greater than the last applied index
     */
    private int getAppliedSlot(long index) {
        final long offset = this.lastAppliedIndex - index;
        Preconditions.checkArgument(offset >= 0);
        if (index <= 0 || offset >= MAX_APPLIED)
            return -1;
        return (int)(index % MAX_APPLIED);
    }

    boolean checkState() {

        // Check unapplied entries
        assert this.lastAppliedTerm >= 0;
        assert this.lastAppliedIndex >= 0;
        if (!this.unapplied.isEmpty()) {
            long index = this.lastAppliedIndex;
            long term = this.lastAppliedTerm;
            for (LogEntry logEntry : this.unapplied) {
                assert logEntry.getIndex() == index + 1;
                assert logEntry.getTerm() >= term;
                index = logEntry.getIndex();
                term = logEntry.getTerm();
            }
        }

        // Check configured vs. unconfigured
        if (this.getLastIndex() > 0) {
            assert !this.buildCurrentConfig().isEmpty();
            assert this.getLastTerm() > 0;
            assert this.getLastIndex() > 0;
            assert this.getNumApplied() >= 0;
            assert this.getNumApplied() <= MAX_APPLIED;
            for (int i = 0; i <= MAX_APPLIED; i++) {
                final long index = this.lastAppliedIndex - i;
                final int appliedSlot = this.getAppliedSlot(index);
                if (index <= 0 || i == MAX_APPLIED) {
                    assert appliedSlot == -1;
                    break;
                }
                assert appliedSlot != -1;
                final LogEntry logEntry = this.applied[appliedSlot];
                assert (logEntry != null) == (i < this.numApplied);
            }
        } else {
            assert this.lastAppliedTerm == 0;
            assert this.lastAppliedIndex == 0;
            assert this.lastAppliedConfig.isEmpty();
            assert this.getNumApplied() == 0;
            assert this.getNumUnapplied() == 0;
            assert Arrays.equals(this.applied, new LogEntry[MAX_APPLIED]);
        }
        return true;
    }
}
