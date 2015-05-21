
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * State maintained by leaders about followers during one term.
 */
class Follower {

    private final String identity;                      // follower's unique identity
    private RaftKVDatabase.Timer updateTimer;           // heartbeat/update timer

    // Used to avoid sending data for log entry back to the follower if the follower, as the originator, already has the data
    private final HashSet<LogEntry> skipDataLogEntries = new HashSet<>();

    // Used to keep track of which leader lease timeouts this follower is waiting on to commit (read-only) transactions.
    // This is so when the leaderLeaseTimeout exceeds one of these values, we know to immediately notify the follower.
    private final TreeSet<Timestamp> commitLeaseTimeouts = new TreeSet<>();

    private long nextIndex;                             // index of the next log entry to send to peer
    private long matchIndex;                            // index of highest log entry in follower that we know matches ours
    private long leaderCommit;                          // highest leaderCommit we have sent to this follower
    private Timestamp leaderTimestamp;                  // most recent leaderTimestamp received in any AppendResponse
    private boolean synced;                             // if previous AppendEntryRequest was successful
    private SnapshotTransmit snapshotTransmit;          // in-progress snapshot transfer, if any

// Construtors

    public Follower(String identity, long lastLogIndex) {
        this.identity = identity;
        this.nextIndex = lastLogIndex + 1;
        this.leaderTimestamp = new Timestamp().offset(-24 * 60 * 60 * 1000);                // set it to a long time ago
    }

// Properties

    public String getIdentity() {
        return this.identity;
    }

    public long getNextIndex() {
        return this.nextIndex;
    }
    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return this.matchIndex;
    }
    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Timestamp getLeaderTimestamp() {
        return this.leaderTimestamp;
    }
    public void setLeaderTimestamp(Timestamp leaderTimestamp) {
        this.leaderTimestamp = leaderTimestamp;
    }

    public long getLeaderCommit() {
        return this.leaderCommit;
    }
    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    /**
     * Determine whether we believe this follower is "synchronized". By "synchronized" we mean the most recently received
     * {@link org.jsimpledb.kv.raft.msg.AppendResponse} indicated a successful match of the previous log entry.
     *
     * @return true if synchronized
     */
    public boolean isSynced() {
        return this.synced;
    }
    public void setSynced(boolean synced) {
        this.synced = synced;
    }

    public SnapshotTransmit getSnapshotTransmit() {
        return this.snapshotTransmit;
    }
    public void setSnapshotTransmit(SnapshotTransmit snapshotTransmit) {
        this.snapshotTransmit = snapshotTransmit;
    }

    public Set<LogEntry> getSkipDataLogEntries() {
        return this.skipDataLogEntries;
    }

    public NavigableSet<Timestamp> getCommitLeaseTimeouts() {
        return this.commitLeaseTimeouts;
    }

    public RaftKVDatabase.Timer getUpdateTimer() {
        return this.updateTimer;
    }
    public void setUpdateTimer(RaftKVDatabase.Timer updateTimer) {
        this.updateTimer = updateTimer;
    }

    public void cancelSnapshotTransmit() {
        if (this.snapshotTransmit != null)  {
            this.matchIndex = Math.min(this.matchIndex, this.snapshotTransmit.getSnapshotIndex());
            this.snapshotTransmit.close();
            this.snapshotTransmit = null;
            this.synced = false;
        }
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[identity=\"" + this.identity + "\""
          + ",nextIndex=" + this.nextIndex
          + ",matchIndex=" + this.matchIndex
          + ",leaderCommit=" + this.leaderCommit
          + ",leaderTimestamp=" + String.format("%+dms", this.leaderTimestamp.offsetFromNow())
          + ",synced=" + this.synced
          + (!this.skipDataLogEntries.isEmpty() ? ",skipDataLogEntries=" + this.skipDataLogEntries : "")
          + (this.snapshotTransmit != null ? ",snapshotTransmit=" + this.snapshotTransmit : "")
          + "]";
    }
}

