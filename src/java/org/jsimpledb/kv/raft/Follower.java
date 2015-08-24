
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Contains information maintained by leaders about followers.
 */
public class Follower {

    /**
     * Sorts instances by their {@linkplain Follower#getIdentity identities}.
     */
    public static final Comparator<Follower> SORT_BY_IDENTITY = new Comparator<Follower>() {
        @Override
        public int compare(Follower f1, Follower f2) {
            return f1.getIdentity().compareTo(f1.getIdentity());
        }
    };

    private final String identity;                      // follower's unique identity
    private final String address;                       // follower's network address
    private Timer updateTimer;                          // heartbeat/update timer

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

    Follower(String identity, String address, long lastLogIndex) {
        assert identity != null;
        assert address != null;
        assert lastLogIndex >= 0;
        this.identity = identity;
        this.address = address;
        this.nextIndex = lastLogIndex + 1;
    }

// Status

    /**
     * Get the identity of this follower.
     *
     * @return follower identity
     */
    public String getIdentity() {
        return this.identity;
    }

    /**
     * Get the address of this follower.
     *
     * @return follower address
     */
    public String getAddress() {
        return this.address;
    }

    /**
     * Get the index of the next log entry to send to this follower.
     *
     * @return follower next index
     */
    public long getNextIndex() {
        return this.nextIndex;
    }
    void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    /**
     * Get the index of the highest log entry in the follower's log known to match the leader's log.
     *
     * @return follower next index
     */
    public long getMatchIndex() {
        return this.matchIndex;
    }
    void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    /**
     * Get the most recent (leader's) request timestamp returned by this follower in a response, if any.
     *
     * @return follower leader timestamp, or null if no response has been received yet from this follower
     */
    public Timestamp getLeaderTimestamp() {
        return this.leaderTimestamp;
    }
    void setLeaderTimestamp(Timestamp leaderTimestamp) {
        this.leaderTimestamp = leaderTimestamp;
    }

    /**
     * Get the leader commit index most recently sent to this follower.
     *
     * @return follower leader commit index
     */
    public long getLeaderCommit() {
        return this.leaderCommit;
    }
    void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    /**
     * Determine whether we believe this follower is "synchronized".
     *
     * <p>
     * By "synchronized" we mean the most recently received {@link org.jsimpledb.kv.raft.msg.AppendResponse}
     * indicated a successful match of the previous log entry. We only send "probes" to unsynchronized followers.
     *
     * @return true if synchronized
     */
    public boolean isSynced() {
        return this.synced;
    }
    void setSynced(boolean synced) {
        this.synced = synced;
    }

    /**
     * Determine whether this follower is currently being sent a whole database snapshot download.
     *
     * @return true if snapshot install is in progress
     */
    public boolean isReceivingSnapshot() {
        return this.snapshotTransmit != null;
    }

// Package-access methods

    SnapshotTransmit getSnapshotTransmit() {
        return this.snapshotTransmit;
    }
    void setSnapshotTransmit(SnapshotTransmit snapshotTransmit) {
        this.snapshotTransmit = snapshotTransmit;
    }

    Set<LogEntry> getSkipDataLogEntries() {
        return this.skipDataLogEntries;
    }

    NavigableSet<Timestamp> getCommitLeaseTimeouts() {
        return this.commitLeaseTimeouts;
    }

    Timer getUpdateTimer() {
        return this.updateTimer;
    }
    void setUpdateTimer(Timer updateTimer) {
        this.updateTimer = updateTimer;
    }

    void updateNow() {
        this.updateTimer.timeoutNow();
    }

    void cancelSnapshotTransmit() {
        if (this.snapshotTransmit != null)  {
            this.matchIndex = Math.min(this.matchIndex, this.snapshotTransmit.getSnapshotIndex());
            this.snapshotTransmit.close();
            this.snapshotTransmit = null;
            this.synced = false;
        }
    }

// Clean up this follower

    void cleanup() {
        this.cancelSnapshotTransmit();
        this.updateTimer.cancel();
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[identity=\"" + this.identity + "\""
          + ",nextIndex=" + this.nextIndex
          + ",matchIndex=" + this.matchIndex
          + ",leaderCommit=" + this.leaderCommit
          + (this.leaderTimestamp != null ?
            ",leaderTimestamp=" + String.format("%+dms", this.leaderTimestamp.offsetFromNow()) : "")
          + ",synced=" + this.synced
          + (!this.skipDataLogEntries.isEmpty() ? ",skipDataLogEntries=" + this.skipDataLogEntries : "")
          + (this.snapshotTransmit != null ? ",snapshotTransmit=" + this.snapshotTransmit : "")
          + "]";
    }
}

