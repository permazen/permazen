
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;

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

    private final RaftKVDatabase raft;
    private final String identity;                      // follower's unique identity
    private final String address;                       // follower's network address

    @GuardedBy("raft")
    private Timer updateTimer;                          // heartbeat/update timer

    // Used to avoid sending data for log entry back to the follower if the follower, as the originator, already has the data
    @GuardedBy("raft")
    private final HashSet<LogEntry> skipDataLogEntries = new HashSet<>();

    // Used to keep track of which leader lease timeouts this follower is waiting on to commit (read-only) transactions.
    // This is so when the leaderLeaseTimeout exceeds one of these values, we know to immediately notify the follower.
    @GuardedBy("raft")
    private final TreeSet<Timestamp> commitLeaseTimeouts = new TreeSet<>();

    @GuardedBy("raft")
    private long nextIndex;                             // index of the next log entry to send to peer
    @GuardedBy("raft")
    private long matchIndex;                            // index of highest log entry in follower that we know matches ours
    @GuardedBy("raft")
    private long leaderCommit;                          // highest leaderCommit we have sent to this follower
    @GuardedBy("raft")
    private Timestamp leaderTimestamp;                  // most recent leaderTimestamp received in any AppendResponse
    @GuardedBy("raft")
    private Timestamp snapshotTimestamp;                // timestamp of the most recent snapshot install
    @GuardedBy("raft")
    private boolean synced;                             // if previous AppendEntryRequest was successful
    @GuardedBy("raft")
    private SnapshotTransmit snapshotTransmit;          // in-progress snapshot transfer, if any

// Construtors

    Follower(RaftKVDatabase raft, String identity, String address, long lastLogIndex) {
        assert raft != null;
        assert identity != null;
        assert address != null;
        assert lastLogIndex >= 0;
        this.raft = raft;
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
        synchronized (this.raft) {
            return this.identity;
        }
    }

    /**
     * Get the address of this follower.
     *
     * @return follower address
     */
    public String getAddress() {
        synchronized (this.raft) {
            return this.address;
        }
    }

    /**
     * Get the index of the next log entry to send to this follower.
     *
     * @return follower next index
     */
    public long getNextIndex() {
        synchronized (this.raft) {
            return this.nextIndex;
        }
    }
    void setNextIndex(long nextIndex) {
        assert Thread.holdsLock(this.raft);
        this.nextIndex = nextIndex;
    }

    /**
     * Get the index of the highest log entry in the follower's log known to match the leader's log.
     *
     * @return follower next index
     */
    public long getMatchIndex() {
        synchronized (this.raft) {
            return this.matchIndex;
        }
    }
    void setMatchIndex(long matchIndex) {
        assert Thread.holdsLock(this.raft);
        this.matchIndex = matchIndex;
    }

    /**
     * Get the most recent (leader's) request timestamp returned by this follower in a response, if any.
     *
     * @return follower leader timestamp, or null if no response has been received yet from this follower
     */
    public Timestamp getLeaderTimestamp() {
        synchronized (this.raft) {
            return this.leaderTimestamp;
        }
    }
    void setLeaderTimestamp(Timestamp leaderTimestamp) {
        assert Thread.holdsLock(this.raft);
        this.leaderTimestamp = leaderTimestamp;
    }

    /**
     * Get the (leader's) timestamp of the most recent snapshot install sent to this follower, if any.
     *
     * @return follower leader timestamp, or null if no response has been received yet from this follower
     */
    public Timestamp getSnapshotTimestamp() {
        synchronized (this.raft) {
            return this.snapshotTimestamp;
        }
    }
    void setSnapshotTimestamp(Timestamp snapshotTimestamp) {
        assert Thread.holdsLock(this.raft);
        this.snapshotTimestamp = snapshotTimestamp;
    }

    /**
     * Get the leader commit index most recently sent to this follower.
     *
     * @return follower leader commit index
     */
    public long getLeaderCommit() {
        synchronized (this.raft) {
            return this.leaderCommit;
        }
    }
    void setLeaderCommit(long leaderCommit) {
        assert Thread.holdsLock(this.raft);
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
        synchronized (this.raft) {
            return this.synced;
        }
    }
    void setSynced(boolean synced) {
        assert Thread.holdsLock(this.raft);
        this.synced = synced;
    }

    /**
     * Determine whether this follower is currently being sent a whole database snapshot download.
     *
     * @return true if snapshot install is in progress
     */
    public boolean isReceivingSnapshot() {
        synchronized (this.raft) {
            return this.snapshotTransmit != null;
        }
    }

// Package-access methods

    boolean hasLogEntry(long index) {
        assert Thread.holdsLock(this.raft);
        return this.matchIndex >= index && this.raft.isClusterMember(this.identity);
    }

    SnapshotTransmit getSnapshotTransmit() {
        assert Thread.holdsLock(this.raft);
        return this.snapshotTransmit;
    }
    void setSnapshotTransmit(SnapshotTransmit snapshotTransmit) {
        assert Thread.holdsLock(this.raft);
        this.snapshotTransmit = snapshotTransmit;
    }

    Set<LogEntry> getSkipDataLogEntries() {
        assert Thread.holdsLock(this.raft);
        return this.skipDataLogEntries;
    }

    NavigableSet<Timestamp> getCommitLeaseTimeouts() {
        assert Thread.holdsLock(this.raft);
        return this.commitLeaseTimeouts;
    }

    Timer getUpdateTimer() {
        assert Thread.holdsLock(this.raft);
        return this.updateTimer;
    }
    void setUpdateTimer(Timer updateTimer) {
        assert Thread.holdsLock(this.raft);
        this.updateTimer = updateTimer;
    }

    void updateNow() {
        assert Thread.holdsLock(this.raft);
        this.updateTimer.timeoutNow();
    }

    void cancelSnapshotTransmit() {
        assert Thread.holdsLock(this.raft);
        if (this.snapshotTransmit != null)  {
            this.matchIndex = Math.min(this.matchIndex, this.snapshotTransmit.getSnapshotIndex());
            this.snapshotTransmit.close();
            this.snapshotTransmit = null;
            this.synced = false;
        }
    }

// Clean up this follower

    void cleanup() {
        assert Thread.holdsLock(this.raft);
        this.cancelSnapshotTransmit();
        this.updateTimer.cancel();
    }

// Object

    @Override
    public String toString() {
        synchronized (this.raft) {
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
}
