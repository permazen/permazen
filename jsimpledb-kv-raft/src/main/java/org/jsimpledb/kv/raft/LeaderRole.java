
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

import net.jcip.annotations.GuardedBy;

import org.dellroad.stuff.io.ByteBufferInputStream;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.mvcc.Reads;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.raft.msg.AppendRequest;
import org.jsimpledb.kv.raft.msg.AppendResponse;
import org.jsimpledb.kv.raft.msg.CommitRequest;
import org.jsimpledb.kv.raft.msg.CommitResponse;
import org.jsimpledb.kv.raft.msg.GrantVote;
import org.jsimpledb.kv.raft.msg.InstallSnapshot;
import org.jsimpledb.kv.raft.msg.Message;
import org.jsimpledb.kv.raft.msg.RequestVote;

/**
 * Raft leader role.
 */
public class LeaderRole extends Role {

    // Timestamp scrub interval
    private static final int TIMESTAMP_SCRUB_INTERVAL = 24 * 60 * 60 * 1000;            // once a day

    // Our followers
    @GuardedBy("raft")
    private final HashMap<String, Follower> followerMap = new HashMap<>();

    // Our leadership "lease" timeout - i.e., the earliest time another leader could possibly be elected
    @GuardedBy("raft")
    private Timestamp leaseTimeout;

    // Service tasks
    private final Service updateLeaderCommitIndexService = new Service(this, "update leader commitIndex") {
        @Override
        public void run() {
            LeaderRole.this.updateLeaderCommitIndex();
        }
    };
    private final Service updateLeaseTimeoutService = new Service(this, "update lease timeout") {
        @Override
        public void run() {
            LeaderRole.this.updateLeaseTimeout();
        }
    };
    private final Service updateKnownFollowersService = new Service(this, "update known followers") {
        @Override
        public void run() {
            LeaderRole.this.updateKnownFollowers();
        }
    };
    private final Timer checkApplyTimer = new Timer(this.raft, "check apply entries", new Service(this, "check apply entries") {
        @Override
        public void run() {
            LeaderRole.this.checkApplyEntries();
        }
    });
    private final Timer timestampScrubTimer = new Timer(this.raft, "scrub timestamps", new Service(this, "scrub timestamps") {
        @Override
        public void run() {
            LeaderRole.this.scrubTimestamps();
        }
    });

// Constructors

    LeaderRole(RaftKVDatabase raft) {
        super(raft);
    }

// Status & Debugging

    /**
     * Get this leader's known followers.
     *
     * <p>
     * The returned list is a copy; changes have no effect on this instance.
     *
     * @return this leader's followers
     */
    public List<Follower> getFollowers() {
        final ArrayList<Follower> list;
        synchronized (this.raft) {
            list = new ArrayList<>(this.followerMap.values());
        }
        Collections.sort(list, Follower.SORT_BY_IDENTITY);
        return list;
    }

    /**
     * Get this leader's "lease timeout".
     *
     * <p>
     * This is the earliest possible time at which some other, new leader could be elected in a new term.
     * Consequently, it is the earliest possible time at which any entry that this leader is unaware of
     * could be appended to the Raft log.
     *
     * <p>
     * Normally, if followers are responding to {@link AppendRequest}s properly, this should be a value
     * in the (near) future. This allows the leader to make the assumption, up until that point in time,
     * that its log is fully up-to-date.
     *
     * <p>
     * Until it hears from a majority of followers, a leader will not have a lease timeout established yet.
     * In that case this method returns null.
     *
     * <p>
     * This method may also return null if a previous lease timeout has gotten very stale (e.g., isolated leader).
     *
     * @return this leader's lease timeout, or null if none is established yet
     */
    public Timestamp getLeaseTimeout() {
        synchronized (this.raft) {
            return this.leaseTimeout;
        }
    }

    /**
     * Force this leader to step down.
     *
     * @throws IllegalStateException if this role is no longer active or election timer is not running
     */
    public void stepDown() {
        synchronized (this.raft) {
            Preconditions.checkState(this.raft.role == this, "role is no longer active");
            this.debug("stepping down as leader due to invocation of stepDown()");
            this.raft.changeRole(new FollowerRole(this.raft));
        }
    }

// Lifecycle

    @Override
    void setup() {
        assert Thread.holdsLock(this.raft);
        super.setup();
        if (this.log.isDebugEnabled())
            this.debug("entering leader role in term " + this.raft.currentTerm);

        // Generate follower list
        this.updateKnownFollowers();

        // Append a "dummy" log entry with my current term. This allows us to advance the commit index when the last
        // entry in our log is from a prior term. This is needed to avoid the problem where a transaction could end up
        // waiting indefinitely for its log entry with a prior term number to be committed.
        final LogEntry logEntry;
        try {
            logEntry = this.applyNewLogEntry(new NewLogEntry(this.raft, new LogEntry.Data(new Writes(), null)));
        } catch (Exception e) {
            this.error("error attempting to apply initial log entry", e);
            return;
        }
        if (this.log.isDebugEnabled())
            this.debug("added log entry " + logEntry + " to commit at the beginning of my new term");

        // Rebase transactions
        this.rebaseTransactions();

        // Start check apply timer
        if (!this.raft.raftLog.isEmpty())
            this.checkApplyTimer.timeoutAfter(this.raft.heartbeatTimeout);

        // Start timestamp scrub timer
        this.timestampScrubTimer.timeoutAfter(TIMESTAMP_SCRUB_INTERVAL);
    }

    @Override
    void shutdown() {
        assert Thread.holdsLock(this.raft);
        for (Follower follower : this.followerMap.values())
            follower.cleanup();
        this.checkApplyTimer.cancel();
        this.timestampScrubTimer.cancel();
        super.shutdown();
    }

// Service

    @Override
    void outputQueueEmpty(String address) {
        assert Thread.holdsLock(this.raft);

        // Find matching follower(s) and update them if needed
        for (Follower follower : this.followerMap.values()) {
            if (follower.getAddress().equals(address)) {
                if (this.log.isTraceEnabled())
                    this.trace("updating peer \"" + follower.getIdentity() + "\" after queue empty notification");
                this.raft.requestService(new UpdateFollowerService(follower));
            }
        }
    }

    @Override
    void applyCommittedLogEntries() {
        assert Thread.holdsLock(this.raft);
        super.applyCommittedLogEntries();

        // Stop check apply timer if there are none left
        if (this.raft.raftLog.isEmpty() && this.checkApplyTimer.isRunning())
            this.checkApplyTimer.cancel();
    }

    @Override
    boolean roleMayApplyLogEntry(LogEntry logEntry) {
        assert Thread.holdsLock(this.raft);

        // If any snapshots are in progress, we don't want to apply any log entries with index greater than the snapshot's
        // index, because then we'd lose the ability to update the follower with that log entry, and as a result just have
        // to send a snapshot again. However, we impose a limit on how long we'll wait for a slow follower.
        for (Follower follower : this.followerMap.values()) {
            final SnapshotTransmit snapshotTransmit = follower.getSnapshotTransmit();
            if (snapshotTransmit == null)
                continue;
            if (snapshotTransmit.getSnapshotIndex() < logEntry.getIndex()
              && snapshotTransmit.getAge() < RaftKVDatabase.MAX_SNAPSHOT_TRANSMIT_AGE) {
                if (this.log.isTraceEnabled()) {
                    this.trace("delaying application of " + logEntry + " because of in-progress snapshot install of "
                      + snapshotTransmit.getSnapshotIndex() + "t" + snapshotTransmit.getSnapshotTerm()
                      + " to " + follower);
                }
                return false;
            }
        }

        // If some follower does not yet have the log entry, wait for them to get it (up to some maximum time).
        // If the follower appears to be offline, don't bother waiting.
        final int maxLogEntryAge = this.raft.maxFollowerAckHeartbeats * this.raft.heartbeatTimeout;
        if (logEntry.getAge() < maxLogEntryAge) {
            final Timestamp minLeaderTimestamp = new Timestamp().offset(-maxLogEntryAge);
            for (Follower follower : this.followerMap.values()) {

                // Has this follower acknowledged reciept of the log entry?
                // If so, then the follower has already rebased any rebasable transactions.
                if (follower.getMatchIndex() >= logEntry.getIndex())
                    continue;

                // If we haven't heard from this follower in a while, don't bother waiting for it
                final Timestamp leaderTimestamp = follower.getLeaderTimestamp();
                if (leaderTimestamp == null || leaderTimestamp.compareTo(minLeaderTimestamp) <= 0)
                    continue;

                // Wait for follower to do so before applying to state machine
                if (this.log.isTraceEnabled()) {
                    this.trace("delaying application of " + logEntry + " (age "
                      + logEntry.getAge() + " < " + maxLogEntryAge + ") because of slow " + follower);
                }
                return false;
            }
        }

        // OK
        return true;
    }

    // We have to periodically check if we can apply log entries, because the condition is time-dependent
    private void checkApplyEntries() {
        assert Thread.holdsLock(this.raft);
        this.raft.requestService(this.applyCommittedLogEntriesService);
        if (!this.raft.raftLog.isEmpty())
            this.checkApplyTimer.timeoutAfter(this.raft.heartbeatTimeout);
    }

    /**
     * Update my {@code commitIndex} based on followers' {@code matchIndex}'s.
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After any log entry has been added to the log, if we have zero followers</li>
     *  <li>After a log entry that contains a configuration change has been added to the log</li>
     *  <li>After a follower's {@linkplain Follower#getMatchIndex match index} has advanced</li>
     * </ul>
     */
    private void updateLeaderCommitIndex() {
        assert Thread.holdsLock(this.raft);

        // Find highest index for which a majority of cluster members have ack'd the corresponding log entry from my term
        final int totalCount = this.raft.currentConfig.size();                          // total possible nodes
        final int requiredCount = totalCount / 2 + 1;                                   // require a majority
        final int startingCount = this.raft.isClusterMember() ? 1 : 0;                  // count myself, if member
        long maxCommitIndex = this.raft.commitIndex;
        int commitCount = -1;
        for (long index = this.raft.commitIndex + 1; index <= this.raft.getLastLogIndex(); index++) {

            // Count the number of nodes (possibly including myself) that have a copy of the log entry at index
            final int count = startingCount + this.countFollowersWithLogEntry(index);

            // The log entry term must match my current term (exception: unless every node has it)
            final long term = this.raft.getLogTermAtIndex(index);
            if (count < totalCount && term != this.raft.currentTerm)
                continue;

            // Do a majority of cluster nodes have this log entry?
            if (count < requiredCount) {
                if (term >= this.raft.currentTerm)                                      // there's no point in going further
                    break;
                continue;                                                               // a later term log entry might work
            }

            // We have a winner
            maxCommitIndex = index;
            commitCount = count;
        }

        // Update commit index if it advanced
        if (maxCommitIndex > this.raft.commitIndex) {

            // Update index
            if (this.log.isDebugEnabled()) {
                this.debug("advancing commit index from " + this.raft.commitIndex + " -> " + maxCommitIndex + " based on "
                  + commitCount + "/" + totalCount + " nodes having received " + this.raft.getLogEntryAtIndex(maxCommitIndex));
            }
            this.raft.commitIndex = maxCommitIndex;

            // Perform various service
            this.raft.requestService(this.checkReadyTransactionsService);
            this.raft.requestService(this.checkWaitingTransactionsService);
            this.raft.requestService(this.triggerKeyWatchesService);
            this.raft.requestService(this.applyCommittedLogEntriesService);

            // Notify all (up-to-date) followers with the updated leaderCommit
            this.updateAllSynchronizedFollowersNow();

            // If we are no longer a member of the cluster, step down after the most recent config change is committed
            if (!this.raft.isClusterMember() && this.raft.commitIndex >= this.findMostRecentConfigChange()) {
                if (this.log.isDebugEnabled())
                    this.log.debug("stepping down as leader of cluster (no longer a member)");
                this.stepDown();
            }
        }
    }

    private int countFollowersWithLogEntry(long index) {
        assert index <= this.raft.getLastLogIndex();

        // Count the number of followers (who are also cluster members) that have a copy of the log entry at the specified index
        int nodesWithLogEntry = 0;
        for (Follower follower : this.followerMap.values()) {
            if (follower.hasLogEntry(index))
                nodesWithLogEntry++;
        }

        // Done
        return nodesWithLogEntry;
    }

    /**
     * Update my {@code leaseTimeout} based on followers' returned {@code leaderTimeout}'s.
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After a follower has replied with an {@link AppendResponse} containing a newer
     *      {@linkplain AppendResponse#getLeaderTimestamp leader timestamp} than before</li>
     * </ul>
     */
    private void updateLeaseTimeout() {
        assert Thread.holdsLock(this.raft);

        // Only needed when we have followers
        final int numFollowers = this.followerMap.size();
        if (numFollowers == 0)
            return;

        // Get all cluster member leader timestamps, sorted in increasing order
        final Timestamp[] leaderTimestamps = new Timestamp[this.raft.currentConfig.size()];
        int index = 0;
        if (this.raft.isClusterMember())
            leaderTimestamps[index++] = new Timestamp();                        // this represents my own vote
        for (Follower follower : this.followerMap.values()) {
            if (this.raft.isClusterMember(follower.getIdentity()))
                leaderTimestamps[index++] = follower.getLeaderTimestamp();      // note follower timestamps could be null
        }
        Arrays.sort(leaderTimestamps, Timestamp.NULL_FIRST_SORT);

        //
        // Calculate highest leaderTimeout shared by a majority of cluster members, based on sorted array:
        //
        //  # nodes    timestamps
        //  -------    ----------
        //     5       [ ][ ][x][x][x]        3/5 x's make a majority at index (5 - 1)/2 = 2
        //     6       [ ][ ][x][x][x][x]     4/6 x's make a majority at index (6 - 1)/2 = 2
        //
        // The minimum leaderTimeout shared by a majority of nodes is at index (leaderTimestamps.length - 1) / 2.
        // We then add the minimum election timeout, then subtract a little for clock drift.
        //
        final Timestamp newLeaseTimeout = leaderTimestamps[(leaderTimestamps.length + 1) / 2]
          .offset((int)(this.raft.minElectionTimeout * (1.0f - RaftKVDatabase.MAX_CLOCK_DRIFT) - 1));
        if (Timestamp.NULL_FIRST_SORT.compare(newLeaseTimeout, this.leaseTimeout) > 0) {
            assert newLeaseTimeout != null;

            // Update my leader lease timeout
            if (this.log.isTraceEnabled())
                this.trace("updating my lease timeout from " + this.leaseTimeout + " -> " + newLeaseTimeout);
            this.leaseTimeout = newLeaseTimeout;

            // Notify any followers who care
            for (Follower follower : this.followerMap.values()) {
                final NavigableSet<Timestamp> timeouts = follower.getCommitLeaseTimeouts().headSet(this.leaseTimeout, true);
                if (!timeouts.isEmpty()) {
                    follower.updateNow();                           // notify follower so it can commit waiting transaction(s)
                    timeouts.clear();
                }
            }
        }
    }

    /**
     * Scrub timestamps to avoid roll-over.
     *
     * <p>
     * This should be invoked periodically, e.g., once a day.
     */
    private void scrubTimestamps() {
        assert Thread.holdsLock(this.raft);
        if (this.log.isTraceEnabled())
            this.trace("scrubbing timestamps");
        for (Follower follower : this.followerMap.values()) {
            final Timestamp leaderTimestamp = follower.getLeaderTimestamp();
            if (leaderTimestamp != null && leaderTimestamp.isRolloverDanger()) {
                if (this.log.isDebugEnabled())
                    this.debug("scrubbing " + follower + " leader timestamp " + leaderTimestamp);
                follower.setLeaderTimestamp(null);
            }
            final Timestamp snapshotTimestamp = follower.getSnapshotTimestamp();
            if (snapshotTimestamp != null && snapshotTimestamp.isRolloverDanger()) {
                if (this.log.isDebugEnabled())
                    this.debug("scrubbing " + follower + " snapshot timestamp " + snapshotTimestamp);
                follower.setSnapshotTimestamp(null);
            }
            for (Iterator<Timestamp> i = follower.getCommitLeaseTimeouts().iterator(); i.hasNext(); ) {
                final Timestamp leaseTimestamp = i.next();
                if (leaseTimestamp.isRolloverDanger()) {
                    if (this.log.isDebugEnabled())
                        this.debug("scrubbing " + follower + " commit lease timestamp " + leaseTimestamp);
                    i.remove();
                }
            }
        }
        if (this.leaseTimeout != null && this.leaseTimeout.isRolloverDanger()) {
            if (this.log.isDebugEnabled())
                this.debug("scrubbing leader lease timestamp " + this.leaseTimeout);
            this.leaseTimeout = null;
        }
    }

    /**
     * Update our list of followers to match our current configuration.
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After a log entry that contains a configuration change has been added to the log</li>
     *  <li>When the {@linkplain Follower#getNextIndex next index} of a follower not in the current config advances</li>
     * </ul>
     */
    private void updateKnownFollowers() {
        assert Thread.holdsLock(this.raft);

        // Compare known followers with the current config and determine who needs to be be added or removed
        final HashSet<String> adds = new HashSet<>(this.raft.currentConfig.keySet());
        adds.removeAll(this.followerMap.keySet());
        adds.remove(this.raft.identity);
        final HashSet<String> dels = new HashSet<>(this.followerMap.keySet());
        dels.removeAll(this.raft.currentConfig.keySet());

        // Keep around a follower after its removal until it receives the config change that removed it
        for (Follower follower : this.followerMap.values()) {

            // Is this follower scheduled for deletion?
            final String peer = follower.getIdentity();
            if (!dels.contains(peer))
                continue;

            // Find the most recent log entry containing a config change in which the follower was removed
            final String node = follower.getIdentity();
            final long index = this.findMostRecentConfigChangeMatching(new Predicate<String[]>() {
                @Override
                public boolean apply(String[] configChange) {
                    return configChange[0].equals(node) && configChange[1] == null;
                }
            });

            // If follower has not received that log entry yet, keep on updating them until they do
            if (follower.getMatchIndex() < index)
                dels.remove(peer);
        }

        // Add new followers
        for (String peer : adds) {
            final String address = this.raft.currentConfig.get(peer);
            final Follower follower = new Follower(this.raft, peer, address, this.raft.getLastLogIndex());
            if (this.log.isDebugEnabled())
                this.debug("adding new follower \"" + peer + "\" at " + address);
            follower.setUpdateTimer(new Timer(this.raft, "update timer for \"" + peer + "\"", new UpdateFollowerService(follower)));
            this.followerMap.put(peer, follower);
            follower.updateNow();                                               // schedule an immediate update
        }

        // Remove old followers
        for (String peer : dels) {
            final Follower follower = this.followerMap.remove(peer);
            if (this.log.isDebugEnabled())
                this.debug("removing old follower \"" + peer + "\"");
            follower.cleanup();
        }
    }

    /**
     * Check whether a follower needs an update and send one if so.
     *
     * <p>
     * This should be invoked:
     * <ul>
     *  <li>After a new follower has been added</li>
     *  <li>When the output queue for a follower goes from non-empty to empty</li>
     *  <li>After the follower's {@linkplain Follower#getUpdateTimer update timer} has expired</li>
     *  <li>After a new log entry has been added to the log (all followers)</li>
     *  <li>After receiving an {@link AppendResponse} that caused the follower's
     *      {@linkplain Follower#getNextIndex next index} to change</li>
     *  <li>After receiving the first positive {@link AppendResponse} to a probe</li>
     *  <li>After our {@code commitIndex} has advanced (all followers)</li>
     *  <li>After our {@code leaseTimeout} has advanced past one or more of a follower's
     *      {@linkplain Follower#getCommitLeaseTimeouts commit lease timeouts} (with update timer reset)</li>
     *  <li>After sending a {@link CommitResponse} with a non-null {@linkplain CommitResponse#getCommitLeaderLeaseTimeout
     *      commit leader lease timeout} (all followers) to probe for updated leader timestamps</li>
     *  <li>After starting, aborting, or completing a snapshot install for a follower</li>
     * </ul>
     */
    private void updateFollower(Follower follower) {
        assert Thread.holdsLock(this.raft);

        // If follower has an in-progress snapshot that has become too stale, abort it
        final String peer = follower.getIdentity();
        SnapshotTransmit snapshotTransmit = follower.getSnapshotTransmit();
        if (snapshotTransmit != null && snapshotTransmit.getSnapshotIndex() < this.raft.lastAppliedIndex) {
            if (this.log.isDebugEnabled())
                this.debug("aborting stale snapshot install for " + follower);
            follower.cancelSnapshotTransmit();
            follower.updateNow();
        }

        // Is follower's queue empty? If not, hold off until then
        if (this.raft.isTransmitting(follower.getAddress())) {
            if (this.log.isTraceEnabled())
                this.trace("no update for \"" + peer + "\": output queue still not empty");
            return;
        }

        // Handle any in-progress snapshot install
        if ((snapshotTransmit = follower.getSnapshotTransmit()) != null) {

            // Send the next chunk in transmission, if any
            final long pairIndex = snapshotTransmit.getPairIndex();
            final ByteBuffer chunk = snapshotTransmit.getNextChunk();
            boolean synced = true;
            if (chunk != null) {

                // Send next chunk
                final InstallSnapshot msg = new InstallSnapshot(this.raft.clusterId, this.raft.identity, peer,
                  this.raft.currentTerm, snapshotTransmit.getSnapshotTerm(), snapshotTransmit.getSnapshotIndex(), pairIndex,
                  pairIndex == 0 ? snapshotTransmit.getSnapshotConfig() : null, !snapshotTransmit.hasMoreChunks(), chunk);
                if (this.raft.sendMessage(msg)) {
                    follower.setSnapshotTimestamp(new Timestamp());
                    return;
                }
                if (this.log.isDebugEnabled())
                    this.debug("canceling snapshot install for " + follower + " due to failure to send " + msg);

                // Message failed -> snapshot is fatally wounded, so cancel it
                synced = false;
            }
            if (synced) {
                if (this.log.isDebugEnabled())
                    this.debug("completed snapshot install for out-of-date " + follower);
            }

            // Snapshot transmit is complete (or failed)
            follower.cancelSnapshotTransmit();

            // Trigger an immediate regular update
            follower.setNextIndex(snapshotTransmit.getSnapshotIndex() + 1);
            follower.setSynced(synced);
            follower.updateNow();
            this.raft.requestService(new UpdateFollowerService(follower));
            return;
        }

        // Are we still waiting for the update timer to expire?
        if (!follower.getUpdateTimer().pollForTimeout()) {
            boolean waitForTimerToExpire = true;

            // Don't wait for the update timer to expire if:
            //  (a) The follower is sync'd; AND
            //      (y) We have a new log entry that the follower doesn't have; OR
            //      (y) We have a new leaderCommit that the follower doesn't have
            // The effect is that we will pipeline updates to synchronized followers.
            if (follower.isSynced()
              && (follower.getLeaderCommit() != this.raft.commitIndex
               || follower.getNextIndex() <= this.raft.getLastLogIndex()))
                waitForTimerToExpire = false;

            // Wait for timer to expire
            if (waitForTimerToExpire) {
                if (this.log.isTraceEnabled()) {
                    this.trace("no update for \"" + follower.getIdentity() + "\": timer not expired yet, and follower is "
                      + (follower.isSynced() ? "up to date" : "not synced"));
                }
                return;
            }
        }

        // Get index of the next log entry to send to follower
        final long nextIndex = follower.getNextIndex();

        // If follower is too far behind, we must do a snapshot install
        if (nextIndex <= this.raft.lastAppliedIndex) {
            final MostRecentView view = new MostRecentView(this.raft, true);
            follower.setSnapshotTransmit(new SnapshotTransmit(view.getTerm(),
              view.getIndex(), view.getConfig(), view.getSnapshot(), view.getView()));
            if (this.log.isDebugEnabled())
                this.debug("started snapshot install for out-of-date " + follower);
            this.raft.requestService(new UpdateFollowerService(follower));
            return;
        }

        // Restart update timer here (to avoid looping if an error occurs below)
        follower.getUpdateTimer().timeoutAfter(this.raft.heartbeatTimeout);

        // Send actual data if follower is synced and there is a log entry to send; otherwise, just send a probe
        final AppendRequest msg;
        if (!follower.isSynced() || nextIndex > this.raft.getLastLogIndex()) {

            // Create probe-only message
            msg = new AppendRequest(this.raft.clusterId, this.raft.identity, peer, this.raft.currentTerm, new Timestamp(),
              this.leaseTimeout, this.raft.commitIndex, this.raft.getLogTermAtIndex(nextIndex - 1), nextIndex - 1);
        } else {

            // Get log entry to send
            final LogEntry logEntry = this.raft.getLogEntryAtIndex(nextIndex);

            // If the log entry correspond's to follower's transaction, don't send the data because follower already has it.
            // But only do this optimization the first time, in case something goes wrong on the follower's end.
            ByteBuffer mutationData = null;
            if (!follower.getSkipDataLogEntries().remove(logEntry)) {
                try {
                    mutationData = logEntry.getContent();
                } catch (IOException e) {
                    this.error("error reading log file " + logEntry.getFile(), e);
                    return;
                }
            }

            // Create message
            msg = new AppendRequest(this.raft.clusterId, this.raft.identity, peer, this.raft.currentTerm, new Timestamp(),
              this.leaseTimeout, this.raft.commitIndex, this.raft.getLogTermAtIndex(nextIndex - 1), nextIndex - 1,
              logEntry.getTerm(), mutationData);
        }

        // Send update
        final boolean sent = this.raft.sendMessage(msg);

        // Advance next index if a log entry was sent; we allow pipelining log entries when synchronized
        if (sent && !msg.isProbe()) {
            assert follower.isSynced();
            follower.setNextIndex(Math.min(follower.getNextIndex(), this.raft.getLastLogIndex()) + 1);
        }

        // Update the leaderCommit we sent to the follower
        if (sent)
            follower.setLeaderCommit(msg.getLeaderCommit());
    }

    private void updateAllSynchronizedFollowersNow() {
        assert Thread.holdsLock(this.raft);
        for (Follower follower : this.followerMap.values()) {
            if (follower.isSynced())
                follower.updateNow();
        }
    }

    private class UpdateFollowerService extends Service {

        private final Follower follower;

        UpdateFollowerService(Follower follower) {
            super(LeaderRole.this, "update follower \"" + follower.getIdentity() + "\"");
            this.follower = follower;
        }

        @Override
        public void run() {
            LeaderRole.this.updateFollower(this.follower);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final UpdateFollowerService that = (UpdateFollowerService)obj;
            return this.follower.equals(that.follower);
        }

        @Override
        public int hashCode() {
            return this.follower.hashCode();
        }
    }

// Transactions

    @Override
    void checkReadyLinearizableTransaction(RaftKVTransaction tx, boolean readOnly) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        assert tx.getState().equals(TxState.COMMIT_READY);

        // Check for conflicts
        final String error = this.checkConflicts(tx.baseTerm, tx.baseIndex, tx.view.getReads(),
          this.raft.dumpConflicts ? "local txId=" + tx.txId : null);
        if (error != null) {
            if (this.log.isDebugEnabled())
                this.debug("local transaction " + tx + " failed due to conflict: " + error);
            throw new RetryTransactionException(tx, error);
        }

        // Handle read-only vs. read-write transaction
        if (readOnly) {
            if (this.leaseTimeout != null && this.leaseTimeout.offsetFromNow() > 0)
                this.advanceReadyTransaction(tx, tx.baseTerm, tx.baseIndex);
            else
                this.advanceReadyTransaction(tx, this.raft.getLastLogTerm(), this.raft.getLastLogIndex());
            return;
        } else {

            // If a config change is involved, check whether we can safely apply it
            if (tx.getConfigChange() != null && !this.mayApplyNewConfigChange())
                return;

            // Commit transaction as a new log entry
            final LogEntry logEntry;
            try {
                logEntry = this.applyNewLogEntry(new NewLogEntry(tx));
            } catch (IllegalStateException e) {
                throw new RetryTransactionException(tx, e.getMessage());
            } catch (Exception e) {
                throw new KVTransactionException(tx, "error attempting to persist transaction", e);
            }
            if (this.log.isDebugEnabled())
                this.debug("added log entry " + logEntry + " for local transaction " + tx);

            // Update transaction
            this.advanceReadyTransaction(tx, logEntry.getTerm(), logEntry.getIndex());

            // Rebase transactions
            this.rebaseTransactions();
        }
    }

    // Determine whether it's safe to append a log entry with a configuration change
    private boolean mayApplyNewConfigChange() {
        assert Thread.holdsLock(this.raft);

        // Rule #1: this leader must have committed at least one log entry in this term
        assert this.raft.commitIndex >= this.raft.lastAppliedIndex;
        if (this.raft.getLogTermAtIndex(this.raft.commitIndex) < this.raft.currentTerm)
            return false;

        // Rule #2: there must be no previous config change that is still uncommitted
        for (int i = (int)(this.raft.commitIndex - this.raft.lastAppliedIndex) + 1; i < this.raft.raftLog.size(); i++) {
            if (this.raft.raftLog.get(i).getConfigChange() != null)
                return false;
        }

        // OK
        return true;
    }

// Message

    @Override
    void caseAppendRequest(AppendRequest msg, NewLogEntry newLogEntry) {
        assert Thread.holdsLock(this.raft);
        this.failDuplicateLeader(msg);
        if (newLogEntry != null)
            newLogEntry.cancel();
    }

    @Override
    void caseAppendResponse(AppendResponse msg) {
        assert Thread.holdsLock(this.raft);

        // Find follower
        final Follower follower = this.findFollower(msg);
        if (follower == null)
            return;

        // Update follower's last rec'd leader timestamp
        if (follower.getLeaderTimestamp() == null || msg.getLeaderTimestamp().compareTo(follower.getLeaderTimestamp()) > 0) {
            follower.setLeaderTimestamp(msg.getLeaderTimestamp());
            this.raft.requestService(this.updateLeaseTimeoutService);
        }

        // Ignore if a snapshot install is in progress
        if (follower.getSnapshotTransmit() != null) {
            if (this.log.isTraceEnabled())
                this.trace("rec'd " + msg + " while sending snapshot install; ignoring");
            return;
        }

        // Ignore a response to a request that was sent prior to the most resent snapshot install
        if (follower.getSnapshotTimestamp() != null && msg.getLeaderTimestamp().compareTo(follower.getSnapshotTimestamp()) < 0) {
            if (this.log.isTraceEnabled())
                this.trace("rec'd " + msg + " sent prior to snapshot install; ignoring");
            return;
        }

        // Flag indicating we might want to update follower when done
        boolean updateFollowerAgain = false;

        // Update follower's match index
        if (msg.getMatchIndex() > follower.getMatchIndex()) {
            follower.setMatchIndex(msg.getMatchIndex());
            this.raft.requestService(this.updateLeaderCommitIndexService);
            if (!this.raft.isClusterMember(follower.getIdentity()))
                this.raft.requestService(this.updateKnownFollowersService);
        }

        // Check result and update follower's next index
        final boolean wasSynced = follower.isSynced();
        final long previousNextIndex = follower.getNextIndex();
        if (!msg.isSuccess())
            follower.setNextIndex(Math.max(follower.getNextIndex() - 1, 1));
        follower.setSynced(msg.isSuccess());
        if (follower.isSynced() != wasSynced) {
            if (this.log.isDebugEnabled()) {
                this.debug("sync status of \"" + follower.getIdentity() + "\" changed -> "
                  + (!follower.isSynced() ? "not " : "") + "synced");
            }
            updateFollowerAgain = true;
        }

        // Use follower's match index as a lower bound on follower's next index.
        follower.setNextIndex(Math.max(follower.getNextIndex(), follower.getMatchIndex() + 1));

        // Use follower's last log index as an upper bound on follower's next index.
        follower.setNextIndex(Math.min(msg.getLastLogIndex() + 1, follower.getNextIndex()));

        // Update follower again if next index has changed
        updateFollowerAgain |= follower.getNextIndex() != previousNextIndex;

        // Debug
        if (this.log.isTraceEnabled())
            this.trace("updated follower: " + follower + ", update again = " + updateFollowerAgain);

        // Immediately update follower again (if appropriate)
        if (updateFollowerAgain)
            this.raft.requestService(new UpdateFollowerService(follower));
    }

    @Override
    void caseCommitRequest(CommitRequest msg, NewLogEntry newLogEntry) {
        assert Thread.holdsLock(this.raft);

        // Find follower
        final Follower follower = this.findFollower(msg);
        if (follower == null) {
            if (newLogEntry != null)
                newLogEntry.cancel();
            return;
        }

        // Decode reads, if any, and check for conflicts
        final ByteBuffer readsData = msg.getReadsData();
        if (readsData != null) {

            // Decode reads
            final Reads reads;
            try {
                reads = new Reads(new ByteBufferInputStream(msg.getReadsData()));
            } catch (Exception e) {
                this.error("error decoding reads data in " + msg, e);
                this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), "error decoding reads data: " + e));
                if (newLogEntry != null)
                    newLogEntry.cancel();
                return;
            }

            // Check for conflict
            final String conflictMsg = this.checkConflicts(msg.getBaseTerm(), msg.getBaseIndex(), reads,
              this.raft.dumpConflicts ? msg.getSenderId() + " txId=" + msg.getTxId() : null);
            if (conflictMsg != null) {
                if (this.log.isDebugEnabled())
                    this.debug("commit request " + msg + " failed due to conflict: " + conflictMsg);
                this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), conflictMsg));
                if (newLogEntry != null)
                    newLogEntry.cancel();
                return;
            }
        }

        // Handle read-only vs. read-write transaction
        if (msg.isReadOnly()) {
            assert newLogEntry == null;

            // Get current time
            final Timestamp minimumLeaseTimeout = new Timestamp();

            // The follower may commit as soon as it sees the transaction's BASE log entry get committed.
            // Note, we don't need to wait for any subsequent log entries to be committed, because if they
            // are committed they are invisible to the transaction, and if they aren't ever committed then
            // whatever log entries replace them will necessarily have been created sometime after now.
            final CommitResponse response;
            if (this.leaseTimeout != null && this.leaseTimeout.compareTo(minimumLeaseTimeout) > 0) {

                // No other leader could have been elected yet as of right now, so the transaction can commit immediately
                response = new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), msg.getBaseTerm(), msg.getBaseIndex());
            } else {

                // Remember that this follower is now going to be waiting for this particular leaseTimeout
                follower.getCommitLeaseTimeouts().add(minimumLeaseTimeout);

                // Send immediate probes to all (up-to-date) followers in an attempt to increase our leaseTimeout quickly
                this.updateAllSynchronizedFollowersNow();

                // Build response
                response = new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), msg.getBaseTerm(), msg.getBaseIndex(), minimumLeaseTimeout);
            }

            // Send response
            this.raft.sendMessage(response);
        } else {
            assert newLogEntry != null;

            // If the client is requesting a config change, we could check for an outstanding config change now and if so
            // delay our response until it completes, but that's not worth the trouble. Instead, applyNewLogEntry() will
            // throw an exception and the client will just just have to retry the transaction.

            // Commit mutations as a new log entry
            final LogEntry logEntry;
            try {
                logEntry = this.applyNewLogEntry(newLogEntry);
            } catch (Exception e) {
                if (!(e instanceof IllegalStateException))
                    this.error("error appending new log entry for " + msg, e);
                else if (this.log.isDebugEnabled())
                    this.debug("error appending new log entry for " + msg + ": " + e);
                this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), e.getMessage() != null ? e.getMessage() : "" + e));
                newLogEntry.cancel();
                return;
            }
            if (this.log.isDebugEnabled())
                this.debug("added log entry " + logEntry + " for rec'd " + msg);

            // Rebase transactions
            this.rebaseTransactions();

            // Follower transaction data optimization
            follower.getSkipDataLogEntries().add(logEntry);

            // Send response
            this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
              this.raft.currentTerm, msg.getTxId(), logEntry.getTerm(), logEntry.getIndex()));
        }
    }

    @Override
    void caseCommitResponse(CommitResponse msg) {
        assert Thread.holdsLock(this.raft);
        this.failDuplicateLeader(msg);
    }

    @Override
    void caseInstallSnapshot(InstallSnapshot msg) {
        assert Thread.holdsLock(this.raft);
        this.failDuplicateLeader(msg);
    }

    @Override
    void caseRequestVote(RequestVote msg) {
        assert Thread.holdsLock(this.raft);

        // Too late dude, I already won the election
        if (this.log.isDebugEnabled())
            this.debug("ignoring " + msg + " rec'd while in " + this);
    }

    @Override
    void caseGrantVote(GrantVote msg) {
        assert Thread.holdsLock(this.raft);

        // Thanks and all, but I already won the election
        if (this.log.isDebugEnabled())
            this.debug("ignoring " + msg + " rec'd while in " + this);
    }

    private void failDuplicateLeader(Message msg) {
        assert Thread.holdsLock(this.raft);

        // This should never happen - same term but two different leaders
        final boolean defer = this.raft.identity.compareTo(msg.getSenderId()) <= 0;
        this.error("detected a duplicate leader in " + msg + " - should never happen; possible inconsistent cluster"
          + " configuration on " + msg.getSenderId() + " (mine: " + this.raft.currentConfig + "); "
          + (defer ? "reverting to follower" : "ignoring"));
        if (defer)
            this.raft.changeRole(new FollowerRole(this.raft, msg.getSenderId(), this.raft.returnAddress));
    }

// Object

    @Override
    public String toString() {
        synchronized (this.raft) {
            return this.toStringPrefix()
              + ",followerMap=" + this.followerMap
              + "]";
        }
    }

// Debug

    @Override
    boolean checkState() {
        assert Thread.holdsLock(this.raft);
        assert this.checkApplyTimer.isRunning() == !this.raft.raftLog.isEmpty();
        for (Follower follower : this.followerMap.values()) {
            assert follower.getNextIndex() <= this.raft.getLastLogIndex() + 1;
            assert follower.getMatchIndex() <= this.raft.getLastLogIndex() + 1;
            assert follower.getLeaderCommit() <= this.raft.commitIndex;
            assert follower.getUpdateTimer().isRunning() || follower.getSnapshotTransmit() != null;
        }
        assert this.timestampScrubTimer.isRunning();
        return true;
    }

// Internal methods

    /**
     * Find the index of the most recent unapplied log entry having an associated config change.
     *
     * @return most recent config change log entry, or zero if none found
     */
    private long findMostRecentConfigChange() {
        return this.findMostRecentConfigChangeMatching(Predicates.<String[]>alwaysTrue());
    }

    /**
     * Find the index of the most recent unapplied log entry having an associated config change matching the given predicate.
     *
     * @return most recent matching log entry, or zero if none found
     */
    private long findMostRecentConfigChangeMatching(Predicate<String[]> predicate) {
        assert Thread.holdsLock(this.raft);
        for (long index = this.raft.getLastLogIndex(); index > this.raft.lastAppliedIndex; index--) {
            final String[] configChange = this.raft.getLogEntryAtIndex(index).getConfigChange();
            if (configChange != null && predicate.apply(configChange))
                return index;
        }
        return 0;
    }

    /**
     * Apply a new log entry to the Raft log; if operation fails, {@link NewLogEntry#cancel cancel()} {@code newLogEntry}.
     *
     * @throws IllegalStateException if a config change would not be safe at the current time
     * @throws IllegalArgumentException if the config change attempts to remove the last node
     */
    private LogEntry applyNewLogEntry(NewLogEntry newLogEntry) throws Exception {
        assert Thread.holdsLock(this.raft);

        // Create log entry
        final LogEntry logEntry;
        final String[] configChange;
        boolean success = false;
        try {

            // Do a couple of extra checks if a config change is included
            if ((configChange = newLogEntry.getData().getConfigChange()) != null) {

                // If a config change is involved, check whether we can safely apply it
                if (!this.mayApplyNewConfigChange())
                    throw new IllegalStateException("config change cannot be safely applied at this time");

                // Disallow a configuration change that removes the last node in a cluster
                if (this.raft.currentConfig.size() == 1 && configChange[1] == null) {
                    final String lastNode = this.raft.currentConfig.keySet().iterator().next();
                    if (configChange[0].equals(lastNode))
                        throw new IllegalArgumentException("can't remove the last node in a cluster (\"" + lastNode + "\")");
                }
            }

            // Append new log entry to the Raft log
            logEntry = this.raft.appendLogEntry(this.raft.currentTerm, newLogEntry);
            success = true;
        } finally {
            if (!success)
                newLogEntry.cancel();
        }

        // Update follower list if configuration changed
        if (configChange != null)
            this.raft.requestService(this.updateKnownFollowersService);

        // Update commit index (this is only needed if config has changed, or in the single node case)
        if (configChange != null || this.followerMap.isEmpty())
            this.raft.requestService(this.updateLeaderCommitIndexService);

        // Immediately update all up-to-date followers
        this.updateAllSynchronizedFollowersNow();

        // Start check apply timer if not already running
        if (!this.checkApplyTimer.isRunning())
            this.checkApplyTimer.timeoutAfter(this.raft.heartbeatTimeout);

        // Done
        return logEntry;
    }

    /**
     * Check whether a proposed transaction can commit without any MVCC conflict.
     *
     * @param baseTerm the term of the log entry on which the transaction is based
     * @param baseIndex the index of the log entry on which the transaction is based
     * @param reads reads performed by the transaction
     * @return error message on failure, null for success
     */
    private String checkConflicts(long baseTerm, long baseIndex, Reads reads, String dumpDescription) {
        assert Thread.holdsLock(this.raft);

        // Validate the index of the log entry on which the transaction is based
        final long minIndex = this.raft.lastAppliedIndex;
        final long maxIndex = this.raft.getLastLogIndex();
        if (baseIndex < minIndex)
            return "transaction is too old: base index " + baseIndex + " < last applied log index " + minIndex;
        if (baseIndex > maxIndex)
            return "transaction is too new: base index " + baseIndex + " > most recent log index " + maxIndex;

        // Validate the term of the log entry on which the transaction is based
        final long actualBaseTerm = this.raft.getLogTermAtIndex(baseIndex);
        if (baseTerm != actualBaseTerm) {
            return "transaction is based on an overwritten log entry with index "
              + baseIndex + " and term " + baseTerm + " != " + actualBaseTerm;
        }

        // Check for conflicts from intervening commits
        for (long index = baseIndex + 1; index <= maxIndex; index++) {
            final LogEntry logEntry = this.raft.getLogEntryAtIndex(index);
            if (reads.isConflict(logEntry.getWrites())) {
                if (dumpDescription != null)
                    this.dumpConflicts(reads, logEntry, dumpDescription);
                return "writes of committed transaction at index " + index
                  + " conflict with transaction reads from transaction base index " + baseIndex;
            }
        }

        // No conflict
        return null;
    }

    private Follower findFollower(Message msg) {
        assert Thread.holdsLock(this.raft);
        final Follower follower = this.followerMap.get(msg.getSenderId());
        if (follower == null)
            this.warn("rec'd " + msg + " from unknown follower \"" + msg.getSenderId() + "\", ignoring");
        return follower;
    }
}

