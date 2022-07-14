
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import io.permazen.kv.raft.msg.AppendResponse;
import io.permazen.kv.raft.msg.CommitRequest;

/**
 * Support superclass for the {@linkplain FollowerRole follower} and {@linkplain CandidateRole candidate} roles,
 * both of which have an election timer.
 */
public abstract class NonLeaderRole extends Role {

    final Timer electionTimer = new Timer(this.raft, "election timer",
      new Service(this, "election timeout", this::checkElectionTimeout));
    private final boolean startElectionTimer;

// Constructors

    NonLeaderRole(final RaftKVDatabase raft, final boolean startElectionTimer) {
        super(raft);
        this.startElectionTimer = startElectionTimer;
    }

// Status & Debugging

    /**
     * Get the election timer deadline, if currently running.
     *
     * <p>
     * For a follower that is not a member of its cluster, this will return null because no election timer is running.
     * For all other cases, this will return the time at which the election timer expires.
     *
     * @return current election timer expiration deadline, or null if not running
     */
    public Timestamp getElectionTimeout() {
        synchronized (this.raft) {
            return this.electionTimer.getDeadline();
        }
    }

    /**
     * Force an immediate election timeout.
     *
     * @throws IllegalStateException if this role is no longer active or election timer is not running
     */
    public void startElection() {
        synchronized (this.raft) {
            Preconditions.checkState(this.raft.role == this, "role is no longer active");
            Preconditions.checkState(this.electionTimer.isRunning(), "election timer is not running");
            this.debug("triggering immediate election timeout due to invocation of startElection()");
            this.electionTimer.timeoutNow();
        }
    }

// Lifecycle

    @Override
    void setup() {
        assert Thread.holdsLock(this.raft);
        super.setup();
        if (this.startElectionTimer)
            this.restartElectionTimer();
    }

    @Override
    void shutdown() {
        assert Thread.holdsLock(this.raft);
        this.electionTimer.cancel();
        super.shutdown();
    }

// Service

    // Check for an election timeout
    private void checkElectionTimeout() {
        assert Thread.holdsLock(this.raft);
        if (this.electionTimer.pollForTimeout()) {
            if (this.log.isDebugEnabled())
                this.debug("election timeout while in {}", this);
            this.handleElectionTimeout();
        }
    }

    void restartElectionTimer() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Generate a randomized election timeout delay
        final int range = this.raft.maxElectionTimeout - this.raft.minElectionTimeout;
        final int randomizedPart = Math.round(this.raft.random.nextFloat() * range);

        // Restart timer
        this.electionTimer.timeoutAfter(this.raft.minElectionTimeout + randomizedPart);
    }

    abstract void handleElectionTimeout();

// MessageSwitch

    @Override
    void caseAppendResponse(AppendResponse msg) {
        assert Thread.holdsLock(this.raft);
        this.failUnexpectedMessage(msg);
    }

    @Override
    void caseCommitRequest(CommitRequest msg, NewLogEntry newLogEntry) {
        assert Thread.holdsLock(this.raft);
        this.failUnexpectedMessage(msg);
    }
}

