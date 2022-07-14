
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import io.permazen.kv.raft.msg.AppendRequest;
import io.permazen.kv.raft.msg.CommitResponse;
import io.permazen.kv.raft.msg.GrantVote;
import io.permazen.kv.raft.msg.InstallSnapshot;
import io.permazen.kv.raft.msg.RequestVote;

import java.util.HashSet;

import javax.annotation.concurrent.GuardedBy;

/**
 * Raft candidate role.
 */
public class CandidateRole extends NonLeaderRole {

    @GuardedBy("raft")
    private final HashSet<String> votes = new HashSet<>();
    private final Service checkElectionResultService = new Service(this, "check election result", this::checkElectionResult);

// Constructors

    CandidateRole(RaftKVDatabase raft) {
        super(raft, true);
    }

// Status

    /**
     * Get the number of votes required to win the election.
     *
     * @return required votes
     */
    public int getVotesRequired() {
        synchronized (this.raft) {
            return this.raft.currentConfig.size() / 2 + 1;
        }
    }

    /**
     * Get the number of votes received so far. Includes this node's vote.
     *
     * @return received votes
     */
    public int getVotesReceived() {
        synchronized (this.raft) {
            return this.votes.size() + (this.raft.isClusterMember() ? 1 : 0);
        }
    }

// Lifecycle

    @Override
    void setup() {
        assert Thread.holdsLock(this.raft);
        super.setup();

        // Increment term
        if (!this.raft.advanceTerm(this.raft.currentTerm + 1))
            return;

        // Request votes from other peers
        final HashSet<String> voters = new HashSet<>(this.raft.currentConfig.keySet());
        voters.remove(this.raft.identity);
        if (this.log.isDebugEnabled())
            this.debug("entering candidate role in term {}; requesting votes from {}", this.raft.currentTerm, voters);
        for (String voter : voters) {
            this.raft.sendMessage(new RequestVote(this.raft.clusterId, this.raft.identity, voter,
              this.raft.currentTerm, this.raft.log.getLastTerm(), this.raft.log.getLastIndex()));
        }

        // Check election result - needed in case we are the only node in the cluster
        this.raft.requestService(this.checkElectionResultService);
    }

// Service

    @Override
    void outputQueueEmpty(String address) {
        assert Thread.holdsLock(this.raft);
        // nothing to do
    }

    @Override
    void handleElectionTimeout() {
        assert Thread.holdsLock(this.raft);
        this.raft.changeRole(new CandidateRole(this.raft));
    }

    private void checkElectionResult() {
        assert Thread.holdsLock(this.raft);

        // Tally votes
        final int allVotes = this.raft.currentConfig.size();
        final int numVotes = this.getVotesReceived();
        final int votesRequired = this.getVotesRequired();
        if (this.log.isDebugEnabled())
            this.debug("current election tally: {}/{} with {} required to win", numVotes, allVotes, votesRequired);

        // Did we win?
        if (numVotes >= votesRequired) {
            if (this.log.isDebugEnabled())
                this.debug("won the election for term {}; becoming leader", this.raft.currentTerm);
            this.raft.changeRole(new LeaderRole(this.raft));
        }
    }

// Transactions

    @Override
    void checkReadyTransactionNeedingCommitInfo(RaftKVTransaction tx) {

        // Sanity check
        super.checkReadyTransactionNeedingCommitInfo(tx);

        // We can't do anything because we don't have a leader yet
    }

// MessageSwitch

    @Override
    void caseAppendRequest(AppendRequest msg, NewLogEntry newLogEntry) {
        assert Thread.holdsLock(this.raft);
        if (this.log.isDebugEnabled())
            this.debug("rec'd {} in {}; reverting to follower", msg, this);
        this.raft.changeRole(new FollowerRole(this.raft, msg.getSenderId(), this.raft.returnAddress));
        this.raft.receiveMessage(this.raft.returnAddress, msg, -1, newLogEntry);
    }

// MessageSwitch

    @Override
    void caseCommitResponse(CommitResponse msg) {
        assert Thread.holdsLock(this.raft);
        this.failUnexpectedMessage(msg);                    // we could not have ever sent a CommitRequest in this term
    }

    @Override
    void caseInstallSnapshot(InstallSnapshot msg) {
        assert Thread.holdsLock(this.raft);
        this.failUnexpectedMessage(msg);                    // we could not have ever sent an AppendResponse in this term
    }

    @Override
    void caseRequestVote(RequestVote msg) {
        assert Thread.holdsLock(this.raft);

        // Ignore - we are also a candidate and have already voted for ourself
        if (this.log.isDebugEnabled())
            this.debug("ignoring {} rec'd while in {}", msg, this);
    }

    @Override
    void caseGrantVote(GrantVote msg) {
        assert Thread.holdsLock(this.raft);

        // Record vote
        this.votes.add(msg.getSenderId());
        if (this.log.isDebugEnabled())
            this.debug("rec'd election vote from \"{}\" in term {}", msg.getSenderId(), this.raft.currentTerm);

        // Check election result
        this.raft.requestService(this.checkElectionResultService);
    }

// Object

    @Override
    public String toString() {
        synchronized (this.raft) {
            return this.toStringPrefix()
              + ",votes=" + this.votes
              + "]";
        }
    }

// Debug

    @Override
    boolean checkState() {
        assert Thread.holdsLock(this.raft);
        assert this.electionTimer.isRunning();
        assert this.raft.isClusterMember();
        return true;
    }
}

