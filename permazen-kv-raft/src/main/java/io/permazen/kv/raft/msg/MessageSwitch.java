
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

/**
 * Visitor pattern interface for {@link Message}s.
 */
public interface MessageSwitch {

    /**
     * Handle an {@link AppendRequest}.
     *
     * @param msg message received
     */
    void caseAppendRequest(AppendRequest msg);

    /**
     * Handle an {@link AppendResponse}.
     *
     * @param msg message received
     */
    void caseAppendResponse(AppendResponse msg);

    /**
     * Handle a {@link CommitRequest}.
     *
     * @param msg message received
     */
    void caseCommitRequest(CommitRequest msg);

    /**
     * Handle a {@link CommitResponse}.
     *
     * @param msg message received
     */
    void caseCommitResponse(CommitResponse msg);

    /**
     * Handle an {@link GrantVote}.
     *
     * @param msg message received
     */
    void caseGrantVote(GrantVote msg);

    /**
     * Handle a {@link InstallSnapshot}.
     *
     * @param msg message received
     */
    void caseInstallSnapshot(InstallSnapshot msg);

    /**
     * Handle a {@link PingRequest}.
     *
     * @param msg message received
     */
    void casePingRequest(PingRequest msg);

    /**
     * Handle a {@link PingResponse}.
     *
     * @param msg message received
     */
    void casePingResponse(PingResponse msg);

    /**
     * Handle an {@link RequestVote}.
     *
     * @param msg message received
     */
    void caseRequestVote(RequestVote msg);
}
