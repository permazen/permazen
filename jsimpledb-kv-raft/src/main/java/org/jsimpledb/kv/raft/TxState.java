
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

/**
 * {@link RaftKVTransaction} states.
 *
 * <p>
 * Transactions always progress through these states in the forward direction (i.e., in their natural order).
 */
public enum TxState {

    /**
     * The transaction is open, executing locally, and neither {@link RaftKVTransaction#commit}
     * nor {@link RaftKVTransaction#rollback} has been invoked yet.
     *
     * <p>
     * No network communication with any other node has occurred yet on behalf of the transaction.
     */
    EXECUTING,

    /**
     * The transaction is ready for commit.
     *
     * <p>
     * {@link RaftKVTransaction#commit} has been invoked, but the transaction has not otherwise been dealt with yet.
     * If we are a follower, it will be transmitted to the leader once the leader is known and his output queue becomes empty;
     * while we are waiting for the response from the leader, the transaction remains in this state.
     * If we are the leader, it will be dealt with locally as soon as we can get to it.
     */
    COMMIT_READY,

    /**
     * The transaction is waiting for the corresponding Raft log entry to be committed.
     *
     * <p>
     * The transaction has been assigned a {@linkplain RaftKVTransaction#getCommitTerm commit term} and
     * {@linkplain RaftKVTransaction#getCommitIndex index} by the leader.
     * We are waiting for the corresponding Raft log entry to be committed to the Raft log.
     */
    COMMIT_WAITING,

    /**
     * The transaction has committed successfully or failed, and is waiting to be cleaned up.
     *
     * <p>
     * The thread that invoked {@link RaftKVTransaction#commit} or {@link RaftKVTransaction#rollback} has not yet
     * woken up and returned from the invocation.
     */
    COMPLETED,

    /**
     * The transaction is closed.
     */
    CLOSED;
}

