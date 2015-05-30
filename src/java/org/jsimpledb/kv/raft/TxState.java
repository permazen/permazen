
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

/**
 * Transaction states.
 */
enum TxState {

    /**
     * Transaction is open, executing locally, and has not been committed or rolled back yet.
     */
    EXECUTING,

    /**
     * commit() has been invoked, but the transaction has not otherwise been dealt with yet.
     * If we are a follower, it will be transmitted to the leader once the leader is known
     * and his output queue becomes empty. If we are the leader, it will be committed locally
     * as soon as we can get to it.
     */
    COMMIT_READY,

    /**
     * The transaction has been assigned a commit term and index by the leader.
     * We are waiting for the corresponding Raft log entry to be committed to the Raft log.
     */
    COMMIT_WAITING,

    /**
     * Transaction has committed successfully or failed; client thread will wakeup soon and clean it up.
     * The commit's {@link Future} has been notified.
     */
    COMPLETED,

    /**
     * Transaction is closed.
     */
    CLOSED;
}

