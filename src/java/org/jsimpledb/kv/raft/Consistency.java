
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

/**
 * {@link RaftKVTransaction} supported consistency levels.
 */
public enum Consistency {

    /**
     * Uncommitted consistency.
     *
     * <p>
     * Same as {@link #EVENTUAL}, but with the additional caveat that the view that the transaction sees may never
     * actually be committed. This can happen, e.g., if there is a Raft leadership change during the transaction.
     *
     * <p>
     * Transactions at this level may result in less network communication and/or commit faster than {@link #EVENTUAL}.
     * Read-only transactions at this level require no network communication, commit immediately, and never result in
     * {@link org.jsimpledb.kv.RetryTransactionException}s; read-only transactions are equivalent to invoking
     * {@link RaftKVTransaction#rollback rollback()} instead of {@link RaftKVTransaction#commit commit()}.
     */
    UNCOMMITTED,

    /**
     * Eventual consistency.
     *
     * <p>
     * Transactions see a consistent, committed view of the database as it existed at some point in the "recent past".
     * The view is not guaranteed to be up-to-date; it's only guaranteed to be as up-to-date as is known to this node
     * when the transaction was opened. For example, it will always include changes from the transaction most recently
     * committed on this node. However, in general (for example, if stuck in a network partition minority), the view
     * could be a view of the database from arbitrarily far in the past.
     *
     * <p>
     * Transactions at this level may result in less network communication and/or commit faster than {@link #LINEARIZABLE}.
     */
    EVENTUAL,

    /**
     * Linearizable consistency.
     *
     * <p>
     * Transactions see a database that evolves step-wise through a global, linear sequence of atomic changes, where it appears as
     * if each change occurs instantaneously at some point in time between the start and end of the corresponding transaction.
     *
     * <p>
     * This is the default consistency level.
     */
    LINEARIZABLE;
}

