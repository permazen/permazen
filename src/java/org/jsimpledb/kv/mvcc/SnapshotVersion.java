
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jsimpledb.kv.KVStore;

/**
 * Represents one {@link SnapshotKVDatabase} MVCC version.
 *
 * <p>
 * Information included:
 *  <ul>
 *  <li>MVCC version number</li>
 *  <li>{@link KVStore} snapshot</li>
 *  <li>Open transactions based on this version</li>
 *  <li>The transaction that was committed on this version, if any</li>
 *  </ul>
 */
public class SnapshotVersion {

    private final long version;
    private final KVStore snapshot;
    private final HashSet<SnapshotKVTransaction> openTransactions = new HashSet<>(2);

    private SnapshotKVTransaction committedTransaction;

    /**
     * Constructor.
     *
     * @param version version number
     * @param snapshot database snapshot
     */
    SnapshotVersion(long version, KVStore snapshot) {
        this.version = version;
        this.snapshot = snapshot;
    }

    /**
     * Get this instance's unique version number.
     *
     * @return MVCC version number
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Get the {@link KVStore} view of this version's snapshot.
     *
     * @return unmodifiable {@link KVStore}
     */
    public KVStore getSnapshot() {
        return this.snapshot;
    }

    /**
     * Get transactions based on this version's snapshot that are still open.
     *
     * @return immutable view of open transactions associated with this version
     */
    public Set<SnapshotKVTransaction> getOpenTransactions() {
        return Collections.unmodifiableSet(this.openTransactions);
    }

    /**
     * Get the transaction based on this version's snapshot that was eventually committed, if any.
     *
     * @return the transaction committed on this version, or null if there is none
     */
    public SnapshotKVTransaction getCommittedTransaction() {
        return this.committedTransaction;
    }

// Package methods

    void addOpenTransaction(SnapshotKVTransaction tx) {
        this.openTransactions.add(tx);
    }

    void removeOpenTransaction(SnapshotKVTransaction tx) {
        this.openTransactions.remove(tx);
    }

    void setCommittedTransaction(SnapshotKVTransaction committedTransaction) {
        this.committedTransaction = committedTransaction;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[vers=" + this.version
          + ",snapshot=" + this.snapshot
          + (!this.openTransactions.isEmpty() ? ",openTx=" + this.openTransactions : "")
          + (this.committedTransaction != null ? ",commitTx=" + this.committedTransaction : "")
          + "]";
    }
}

