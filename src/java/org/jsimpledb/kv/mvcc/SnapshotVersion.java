
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jsimpledb.kv.CloseableKVStore;

/**
 * Represents one {@link SnapshotKVDatabase} MVCC version.
 *
 * <p>
 * Information included:
 *  <ul>
 *  <li>MVCC version number</li>
 *  <li>{@link CloseableKVStore} snapshot</li>
 *  <li>Open transactions based on this version</li>
 *  <li>The {@link Writes} of the transaction that was committed on this version, if any</li>
 *  </ul>
 */
public class SnapshotVersion {

    private final long version;
    private final CloseableKVStore snapshot;
    private final HashSet<SnapshotKVTransaction> openTransactions = new HashSet<>(2);

    private Writes committedWrites;

    /**
     * Constructor.
     *
     * @param version version number
     * @param snapshot database snapshot
     */
    SnapshotVersion(long version, CloseableKVStore snapshot) {
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
     * Get the {@link CloseableKVStore} view of this version's snapshot.
     *
     * @return unmodifiable {@link CloseableKVStore}
     */
    public CloseableKVStore getSnapshot() {
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
     * Get the {@link Writes} of the transaction based on this version's snapshot that was eventually committed, if any.
     *
     * @return writes of the transaction committed on this version, or null if there is none
     */
    public Writes getCommittedWrites() {
        return this.committedWrites;
    }

// Package methods

    void addOpenTransaction(SnapshotKVTransaction tx) {
        this.openTransactions.add(tx);
    }

    void removeOpenTransaction(SnapshotKVTransaction tx) {
        this.openTransactions.remove(tx);
    }

    void setCommittedWrites(Writes committedWrites) {
        this.committedWrites = committedWrites;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[vers=" + this.version
          + ",snapshot=" + this.snapshot
          + (!this.openTransactions.isEmpty() ? ",openTx=" + this.openTransactions : "")
          + (this.committedWrites != null ? ",writes=" + this.committedWrites : "")
          + "]";
    }
}

