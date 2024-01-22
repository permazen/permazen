
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.Permazen;
import io.permazen.SnapshotJTransaction;
import io.permazen.ValidationMode;
import io.permazen.core.Transaction;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.CloseableForwardingKVStore;

import java.util.Iterator;

/**
 * Specialized {@link JObjectContainer} for use with key/value stores that implement
 * {@link KVTransaction#readOnlySnapshot KVTransaction.readOnlySnapshot()}.
 *
 * <p>
 * This container is loaded by taking a key/value store snapshot and then iterating the desired backing objects
 * directly via {@link #iterateObjects}; no copying of objects is required.
 *
 * <p>
 * Instances are (re)loaded at any time by invoking {@link #reload}. During reload, the container opens a {@link PermazenTransaction}
 * and then creates a {@link SnapshotJTransaction} using the {@link CloseableKVStore} returned by
 * {@link KVTransaction#readOnlySnapshot KVTransaction.readOnlySnapshot()}.
 * This {@link SnapshotJTransaction} is set as the current transaction while {@link #iterateObjects}
 * returns the {@link PermazenObject}s to be actually included in the container. The {@link CloseableKVStore} will
 * remain open until the container is {@link #reload}'ed or {@link #disconnect}'ed.
 */
@SuppressWarnings("serial")
public abstract class SnapshotJObjectContainer extends ReloadableJObjectContainer {

    private CloseableKVStore kvstore;

    /**
     * Constructor.
     *
     * @param pdb {@link Permazen} database
     * @param type type restriction, or null for no restriction
     * @throws IllegalArgumentException if {@code pdb} is null
     */
    protected SnapshotJObjectContainer(Permazen pdb, Class<?> type) {
        super(pdb, type);
    }

// Connectable

    @Override
    public void disconnect() {
        super.disconnect();
        if (this.kvstore != null) {
            this.kvstore.close();
            this.kvstore = null;
        }
    }

    /**
     * (Re)load this container.
     *
     * <p>
     * This method opens a {@link PermazenTransaction}, creates a {@link SnapshotJTransaction} using the {@link CloseableKVStore}
     * returned by {@link KVTransaction#readOnlySnapshot KVTransaction.readOnlySnapshot()}, and loads
     * the container using the objects returned by {@link #iterateObjects}.
     */
    @Override
    public void reload() {

        // Grab KVStore snapshot
        final CloseableKVStore[] snapshotHolder = new CloseableKVStore[1];
        this.doInTransaction(() -> {
            final PermazenTransaction jtx = PermazenTransaction.getCurrent();
            final CloseableKVStore snapshot = jtx.getTransaction().getKVTransaction().readOnlySnapshot();
            jtx.getTransaction().addCallback(new Transaction.CallbackAdapter() {
                @Override
                public void afterCompletion(boolean committed) {
                    if (!committed)
                        snapshot.close();
                }
            });
            snapshotHolder[0] = snapshot;
        });
        final CloseableKVStore snapshot = snapshotHolder[0];

        // Replace previous KVStore snapshot (if any)
        if (this.kvstore != null) {
            this.kvstore.close();
            this.kvstore = null;
        }
        this.kvstore = new CloseableForwardingKVStore(new MutableView(snapshot), snapshot::close);

        // Create snapshot transaction based on the key/value store snapshot and load container
        final SnapshotJTransaction snapshotTx = this.pdb.createSnapshotTransaction(this.kvstore, false, ValidationMode.MANUAL);
        snapshotTx.performAction(() -> this.load(this.iterateObjects()));
    }

    /**
     * Iterate the database objects that will be used to fill this container.
     * Objects should be returned in the desired order; duplicates and null values will be ignored.
     *
     * <p>
     * A {@link PermazenTransaction} will be open in the current thread when this method is invoked.
     *
     * @return database objects
     */
    protected abstract Iterator<? extends PermazenObject> iterateObjects();
}
