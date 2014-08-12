
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SnapshotTransaction;

/**
 * An initially empty, in-memory {@link JTransaction} that persists indefinitely.
 *
 * <p>
 * {@link SnapshotJTransaction}s hold a "snapshot" some portion of the state of a {@link JTransaction} for later use.
 * Each {@link SnapshotJTransaction}s contains its own set of "snapshot" {@link JObject}s that are copies of
 * corresponding "real" database {@link JObject}s (as of the time they were copied). Because a {@link SnapshotJTransaction}
 * lives in memory indefinitely, these objects can be used just like normal Java objects, outside of any regular transaction.
 * However, in addition {@link JSimpleDB} features such as indexing, listeners, validation, etc. will continue to work normally.
 * </p>
 *
 * <p>
 * Typically, {@link SnapshotJTransaction}s are not utilized directly; instead each {@link JTransaction} has an
 * {@linkplain JTransaction#getSnapshotTransaction associated} default {@link SnapshotJTransaction} instance,
 * and {@link JObject#copyOut JObject.copyOut()} is invoked to copy an object there.
 * However, more general usage is possible by creating instances of this class directly.
 * </p>
 *
 * <p>
 * This class guarantees that for each {@link ObjId}, there will only exist a single {@link JObject} instance associated with it.
 * While there is a single pool of globally unique "database" {@link JObject}s per {@link JSimpleDB},
 * each {@link SnapshotJTransaction} maintains its own pool of "snapshot" {@link JObject} instances.
 * In turn, each snapshot {@link JObject} instance is {@linkplain JObject#getTransaction associated}
 * with a specific {@link SnapshotJTransaction}.
 * </p>
 */
public class SnapshotJTransaction extends JTransaction {

    final JObjectCache jobjectCache;

    /**
     * Create a new instance based on the specified transaction. This new instance will be initially empty
     * but will have the same recorded schema history as {@code jtx}.
     *
     * <p>
     * It is not normally necessary to create {@link SnapshotJTransaction}s, as every {@link JTransaction}
     * automatically has an associated default {@link SnapshotJTransaction} created for it and made available
     * via {@link JTransaction#getSnapshotTransaction JTransaction.getSnapshotTransaction()}.
     * </p>
     *
     * @param jtx open transaction from which to snapshot objects
     * @param validationMode the {@link ValidationMode} to use for this transaction
     * @throws IllegalArgumentException if either parameter is null
     * @throws org.jsimpledb.core.StaleTransactionException if {@code jtx} is no longer usable
     */
    public SnapshotJTransaction(JTransaction jtx, ValidationMode validationMode) {
        super(jtx.jdb, jtx.tx.createSnapshotTransaction(), validationMode);
        this.jobjectCache = new JObjectCache(jtx.jdb) {
            @Override
            protected JObject instantiate(JClass<?> jclass, ObjId id) throws Exception {
                return (JObject)jclass.getSnapshotConstructor().newInstance(id, SnapshotJTransaction.this);
            }
        };
    }

    /**
     * Delete all objects contained in this snapshot transaction and reset it back to its initial state.
     *
     * <p>
     * It will contain schema meta-data but no objects.
     * </p>
     */
    public void reset() {
        this.resetValidationQueue();
        ((SnapshotTransaction)this.tx).reset();
    }

    /**
     * Commit this transaction.
     *
     * <p>
     * {@link SnapshotJTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     * </p>
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("snapshot transactions");
    }

    /**
     * Roll back this transaction.
     *
     * <p>
     * {@link SnapshotJTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     * </p>
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("snapshot transactions");
    }

    /**
     * Determine whether this transaction is still valid.
     *
     * <p>
     * {@link SnapshotJTransaction}s are always valid.
     * </p>
     *
     * @return true always
     */
    @Override
    public boolean isValid() {
        return true;
    }

// Object Cache

    JObjectCache getJObjectCache() {
        return this.jobjectCache;
    }
}

