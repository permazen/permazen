
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.SnapshotTransaction;

/**
 * A "snapshot" {@link JTransaction} that persists indefinitely.
 *
 * <p>
 * {@link SnapshotJTransaction}s hold a "snapshot" some portion of the state of a {@link JTransaction} for later use.
 * Each {@link SnapshotJTransaction} contains its own set of "snapshot" {@link JObject}s that are (usually) copies of
 * the corresponding "real" database {@link JObject}s. Because a {@link SnapshotJTransaction}
 * lives indefinitely, these objects can be used just like normal Java objects, outside of any regular transaction.
 * In addition, {@link JSimpleDB} features such as indexing, listeners, validation, etc. also continue to work normally.
 *
 * <p>
 * For convenience, each {@link JTransaction} has a default {@link SnapshotJTransaction} instance
 * {@linkplain JTransaction#getSnapshotTransaction associated} with it; {@link JObject#copyOut JObject.copyOut()}
 * copies objects there.
 *
 * <p>
 * More general usage is possible via {@link JTransaction#createSnapshotTransaction JTransaction.createSnapshotTransaction()}.
 * For example, for {@link io.permazen.kv.KVDatabase}s that support it, using the key/value store snapshot returned by
 * {@link io.permazen.kv.KVTransaction#mutableSnapshot} allows an efficient copying of the entire database.
 *
 * @see JTransaction#createSnapshotTransaction Transaction.createSnapshotTransaction()
 * @see JSimpleDB#createSnapshotTransaction JSimpleDB.createSnapshotTransaction()
 * @see io.permazen.core.SnapshotTransaction
 */
public class SnapshotJTransaction extends JTransaction {

    SnapshotJTransaction(JSimpleDB jdb, SnapshotTransaction tx, ValidationMode validationMode) {
        super(jdb, tx, validationMode);
    }

    @Override
    public boolean isSnapshot() {
        return true;
    }

    /**
     * Get the {@link SnapshotTransaction} associated with this instance.
     *
     * @return the associated core API snapshot transaction
     */
    @Override
    public SnapshotTransaction getTransaction() {
        return (SnapshotTransaction)this.tx;
    }

    /**
     * Delete all objects contained in this snapshot transaction and reset it back to its initial state.
     *
     * <p>
     * It will contain schema meta-data but no objects.
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
     *
     * @return true always
     */
    @Override
    public boolean isValid() {
        return true;
    }
}

