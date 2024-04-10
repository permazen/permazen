
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.core.DetachedTransaction;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;

import java.io.Closeable;

/**
 * A {@link PermazenTransaction} that is not actually connected to a {@link Permazen} database instance, but instead exists
 * just to hold objects in memory.
 *
 * <p>
 * {@link DetachedPermazenTransaction}s are typically used to "snapshot" some portion of a normal {@link PermazenTransaction}
 * for later use. As with all transactions, each {@link DetachedPermazenTransaction} contains its own object data.
 *
 * <p>
 * For convenience, each {@link PermazenTransaction} has a default, initially empty {@link DetachedPermazenTransaction} instance
 * {@linkplain PermazenTransaction#getDetachedTransaction associated} with it;
 * {@link PermazenObject#copyOut PermazenObject.copyOut()} copies objects there.
 *
 * <p>
 * Because {@link DetachedPermazenTransaction}s live indefinitely, their objects can be used just like normal Java objects,
 * outside of any regular transaction, yet all of the usual {@link Permazen} features such as indexing, listeners,
 * validation, etc., continue to function normally.
 *
 * <p>
 * Because they typically contain only a portion of the database's objects and therefore may have dangling references,
 * the referential integrity function of {@link PermazenField#allowDeleted &#64;PermazenField.allowDeleted()} is disabled in
 * {@link DetachedPermazenTransaction}s.
 *
 * <p><b>Other Uses</b>
 *
 * <p>
 * More general usage beyond copies of regular transactions is possible: an empty {@link DetachedPermazenTransaction} can be created
 * on the fly via {@link PermazenTransaction#createDetachedTransaction PermazenTransaction.createDetachedTransaction()} and then
 * used as simple in-memory transaction workspace. The resulting key/value pairs could then be (de)serialized and sent over the
 * network; see for example {@link io.permazen.spring.PermazenObjectHttpMessageConverter}.
 *
 * <p>
 * For {@link KVDatabase}'s that support it, using the key/value store snapshot returned by {@link KVTransaction#readOnlySnapshot}
 * allows an efficient "copy" of the entire database into a {@link DetachedPermazenTransaction}; see
 * {@link PermazenTransaction#createSnapshotTransaction Permazen.createSnapshotTransaction()}.
 *
 * <p><b>Lifecycle Management</b>
 *
 * <p>
 * Instances of this class should be {@link #close}'d when no longer needed to release any associated resources.
 *
 * @see PermazenTransaction#createDetachedTransaction PermazenTransaction.createDetachedTransaction()
 * @see PermazenTransaction#createSnapshotTransaction PermazenTransaction.createSnapshotTransaction()
 * @see Permazen#createDetachedTransaction Permazen.createDetachedTransaction()
 * @see DetachedTransaction
 */
public class DetachedPermazenTransaction extends PermazenTransaction implements Closeable {

    DetachedPermazenTransaction(Permazen pdb, DetachedTransaction tx, ValidationMode validationMode) {
        super(pdb, tx, validationMode);
    }

    @Override
    public final boolean isDetached() {
        return true;
    }

    /**
     * Get the {@link DetachedTransaction} associated with this instance.
     *
     * @return the associated core API detached transaction
     */
    @Override
    public DetachedTransaction getTransaction() {
        return (DetachedTransaction)this.tx;
    }

    /**
     * Delete all objects contained in this detached transaction and reset it back to its initial state.
     *
     * <p>
     * It will contain schema meta-data but no objects.
     */
    public void reset() {
        this.resetValidationQueue();
        ((DetachedTransaction)this.tx).reset();
    }

    /**
     * Commit this transaction.
     *
     * <p>
     * {@link DetachedPermazenTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Roll back this transaction.
     *
     * <p>
     * {@link DetachedPermazenTransaction}s do not support this method and will always throw {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("detached transaction");
    }

    /**
     * Determine whether this transaction is still valid.
     *
     * <p>
     * {@link DetachedPermazenTransaction}s are always valid.
     *
     * @return true always
     */
    @Override
    public boolean isOpen() {
        return true;
    }

// Closeable

    /**
     * Close this instance and release any resources associated with it.
     *
     * <p>
     * The implementation in {@link DetachedPermazenTransaction}s closes the underlying {@link DetachedTransaction}.
     *
     * @see DetachedTransaction#close
     */
    @Override
    public void close() {
        ((DetachedTransaction)this.tx).close();
    }
}
