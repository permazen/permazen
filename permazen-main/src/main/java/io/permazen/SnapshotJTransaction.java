
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.SnapshotTransaction;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;

import java.io.Closeable;

/**
 * An in-memory {@link JTransaction} that persists indefinitely.
 *
 * <p>
 * The purpose of {@link SnapshotJTransaction}s is to hold a "snapshot" some portion of the state of a parent {@link JTransaction}
 * for later use after the parent transaction closes. Each {@link SnapshotJTransaction} contains its own set of "snapshot"
 * {@link JObject}s that are (usually) copies of the corresponding "real" database {@link JObject}s. A {@link SnapshotJTransaction}
 * has the same {@linkplain Permazen#getSchemaModel schema} as its parent {@link JTransaction}.
 *
 * <p>
 * For convenience, each {@link JTransaction} has a default {@link SnapshotJTransaction} instance
 * {@linkplain JTransaction#getSnapshotTransaction associated} with it; {@link JObject#copyOut JObject.copyOut()}
 * copies objects there.
 *
 * <p>
 * Because {@link SnapshotJTransaction}s live indefinitely, their objects can be used just like normal Java objects,
 * outside of any regular transaction, yet all of the usual {@link Permazen} features such as indexing, listeners,
 * validation, etc., continue to function normally.
 *
 * <p><b>Other Uses</b>
 *
 * <p>
 * More general usage beyond snapshots of regular transactions is possible: an empty {@link SnapshotJTransaction} can be created
 * on the fly via {@link JTransaction#createSnapshotTransaction JTransaction.createSnapshotTransaction()} and then used as simple
 * in-memory transaction "workspace". The resulting key/value pairs could then be (de)serialized and sent over the network;
 * see for example {@link io.permazen.spring.JObjectHttpMessageConverter}.
 *
 * <p>
 * For {@link KVDatabase}'s that support it, using the key/value store snapshot returned by
 * {@link KVTransaction#mutableSnapshot} allows an efficient "copy" of the entire database into a {@link SnapshotJTransaction}
 * using {@link Permazen#createSnapshotTransaction(KVStore, boolean, ValidationMode) Permazen.createSnapshotTransaction()}.
 *
 * @see JTransaction#createSnapshotTransaction Transaction.createSnapshotTransaction()
 * @see Permazen#createSnapshotTransaction Permazen.createSnapshotTransaction()
 * @see SnapshotTransaction
 */
public class SnapshotJTransaction extends JTransaction implements Closeable {

    SnapshotJTransaction(Permazen jdb, SnapshotTransaction tx, ValidationMode validationMode) {
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

// Closeable

    /**
     * Close this instance and release any resources associated with it.
     *
     * <p>
     * The implementation in {@link SnapshotJTransaction}s closes the underlying {@link SnapshotTransaction}.
     *
     * @see SnapshotTransaction#close
     */
    @Override
    public void close() {
        ((SnapshotTransaction)this.tx).close();
    }
}
