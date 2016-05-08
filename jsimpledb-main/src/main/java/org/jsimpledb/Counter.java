
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

/**
 * Represents a 64-bit counter value that can be adjusted concurrently by multiple transactions,
 * typically without locking (depending on the underlying key/value store).
 *
 * <p>
 * To define a field of type {@link Counter}, annotate the field's getter method as a normal field using
 * {@link org.jsimpledb.annotation.JField &#64;JField}. No setter method should be defined.
 * Counter fields do not support indexing or change listeners.
 * </p>
 *
 * <p>
 * Note: during schema version change notification, counter field values appear as plain {@code long} values.
 * </p>
 */
public class Counter {

    private final Transaction tx;
    private final ObjId id;
    private final int storageId;
    private final boolean updateVersion;

    Counter(Transaction tx, ObjId id, int storageId, boolean updateVersion) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(id != null, "null id");
        this.tx = tx;
        this.id = id;
        this.storageId = storageId;
        this.updateVersion = updateVersion;
    }

    /**
     * Read this counter's current value. Invoking this method will typically disable the lock-free
     * behavior of {@link #adjust adjust()} in the current transaction.
     *
     * @return current value of the counter
     * @throws org.jsimpledb.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws org.jsimpledb.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public long get() {
        return this.tx.readCounterField(this.id, this.storageId, this.updateVersion);
    }

    /**
     * Set this counter's value. Invoking this method will typically disable the lock-free
     * behavior of {@link #adjust adjust()} in the current transaction.
     *
     * @param value new value for the counter
     * @throws org.jsimpledb.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws org.jsimpledb.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void set(long value) {
        this.tx.writeCounterField(this.id, this.storageId, value, this.updateVersion);
    }

    /**
     * Adjust this counter's value by the specified amount.
     *
     * @param offset amount to add to counter
     * @throws org.jsimpledb.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws org.jsimpledb.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void adjust(long offset) {
        this.tx.adjustCounterField(this.id, this.storageId, offset, this.updateVersion);
    }

    /**
     * Increment this counter's value by one.
     *
     * @throws org.jsimpledb.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws org.jsimpledb.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void increment() {
        this.adjust(1);
    }

    /**
     * Decrement this counter's value by one.
     *
     * @throws org.jsimpledb.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws org.jsimpledb.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void decrement() {
        this.adjust(-1);
    }
}

