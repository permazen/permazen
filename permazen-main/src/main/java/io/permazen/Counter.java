
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.JField;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;

/**
 * Represents a 64-bit counter value that can be adjusted concurrently by multiple transactions,
 * typically without locking (depending on the underlying key/value store).
 *
 * <p>
 * To define a field of type {@link Counter}, annotate the field's getter method as a normal field using
 * {@link JField &#64;JField}. No setter method should be defined.
 * Counter fields do not support indexing or change listeners.
 *
 * <p>
 * Note: during schema change notifications, counter field values appear as plain {@code Long} values.
 */
public class Counter {

    private final Transaction tx;
    private final ObjId id;
    private final String name;
    private final boolean updateVersion;

    Counter(Transaction tx, ObjId id, String name, boolean updateVersion) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        this.tx = tx;
        this.id = id;
        this.name = name;
        this.updateVersion = updateVersion;
    }

    /**
     * Read this counter's current value. Invoking this method will typically disable the lock-free
     * behavior of {@link #adjust adjust()} in the current transaction.
     *
     * @return current value of the counter
     * @throws io.permazen.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws io.permazen.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public long get() {
        return this.tx.readCounterField(this.id, this.name, this.updateVersion);
    }

    /**
     * Set this counter's value. Invoking this method will typically disable the lock-free
     * behavior of {@link #adjust adjust()} in the current transaction.
     *
     * @param value new value for the counter
     * @throws io.permazen.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws io.permazen.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void set(long value) {
        this.tx.writeCounterField(this.id, this.name, value, this.updateVersion);
    }

    /**
     * Adjust this counter's value by the specified amount.
     *
     * @param offset amount to add to counter
     * @throws io.permazen.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws io.permazen.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void adjust(long offset) {
        this.tx.adjustCounterField(this.id, this.name, offset, this.updateVersion);
    }

    /**
     * Increment this counter's value by one.
     *
     * @throws io.permazen.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws io.permazen.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void increment() {
        this.adjust(1);
    }

    /**
     * Decrement this counter's value by one.
     *
     * @throws io.permazen.kv.StaleTransactionException if the transaction from which this instance
     *  was read is no longer usable
     * @throws io.permazen.core.DeletedObjectException if the object from which this instance was read no longer exists
     */
    public void decrement() {
        this.adjust(-1);
    }
}
