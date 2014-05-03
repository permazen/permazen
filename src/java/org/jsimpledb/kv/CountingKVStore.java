
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

/**
 * Implemented by {@link KVStore}s that support encoding a 64 bit counter in a key/value pair such that the counter
 * can be efficiently {@linkplain #adjustCounter adjusted} by concurrent transactions without conflict.
 * In practice this means no locking is required to increment or decrement the counter by some amount, as long as
 * it's not necessary to actually directly read or write the counter value in the same transaction.
 *
 * <p>
 * How counters are encoded is specific to the implementation. Clients needing to read or write counter values directly
 * should use {@link #decodeCounter decodeCounter()} and {@link #encodeCounter encodeCounter()}, respectively.
 * Counters are removed using the normal methods (i.e., {@link #remove remove()} and {@link #removeRange removeRange()}).
 * </p>
 */
public interface CountingKVStore extends KVStore {

    /**
     * Encode a counter value into a {@code byte[]} value suitable for use with {@link #decodeCounter decodeCounter()}
     * and/or {@link #adjustCounter adjustCounter()}.
     *
     * @param value desired counter value
     * @return encoded counter value
     */
    byte[] encodeCounter(long value);

    /**
     * Decode a counter value.
     *
     * @param value encoded counter value
     * @return decoded counter value
     * @throws IllegalArgumentException if {@code value} is not a valid counter value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code value} is null
     */
    long decodeCounter(byte[] value);

    /**
     * Adjust the counter at the given key by the given amount.
     *
     * <p>
     * This operation should behave in a lock-free manner, so that concurrent transactions can invoke it without
     * conflict. However, if {@code key} is read or written to in this transaction, then this behavior may no
     * longer be guaranteed.
     * </p>
     *
     * <p>
     * If there is no value associated with {@code key}, or {@code key}'s value is not a valid counter encoding as
     * would be acceptable to {@link #decodeCounter decodeCounter()}, then how this operation affects {@code key}'s
     * value, if at all, is undefined.
     * </p>
     *
     * @param key key
     * @param amount amount to adjust counter value by
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code key} is null
     */
    void adjustCounter(byte[] key, long amount);
}

