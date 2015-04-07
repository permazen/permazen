
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.simple;

import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.util.AbstractCountingKVStore;
import org.jsimpledb.kv.util.LockOwner;
import org.jsimpledb.util.ByteUtil;

/**
 * {@link KVTransaction} implementation for {@link SimpleKVDatabase}.
 *
 * <p>
 * Locking note: all fields in this class are protected by the Java monitor of the associated {@link SimpleKVDatabase},
 * not the Java monitor of this instance.
 * </p>
 */
public class SimpleKVTransaction extends AbstractCountingKVStore implements KVTransaction {

    final SimpleKVDatabase kvdb;
    final TreeSet<Mutation> mutations = new TreeSet<>(KeyRange.SORT_BY_MIN);
    final LockOwner lockOwner = new LockOwner();

    boolean stale;
    long waitTimeout;

    /**
     * Constructor.
     *
     * @param kvdb associated database
     * @param waitTimeout wait timeout for this transaction
     * @throws IllegalArgumentException if {@code kvdb} is null
     * @throws IllegalArgumentException if {@code waitTimeout} is negative
     */
    protected SimpleKVTransaction(SimpleKVDatabase kvdb, long waitTimeout) {
        if (kvdb == null)
            throw new IllegalArgumentException("null kvdb");
        this.kvdb = kvdb;
        this.setTimeout(waitTimeout);
    }

    @Override
    public SimpleKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public void setTimeout(long timeout) {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout < 0");
        this.waitTimeout = timeout;
    }

    @Override
    public byte[] get(byte[] key) {
        return this.kvdb.get(this, key);
    }

    @Override
    public KVPair getAtLeast(byte[] key) {
        return this.kvdb.getAtLeast(this, key);
    }

    @Override
    public KVPair getAtMost(byte[] key) {
        return this.kvdb.getAtMost(this, key);
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (minKey == null)
            minKey = ByteUtil.EMPTY;
        return new KVPairIterator(this, new KeyRange(minKey, maxKey), null, reverse);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.kvdb.put(this, key, value);
    }

    @Override
    public void remove(byte[] key) {
        this.kvdb.remove(this, key);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.kvdb.removeRange(this, minKey, maxKey);
    }

    @Override
    public void commit() {
        this.kvdb.commit(this);
    }

    @Override
    public void rollback() {
        this.kvdb.rollback(this);
    }

    // Find the mutation that overlaps with the given key, if any.
    // This method assumes we are already synchronized on the associated database.
    Mutation findMutation(byte[] key) {

        // Sanity check during unit testing
        assert !this.hasOverlaps() && !this.hasEmpties();

        // Get all mutations starting at or prior to `key' and look for overlap
        final SortedSet<Mutation> left = this.mutations.headSet(Mutation.key(ByteUtil.getNextKey(key)));
        if (!left.isEmpty()) {
            final Mutation last = left.last();
            if (last.contains(key))
                return last;
        }
        return null;
    }

    /**
     * Ensure transaction is eventually rolled back if leaked due to an application bug.
     */
    protected void finalize() throws Throwable {
        try {
            try {
                this.rollback();
            } catch (StaleTransactionException e) {
                // ignore
            }
        } finally {
            super.finalize();
        }
    }

    private boolean hasEmpties() {
        for (Mutation mutation : this.mutations) {
            final byte[] minKey = mutation.getMin();
            final byte[] maxKey = mutation.getMax();
            if (minKey != null && maxKey != null && Arrays.equals(minKey, maxKey))
                return true;
        }
        return false;
    }

    private boolean hasOverlaps() {
        Mutation previous = null;
        for (Mutation mutation : this.mutations) {
            if (previous != null && mutation.overlaps(previous))
                return true;
            previous = mutation;
        }
        return false;
    }
}

