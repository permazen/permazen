
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.LockOwner;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.util.CloseableForwardingKVStore;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.LoggerFactory;

/**
 * {@link KVTransaction} implementation for {@link SimpleKVDatabase}.
 *
 * <p>
 * Locking note: all fields in this class are protected by the Java monitor of the associated {@link SimpleKVDatabase},
 * not the Java monitor of this instance.
 */
public class SimpleKVTransaction extends AbstractKVStore implements KVTransaction {

    final SimpleKVDatabase kvdb;
    final TreeSet<Mutation> mutations = new TreeSet<>(KeyRange.SORT_BY_MIN);
    final LockOwner lockOwner = new LockOwner();

    boolean stale;
    long waitTimeout;

    private volatile boolean readOnly;

    /**
     * Constructor.
     *
     * @param kvdb associated database
     * @param waitTimeout wait timeout for this transaction
     * @throws IllegalArgumentException if {@code kvdb} is null
     * @throws IllegalArgumentException if {@code waitTimeout} is negative
     */
    protected SimpleKVTransaction(SimpleKVDatabase kvdb, long waitTimeout) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        this.kvdb = kvdb;
        this.setTimeout(waitTimeout);
    }

    @Override
    public SimpleKVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public void setTimeout(long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.waitTimeout = timeout;
    }

    @Override
    public ListenableFuture<Void> watchKey(byte[] key) {
        return this.kvdb.watchKey(key);
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
    public boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public void commit() {
        this.kvdb.commit(this, this.readOnly);
    }

    @Override
    public void rollback() {
        this.kvdb.rollback(this);
    }

    @Override
    public CloseableKVStore mutableSnapshot() {

        // Build copy
        CloseableKVStore kvstore;
        if (this.kvdb.kv instanceof NavigableMapKVStore) {
            final NavigableMapKVStore kv;
            synchronized (this.kvdb) {
                kv = ((NavigableMapKVStore)this.kvdb.kv).clone();
            }
            kvstore = new CloseableForwardingKVStore(kv.clone());
        } else if (this.kvdb.kv instanceof AtomicKVStore) {
            final AtomicKVStore kv = (AtomicKVStore)this.kvdb.kv;
            final CloseableKVStore snapshot = kv.snapshot();
            final MutableView view = new MutableView(snapshot);
            view.disableReadTracking();
            kvstore = new CloseableForwardingKVStore(view, snapshot);
        } else {
            throw new UnsupportedOperationException("underlying KVStore "
              + this.kvdb.kv.getClass().getSimpleName() + " is not an AtomicKVStore");
        }

        // Apply mutations
        synchronized (this.kvdb) {
            for (Mutation mutation : this.mutations)
                mutation.apply(kvstore);
        }

        // Done
        return kvstore;
    }

    // Find the mutation that overlaps with the given key, if any.
    // This method assumes we are already synchronized on the associated database.
    Mutation findMutation(byte[] key) {

        // Sanity check during unit testing
        assert Thread.holdsLock(this.kvdb);
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
    @Override
    protected void finalize() throws Throwable {
        try {
            if (!this.stale)
               LoggerFactory.getLogger(this.getClass()).warn(this + " leaked without commit() or rollback()");
            this.rollback();
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

