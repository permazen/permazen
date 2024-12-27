
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVPairIterator;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRange;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.SortedSet;
import java.util.TreeSet;

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
     * @param timeout timeout for this transaction
     * @throws IllegalArgumentException if {@code kvdb} is null
     * @throws IllegalArgumentException if {@code waitTimeout} is negative
     */
    protected SimpleKVTransaction(SimpleKVDatabase kvdb, long timeout) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.kvdb = kvdb;
        this.waitTimeout = timeout;
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
    public ListenableFuture<Void> watchKey(ByteData key) {
        return this.kvdb.watchKey(key);
    }

    @Override
    public ByteData get(ByteData key) {
        return this.kvdb.get(this, key);
    }

    @Override
    public KVPair getAtLeast(ByteData min, ByteData max) {
        return this.kvdb.getAtLeast(this, min, max);
    }

    @Override
    public KVPair getAtMost(ByteData max, ByteData min) {
        return this.kvdb.getAtMost(this, max, min);
    }

    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        if (minKey == null)
            minKey = ByteData.empty();
        return new KVPairIterator(this, new KeyRange(minKey, maxKey), null, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        this.kvdb.put(this, key, value);
    }

    @Override
    public void remove(ByteData key) {
        this.kvdb.remove(this, key);
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
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
    public CloseableKVStore readOnlySnapshot() {

        // Get a snapshot of the underlying key/value store
        CloseableKVStore snapshot = this.kvdb.kv.readOnlySnapshot();

        // If there are any mutations in this transaction, overlay a MutableView and copy them into it
        synchronized (this.kvdb) {
            if (!this.mutations.isEmpty()) {

                // Wrap snapshot in a MutableView that closes the snapshot on close()
                snapshot = new CloseableForwardingKVStore(new MutableView(snapshot, false), snapshot::close);

                // Apply mutations
                for (Mutation mutation : this.mutations)
                    mutation.apply(snapshot);
            }
        }

        // Done
        return snapshot;
    }

    // Find the mutation that overlaps with the given key, if any.
    // This method assumes we are already synchronized on the associated database.
    Mutation findMutation(ByteData key) {

        // Sanity check during unit testing
        assert Thread.holdsLock(this.kvdb);
        assert !this.hasOverlaps() && !this.hasEmpties();

        // Get all mutations starting at or prior to "key" and look for overlap
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
    @SuppressWarnings("deprecation")
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
            final ByteData minKey = mutation.getMin();
            final ByteData maxKey = mutation.getMax();
            if (minKey != null && maxKey != null && minKey.equals(maxKey))
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
