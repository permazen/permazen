
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVPairIterator;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRange;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.kv.util.UnmodifiableKVStore;
import io.permazen.util.ByteData;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.LoggerFactory;

/**
 * Wrapper class that presents an {@link AtomicKVStore} view of a {@link KVDatabase},
 * using individual transactions for each operation.
 *
 * <p>
 * Warning: this class is only appropriate for use with {@link KVDatabase}s that implement some form of MVCC;
 * {@link KVDatabase}s that use locking will likely generate conflicts if {@link #readOnlySnapshot} is used concurrently
 * with other methods.
 *
 * @see SnapshotKVDatabase
 */
public class AtomicKVDatabase extends AbstractKVStore implements AtomicKVStore {

    protected final KVDatabase kvdb;

    /**
     * Constructor.
     *
     * @param kvdb underlying database
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public AtomicKVDatabase(KVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        this.kvdb = kvdb;
    }

// KVStore

    @Override
    public ByteData get(final ByteData key) {
        return this.computeInTransaction(kv -> kv.get(key));
    }

    @Override
    public KVPair getAtLeast(final ByteData minKey, final ByteData maxKey) {
        return this.computeInTransaction(kv -> kv.getAtLeast(minKey, maxKey));
    }

    @Override
    public KVPair getAtMost(final ByteData maxKey, final ByteData minKey) {
        return this.computeInTransaction(kv -> kv.getAtMost(maxKey, minKey));
    }

    @Override
    public KVPairIterator getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        return new KVPairIterator(this, new KeyRange(minKey != null ? minKey : ByteData.empty(), maxKey), null, reverse);
    }

    @Override
    public void put(final ByteData key, final ByteData value) {
        this.doInTransaction(kv -> kv.put(key, value));
    }

    @Override
    public void remove(final ByteData key) {
        this.doInTransaction(kv -> kv.remove(key));
    }

    @Override
    public void removeRange(final ByteData minKey, final ByteData maxKey) {
        this.doInTransaction(kv -> kv.removeRange(minKey, maxKey));
    }

    @Override
    public void adjustCounter(final ByteData key, final long amount) {
        this.doInTransaction(kv -> kv.adjustCounter(key, amount));
    }

    @Override
    public ByteData encodeCounter(final long value) {
        return this.computeInTransaction(kv -> kv.encodeCounter(value));
    }

    @Override
    public long decodeCounter(final ByteData bytes) {
        return this.computeInTransaction(kv -> kv.decodeCounter(bytes));
    }

// AtomicKVStore

    @Override
    @PostConstruct
    public void start() {
        this.kvdb.start();
    }

    @Override
    @PreDestroy
    public void stop() {
        this.kvdb.stop();
    }

    @Override
    public CloseableKVStore readOnlySnapshot() {
        final KVTransaction kvtx = this.kvdb.createTransaction();
        boolean success = false;
        try {
            try {
                kvtx.setTimeout(Integer.MAX_VALUE);
            } catch (UnsupportedOperationException e) {
                // ignore
            }
            final SnapshotKVStore kvstore = new SnapshotKVStore(kvtx);
            success = true;
            return kvstore;
        } finally {
            if (!success)
                kvtx.rollback();
        }
    }

    @Override
    public void apply(final Mutations mutations, boolean sync) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        this.doInTransaction(kv -> kv.apply(mutations));
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[kvdb=" + this.kvdb + "]";
    }

// Internal methods

    private <R> R computeInTransaction(Function<? super KVStore, R> action) {
        int count = 0;
        while (true) {
            try {
                final KVTransaction kvtx = this.kvdb.createTransaction();
                boolean success = false;
                try {
                    final R result = action.apply(kvtx);
                    success = true;
                    return result;
                } finally {
                    if (success)
                        kvtx.commit();
                    else
                        kvtx.rollback();
                }
            } catch (RetryKVTransactionException e) {
                if (count++ < 3) {
                    Thread.yield();
                    continue;
                }
                throw e;
            }
        }
    }

    private void doInTransaction(Consumer<? super KVStore> action) {
        this.computeInTransaction(kv -> {
            action.accept(kv);
            return null;
        });
    }

// Snapshot

    private static class SnapshotKVStore extends ForwardingKVStore implements CloseableKVStore {

        private final KVTransaction kvtx;
        private final UnmodifiableKVStore delegate;

        private volatile boolean closed;

        SnapshotKVStore(KVTransaction kvtx) {
            this.kvtx = kvtx;
            this.delegate = new UnmodifiableKVStore(this.kvtx);
        }

        @Override
        protected UnmodifiableKVStore delegate() {
            return this.delegate;
        }

    // Object

        @Override
        @SuppressWarnings("deprecation")
        protected void finalize() throws Throwable {
            try {
                if (!this.closed)
                    LoggerFactory.getLogger(this.getClass()).warn(this + " leaked without invoking close()");
                this.close();
            } finally {
                super.finalize();
            }
        }

    // Closeable

        @Override
        public void close() {
            this.closed = true;
            this.kvtx.rollback();
        }
    }
}
