
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.LoggerFactory;

/**
 * Holds a count of references to a {@link CloseableKVStore} and actually {@link CloseableKVStore#close close()}'s it
 * when the reference count reaches zero.
 *
 * <p>
 * Instances are thread safe.
 */
@ThreadSafe
public class SnapshotRefs {

    private final CloseableKVStore snapshot;
    private final AtomicInteger refs = new AtomicInteger(1);

    /**
     * Constructor.
     *
     * <p>
     * Initially the reference count is set to one.
     *
     * @param snapshot database snapshot
     */
    public SnapshotRefs(CloseableKVStore snapshot) {
        Preconditions.checkArgument(snapshot != null, "null snapshot");
        this.snapshot = snapshot;
    }

    /**
     * Get the underlying database snapshot as a {@link KVStore} (which should not be closed).
     *
     * @return the underlying {@link CloseableKVStore}
     * @throws IllegalStateException if reference count is already zeroed
     */
    public KVStore getKVStore() {
        Preconditions.checkState(this.refs.get() > 0, "no longer referenced");
        return this.snapshot;
    }

    /**
     * Increment reference count.
     *
     * @throws IllegalStateException if reference count is already zeroed
     */
    public void ref() {
        Preconditions.checkState(this.refs.get() > 0, "no longer referenced");
        this.refs.incrementAndGet();
    }

    /**
     * Decrement reference count.
     *
     * @throws IllegalStateException if reference count is already zeroed
     */
    public void unref() {
        Preconditions.checkState(this.refs.get() > 0, "no longer referenced");
        if (this.refs.decrementAndGet() == 0)
            this.snapshot.close();
    }

    /**
     * Get current reference count.
     *
     * @return the current number of references to this instance
     */
    public int refs() {
        return this.refs.get();
    }

    /**
     * Create an empty {@link Closeable} that, when {@link Closeable#close close()}'d, decrements the reference count.
     *
     * @return unreferencing {@link Closeable}
     */
    public Closeable getUnrefCloseable() {
        return new Closeable() {

            private boolean closed;

            @Override
            public synchronized void close() {
                if (this.closed)
                    return;
                SnapshotRefs.this.unref();
                this.closed = true;
            }
        };
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            final int remaining = this.refs.get();
            if (remaining > 0) {
                LoggerFactory.getLogger(this.getClass()).warn(this + " leaked with " + remaining + " remaining reference(s)");
                this.refs.set(0);
                this.snapshot.close();
            }
        } finally {
            super.finalize();
        }
    }
}
