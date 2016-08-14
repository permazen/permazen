
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.ThreadSafe;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVStore;
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
    private final AtomicInteger refs;

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
        this.refs = new AtomicInteger(1);
    }

    public KVStore getKVStore() {
        Preconditions.checkState(this.refs.get() > 0, "no longer referenced");
        return this.snapshot;
    }

    public void ref() {
        Preconditions.checkState(this.refs.get() > 0, "no longer referenced");
        this.refs.incrementAndGet();
    }

    public void unref() {
        if (this.refs.getAndDecrement() <= 0)
            throw new IllegalStateException("no longer referenced");
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

