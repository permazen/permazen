
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ForwardingKVStore} also implementing {@link CloseableKVStore}, with some associated
 * underlying {@link Closeable} resource to close when {@link #close}'d.
 */
public class CloseableForwardingKVStore extends ForwardingKVStore implements CloseableKVStore {

    private static final boolean TRACK_ALLOCATIONS = Boolean.parseBoolean(
      System.getProperty(CloseableForwardingKVStore.class.getName() + ".TRACK_ALLOCATIONS", "false"));

    private final KVStore kvstore;
    private final Closeable closeable;
    private final Throwable allocation;

    private boolean closed;

    /**
     * Constructor for a do-nothing {@link #close}.
     *
     * <p>
     * With this construtor, nothing will actually be closed when {@link #close} is invoked.
     *
     * @param kvstore key/value store for forwarding
     */
    public CloseableForwardingKVStore(KVStore kvstore) {
        this(kvstore, null);
    }

    /**
     * Convenience constructor.
     *
     * @param kvstore key/value store for forwarding; will be closed on {@link #close}
     */
    public CloseableForwardingKVStore(CloseableKVStore kvstore) {
        this(kvstore, kvstore);
    }

    /**
     * Primary constructor.
     *
     * @param kvstore key/value store for forwarding
     * @param resource {@link Closeable} resource, or null for none
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public CloseableForwardingKVStore(KVStore kvstore, Closeable resource) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        this.kvstore = kvstore;
        this.closeable = resource;
        this.allocation = CloseableForwardingKVStore.TRACK_ALLOCATIONS ? new Throwable("allocated was here") : null;
    }

    @Override
    protected KVStore delegate() {
        return this.kvstore;
    }

    @Override
    public synchronized void close() {
        if (this.closed)
            return;
        try {
            if (this.closeable != null)
                this.closeable.close();
        } catch (IOException e) {
            // ignore
        }
        this.closed = true;
    }

    /**
     * Ensure the associated resource is {@link #close}'d before reclaiming memory.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            final boolean leaked;
            synchronized (this) {
                leaked = !this.closed;
            }
            if (leaked) {
                final Logger log = LoggerFactory.getLogger(this.getClass());
                final String msg = this.getClass().getSimpleName() + "[" + this.closeable + "] leaked without invoking close()";
                if (this.allocation != null)
                    log.warn(msg, this.allocation);
                else
                    log.warn(msg);
                this.close();
            }
        } finally {
            super.finalize();
        }
    }
}

