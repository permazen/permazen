
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;

import java.io.Closeable;

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
    private final Runnable closeAction;
    private final Throwable allocation;

    private boolean closed;

// Constructors

    /**
     * Constructor.
     *
     * @param kvstore key/value store for forwarding
     * @param closeAction action to perform on {@link #close}, or null for none
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public CloseableForwardingKVStore(KVStore kvstore, Runnable closeAction) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        this.kvstore = kvstore;
        this.closeAction = closeAction;
        this.allocation = CloseableForwardingKVStore.TRACK_ALLOCATIONS && this.closeAction != null ?
          new Throwable("allocated here") : null;
    }

// ForwardingKVStore

    @Override
    protected KVStore delegate() {
        return this.kvstore;
    }

// CloseableKVStore

    @Override
    public synchronized void close() {
        if (!this.closed) {
            this.closed = true;
            if (this.closeAction != null)
                this.closeAction.run();
        }
    }

// Object

    /**
     * Ensure the associated resource is {@link #close}'d before reclaiming memory.
     */
    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            final boolean leaked;
            synchronized (this) {
                leaked = !this.closed;
            }
            if (leaked) {
                final Logger log = LoggerFactory.getLogger(this.getClass());
                final String msg = this.getClass().getSimpleName() + "[" + this.closeAction + "] leaked without invoking close()";
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
