
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import java.io.Closeable;

/**
 * Implemented by {@link KVStore}s that must be {@link #close}ed when no longer in use.
 *
 * <p>
 * Note that the {@link #close} method of this interface does not throw {@link java.io.IOException}.
 */
public interface CloseableKVStore extends KVStore, Closeable {

    /**
     * Close this {@link KVStore} and release any resources associated with it.
     *
     * <p>
     * If this instance is already closed, then nothing happens.
     */
    @Override
    void close();
}
