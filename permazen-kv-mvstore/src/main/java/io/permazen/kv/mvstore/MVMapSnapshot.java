
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.util.ByteData;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * A read-only snapshot an {@link MVMap}.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to allow the underlying {@link MVMap} version to be released.
 *
 * <p>
 * Mutation operations throw {@link UnsupportedOperationException}.
 */
@ThreadSafe
public class MVMapSnapshot extends MVMapKVStore implements CloseableKVStore {

    @GuardedBy("this")
    private MVStore.TxCounter counter;

    /**
     * Constructor.
     *
     * @param mvmap the underlying {@link MVMap} to snapshot
     * @throws NullPointerException if {@code mvmap} is null
     */
    public MVMapSnapshot(MVMap<ByteData, ByteData> mvmap) {
        super(mvmap == null || mvmap.isReadOnly() || mvmap.getVersion() == -1 ? mvmap : mvmap.openVersion(mvmap.getVersion()));
        Preconditions.checkArgument(mvmap != null, "null mvmap");
        synchronized (this) {
            this.counter = mvmap.isReadOnly() ? null : mvmap.getStore().registerVersionUsage();
        }
    }

    @Override
    public synchronized void close() {
        if (this.counter != null) {
            this.getMVMap().getStore().deregisterVersionUsage(this.counter);
            this.counter = null;
        }
    }
}
