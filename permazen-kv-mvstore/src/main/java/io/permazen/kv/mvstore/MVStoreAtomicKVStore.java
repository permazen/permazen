
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.Mutations;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * An {@link AtomicKVStore} implementation based on an {@link MVMap} in an {@link MVStore}.
 *
 * @see <a href="https://www.h2database.com/html/mvstore.html">MVStore</a>
 */
@ThreadSafe
public class MVStoreAtomicKVStore extends AbstractMVStoreKVStore implements AtomicKVStore {

    /**
     * The {@link MVMap.Builder} used by this class.
     *
     * <p>
     * Note this builder is configured with {@link MVMap.Builder#singleWriter}.
     */
    public static final MVMap.Builder<byte[], byte[]> MAP_BUILDER = new MVMap.Builder<byte[], byte[]>()
      .keyType(ByteArrayDataType.INSTANCE)
      .valueType(ByteArrayDataType.INSTANCE)
      .singleWriter();

    // Runtime info
    @GuardedBy("this")
    private MVMap<byte[], byte[]> mvmap;

// AbstractMVStoreKVStore

    @Override
    public synchronized MVMap<byte[], byte[]> getMVMap() {
        Preconditions.checkState(this.mvmap != null, "not started");
        return this.mvmap;
    }

    @Override
    protected synchronized void doOpen() {
        super.doOpen();
        final String name = this.mapName != null ? this.mapName : MVStoreKVImplementation.DEFAULT_MAP_NAME;
        this.mvmap = this.mvstore.openMap(name, MAP_BUILDER);
    }

    @Override
    protected synchronized void doClose() {
        this.mvmap = null;
        super.doClose();
    }

    @Override
    protected synchronized void doCloseImmediately() {
        this.mvmap = null;
        super.doCloseImmediately();
    }

// KVStore

    // We synchronize the mutating methods to ensure that they apply and commit atomically

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        super.put(key, value);
        this.commitOrRollback();
    }

    @Override
    public synchronized void remove(byte[] key) {
        super.remove(key);
        this.commitOrRollback();
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {
        super.removeRange(minKey, maxKey);
        this.commitOrRollback();
    }

// AtomicKVStore

    @Override
    public synchronized MVMapSnapshot snapshot() {
        Preconditions.checkState(this.mvstore != null, "closed");
        return new MVMapSnapshot(this.mvmap);
    }

    @Override
    public synchronized void mutate(Mutations mutations, boolean sync) {
        Preconditions.checkState(this.mvstore != null, "closed");
        this.apply(mutations);
        this.commitOrRollback();
    }

    /**
     * Commit outstanding changes, unless there is an exception when doing so, in which case rollback changes.
     */
    protected void commitOrRollback() {
        Preconditions.checkState(this.mvstore != null, "closed");
        assert Thread.holdsLock(this);
        boolean success = false;
        try {
            this.mvstore.commit();
            success = true;
        } finally {
            if (!success)
                this.mvstore.rollback();
        }
    }
}
