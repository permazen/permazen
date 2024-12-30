
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRange;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.util.ByteData;

import java.util.Map;
import java.util.stream.Stream;

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
    public static final MVMap.Builder<ByteData, ByteData> MAP_BUILDER = new MVMap.Builder<ByteData, ByteData>()
      .keyType(ByteDataDataType.INSTANCE)
      .valueType(ByteDataDataType.INSTANCE)
      .singleWriter();

    // Runtime info
    @GuardedBy("this")
    private MVMap<ByteData, ByteData> mvmap;

// AbstractMVStoreKVStore

    @Override
    public synchronized MVMap<ByteData, ByteData> getMVMap() {
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
    public synchronized void put(ByteData key, ByteData value) {
        super.put(key, value);
        this.commitOrRollback();
    }

    @Override
    public synchronized void remove(ByteData key) {
        super.remove(key);
        this.commitOrRollback();
    }

    @Override
    public synchronized void removeRange(ByteData minKey, ByteData maxKey) {
        super.removeRange(minKey, maxKey);
        this.commitOrRollback();
    }

// AtomicKVStore

    @Override
    public synchronized MVMapSnapshot readOnlySnapshot() {
        Preconditions.checkState(this.mvstore != null, "closed");
        return new MVMapSnapshot(this.mvmap);
    }

    @Override
    public synchronized void apply(Mutations mutations, boolean sync) {
        Preconditions.checkState(this.mvstore != null, "closed");
        try (Stream<KeyRange> stream = mutations.getRemoveRanges()) {
            stream.iterator()
              .forEachRemaining(this::removeRange);
        }
        try (Stream<Map.Entry<ByteData, ByteData>> stream = mutations.getPutPairs()) {
            stream.iterator()
              .forEachRemaining(entry -> this.put(entry.getKey(), entry.getValue()));
        }
        try (Stream<Map.Entry<ByteData, Long>> stream = mutations.getAdjustPairs()) {
            stream.iterator()
              .forEachRemaining(entry -> this.adjustCounter(entry.getKey(), entry.getValue()));
        }
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
