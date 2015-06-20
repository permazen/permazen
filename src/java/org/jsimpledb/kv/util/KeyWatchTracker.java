
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.util.ByteUtil;

/**
 * Utility class used to track key watches.
 *
 * <p>
 * For space efficiency, this class does not track original values. Therefore, spurious notifications can result.
 *
 * <p>
 * Instances are thread safe.
 *
 * @see org.jsimpledb.kv.KVTransaction#watchKey
 */
public class KeyWatchTracker {

    private final TreeMap<byte[], KeyInfo> keyInfos = new TreeMap<>(ByteUtil.COMPARATOR);

    /**
     * Register a new watch.
     *
     * <p>
     * If the returned {@link java.util.concurrent.Future} is {@link java.util.concurrent.Future#cancel cancel()}'ed,
     * the watch is automatically unregistered.
     *
     * @param key the key to watch
     * @return a {@link ListenableFuture} that returns {@code key} when the value associated with {@code key} is modified
     * @throws IllegalArgumentException if {@code key} is null
     */
    public synchronized ListenableFuture<Void> register(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        KeyInfo keyInfo = this.keyInfos.get(key);
        if (keyInfo == null) {
            key = key.clone();                              // avoid external mutation of key contents
            keyInfo = new KeyInfo(key);
            this.keyInfos.put(key, keyInfo);
        }
        return keyInfo.add();
    }

    /**
     * Count the number of keys being watched.
     *
     * <p>
     * Note that the same key can be watched more than once, so this only counts keys being watched, not total watches.
     *
     * @return number of keys being watched
     */
    public synchronized int getNumKeysWatched() {
        return this.keyInfos.size();
    }

    /**
     * Trigger all watches associated with the given key.
     *
     * @param key the key that has been modified
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code key} is null
     */
    public boolean trigger(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        final KeyInfo keyInfo;
        synchronized (this) {
            keyInfo = KeyWatchTracker.this.keyInfos.remove(key);
        }
        if (keyInfo != null) {
            keyInfo.succeed();
            return true;
        }
        return false;
    }

    /**
     * Trigger all watches associated with the given keys.
     *
     * @param keys keys that have been modified
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code keys} is null
     */
    public boolean trigger(Iterable<byte[]> keys) {
        Preconditions.checkArgument(keys != null, "null keys");
        final ArrayList<KeyInfo> triggerList = new ArrayList<>();
        synchronized (this) {
            for (byte[] key : keys) {
                final KeyInfo keyInfo = KeyWatchTracker.this.keyInfos.remove(key);
                if (keyInfo != null)
                    triggerList.add(keyInfo);
            }
        }
        for (KeyInfo keyInfo : triggerList)
            keyInfo.succeed();
        return !triggerList.isEmpty();
    }

    /**
     * Trigger all watches associated with keys in the given range.
     *
     * @param range range of keys that have been modified
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean trigger(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        final ArrayList<KeyInfo> triggerList = new ArrayList<>();
        synchronized (this) {
            final NavigableMap<byte[], KeyInfo> subMap = range.getMax() != null ?
              this.keyInfos.subMap(range.getMin(), true, range.getMax(), false) : this.keyInfos.tailMap(range.getMin(), true);
            triggerList.addAll(subMap.values());
            subMap.clear();
        }
        for (KeyInfo keyInfo : triggerList)
            keyInfo.succeed();
        return !triggerList.isEmpty();
    }

    /**
     * Trigger all watches associated with the given mutations.
     *
     * @param mutations mutations
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code mutations} is null
     */
    public boolean trigger(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        boolean result = false;
        for (KeyRange range : mutations.getRemoveRanges())
            result |= this.trigger(range);
        final EntryKeyFunction keyFunction = new EntryKeyFunction();
        result |= this.trigger(Iterables.<Map.Entry<byte[], byte[]>, byte[]>transform(mutations.getPutPairs(), keyFunction));
        result |= this.trigger(Iterables.<Map.Entry<byte[], Long>, byte[]>transform(mutations.getAdjustPairs(), keyFunction));
        return result;
    }

    /**
     * Trigger all watches.
     */
    public synchronized void triggerAll() {
        for (KeyInfo keyInfo : this.keyInfos.values())
            keyInfo.succeed();
        this.keyInfos.clear();
    }

    /**
     * Discard all outstanding key watches and fail them with the given exception.
     *
     * @param e failing exception
     */
    public synchronized void failAll(Exception e) {
        for (KeyInfo keyInfo : this.keyInfos.values())
            keyInfo.fail(e);
        this.keyInfos.clear();
    }

// KeyInfo

    private class KeyInfo {

        private final byte[] key;
        private final HashSet<KeyFuture> futures = new HashSet<>(1);

        KeyInfo(byte[] key) {
            assert key != null;
            this.key = key;
        }

        KeyFuture add() {
            assert Thread.holdsLock(KeyWatchTracker.this);
            final KeyFuture future = new KeyFuture(this);
            this.futures.add(future);
            return future;
        }

        void cancel(KeyFuture future) {
            synchronized (KeyWatchTracker.this) {
                if (this.futures.remove(future) && this.futures.isEmpty())
                    KeyWatchTracker.this.keyInfos.remove(this.key);
            }
        }

        void succeed() {
            for (KeyFuture future : this.futures)
                future.set(null);
        }

        void fail(Exception e) {
            for (KeyFuture future : this.futures)
                future.setException(e);
        }
    }

// KeyFuture

    private static class KeyFuture extends AbstractFuture<Void> {

        private final KeyInfo keyInfo;

        KeyFuture(KeyInfo keyInfo) {
            assert keyInfo != null;
            this.keyInfo = keyInfo;
        }

        @Override
        protected boolean set(Void value) {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable t) {
            return super.setException(t);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            this.keyInfo.cancel(this);
            return super.cancel(mayInterruptIfRunning);
        }
    }

// EntryKeyFunction

    private static class EntryKeyFunction implements Function<Map.Entry<byte[], ?>, byte[]> {

        @Override
        public byte[] apply(Map.Entry<byte[], ?> entry) {
            return entry.getKey();
        }
    }
}

