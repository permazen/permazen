
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.LoggerFactory;

/**
 * Utility class used to track key watches.
 *
 * <p>
 * To limit memory consumption, instances are configured with a maximum maximum number of key watches supported,
 * as well as a maximum lifetime for each key watch. When these limits are exceeded, one or more key watches
 * is evicted and a corresponding spurious notification occurs.
 *
 * <p>
 * Instances can optionally be configured to only weakly reference the returned {@link ListenableFuture}'s.
 * This prevents memory leaks if the user of this class is sloppy and fails to {@link ListenableFuture#cancel cancel()}
 * them when no longer needed; however, it can also lead to missed notifications if the user of this class relies
 * on the {@linkplain ListenableFuture#addListener listener registration functionality} provided in the
 * {@link ListenableFuture} interface, because with a listener registration, there is no longer any need
 * to directly reference the {@link ListenableFuture}. Therefore, by default strong references are used.
 *
 * <p>
 * For space efficiency, this class does not track the original values associated with a key. Therefore,
 * spurious notifications can also occur if a value is changed, and then changed back to its original value.
 *
 * <p>
 * Instances are thread safe.
 *
 * @see org.jsimpledb.kv.KVTransaction#watchKey
 */
@ThreadSafe
public class KeyWatchTracker implements Closeable {

    /**
     * Default capacity ({@value #DEFAULT_CAPACITY}).
     */
    public static final long DEFAULT_CAPACITY = 10000;

    /**
     * Default maximum lifetime in seconds ({@value #DEFAULT_MAXIMUM_LIFETIME}).
     */
    public static final long DEFAULT_MAXIMUM_LIFETIME = 2592000;        // 30 days

    /**
     * Default for the weak reference option ({@value DEFAULT_MAXIMUM_LIFETIME}).
     */
    public static final boolean DEFAULT_WEAK_REFERENCE = false;

    @GuardedBy("this")
    private final TreeMap<byte[], KeyInfo> keyInfos = new TreeMap<>(ByteUtil.COMPARATOR);
    private final Cache<KeyFuture, KeyInfo> futureMap;

    /**
     * Default constructor.
     *
     * <p>
     * Configures capacity {@value #DEFAULT_CAPACITY}, lifetime {@value #DEFAULT_MAXIMUM_LIFETIME} seconds,
     * and strong references.
     */
    public KeyWatchTracker() {
        this(DEFAULT_CAPACITY, DEFAULT_MAXIMUM_LIFETIME, DEFAULT_WEAK_REFERENCE);
    }

    /**
     * Constructor.
     *
     * @param capacity maximum number of key watches allowed
     * @param maxLifetime maximum lifetime for a key watch in seconds
     * @param weakReferences true to only weakly reference registered {@link ListenableFuture}s
     * @throws IllegalArgumentException if {@code capacity} or {@code maxLifetime} is zero or negative
     */
    public KeyWatchTracker(long capacity, long maxLifetime, boolean weakReferences) {

        // Sanity check
        Preconditions.checkArgument(capacity > 0, "capacity <= 0");
        Preconditions.checkArgument(maxLifetime > 0, "maxLifetime <= 0");

        // Initialize
        CacheBuilder<KeyFuture, KeyInfo> cacheBuilder = CacheBuilder.newBuilder()
          .maximumSize(capacity)
          .expireAfterWrite(maxLifetime, TimeUnit.SECONDS)
          .<KeyFuture, KeyInfo>removalListener(new RemovalListener<KeyFuture, KeyInfo>() {
            @Override
            public void onRemoval(RemovalNotification<KeyFuture, KeyInfo> notification) {
                notification.getValue().handleRemoval(notification.getKey());
            }
          });
        if (weakReferences)
            cacheBuilder = cacheBuilder.weakKeys();
        this.futureMap = cacheBuilder.build();
    }

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

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Get/create KeyInfo object for this key
        KeyInfo keyInfo;
        synchronized (this) {
            if ((keyInfo = this.keyInfos.get(key)) == null) {
                key = key.clone();                              // avoid external mutation of key contents
                keyInfo = new KeyInfo(key);
                this.keyInfos.put(key, keyInfo);
            }
        }

        // Create new future for this key
        return keyInfo.createFuture();
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

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Extract KeyInfo object for this key
        final KeyInfo keyInfo;
        synchronized (this) {
            if ((keyInfo = KeyWatchTracker.this.keyInfos.remove(key)) == null)
                return false;
        }

        // Trigger all associated futures
        keyInfo.triggerAll();
        return true;
    }

    /**
     * Trigger all watches associated with the given keys.
     *
     * @param keys keys that have been modified
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code keys} is null
     */
    public boolean trigger(Iterable<byte[]> keys) {

        // Sanity check
        Preconditions.checkArgument(keys != null, "null keys");

        // Extract KeyInfo objects for all keys
        final ArrayList<KeyInfo> triggerList = new ArrayList<>();
        synchronized (this) {
            for (byte[] key : keys) {
                final KeyInfo keyInfo = KeyWatchTracker.this.keyInfos.remove(key);
                if (keyInfo != null)
                    triggerList.add(keyInfo);
            }
        }
        if (triggerList.isEmpty())
            return false;

        // Trigger all associated futures
        for (KeyInfo keyInfo : triggerList)
            keyInfo.triggerAll();
        return true;
    }

    /**
     * Trigger all watches associated with keys in the given range.
     *
     * @param range range of keys that have been modified
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code range} is null
     */
    public boolean trigger(KeyRange range) {

        // Sanity check
        Preconditions.checkArgument(range != null, "null range");

        // Extract KeyInfo objects for all keys in the range
        final ArrayList<KeyInfo> triggerList = new ArrayList<>();
        synchronized (this) {
            final NavigableMap<byte[], KeyInfo> subMap = range.getMax() != null ?
              this.keyInfos.subMap(range.getMin(), true, range.getMax(), false) :
              this.keyInfos.tailMap(range.getMin(), true);
            triggerList.addAll(subMap.values());
            subMap.clear();
        }
        if (triggerList.isEmpty())
            return false;

        // Trigger all associated futures
        for (KeyInfo keyInfo : triggerList)
            keyInfo.triggerAll();
        return true;
    }

    /**
     * Trigger all watches associated with the given mutations.
     *
     * @param mutations mutations
     * @return true if any watches were triggered, otherwise false
     * @throws IllegalArgumentException if {@code mutations} is null
     */
    public boolean trigger(Mutations mutations) {

        // Sanity check
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Trigger all keys affected by any mutation
        boolean result = false;
        for (KeyRange range : mutations.getRemoveRanges())
            result |= this.trigger(range);
        final EntryKeyFunction keyFunction = new EntryKeyFunction();
        result |= this.trigger(this.applyEntryKeyFunction(mutations.getPutPairs(), keyFunction));
        result |= this.trigger(this.applyEntryKeyFunction(mutations.getAdjustPairs(), keyFunction));

        // Done
        return result;
    }

    // This method exists solely to bind the generic type parameters
    private <E extends Map.Entry<byte[], ?>> Iterable<byte[]> applyEntryKeyFunction(Iterable<E> i, EntryKeyFunction keyFunction) {
        return Iterables.<E, byte[]>transform(i, keyFunction);
    }

    /**
     * Trigger all watches.
     *
     * @return true if any watches were triggered, otherwise false
     */
    public boolean triggerAll() {
        return this.trigger(KeyRange.FULL);
    }

    /**
     * Discard all outstanding key watches and fail them with the given exception.
     *
     * @param e failing exception
     */
    public void failAll(Exception e) {

        // Sanity check
        Preconditions.checkArgument(e != null, "null e");

        // Extract KeyInfo objects for all keys and fail all associated futures
        for (KeyInfo keyInfo : this.removeAllKeyInfos())
            keyInfo.failAll(e);
    }

    /**
     * Absorb all of the watches from the given instance into this one.
     * On return, this instance will contain all of the given instance's watches, and the given instance will be empty.
     *
     * @param that the instance to absorb into this one
     */
    public void absorb(KeyWatchTracker that) {

        // Grab all KeyInfo objects from 'that'
        final KeyInfo[] thatKeyInfos = that.removeAllKeyInfos();

        // Add all of their futures to this instance
        for (KeyInfo thatKeyInfo : thatKeyInfos) {
            final byte[] key = thatKeyInfo.getKey();
            KeyInfo thisKeyInfo;
            synchronized (this) {
                if ((thisKeyInfo = this.keyInfos.get(key)) == null) {
                    thisKeyInfo = new KeyInfo(key);
                    this.keyInfos.put(key, thisKeyInfo);
                }
            }
            for (KeyFuture future : thatKeyInfo.removeAllFutures()) {
                thisKeyInfo.addFuture(future);
                future.setOwner(this.futureMap);
                if (future.isDone())
                    this.futureMap.invalidate(future);          // handle race with future's owner vs. future completion
            }
        }

        // Empty that's future map
        that.futureMap.invalidateAll();
    }

    private synchronized KeyInfo[] removeAllKeyInfos() {
        final Collection<KeyInfo> allKeyInfos = this.keyInfos.values();
        final KeyInfo[] result = allKeyInfos.toArray(new KeyInfo[allKeyInfos.size()]);
        this.keyInfos.clear();
        return result;
    }

// Closeable

    /**
     * Close this instance.
     *
     * <p>
     * The implementation in {@link KeyWatchTracker} invokes {@link #failAll failAll()}.
     */
    @Override
    public void close() {
        this.failAll(new Exception("key watch tracker closed"));
    }

// KeyInfo

    // Note locking order: KeyInfo, then KeyWatchTracker
    private class KeyInfo {

        private final byte[] key;
        private final HashSet<KeyFuture> futures = new HashSet<>(1);        // protected by synchronized (this)

        KeyInfo(byte[] key) {
            assert key != null;
            this.key = key;
        }

        public byte[] getKey() {
            return this.key;
        }

        KeyFuture createFuture() {
            final KeyFuture future = new KeyFuture(KeyWatchTracker.this.futureMap);
            this.addFuture(future);
            return future;
        }

        void addFuture(KeyFuture future) {
            KeyWatchTracker.this.futureMap.put(future, this);
            synchronized (this) {
                this.futures.add(future);
            }
        }

        void handleRemoval(KeyFuture future) {
            if (this.removeFuture(future) && future.getOwner() == KeyWatchTracker.this.futureMap)
                future.set(null);                           // if future has not completed yet, trigger a spurious notification
        }

        // This assumes this instance is already removed from KeyWatchTracker.this.keyInfos
        void triggerAll() {
            for (KeyFuture future : this.removeAllFutures())
                future.set(null);
        }

        // This assumes this instance is already removed from KeyWatchTracker.this.keyInfos
        void failAll(Exception e) {
            for (KeyFuture future : this.removeAllFutures())
                future.setException(e);
        }

        /**
         * The given {@link KeyFuture} has completed, was canceled, or has failed, so stop tracking it.
         *
         * @return true if eliminated, false if it was already eliminated
         */
        private boolean removeFuture(KeyFuture future) {
            final boolean removed;
            synchronized (this) {
                removed = this.futures.remove(future);
                if (this.futures.isEmpty()) {                           // discard this instance if there are no futures left
                    synchronized (KeyWatchTracker.this) {
                        KeyWatchTracker.this.keyInfos.remove(this.key);
                    }
                }
            }
            return removed;
        }

        /**
         * Stop tracking all {@link KeyFuture}s.
         *
         * We assume this instance is already removed from {@code KeyWatchTracker.this.keyInfos}.
         */
        ArrayList<KeyFuture> removeAllFutures() {
            final ArrayList<KeyFuture> futureList;
            synchronized (this) {
                futureList = new ArrayList<>(this.futures);
                this.futures.clear();
            }
            KeyWatchTracker.this.futureMap.invalidateAll(futureList);
            return futureList;
        }
    }

// KeyFuture

    private static class KeyFuture extends AbstractFuture<Void> {

        private volatile Cache<KeyFuture, KeyInfo> futureMap;

        KeyFuture(Cache<KeyFuture, KeyInfo> futureMap) {
            this.futureMap = futureMap;
        }

        @Override
        protected boolean set(Void value) {
            this.futureMap.invalidate(this);
            try {
                return super.set(value);
            } catch (Throwable t2) {
                LoggerFactory.getLogger(this.getClass()).error("exception from key watch listener", t2);
                return true;
            }
        }

        @Override
        protected boolean setException(Throwable t) {
            this.futureMap.invalidate(this);
            try {
                return super.setException(t);
            } catch (Throwable t2) {
                LoggerFactory.getLogger(this.getClass()).error("exception from key watch listener", t2);
                return true;
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            this.futureMap.invalidate(this);
            return super.cancel(mayInterruptIfRunning);
        }

        Cache<KeyFuture, KeyInfo> getOwner() {
            return this.futureMap;
        }
        void setOwner(Cache<KeyFuture, KeyInfo> futureMap) {
            this.futureMap = futureMap;
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

