
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.LockManager;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.kv.util.KeyWatchTracker;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of the {@link KVDatabase} interface that provides a concurrent, transactional view
 * of an underlying {@link KVStore} with strong ACID semantics (<b>D</b>urability must be provided by the {@link KVStore}).
 *
 * <p>
 * The ACID semantics, as well as {@linkplain #getWaitTimeout wait timeouts} and {@linkplain #getHoldTimeout hold timeouts},
 * are provided by the {@link LockManager} class. If the wait timeout is exceeded, a {@link RetryTransactionException}
 * is thrown. If the hold timeout is exceeded, a {@link TransactionTimeoutException} is thrown.
 *
 * <p>
 * Instances wrap an underlying {@link KVStore} which provides persistence and from which committed data is read and written.
 * During a transaction, all mutations are recorded internally; if/when the transaction is committed, those mutations are
 * applied to the underlying {@link KVStore} all at once, and this operation is bracketed by calls to
 * {@link #preCommit preCommit()} and {@link #postCommit postCommit()}. If the underlying {@link KVStore}
 * is a {@link AtomicKVStore}, then {@link AtomicKVStore#mutate AtomicKVStore.mutate()} is used.
 *
 * <p>
 * {@linkplain SimpleKVTransaction#watchKey Key watches} are supported.
 *
 * @see LockManager
 */
public class SimpleKVDatabase implements KVDatabase {

    /**
     * Default {@linkplain #getWaitTimeout wait timeout} for newly created transactions in milliseconds
     * ({@value DEFAULT_WAIT_TIMEOUT}).
     */
    public static final long DEFAULT_WAIT_TIMEOUT = 500;

    /**
     * Default {@linkplain #getHoldTimeout hold timeout} in milliseconds ({@value DEFAULT_HOLD_TIMEOUT}).
     */
    public static final long DEFAULT_HOLD_TIMEOUT = 5000;

    /**
     * The {@link KVStore} for the committed data.
     */
    protected final KVStore kv;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final LockManager lockManager = new LockManager(this);
    private final KeyWatchTracker keyWatchTracker = new KeyWatchTracker();

    private long waitTimeout;

    /**
     * Constructor. Uses an internal in-memory {@link NavigableMapKVStore} and the default wait and hold timeouts.
     */
    public SimpleKVDatabase() {
        this(new NavigableMapKVStore());
    }

    /**
     * Constructor taking caller-supplied storage. Will use the default wait and hold timeouts.
     *
     * @param kv {@link KVStore} for the committed data, or null for an in-memory {@link KVStore}
     */
    public SimpleKVDatabase(KVStore kv) {
        this(kv, DEFAULT_WAIT_TIMEOUT, DEFAULT_HOLD_TIMEOUT);
    }

    /**
     * Constructor taking timeout settings. Uses an internal in-memory {@link KVStore}.
     *
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @param holdTimeout how long a thread may hold a contestested lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code waitTimeout} or {@code holdTimeout} is negative
     */
    public SimpleKVDatabase(long waitTimeout, long holdTimeout) {
        this(null, waitTimeout, holdTimeout);
    }

    /**
     * Primary constructor.
     *
     * @param kv {@link KVStore} for the committed data, or null for an in-memory {@link NavigableMapKVStore}
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @param holdTimeout how long a thread may hold a contestested lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code waitTimeout} or {@code holdTimeout} is negative
     */
    public SimpleKVDatabase(KVStore kv, long waitTimeout, long holdTimeout) {
        this.kv = kv != null ? kv : new NavigableMapKVStore();
        this.setWaitTimeout(waitTimeout);
        this.setHoldTimeout(holdTimeout);
    }

    /**
     * Get the wait timeout for newly created transactions.
     *
     * <p>
     * The wait timeout limits how long a thread will wait for a contested lock before giving up and throwing
     * {@link RetryTransactionException}.
     * </p>
     *
     * @return wait timeout in milliseconds
     */
    public synchronized long getWaitTimeout() {
        return this.waitTimeout;
    }

    /**
     * Set the wait timeout for newly created transactions. Default is {@link #DEFAULT_WAIT_TIMEOUT}.
     *
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryTransactionException}
     *  in milliseconds (default), or zero for unlimited
     * @throws IllegalArgumentException if {@code waitTimeout} is negative
     */
    public synchronized void setWaitTimeout(long waitTimeout) {
        Preconditions.checkArgument(waitTimeout >= 0, "waitTimeout < 0");
        this.waitTimeout = waitTimeout;
    }

    /**
     * Get the hold timeout configured for this instance.
     *
     * <p>
     * The hold timeout limits how long a thread may hold on to a contested lock before being forced to release
     * all of its locks; after that, the next attempted operation will fail with {@link RetryTransactionException}.
     * </p>
     *
     * @return hold timeout in milliseconds
     */
    public long getHoldTimeout() {
        return this.lockManager.getHoldTimeout();
    }

    /**
     * Set the hold timeout for this instance. Default is {@link #DEFAULT_HOLD_TIMEOUT}.
     *
     * @param holdTimeout how long a thread may hold a contestested lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code holdTimeout} is negative
     */
    public void setHoldTimeout(long holdTimeout) {
        this.lockManager.setHoldTimeout(holdTimeout);
    }

// KVDatabase

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        this.keyWatchTracker.failAll(new Exception("database stopped"));
    }

    @Override
    public synchronized SimpleKVTransaction createTransaction() {
        return new SimpleKVTransaction(this, this.waitTimeout);
    }

// Key Watches

    synchronized ListenableFuture<Void> watchKey(byte[] key) {
        return this.keyWatchTracker.register(key);
    }

// Subclass hooks

    /**
     * Invoked during transaction commit just prior to writing changes to the underlying {@link KVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #postCommit postCommit()} will be invoked in matching pairs,
     * and that this instance will be locked when these methods are invoked.
     * </p>
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     * </p>
     *
     * @param tx the transaction about to be committed
     * @throws RetryTransactionException if this transaction must be retried and is no longer usable
     */
    protected void preCommit(SimpleKVTransaction tx) {
    }

    /**
     * Invoked during transaction commit just after writing changes to the underlying {@link KVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #preCommit preCommit()} will be invoked in matching pairs,
     * and that this instance will be locked when these methods are invoked.
     * </p>
     *
     * <p>
     * This method is invoked even if the underlying {@link KVStore} throws an exception while changes were being written to it.
     * In that case, {@code successful} will be false.
     * </p>
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     * </p>
     *
     * @param tx the transaction that was committed
     * @param successful true if all changes were written back successfully,
     *  false if the underlying {@link KVStore} threw an exception during commit update
     */
    protected void postCommit(SimpleKVTransaction tx, boolean successful) {
    }

    /**
     * Apply mutations to the underlying {@link KVStore}.
     */
    void applyMutations(final Iterable<Mutation> mutations) {
        if (this.kv instanceof AtomicKVStore) {
            ((AtomicKVStore)this.kv).mutate(new Mutations() {
                @Override
                public Iterable<Del> getRemoveRanges() {
                    return Iterables.filter(mutations, Del.class);
                }
                @Override
                public Iterable<Map.Entry<byte[], byte[]>> getPutPairs() {
                    return Iterables.transform(Iterables.filter(mutations, Put.class),
                      new Function<Put, Map.Entry<byte[], byte[]>>() {
                        @Override
                        public Map.Entry<byte[], byte[]> apply(Put put) {
                            return new AbstractMap.SimpleEntry<byte[], byte[]>(put.getKey(), put.getValue());
                        }
                    });
                }
                @Override
                public Iterable<Map.Entry<byte[], Long>> getAdjustPairs() {
                    return Collections.<Map.Entry<byte[], Long>>emptySet();
                }
            }, true);
        } else {
            for (Mutation mutation : mutations)
                mutation.apply(this.kv);
        }
    }

    /**
     * Verify that the given transaction is still usable.
     *
     * <p>
     * This method is invoked at the start of the {@link KVStore} data access and {@link SimpleKVTransaction#commit commit()}
     * methods of the {@link SimpleKVTransaction} associated with this instance. This allows for any checks which depend on
     * a consistent view of the transaction and database together. This instance's lock will be held when this method is invoked.
     * Note: transaction state is also protected by this instance's lock.
     * </p>
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     * </p>
     *
     * @param tx the transaction being accessed
     * @throws StaleTransactionException if this instance is no longer usable
     * @throws RetryTransactionException if this transaction should be retried
     * @throws TransactionTimeoutException if the transaction has timed out
     */
    protected void checkState(SimpleKVTransaction tx) {
    }

    private void checkUsable(SimpleKVTransaction tx) {
        if (tx.stale)
            throw new StaleTransactionException(tx);
        if (this.lockManager.checkHoldTimeout(tx.lockOwner) == -1) {
            this.rollback(tx);
            throw new TransactionTimeoutException(tx,
              "transaction taking too long: hold timeout of " + this.lockManager.getHoldTimeout() + "ms has expired");
        }
    }

// SimpleKVTransaction hooks

    synchronized byte[] get(SimpleKVTransaction tx, byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key.length == 0 || key[0] != (byte)0xff, "key starts with 0xff");
        this.checkUsable(tx);
        this.checkState(tx);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation != null)
            return mutation instanceof Put ? ((Put)mutation).getValue() : null;

        // Read from underlying store
        this.getLock(tx, key, ByteUtil.getNextKey(key), false);
        return this.kv.get(key);
    }

    synchronized KVPair getAtLeast(SimpleKVTransaction tx, byte[] minKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Sanity check
        this.checkUsable(tx);
        this.checkState(tx);

        // Save original min key for locking purposes
        final byte[] originalMinKey = minKey;

        // Look for a mutation starting before minKey but containing it
        if (minKey.length > 0) {
            final Mutation overlap = tx.findMutation(minKey);
            if (overlap != null) {
                if (overlap instanceof Put) {
                    final Put put = (Put)overlap;
                    assert Arrays.equals(put.getKey(), minKey);
                    return new KVPair(put.getKey(), put.getValue());
                }
                assert overlap instanceof Del;
                final byte[] max = overlap.getMax();
                if (max == null)
                    return null;
                minKey = max;
            }
        }

        // Get read lock
        this.getLock(tx, originalMinKey, null, false);

        // Find whichever is first: a transaction Put, or an underlying store entry not covered by a transaction Delete
        SortedSet<Mutation> mutations = tx.mutations;
        while (true) {

            // Get the next mutation and kvstore entry >= minKey (if they exist)
            if (minKey != null)
                mutations = mutations.tailSet(Mutation.key(minKey));
            final Mutation mutation = !mutations.isEmpty() ? mutations.first() : null;
            final KVPair entry = this.kv.getAtLeast(minKey);

            // Handle the case where neither is found
            if (mutation == null && entry == null)
                return null;

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) <= 0)) {
                if (mutation instanceof Del) {
                    if ((minKey = mutation.getMax()) == null)
                        return null;
                    continue;
                }
                final Put put = (Put)mutation;
                return new KVPair(put.getKey(), put.getValue());
            } else
                return entry;
        }
    }

    synchronized KVPair getAtMost(SimpleKVTransaction tx, byte[] maxKey) {

        // Sanity check
        this.checkUsable(tx);
        this.checkState(tx);

        // Get read lock
        this.getLock(tx, ByteUtil.EMPTY, maxKey, false);

        // Find whichever is first: a transaction addition, or an underlying store entry not covered by a transaction deletion
        SortedSet<Mutation> mutations = tx.mutations;
        while (true) {

            // Get the next mutation and kvstore entry < maxKey (if they exist)
            if (maxKey != null)
                mutations = mutations.headSet(Mutation.key(maxKey));
            final Mutation mutation = !mutations.isEmpty() ? mutations.last() : null;
            final KVPair entry = this.kv.getAtMost(maxKey);             // XXX BUG - not locked yet

            // Handle the case where neither is found
            if (mutation == null && entry == null)
                return null;

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) >= 0)) {
                if (mutation instanceof Del) {
                    if ((maxKey = mutation.getMin()) == null)
                        return null;
                    continue;
                }
                final Put put = (Put)mutation;
                return new KVPair(put.getKey(), put.getValue());
            } else
                return entry;
        }
    }

    synchronized void put(SimpleKVTransaction tx, byte[] key, byte[] value) {

        // Sanity check
        if (value == null)
            throw new NullPointerException();
        Preconditions.checkArgument(key.length == 0 || key[0] != (byte)0xff, "key starts with 0xff");
        this.checkUsable(tx);
        this.checkState(tx);
        final byte[] keyNext = ByteUtil.getNextKey(key);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation instanceof Put) {
            assert Arrays.equals(((Put)mutation).getKey(), key);

            // Replace Put with new Put
            tx.mutations.remove(mutation);
            tx.mutations.add(new Put(key, value));
        } else if (mutation instanceof Del) {

            // Split [Del] -> [Del*, Put, Del*]  *if needed
            final Del del = (Del)mutation;
            final byte[] delMin = del.getMin();
            final byte[] delMax = del.getMax();
            tx.mutations.remove(del);
            if (KeyRange.compare(delMin, key) < 0)
                tx.mutations.add(new Del(delMin, key));
            if (KeyRange.compare(keyNext, delMax) < 0)
                tx.mutations.add(new Del(keyNext, delMax));
            tx.mutations.add(new Put(key, value));
        } else {

            // Add write lock and new tx mutation
            this.getLock(tx, key, keyNext, true);
            tx.mutations.add(new Put(key, value));
        }
    }

    synchronized void remove(SimpleKVTransaction tx, byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key.length == 0 || key[0] != (byte)0xff, "key starts with 0xff");
        this.checkUsable(tx);
        this.checkState(tx);
        final byte[] keyNext = ByteUtil.getNextKey(key);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation instanceof Put) {
            assert Arrays.equals(((Put)mutation).getKey(), key);

            // Replace Put with Del
            tx.mutations.remove(mutation);
            tx.mutations.add(new Del(key));
        } else if (mutation == null) {

            // Add write lock and new tx mutation
            this.getLock(tx, key, keyNext, true);
            tx.mutations.add(new Del(key));
        }
    }

    synchronized void removeRange(SimpleKVTransaction tx, byte[] minKey, byte[] maxKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Sanity check
        int diff = KeyRange.compare(minKey, maxKey);
        Preconditions.checkArgument(diff <= 0, "minKey > maxKey");
        this.checkUsable(tx);
        this.checkState(tx);
        if (diff == 0)                                                          // range is empty
            return;
        final byte[] originalMinKey = minKey;
        final byte[] originalMaxKey = maxKey;

        // Deal with partial overlap at the left end of the range
        if (minKey.length > 0) {
            final Mutation leftMutation = tx.findMutation(minKey);
            if (leftMutation instanceof Put) {
                assert Arrays.equals(((Put)leftMutation).getKey(), minKey);
                tx.mutations.remove(leftMutation);                                          // overwritten by this change
            } else if (leftMutation instanceof Del) {
                final Del del = (Del)leftMutation;
                tx.mutations.remove(del);                                                   // will merge into this change
                minKey = del.getMin();                                                      // guaranteed to be <= minKey
                if (KeyRange.compare(del.getMax(), maxKey) > 0)                             // get higher of the two maxKeys
                    maxKey = del.getMax();
            }
        }

        // Deal with partial overlap at the right end of the range
        if (maxKey != null) {
            Mutation rightMutation = null;
            try {
                rightMutation = minKey != null ?
                  tx.mutations.subSet(Mutation.key(minKey), Mutation.key(maxKey)).last() :
                  tx.mutations.headSet(Mutation.key(maxKey)).last();
            } catch (NoSuchElementException e) {
                // ignore
            }
            if (rightMutation instanceof Put)
                tx.mutations.remove(rightMutation);                                         // overwritten by this change
            else if (rightMutation instanceof Del) {
                final Del del = (Del)rightMutation;
                tx.mutations.remove(del);                                                   // will merge into this change
                if (KeyRange.compare(del.getMax(), maxKey) > 0)                             // get higher of the two maxKeys
                    maxKey = del.getMax();
            }
        }

        // Remove all mutations in the middle
        if (originalMinKey == null && originalMaxKey == null)
            tx.mutations.clear();
        else if (originalMinKey == null)
            tx.mutations.headSet(Mutation.key(originalMaxKey)).clear();
        else if (originalMaxKey == null)
            tx.mutations.tailSet(Mutation.key(originalMinKey)).clear();
        else
            tx.mutations.subSet(Mutation.key(originalMinKey), Mutation.key(originalMaxKey)).clear();

        // Add write lock and new tx mutation
        this.getLock(tx, minKey, maxKey, true);
        tx.mutations.add(new Del(minKey, maxKey));
    }

    synchronized void commit(SimpleKVTransaction tx) {

        // Prevent use after commit() or rollback() invoked
        if (tx.stale)
            throw new StaleTransactionException(tx);
        tx.stale = true;

        // Sanity check locking here before releasing locks
        boolean allMutationsWereLocked = true;
        boolean assertionsEnabled = false;
        assert assertionsEnabled = true;
        if (assertionsEnabled) {
            for (Mutation mutation : tx.mutations) {
                if (!this.lockManager.isLocked(tx.lockOwner, mutation.getMin(), mutation.getMax(), true)) {
                    allMutationsWereLocked = false;
                    break;
                }
            }
        }

        // Release all locks
        if (!this.lockManager.release(tx.lockOwner)) {
            throw new TransactionTimeoutException(tx,
              "transaction taking too long: hold timeout of " + this.lockManager.getHoldTimeout() + "ms has expired");
        }
        assert allMutationsWereLocked;

        // Check subclass state
        this.checkState(tx);

        // If there are no mutations, there's no need to write anything
        if (tx.mutations.isEmpty())
            return;

        // Commit mutations
        this.preCommit(tx);
        boolean successful = false;
        try {

            // Apply mutations
            this.applyMutations(tx.mutations);
            successful = true;

            // Trigger key watches
            if (this.keyWatchTracker.getNumKeysWatched() > 0) {
                for (Mutation mutation : tx.mutations)
                    mutation.trigger(this.keyWatchTracker);
            }
        } finally {
            tx.mutations.clear();
            this.postCommit(tx, successful);
        }
    }

    synchronized void rollback(SimpleKVTransaction tx) {

        // Prevent use after commit() or rollback() invoked
        if (tx.stale)
            return;
        tx.stale = true;

        // Release all locks
        this.lockManager.release(tx.lockOwner);
    }

// Internal methods

    private /*synchronized*/ void getLock(SimpleKVTransaction tx, byte[] minKey, byte[] maxKey, boolean write) {

        // Attempt to get the lock
        LockManager.LockResult lockResult;
        try {
            lockResult = this.lockManager.lock(tx.lockOwner, minKey, maxKey, write, tx.waitTimeout);
        } catch (InterruptedException e) {
            this.rollback(tx);
            Thread.currentThread().interrupt();
            throw new RetryTransactionException(tx, "transaction interrupted while waiting to acquire lock", e);
        }

        // Check result
        switch (lockResult) {
        case SUCCESS:
            break;
        case WAIT_TIMEOUT_EXPIRED:
            this.rollback(tx);
            throw new RetryTransactionException(tx, "could not acquire lock after " + tx.waitTimeout + "ms");
        case HOLD_TIMEOUT_EXPIRED:
            this.rollback(tx);
            throw new TransactionTimeoutException(tx,
              "transaction taking too long: hold timeout of " + this.lockManager.getHoldTimeout() + "ms has expired");
        default:
            throw new RuntimeException("internal error");
        }
    }
}

