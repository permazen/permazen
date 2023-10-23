
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.StaleTransactionException;
import io.permazen.kv.TransactionTimeoutException;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.LockManager;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.util.KeyWatchTracker;
import io.permazen.kv.util.NavigableMapKVStore;
import io.permazen.util.ByteUtil;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.stream.Stream;

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
 * <p>
 * Instances implement {@link Serializable} if the underlying {@link KVStore} is; this is the case when the default
 * constructor, which uses a {@link NavigableMapKVStore}, is used. However, key watches and open transactions are not
 * remembered across a (de)serialization cycle.
 *
 * @see LockManager
 */
public class SimpleKVDatabase implements KVDatabase, Serializable {

    /**
     * Default {@linkplain #getWaitTimeout wait timeout} for newly created transactions in milliseconds
     * ({@value DEFAULT_WAIT_TIMEOUT}).
     */
    public static final long DEFAULT_WAIT_TIMEOUT = 500;

    /**
     * Default {@linkplain #getHoldTimeout hold timeout} in milliseconds ({@value DEFAULT_HOLD_TIMEOUT}).
     */
    public static final long DEFAULT_HOLD_TIMEOUT = 5000;

    private static final long serialVersionUID = -6960954436594742251L;

    /**
     * The {@link KVStore} for the committed data.
     */
    protected final KVStore kv;

    protected /*final*/ transient Logger log = LoggerFactory.getLogger(this.getClass());

    private /*final*/ transient LockManager lockManager = new LockManager(this);
    private /*final*/ transient KeyWatchTracker keyWatchTracker;

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
     *
     * @return hold timeout in milliseconds
     */
    public long getHoldTimeout() {
        return this.lockManager.getHoldTimeout();
    }

    /**
     * Set the hold timeout for this instance. Default is {@link #DEFAULT_HOLD_TIMEOUT}.
     *
     * @param holdTimeout how long a thread may hold a contested lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code holdTimeout} is negative
     */
    public void setHoldTimeout(long holdTimeout) {
        this.lockManager.setHoldTimeout(holdTimeout);
    }

// KVDatabase

    @Override
    @PostConstruct
    public synchronized void start() {
    }

    @Override
    @PreDestroy
    public synchronized void stop() {
        if (this.keyWatchTracker != null) {
            this.keyWatchTracker.close();
            this.keyWatchTracker = null;
        }
    }

    @Override
    public SimpleKVTransaction createTransaction(Map<String, ?> options) {
        return this.createTransaction();                                            // no options supported yet
    }

    @Override
    public synchronized SimpleKVTransaction createTransaction() {
        return new SimpleKVTransaction(this, this.waitTimeout);
    }

// Key Watches

    synchronized ListenableFuture<Void> watchKey(byte[] key) {
        if (this.keyWatchTracker == null)
            this.keyWatchTracker = new KeyWatchTracker();
        return this.keyWatchTracker.register(key);
    }

// Subclass hooks

    /**
     * Invoked during transaction commit just prior to writing changes to the underlying {@link KVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #postCommit postCommit()} will be invoked in matching pairs,
     * and that this instance will be locked when these methods are invoked.
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
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
     *
     * <p>
     * This method is invoked even if the underlying {@link KVStore} throws an exception while changes were being written to it.
     * In that case, {@code successful} will be false.
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
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
    void applyMutations(final Collection<Mutation> mutations) {
        if (this.kv instanceof AtomicKVStore) {
            ((AtomicKVStore)this.kv).mutate(new Mutations() {
                @Override
                public Stream<KeyRange> getRemoveRanges() {
                    return mutations.stream()
                      .filter(Del.class::isInstance)
                      .map(Del.class::cast);
                }
                @Override
                public Stream<Map.Entry<byte[], byte[]>> getPutPairs() {
                    return mutations.stream()
                      .filter(Put.class::isInstance)
                      .map(Put.class::cast)
                      .map(Put::toMapEntry);
                }
                @Override
                public Stream<Map.Entry<byte[], Long>> getAdjustPairs() {
                    return Stream.empty();
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
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     *
     * @param tx the transaction being accessed
     * @throws StaleTransactionException if this instance is no longer usable
     * @throws RetryTransactionException if this transaction should be retried
     * @throws TransactionTimeoutException if the transaction has timed out
     */
    protected void checkState(SimpleKVTransaction tx) {
        assert Thread.holdsLock(this);
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

    synchronized KVPair getAtLeast(SimpleKVTransaction tx, byte[] minKey, final byte[] maxKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Sanity check
        this.checkUsable(tx);
        this.checkState(tx);
        if (maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0)
            return null;

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
                if (max == null || (maxKey != null && ByteUtil.compare(max, maxKey) >= 0))
                    return null;
                minKey = max;
            }
        }

        // Get read lock
        this.getLock(tx, originalMinKey, maxKey, false);

        // Find whichever is first: a transaction Put, or an underlying store entry not covered by a transaction Delete
        SortedSet<Mutation> mutations = maxKey != null ? tx.mutations.headSet(Mutation.key(maxKey)) : tx.mutations;
        while (true) {

            // Get the next mutation and kvstore entry >= minKey (if they exist)
            assert minKey != null;
            mutations = mutations.tailSet(Mutation.key(minKey));
            final Mutation mutation = !mutations.isEmpty() ? mutations.first() : null;
            final KVPair entry = this.kv.getAtLeast(minKey, maxKey);
            assert entry == null || ByteUtil.compare(entry.getKey(), minKey) >= 0;
            assert entry == null || maxKey == null || ByteUtil.compare(entry.getKey(), maxKey) < 0;

            // Handle the case where neither is found
            if (mutation == null && entry == null)
                return null;

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) <= 0)) {
                if (mutation instanceof Del) {
                    if ((minKey = mutation.getMax()) == null || (maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0))
                        return null;
                    continue;
                }
                final Put put = (Put)mutation;
                return new KVPair(put.getKey(), put.getValue());
            } else
                return entry;
        }
    }

    synchronized KVPair getAtMost(SimpleKVTransaction tx, byte[] maxKey, byte[] minKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Sanity check
        this.checkUsable(tx);
        this.checkState(tx);
        if (maxKey != null && ByteUtil.compare(minKey, maxKey) >= 0)
            return null;

        // Get read lock
        this.getLock(tx, minKey, maxKey, false);

        // Find whichever is first: a transaction addition, or an underlying store entry not covered by a transaction deletion
        SortedSet<Mutation> mutations = tx.mutations;
        while (true) {

            // Get the next mutation and kvstore entry < maxKey (if they exist)
            if (maxKey != null)
                mutations = mutations.headSet(Mutation.key(maxKey));
            final Mutation mutation = !mutations.isEmpty() ? mutations.last() : null;
            final KVPair entry = this.kv.getAtMost(maxKey, minKey);
            assert entry == null || ByteUtil.compare(entry.getKey(), minKey) >= 0;
            assert entry == null || maxKey == null || ByteUtil.compare(entry.getKey(), maxKey) < 0;

            // Handle the case where neither is found
            if (mutation == null && entry == null)
                return null;

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) >= 0)) {
                if (mutation instanceof Del) {
                    if ((maxKey = mutation.getMin()) == null || ByteUtil.compare(minKey, maxKey) >= 0)
                        return null;
                    continue;
                }
                final Put put = (Put)mutation;
                if (ByteUtil.compare(put.getKey(), minKey) < 0) {
                    assert entry == null;       // because otherwise minKey > mutation >= entry so entry should not have been found
                    return null;
                }
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
        if (originalMinKey.length == 0 && originalMaxKey == null)
            tx.mutations.clear();
        else if (originalMinKey.length == 0)
            tx.mutations.headSet(Mutation.key(originalMaxKey)).clear();
        else if (originalMaxKey == null)
            tx.mutations.tailSet(Mutation.key(originalMinKey)).clear();
        else
            tx.mutations.subSet(Mutation.key(originalMinKey), Mutation.key(originalMaxKey)).clear();

        // Add write lock and new tx mutation
        this.getLock(tx, minKey, maxKey, true);
        tx.mutations.add(new Del(minKey, maxKey));
    }

    synchronized void commit(SimpleKVTransaction tx, boolean readOnly) {

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

        // If transaction is read-only, or there are no mutations, there's no need to write anything
        if (readOnly || tx.mutations.isEmpty())
            return;

        // Commit mutations
        this.preCommit(tx);
        boolean successful = false;
        try {

            // Apply mutations
            this.applyMutations(tx.mutations);
            successful = true;

            // Trigger key watches
            if (this.keyWatchTracker != null && this.keyWatchTracker.getNumKeysWatched() > 0) {
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

// Serialization

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        this.log = LoggerFactory.getLogger(this.getClass());
        this.lockManager = new LockManager(this);
    }
}
