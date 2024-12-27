
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransactionTimeoutException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.util.KeyWatchTracker;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of the {@link KVDatabase} interface that provides a concurrent, transactional view
 * of an underlying {@link AtomicKVStore} with strong ACID semantics (assuming <b>A</b>tomicity and <b>D</b>urability
 * are provided by the underlying {@link AtomicKVStore}).
 *
 * <p>
 * Transaction isolation is implemented via key range locking using a {@link LockManager}. Conflicting access
 * will block, subject to the {@linkplain #getWaitTimeout wait timeout} and {@linkplain #getHoldTimeout hold timeout}.
 * If the wait timeout is exceeded, a {@link RetryKVTransactionException} is thrown. If the hold timeout is exceeded,
 * a {@link KVTransactionTimeoutException} is thrown.
 *
 * <p>
 * Instances wrap an underlying {@link AtomicKVStore} which from which committed data is read and written.
 * During a transaction, all mutations are recorded in memory; if/when the transaction is committed, those mutations are
 * applied to the {@link AtomicKVStore} all at once via {@link AtomicKVStore#apply(Mutations, boolean) AtomicKVStore.apply()}.
 * This commit operation is bracketed by calls to {@link #preCommit preCommit()} and {@link #postCommit postCommit()}.
 *
 * <p>
 * {@linkplain SimpleKVTransaction#watchKey Key watches} are supported.
 *
 * <p>
 * Instances implement {@link Serializable} if the underlying {@link AtomicKVStore} is; this is the case when the default
 * constructor, which uses a {@link MemoryKVStore}, is used. However, key watches and open transactions are not
 * remembered across a (de)serialization cycle.
 *
 * <p>
 * For a simple in-memory implementation, see {@link MemoryKVDatabase}.
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
     * The {@link AtomicKVStore} for the committed data.
     */
    @SuppressWarnings("serial")
    protected final AtomicKVStore kv;

    protected /*final*/ transient Logger log = LoggerFactory.getLogger(this.getClass());

    @SuppressWarnings("this-escape")
    private /*final*/ transient LockManager lockManager = new LockManager(this);
    private /*final*/ transient KeyWatchTracker keyWatchTracker;

    private long waitTimeout;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Uses the default wait and hold timeouts.
     *
     * @param kv storage for the committed data
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public SimpleKVDatabase(AtomicKVStore kv) {
        this(kv, DEFAULT_WAIT_TIMEOUT, DEFAULT_HOLD_TIMEOUT);
    }

    /**
     * Primary constructor.
     *
     * @param kv {@link AtomicKVStore} for the committed data
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryKVTransactionException}
     *  in milliseconds, or zero for unlimited
     * @param holdTimeout how long a thread may hold a contestested lock before throwing {@link RetryKVTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code waitTimeout} or {@code holdTimeout} is negative
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public SimpleKVDatabase(AtomicKVStore kv, long waitTimeout, long holdTimeout) {
        Preconditions.checkArgument(kv != null, "null kv");
        Preconditions.checkArgument(waitTimeout >= 0, "waitTimeout < 0");
        this.kv = kv;
        this.waitTimeout = waitTimeout;
        this.lockManager.setHoldTimeout(holdTimeout);
    }

// Properties

    /**
     * Get the wait timeout for newly created transactions.
     *
     * <p>
     * The wait timeout limits how long a thread will wait for a contested lock before giving up and throwing
     * {@link RetryKVTransactionException}.
     *
     * @return wait timeout in milliseconds
     */
    public synchronized long getWaitTimeout() {
        return this.waitTimeout;
    }

    /**
     * Set the wait timeout for newly created transactions. Default is {@link #DEFAULT_WAIT_TIMEOUT}.
     *
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryKVTransactionException}
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
     * all of its locks; after that, the next attempted operation will fail with {@link RetryKVTransactionException}.
     *
     * @return hold timeout in milliseconds
     */
    public long getHoldTimeout() {
        return this.lockManager.getHoldTimeout();
    }

    /**
     * Set the hold timeout for this instance. Default is {@link #DEFAULT_HOLD_TIMEOUT}.
     *
     * @param holdTimeout how long a thread may hold a contested lock before throwing {@link RetryKVTransactionException}
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

    synchronized ListenableFuture<Void> watchKey(ByteData key) {
        if (this.keyWatchTracker == null)
            this.keyWatchTracker = new KeyWatchTracker();
        return this.keyWatchTracker.register(key);
    }

// Subclass hooks

    /**
     * Invoked during transaction commit just prior to writing changes to the underlying {@link AtomicKVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #postCommit postCommit()} will be invoked in matching pairs,
     * and that this instance will be locked when these methods are invoked.
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     *
     * @param tx the transaction about to be committed
     * @throws RetryKVTransactionException if this transaction must be retried and is no longer usable
     */
    protected void preCommit(SimpleKVTransaction tx) {
    }

    /**
     * Invoked during transaction commit just after writing changes to the underlying {@link AtomicKVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #preCommit preCommit()} will be invoked in matching pairs,
     * and that this instance will be locked when these methods are invoked.
     *
     * <p>
     * This method is invoked even if the underlying {@link AtomicKVStore} throws an exception while changes were being
     * written to it. In that case, {@code successful} will be false.
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     *
     * @param tx the transaction that was committed
     * @param successful true if all changes were written back successfully,
     *  false if the underlying {@link AtomicKVStore} threw an exception during commit update
     */
    protected void postCommit(SimpleKVTransaction tx, boolean successful) {
    }

    /**
     * Apply mutations to the underlying {@link AtomicKVStore}.
     */
    void applyMutations(final Collection<Mutation> mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        this.kv.apply(new Mutations() {
            @Override
            public Stream<KeyRange> getRemoveRanges() {
                return mutations.stream()
                  .filter(Del.class::isInstance)
                  .map(Del.class::cast);
            }
            @Override
            public Stream<Map.Entry<ByteData, ByteData>> getPutPairs() {
                return mutations.stream()
                  .filter(Put.class::isInstance)
                  .map(Put.class::cast)
                  .map(Put::toMapEntry);
            }
            @Override
            public Stream<Map.Entry<ByteData, Long>> getAdjustPairs() {
                return Stream.empty();
            }
        }, true);
    }

    /**
     * Verify that the given transaction is still usable.
     *
     * <p>
     * This method is invoked at the start of the {@link AtomicKVStore} data access and {@link SimpleKVTransaction#commit commit()}
     * methods of the {@link SimpleKVTransaction} associated with this instance. This allows for any checks which depend on
     * a consistent view of the transaction and database together. This instance's lock will be held when this method is invoked.
     * Note: transaction state is also protected by this instance's lock.
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     *
     * @param tx the transaction being accessed
     * @throws StaleKVTransactionException if this instance is no longer usable
     * @throws RetryKVTransactionException if this transaction should be retried
     * @throws KVTransactionTimeoutException if the transaction has timed out
     */
    protected void checkState(SimpleKVTransaction tx) {
        assert Thread.holdsLock(this);
    }

    private void checkUsable(SimpleKVTransaction tx) {
        if (tx.stale)
            throw new StaleKVTransactionException(tx);
        if (this.lockManager.checkHoldTimeout(tx.lockOwner) == -1) {
            this.rollback(tx);
            throw new KVTransactionTimeoutException(tx, String.format(
              "transaction taking too long: hold timeout of %dms has expired", this.lockManager.getHoldTimeout()));
        }
    }

// SimpleKVTransaction hooks

    synchronized ByteData get(SimpleKVTransaction tx, ByteData key) {

        // Sanity check
        Preconditions.checkArgument(key.isEmpty() || key.ubyteAt(0) != 0xff, "key starts with 0xff");
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

    synchronized KVPair getAtLeast(SimpleKVTransaction tx, ByteData minKey, final ByteData maxKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteData.empty();

        // Sanity check
        this.checkUsable(tx);
        this.checkState(tx);
        if (maxKey != null && minKey.compareTo(maxKey) >= 0)
            return null;

        // Save original min key for locking purposes
        final ByteData originalMinKey = minKey;

        // Look for a mutation starting before minKey but containing it
        if (!minKey.isEmpty()) {
            final Mutation overlap = tx.findMutation(minKey);
            if (overlap != null) {
                if (overlap instanceof Put) {
                    final Put put = (Put)overlap;
                    assert put.getKey().equals(minKey);
                    return new KVPair(put.getKey(), put.getValue());
                }
                assert overlap instanceof Del;
                final ByteData max = overlap.getMax();
                if (max == null || (maxKey != null && max.compareTo(maxKey) >= 0))
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
            assert entry == null || entry.getKey().compareTo(minKey) >= 0;
            assert entry == null || maxKey == null || entry.getKey().compareTo(maxKey) < 0;

            // Handle the case where neither is found
            if (mutation == null && entry == null)
                return null;

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) <= 0)) {
                if (mutation instanceof Del) {
                    if ((minKey = mutation.getMax()) == null || (maxKey != null && minKey.compareTo(maxKey) >= 0))
                        return null;
                    continue;
                }
                final Put put = (Put)mutation;
                return new KVPair(put.getKey(), put.getValue());
            } else
                return entry;
        }
    }

    synchronized KVPair getAtMost(SimpleKVTransaction tx, ByteData maxKey, ByteData minKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteData.empty();

        // Sanity check
        this.checkUsable(tx);
        this.checkState(tx);
        if (maxKey != null && minKey.compareTo(maxKey) >= 0)
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
            assert entry == null || entry.getKey().compareTo(minKey) >= 0;
            assert entry == null || maxKey == null || entry.getKey().compareTo(maxKey) < 0;

            // Handle the case where neither is found
            if (mutation == null && entry == null)
                return null;

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) >= 0)) {
                if (mutation instanceof Del) {
                    if ((maxKey = mutation.getMin()) == null || minKey.compareTo(maxKey) >= 0)
                        return null;
                    continue;
                }
                final Put put = (Put)mutation;
                if (put.getKey().compareTo(minKey) < 0) {
                    assert entry == null;       // because otherwise minKey > mutation >= entry so entry should not have been found
                    return null;
                }
                return new KVPair(put.getKey(), put.getValue());
            } else
                return entry;
        }
    }

    synchronized void put(SimpleKVTransaction tx, ByteData key, ByteData value) {

        // Sanity check
        if (value == null)
            throw new NullPointerException();
        Preconditions.checkArgument(key.isEmpty() || key.ubyteAt(0) != 0xff, "key starts with 0xff");
        this.checkUsable(tx);
        this.checkState(tx);
        final ByteData keyNext = ByteUtil.getNextKey(key);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation instanceof Put) {
            assert ((Put)mutation).getKey().equals(key);

            // Replace Put with new Put
            tx.mutations.remove(mutation);
            tx.mutations.add(new Put(key, value));
        } else if (mutation instanceof Del) {

            // Split [Del] -> [Del*, Put, Del*]  *if needed
            final Del del = (Del)mutation;
            final ByteData delMin = del.getMin();
            final ByteData delMax = del.getMax();
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

    synchronized void remove(SimpleKVTransaction tx, ByteData key) {

        // Sanity check
        Preconditions.checkArgument(key.isEmpty() || key.ubyteAt(0) != 0xff, "key starts with 0xff");
        this.checkUsable(tx);
        this.checkState(tx);
        final ByteData keyNext = ByteUtil.getNextKey(key);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation instanceof Put) {
            assert ((Put)mutation).getKey().equals(key);

            // Replace Put with Del
            tx.mutations.remove(mutation);
            tx.mutations.add(new Del(key));
        } else if (mutation == null) {

            // Add write lock and new tx mutation
            this.getLock(tx, key, keyNext, true);
            tx.mutations.add(new Del(key));
        }
    }

    synchronized void removeRange(SimpleKVTransaction tx, ByteData minKey, ByteData maxKey) {

        // Realize minKey
        if (minKey == null)
            minKey = ByteData.empty();

        // Sanity check
        int diff = KeyRange.compare(minKey, maxKey);
        Preconditions.checkArgument(diff <= 0, "minKey > maxKey");
        this.checkUsable(tx);
        this.checkState(tx);
        if (diff == 0)                                                          // range is empty
            return;
        final ByteData originalMinKey = minKey;
        final ByteData originalMaxKey = maxKey;

        // Deal with partial overlap at the left end of the range
        if (!minKey.isEmpty()) {
            final Mutation leftMutation = tx.findMutation(minKey);
            if (leftMutation instanceof Put) {
                assert ((Put)leftMutation).getKey().equals(minKey);
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
        if (originalMinKey.isEmpty() && originalMaxKey == null)
            tx.mutations.clear();
        else if (originalMinKey.isEmpty())
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
            throw new StaleKVTransactionException(tx);
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
            throw new KVTransactionTimeoutException(tx,
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

    private /*synchronized*/ void getLock(SimpleKVTransaction tx, ByteData minKey, ByteData maxKey, boolean write) {

        // Attempt to get the lock
        assert Thread.holdsLock(this);
        LockManager.LockResult lockResult;
        try {
            lockResult = this.lockManager.lock(tx.lockOwner, minKey, maxKey, write, tx.waitTimeout);
        } catch (InterruptedException e) {
            this.rollback(tx);
            Thread.currentThread().interrupt();
            throw new RetryKVTransactionException(tx, "transaction interrupted while waiting to acquire lock", e);
        }

        // Check result
        switch (lockResult) {
        case SUCCESS:
            break;
        case WAIT_TIMEOUT_EXPIRED:
            this.rollback(tx);
            throw new RetryKVTransactionException(tx, String.format("could not acquire lock after %dms", tx.waitTimeout));
        case HOLD_TIMEOUT_EXPIRED:
            this.rollback(tx);
            throw new KVTransactionTimeoutException(tx, String.format(
              "transaction taking too long: hold timeout of %dms has expired", this.lockManager.getHoldTimeout()));
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
