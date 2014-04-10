
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.simple;

import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.kv.util.LockManager;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.util.ByteUtil;

/**
 * Simple implementation of the {@link KVDatabase} interface that provides a concurrent, transactional view
 * of an underlying {@link KVStore} with full isolation and serializable semantics.
 * The latter properties are provided by a {@link LockManager}.
 *
 * <p>
 * Instances wrap an underlying {@link KVStore}, from which committed data is read and written.
 * During a transaction, all mutations are recorded internally; if/when the transaction is committed,
 * the mutations are applied to the underlying {@link KVStore}. This operation is bracketed by calls
 * to {@link #preCommit preCommit()} and {@link #postCommit postCommit()}.
 * </p>
 */
public class SimpleKVDatabase implements KVDatabase {

    // NOTE: The lock order here is first the SimpleKVTransaction, then the SimpleKVDatabase

    /**
     * The {@link KVStore} for the committed data.
     */
    protected final KVStore kv;

    private final LockManager lockManager = new LockManager();

    private long waitTimeout;

    /**
     * Constructor. Uses an internal in-memory {@link KVStore}.
     */
    public SimpleKVDatabase() {
        this(new NavigableMapKVStore());
    }

    /**
     * Constructor taking caller-supplied storage.
     *
     * @param kv {@link KVStore} for the committed data
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public SimpleKVDatabase(KVStore kv) {
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
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
     * Constructor taking (optional) caller-supplied storage and timeout settings.
     *
     * @param kv {@link KVStore} for the committed data, or null for an in-memory {@link KVStore}
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
     * Get the default wait timeout for new transactions for this instance.
     *
     * <p>
     * The wait timeout limits how long a thread will wait for a contested lock before giving up and throwing
     * {@link RetryTransactionException}.
     * </p>
     *
     * @return wait timeout in milliseconds
     */
    public long getWaitTimeout() {
        return this.waitTimeout;
    }

    /**
     * Set the default wait timeout for new transactions for this instance. Default is zero (unlimited).
     *
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryTransactionException}
     *  in milliseconds (default), or zero for unlimited
     * @throws IllegalArgumentException if {@code waitTimeout} is negative
     */
    public void setWaitTimeout(long waitTimeout) {
        if (waitTimeout < 0)
            throw new IllegalArgumentException("waitTimeout < 0");
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
     * Set the hold timeout for this instance. Default is zero (unlimited).
     *
     * @param holdTimeout how long a thread may hold a contestested lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code holdTimeout} is negative
     */
    public void setHoldTimeout(long holdTimeout) {
        this.lockManager.setHoldTimeout(holdTimeout);
    }

    @Override
    public synchronized KVTransaction createTransaction() {
        return new SimpleKVTransaction(this, this.waitTimeout);
    }

    synchronized byte[] get(SimpleKVTransaction tx, byte[] key) {

        // Sanity check
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation != null)
            return mutation instanceof Put ? ((Put)mutation).getValue() : null;

        // Read from underlying store
        this.getLock(tx, key, ByteUtil.getNextKey(key), false);
        return this.kv.get(key);
    }

    synchronized KVPair getAtLeast(SimpleKVTransaction tx, byte[] minKey) {

        // Save original min key for locking purposes
        final byte[] originalMinKey = minKey;

        // Look for a mutation starting before minKey and overlapping it
        final Mutation overlap = minKey != null ? tx.findMutation(minKey) : !tx.mutations.isEmpty() ? tx.mutations.first() : null;
        if (overlap != null) {
            if (overlap instanceof Put) {
                final Put put = (Put)overlap;
                return new KVPair(put.getKey(), put.getValue());
            }
            final byte[] max = overlap.getMax();
            if (max == null)
                return null;
            minKey = max;
        }

        // Find whichever is first: a transaction Put, or an underlying store entry not covered by a transaction Delete
        SortedSet<Mutation> mutations = tx.mutations;
        while (true) {

            // Get the next mutation and kvstore entry >= minKey (if they exist)
            if (minKey != null)
                mutations = mutations.tailSet(Mutation.key(minKey));
            final Mutation mutation = !mutations.isEmpty() ? mutations.first() : null;
            final KVPair entry = this.kv.getAtLeast(minKey);

            // Handle the case where neither is found
            if (mutation == null && entry == null) {
                this.getLock(tx, originalMinKey, null, false);
                return null;
            }

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) <= 0)) {
                if (mutation instanceof Del) {
                    if ((minKey = mutation.getMax()) == null) {
                        this.getLock(tx, originalMinKey, null, false);
                        return null;
                    }
                    continue;
                }
                final Put put = (Put)mutation;
                this.getLock(tx, originalMinKey, ByteUtil.getNextKey(put.getKey()), false);
                return new KVPair(put.getKey(), put.getValue());
            } else {
                this.getLock(tx, originalMinKey, ByteUtil.getNextKey(entry.getKey()), false);
                return entry;
            }
        }
    }

    synchronized KVPair getAtMost(SimpleKVTransaction tx, byte[] maxKey) {

        // Save original max key for locking purposes
        final byte[] originalMaxKey = maxKey;

        // Find whichever is first: a transaction addition, or an underlying store entry not covered by a transaction deletion
        SortedSet<Mutation> mutations = tx.mutations;
        while (true) {

            // Get the next mutation and kvstore entry < maxKey (if they exist)
            if (maxKey != null)
                mutations = mutations.headSet(Mutation.key(maxKey));
            final Mutation mutation = !mutations.isEmpty() ? mutations.last() : null;
            final KVPair entry = this.kv.getAtMost(maxKey);

            // Handle the case where neither is found
            if (mutation == null && entry == null) {
                this.getLock(tx, null, originalMaxKey, false);
                return null;
            }

            // Check for whether mutation or kvstore wins (i.e., which is first)
            if (mutation != null && (entry == null || mutation.compareTo(entry.getKey()) >= 0)) {
                if (mutation instanceof Del) {
                    if ((maxKey = mutation.getMin()) == null) {
                        this.getLock(tx, null, originalMaxKey, false);
                        return null;
                    }
                    continue;
                }
                final Put put = (Put)mutation;
                this.getLock(tx, put.getKey(), originalMaxKey, false);
                return new KVPair(put.getKey(), put.getValue());
            } else {
                this.getLock(tx, entry.getKey(), originalMaxKey, false);
                return entry;
            }
        }
    }

    synchronized void put(SimpleKVTransaction tx, byte[] key, byte[] value) {

        // Sanity check
        if (value == null)
            throw new NullPointerException();
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        final byte[] keyNext = ByteUtil.getNextKey(key);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation instanceof Put) {

            // Replace Put with new Put
            tx.mutations.remove(mutation);
            tx.mutations.add(new Put(key, value));
        } else if (mutation instanceof Del) {

            // Split [Del] -> [Del*, Put, Del*]  *if needed
            final Del del = (Del)mutation;
            final byte[] delMin = del.getMin();
            final byte[] delMax = del.getMax();
            tx.mutations.remove(del);
            if (KeyRange.compare(delMin, KeyRange.MIN, key, KeyRange.MIN) < 0)
                tx.mutations.add(new Del(delMin, key));
            if (KeyRange.compare(keyNext, KeyRange.MAX, delMax, KeyRange.MAX) < 0)
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
        if (key.length > 0 && key[0] == (byte)0xff)
            throw new IllegalArgumentException("key starts with 0xff");
        final byte[] keyNext = ByteUtil.getNextKey(key);

        // Check transaction mutations
        final Mutation mutation = tx.findMutation(key);
        if (mutation instanceof Put) {

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

        // Sanity check
        int diff = KeyRange.compare(minKey, KeyRange.MIN, maxKey, KeyRange.MAX);
        if (diff > 0)
            throw new IllegalArgumentException("minKey > maxKey");
        if (diff == 0)                                                          // range is empty
            return;
        final byte[] originalMinKey = minKey;
        final byte[] originalMaxKey = maxKey;

        // Deal with partial overlap at the left end of the range
        if (minKey != null) {
            final Mutation leftMutation = tx.findMutation(minKey);
            if (leftMutation instanceof Put)
                tx.mutations.remove(leftMutation);                                          // overwritten by this change
            else if (leftMutation instanceof Del) {
                final Del del = (Del)leftMutation;
                tx.mutations.remove(del);                                                   // will merge into this change
                minKey = del.getMin();                                                      // guaranteed to be <= minKey
                if (KeyRange.compare(del.getMax(), KeyRange.MAX, maxKey, KeyRange.MAX) > 0) // get higher of the two maxKeys
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
                if (KeyRange.compare(del.getMax(), KeyRange.MAX, maxKey, KeyRange.MAX) > 0) // get higher of the two maxKeys
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
        switch (this.lockManager.release(tx.lockOwner)) {
        case SUCCESS:
            break;
        case HOLD_TIMEOUT_EXPIRED:
            this.rollback(tx);
            throw new TransactionTimeoutException(tx,
              "transaction taking too long: hold timeout of " + this.lockManager.getHoldTimeout() + "ms has expired");
        default:
            throw new RuntimeException("internal error");
        }
        this.preCommit(this.kv);
        boolean successful = false;
        try {
            for (Mutation mutation : tx.mutations)
                mutation.apply(this.kv);
            successful = true;
        } finally {
            tx.mutations.clear();
            this.postCommit(this.kv, successful);
        }
    }

    synchronized void rollback(SimpleKVTransaction tx) {
        this.lockManager.release(tx.lockOwner);
    }

    /**
     * Invoked during transaction commit just prior to writing changes to the underlying {@link KVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #postCommit postCommit()} will be invoked in matching pairs.
     * </p>
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     * </p>
     *
     * @param kv underlying data store for committed data
     */
    protected void preCommit(KVStore kv) {
    }

    /**
     * Invoked during transaction commit just after writing changes to the underlying {@link KVStore}.
     *
     * <p>
     * {@link SimpleKVDatabase} guarantees this method and {@link #preCommit preCommit()} will be invoked in matching pairs.
     * This method is invoked even if the underlying {@link KVStore} throws an exception while changes were being written to it.
     * In that case, {@code successful} will be false.
     * </p>
     *
     * <p>
     * The implementation in {@link SimpleKVDatabase} does nothing.
     * </p>
     *
     * @param kv underlying data store for committed data
     * @param successful true if all changes were written back successfully,
     *  false if the underlying {@link KVStore} threw an exception
     */
    protected void postCommit(KVStore kv, boolean successful) {
    }

// Internal methods

    private synchronized void getLock(SimpleKVTransaction tx, byte[] minKey, byte[] maxKey, boolean write) {

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

