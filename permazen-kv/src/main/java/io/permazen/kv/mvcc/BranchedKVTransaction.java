
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRange;
import io.permazen.kv.StaleTransactionException;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableRefs;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

/**
 * A {@link KVTransaction} that is based on a snapshot from an original {@link KVTransaction} and that can, at some arbitrary
 * later time, be merged back into a new {@link KVTransaction} from the same database, assuming no conflicts are detected.
 *
 * <p>
 * This class only works with {@link KVDatabase}'s that support {@link KVTransaction#readOnlySnapshot readOnlySnapshot()}.
 *
 * <p>
 * In effect, this class gives the appearance of a regular {@link KVTransaction} that can stay open for an arbitrarily long time.
 *
 * <p>
 * New instances must be explicitly opened via {@link #open}. When that occurs, a regular database transaction is opened,
 * a {@linkplain KVTransaction#readOnlySnapshot snapshot} of the database taken, and then that transaction is immediately closed.
 * Reads and writes in this transaction are then tracked and kept entirely in memory and there is no actual transaction open.
 * Later, when this transaction is {@link #commit}'ed, a new regular database transaction is opened, a conflict check is
 * performed to determine whether any of the keys read by this transaction have since changed, and then if not all of this
 * transaction's accumulated writes are applied. Otherwise, if a conflict is found, a {@link TransactionConflictException}
 * is thrown.
 *
 * <p>
 * The conflict check can also be performed on demand at any time while this transaction is open without actually
 * {@link #commit}'ing anything, via {@link #checkForConflicts()}.
 *
 * <p>
 * There is a limit to how "branched" the transaction can get. The probability for a conflict increases when instances are kept open
 * for a long time and/or there is a high volume of read traffic in this transaction, or write traffic in the underlying database.
 * However, it can be useful in certain scenarios, for example, to support editing a single entity in a GUI application
 * within a single "transaction" that doesn't actually keep open any database resources.
 *
 * <p>
 * The amount of work required for the conflict check scales in proportion to the number of keys read in this transaction;
 * the amount of memory required scales as it does with {@link MutableView}.
 *
 * <p>
 * Instances support {@link #readOnlySnapshot} and {@link #withWeakConsistency withWeakConsistency()}.
 *
 * <p>
 * Instances do not support {@link #setTimeout setTimeout()} or {@link #watchKey watchKey()}.
 */
public class BranchedKVTransaction implements KVTransaction, CloseableKVStore {

    private final KVDatabase kvdb;

    private State state = State.INITIAL;

    // The store for this transaction. Null except in state OPEN. Locking order: (a) this (b) this.view.
    private MutableView view;

    // Tracks when to close() the underlying transaction snapshot, i.e., view.getBaseKVStore().
    private CloseableRefs<CloseableKVStore> snapshotRefs;

// Constructor

    /**
     * Constructor.
     *
     * <p>
     * This instance must be {@link #open}'ed before use.
     *
     * @param kvdb database
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public BranchedKVTransaction(KVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        this.kvdb = kvdb;
    }

// Lifecycle

    /**
     * Open this transaction.
     *
     * <p>
     * This results in a snapshot being taken of the database.
     *
     * @throws UnsupportedOperationException if the database doesn't support {@link KVTransaction#readOnlySnapshot}
     * @throws IllegalStateException if this method has already been invoked
     */
    public synchronized void open() {
        this.checkState(State.INITIAL);
        final KVTransaction tx = this.kvdb.createTransaction();
        try {
            try {
                final CloseableKVStore snapshot = tx.readOnlySnapshot();
                this.snapshotRefs = new CloseableRefs<>(snapshot);
                this.view = new MutableView(snapshot);
                tx.commit();
                this.state = State.OPEN;
            } finally {
                if (this.state != State.OPEN)
                    this.close();
            }
        } finally {
            tx.rollback();
        }
    }

    @Override
    public synchronized void commit() {
        this.checkState(State.OPEN);
        final KVTransaction tx = this.kvdb.createTransaction();
        try {

            // Check for conflicts and apply our changes
            try {
                synchronized (this.view) {
                    this.view.setReadOnly();                // disallow Iterator.remove() from getRange() after commit
                    this.checkForConflicts(tx);
                    this.view.getWrites().applyTo(tx);
                }
            } finally {
                this.close();
            }

            // Commit transaction
            tx.commit();
        } finally {
            tx.rollback();
        }
    }

    @Override
    public void rollback() {
        this.close();
    }

    /**
     * Close this transaction.
     *
     * <p>
     * Equivalent to {@link #rollback}.
     */
    @Override
    public synchronized void close() {
        if (this.state != State.CLOSED) {
            if (this.snapshotRefs != null) {
                this.snapshotRefs.unref();
                this.snapshotRefs = null;
            }
            this.view = null;
            this.state = State.CLOSED;
        }
    }

// KVStore

    @Override
    public synchronized byte[] get(byte[] key) {
        this.checkState(State.OPEN);
        return this.view.get(key);
    }

    @Override
    public synchronized KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        this.checkState(State.OPEN);
        return this.view.getAtLeast(minKey, maxKey);
    }

    @Override
    public synchronized KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        this.checkState(State.OPEN);
        return this.view.getAtMost(minKey, maxKey);
    }

    @Override
    public synchronized CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        this.checkState(State.OPEN);
        return this.view.getRange(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        this.checkState(State.OPEN);
        this.view.put(key, value);
    }

    @Override
    public synchronized void remove(byte[] key) {
        this.checkState(State.OPEN);
        this.view.remove(key);
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {
        this.checkState(State.OPEN);
        this.view.removeRange(minKey, maxKey);
    }

    @Override
    public synchronized byte[] encodeCounter(long value) {
        this.checkState(State.OPEN);
        return this.view.encodeCounter(value);
    }

    @Override
    public synchronized long decodeCounter(byte[] value) {
        this.checkState(State.OPEN);
        return this.view.decodeCounter(value);
    }

    @Override
    public synchronized void adjustCounter(byte[] key, long amount) {
        this.checkState(State.OPEN);
        this.view.adjustCounter(key, amount);
    }

// KVTransaction

    @Override
    public KVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized boolean isReadOnly() {
        this.checkState(State.OPEN);
        return this.view.isReadOnly();
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        this.checkState(State.OPEN);
        this.view.setReadOnly();
    }

    @Override
    public Future<Void> watchKey(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void withWeakConsistency(Runnable action) {
        MutableView theView;
        synchronized (this) {
            this.checkState(State.OPEN);
            theView = this.view;
        }
        theView.withoutReadTracking(true, action);
    }

    @Override
    public synchronized CloseableKVStore readOnlySnapshot() {
        this.checkState(State.OPEN);

        // Clone the current view
        final MutableView snapshot = this.view.clone();
        snapshot.disableReadTracking();
        snapshot.setReadOnly();

        // Count a new reference to the original snapshot
        this.snapshotRefs.ref();

        // Wrap in a CloseableKVStore
        return new CloseableForwardingKVStore(snapshot, this.snapshotRefs::unref);
    }

// Conflict Checking

    /**
     * Check for conflicts between this transaction and the current database contents.
     *
     * <p>
     * This method performs the conflict check that normally occurs during {@link #commit}, but without
     * actually committing anything.
     *
     * <p>
     * It opens a new transaction and checks whether any of the keys read by this transaction have changed
     * since the original snapshot was taken. If so, then the writes from this transaction cannot be consistently
     * merged back into the database and an exception is thrown.
     *
     * @throws TransactionConflictException if any conflicts are found
     */
    public void checkForConflicts() {
        final KVTransaction tx = this.kvdb.createTransaction();
        try {
            this.checkForConflicts(tx);
            tx.commit();
        } finally {
            tx.rollback();
        }
    }

    // Verify that every key (range) that has been read so far in this transaction has not changed in "newKV"
    private synchronized void checkForConflicts(KVTransaction newKV) {
        this.checkState(State.OPEN);
        final KVStore oldKV = this.view.getBaseKVStore();
        synchronized (this.view) {
            for (KeyRange range : this.view.getReads())
                this.checkForConflicts(range, oldKV, newKV);
        }
    }

    // Check whether any key/value pair in "range" differs between "oldKV" and "newKV"
    private void checkForConflicts(KeyRange range, KVStore oldKV, KVStore newKV) {
        if (range.isSingleKey()) {
            final byte[] key = range.getMin();
            final Optional<byte[]> keyOpt = Optional.of(key);
            final KVPair oldPair = keyOpt.map(oldKV::get).map(value -> new KVPair(key, value)).orElse(null);
            final KVPair newPair = keyOpt.map(newKV::get).map(value -> new KVPair(key, value)).orElse(null);
            this.checkForConflicts(oldPair, newPair);
        } else {

            // Compare the values from each store within the range
            try (
              CloseableIterator<KVPair> oldIter = oldKV.getRange(range);
              CloseableIterator<KVPair> newIter = newKV.getRange(range)) {
                while (true) {
                    final KVPair oldPair = oldIter.hasNext() ? oldIter.next() : null;
                    final KVPair newPair = newIter.hasNext() ? newIter.next() : null;
                    if (this.checkForConflicts(oldPair, newPair))
                        break;
                }
            }
        }
    }

    // Check whether the two key/value pairs differ. Each pair is the next key in sequence, if any, else null.
    // Returns true if both are null (i.e., iteration is complete).
    private boolean checkForConflicts(KVPair oldPair, KVPair newPair) {

        // If both iterations are exhausted, no conflict
        if (oldPair == null && newPair == null)
            return true;

        // If either iteration is exhausted, the other is not, so there is a conflict
        if (oldPair == null)
            throw new TransactionConflictException(this, new ReadWriteConflict(newPair.getKey()));
        final byte[] oldKey = oldPair.getKey();
        if (newPair == null)
            throw new TransactionConflictException(this, new ReadRemoveConflict(oldKey));

        // See if one key is "earlier" than then other
        final byte[] newKey = newPair.getKey();
        int diff = ByteUtil.compare(oldKey, newKey);
        if (diff > 0)
            throw new TransactionConflictException(this, new ReadWriteConflict(newKey));
        if (diff < 0)
            throw new TransactionConflictException(this, new ReadRemoveConflict(oldKey));

        // The keys are the same, so the compare values
        final byte[] oldValue = oldPair.getValue();
        final byte[] newValue = newPair.getValue();
        if (!Arrays.equals(oldValue, newValue))
            throw new TransactionConflictException(this, new ReadWriteConflict(oldKey));

        // Done
        return false;
    }

// Internal Methods

    private void checkState(State expectedState) {
        assert Thread.holdsLock(this);
        if (!this.state.equals(expectedState))
            throw this.state.newStateMismatchException(this);
    }

// State

    private enum State {
        INITIAL("transaction is not open yet", (tx, msg) -> new IllegalStateException(msg)),
        OPEN("transaction is already open", (tx, msg) -> new IllegalStateException(msg)),
        CLOSED("transaction is no longer open", StaleTransactionException::new);

        private final String stateMismatchMessage;
        private final BiFunction<KVTransaction, String, RuntimeException> exceptionCreator;

        State(String stateMismatchMessage, BiFunction<KVTransaction, String, RuntimeException> exceptionCreator) {
            this.stateMismatchMessage = stateMismatchMessage;
            this.exceptionCreator = exceptionCreator;
        }

        public RuntimeException newStateMismatchException(BranchedKVTransaction tx) {
            return this.exceptionCreator.apply(tx, this.stateMismatchMessage);
        }
    }
}
